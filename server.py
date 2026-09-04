import asyncio
import json
import logging
import time
import uuid
from typing import Dict, Optional, Tuple, Any

from common import send_json, read_json, relay_streams
from web import WebHandler

logger = logging.getLogger("hajimi")

HTTP_METHODS = (b"GET ", b"POST ", b"PUT ", b"DELETE ", b"HEAD ", b"OPTIONS ", b"PATCH ")


class ClientSession:
    """Represents an active client control connection."""

    def __init__(self, name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.remote_addr = writer.get_extra_info("peername") or ("unknown", 0)
        self.connected_at = time.time()
        self.last_seen = time.time()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "remote_ip": f"{self.remote_addr[0]}:{self.remote_addr[1]}",
            "connected_at": int(self.connected_at),
            "uptime_seconds": int(time.time() - self.connected_at),
        }


class ProxyRule:
    """Represents an active reverse proxy port forwarding rule."""

    def __init__(
        self,
        listen_port: int,
        client_name: str,
        target_host: str,
        target_port: int,
        server: asyncio.Server,
    ):
        self.listen_port = listen_port
        self.client_name = client_name
        self.target_host = target_host
        self.target_port = target_port
        self.server = server
        self.active_connections = 0
        self.total_connections = 0
        self.created_at = time.time()

    def to_dict(self) -> dict:
        return {
            "listen_port": self.listen_port,
            "client_name": self.client_name,
            "target_host": self.target_host,
            "target_port": self.target_port,
            "active_connections": self.active_connections,
            "total_connections": self.total_connections,
            "created_at": int(self.created_at),
        }


class ProxyServer:
    """Master reverse proxy server managing control connections and proxy ports."""

    def __init__(self, host: Optional[str] = "0.0.0.0", port: int = 8000, webui_addr: Optional[str] = None):
        self.host = host
        self.port = port
        self.webui_addr = webui_addr
        self.webui_host = host
        self.webui_port = port
        self.clients: Dict[str, ClientSession] = {}
        self.rules: Dict[int, ProxyRule] = {}
        self.pending_bridges: Dict[str, asyncio.Future] = {}
        self.master_server: Optional[asyncio.Server] = None
        self.web_server: Optional[asyncio.Server] = None
        self.web_handler = WebHandler(self)
        self.start_time = time.time()

    def _format_url(self, host: Optional[str], port: int) -> str:
        """Format an HTTP URL with proper IPv6 bracket handling."""
        display_host = host
        if not display_host or display_host in ("0.0.0.0", "::"):
            display_host = "127.0.0.1"
        elif ":" in display_host and not display_host.startswith("["):
            display_host = f"[{display_host}]"
        return f"http://{display_host}:{port}"

    async def start(self) -> None:
        """Start listening on master port and optional dedicated WebUI port."""
        self.master_server = await asyncio.start_server(
            self._handle_master_connection,
            self.host,
            self.port,
        )
        logger.info(f"[Server] Master listening on {self.host or '*'}:{self.port}")

        # Start dedicated WebUI server if configured on a different address/port
        if self.webui_addr:
            from common import parse_address
            w_host, w_port = parse_address(self.webui_addr, default_host=self.host, default_port=8080)
            self.webui_host = w_host
            self.webui_port = w_port

            if (w_host, w_port) != (self.host, self.port):
                self.web_server = await asyncio.start_server(
                    self.web_handler.handle_http,
                    w_host,
                    w_port,
                )
                logger.info(f"[Server] WebUI listening on dedicated port: {self._format_url(w_host, w_port)}")
            else:
                logger.info(f"[Server] WebUI multiplexed on master port: {self._format_url(w_host, w_port)}")
        else:
            logger.info(f"[Server] WebUI available at {self._format_url(self.host, self.port)}")

    async def stop(self) -> None:
        """Gracefully stop master server, web server, and all proxy rules."""
        # Stop all proxy rules
        for port in list(self.rules.keys()):
            await self.remove_rule(port)

        # Close all client control connections
        for client in list(self.clients.values()):
            try:
                client.writer.close()
                await client.writer.wait_closed()
            except Exception:
                pass
        self.clients.clear()

        # Close dedicated web server if running
        if self.web_server:
            self.web_server.close()
            await self.web_server.wait_closed()

        # Close master server
        if self.master_server:
            self.master_server.close()
            await self.master_server.wait_closed()
        logger.info("[Server] Stopped.")

    async def _handle_master_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Multiplex connection: HTTP WebUI, client register, or data bridge."""
        try:
            # Read first chunk up to 4096 bytes or newline to determine protocol
            initial_chunk = await reader.read(4096)
            if not initial_chunk:
                writer.close()
                return

            # Check if this is an HTTP request
            if any(initial_chunk.startswith(prefix) for prefix in HTTP_METHODS):
                await self.web_handler.handle_http(reader, writer, initial_bytes=initial_chunk)
                return

            # Handle as client control or bridge message
            line = initial_chunk
            if b"\n" not in line:
                rest_of_line = await reader.readline()
                line += rest_of_line

            msg = json.loads(line.decode("utf-8").strip())
            msg_type = msg.get("type")

            if msg_type == "register":
                name = str(msg.get("name", "")).strip()
                if not name:
                    writer.close()
                    return
                await self._register_client(name, reader, writer)
                return

            elif msg_type == "bridge":
                token = msg.get("token")
                fut = self.pending_bridges.get(token)
                if fut and not fut.done():
                    # Hand off connection to the waiting visitor handler
                    fut.set_result((reader, writer))
                    # Do not close writer here, visitor handler manages lifecycle
                    return
                else:
                    logger.warning(f"Bridge token expired or invalid: {token}")
                    writer.close()
                    return

            else:
                logger.warning(f"Unknown message type: {msg_type}")
                writer.close()

        except Exception as e:
            logger.debug(f"Master connection error: {e}")
            try:
                if not writer.is_closing():
                    writer.close()
            except Exception:
                pass

    async def _register_client(
        self,
        name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Register client and maintain keep-alive heartbeat loop."""
        # Clean up existing session with the same name if present
        if name in self.clients:
            logger.info(f"[Server] Displacing existing client session '{name}'")
            try:
                old_client = self.clients[name]
                old_client.writer.close()
            except Exception:
                pass

        session = ClientSession(name, reader, writer)
        self.clients[name] = session
        logger.info(f"[Server] Client registered: '{name}' from {session.remote_addr[0]}:{session.remote_addr[1]}")

        # Send registration confirmation
        await send_json(writer, {"type": "registered", "status": "ok"})

        try:
            # Client heartbeat listening loop
            while True:
                msg = await read_json(reader)
                if msg is None:
                    break
                session.last_seen = time.time()
                if msg.get("type") == "ping":
                    await send_json(writer, {"type": "pong"})
        except Exception as e:
            logger.debug(f"Client loop error '{name}': {e}")
        finally:
            if self.clients.get(name) is session:
                del self.clients[name]
            logger.info(f"[Server] Client disconnected: '{name}'")
            try:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except Exception:
                pass

    async def add_rule(
        self,
        listen_port: int,
        client_name: str,
        target_host: str = "127.0.0.1",
        target_port: int = 80,
        bind_host: Optional[str] = None,
    ) -> dict:
        """Create and bind a new TCP reverse proxy listener (dual-stack IPv4/IPv6)."""
        if listen_port in (self.port, self.webui_port):
            return {"ok": False, "error": f"Port {listen_port} is already used by server"}
        if listen_port in self.rules:
            return {"ok": False, "error": f"Port {listen_port} is already forwarded"}

        try:
            # Create proxy listener (bind_host=None binds both IPv4 and IPv6)
            async def visitor_cb(r, w):
                rule_obj = self.rules.get(listen_port)
                if rule_obj:
                    await self._handle_visitor(rule_obj, r, w)
                else:
                    w.close()

            server = await asyncio.start_server(visitor_cb, bind_host, listen_port)
            rule = ProxyRule(listen_port, client_name, target_host, target_port, server)
            self.rules[listen_port] = rule
            logger.info(f"[Server] Proxy rule created: *:{listen_port} -> [{client_name}] {target_host}:{target_port}")
            return {"ok": True, "message": f"Listening on port {listen_port} -> {client_name}:{target_port}"}
        except Exception as e:
            logger.error(f"[Server] Failed to bind port {listen_port}: {e}")
            return {"ok": False, "error": str(e)}

    async def remove_rule(self, listen_port: int) -> dict:
        """Stop and remove a proxy rule."""
        rule = self.rules.pop(listen_port, None)
        if not rule:
            return {"ok": False, "error": f"Rule on port {listen_port} does not exist"}

        try:
            rule.server.close()
            await rule.server.wait_closed()
            logger.info(f"[Server] Proxy rule stopped: port {listen_port}")
            return {"ok": True, "message": f"Port {listen_port} stopped"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def _handle_visitor(
        self,
        rule: ProxyRule,
        visitor_reader: asyncio.StreamReader,
        visitor_writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming connection to forwarded port and bridge with target client."""
        client = self.clients.get(rule.client_name)
        if not client:
            logger.warning(f"[Proxy {rule.listen_port}] Client '{rule.client_name}' not online")
            visitor_writer.close()
            return

        token = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        bridge_fut = loop.create_future()
        self.pending_bridges[token] = bridge_fut

        try:
            # Ask client to open reverse tunnel
            await send_json(client.writer, {
                "type": "open_tunnel",
                "token": token,
                "target_host": rule.target_host,
                "target_port": rule.target_port,
            })

            # Wait for client's bridge connection with 10s timeout
            bridge_reader, bridge_writer = await asyncio.wait_for(bridge_fut, timeout=10.0)

            # Relaying traffic
            rule.active_connections += 1
            rule.total_connections += 1
            await relay_streams(visitor_reader, visitor_writer, bridge_reader, bridge_writer)

        except asyncio.TimeoutError:
            logger.warning(f"[Proxy {rule.listen_port}] Timeout waiting for client bridge (token {token})")
            visitor_writer.close()
        except Exception as e:
            logger.debug(f"[Proxy {rule.listen_port}] Relay error: {e}")
            visitor_writer.close()
        finally:
            self.pending_bridges.pop(token, None)
            if rule.active_connections > 0:
                rule.active_connections -= 1

    def get_status(self) -> dict:
        """Return server status, online clients, and active forwarding rules."""
        return {
            "uptime_seconds": int(time.time() - self.start_time),
            "master_port": self.port,
            "master_host": self.host or "*",
            "webui_port": self.webui_port,
            "webui_host": self.webui_host or "*",
            "clients": [c.to_dict() for c in self.clients.values()],
            "rules": [r.to_dict() for r in self.rules.values()],
        }

