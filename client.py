import asyncio
import logging
from typing import Optional

from common import send_json, read_json, relay_streams

logger = logging.getLogger("hajimi")


class ProxyClient:
    """Client agent connecting to master server and bridging reverse proxy connections."""

    def __init__(self, master_host: str, master_port: int, client_name: str):
        self.master_host = master_host
        self.master_port = master_port
        self.client_name = client_name
        self.is_running = False
        self._control_writer: Optional[asyncio.StreamWriter] = None

    async def start(self) -> None:
        """Main loop connecting to master server with automatic reconnection."""
        self.is_running = True
        logger.info(f"[Client] Initialized client '{self.client_name}' target master {self.master_host}:{self.master_port}")

        retry_delay = 2
        while self.is_running:
            try:
                logger.info(f"[Client] Connecting to master {self.master_host}:{self.master_port}...")
                reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
                self._control_writer = writer

                # Register client name with master
                await send_json(writer, {"type": "register", "name": self.client_name})
                ack = await read_json(reader)
                if not ack or ack.get("status") != "ok":
                    logger.warning("[Client] Master rejected registration.")
                    writer.close()
                    await asyncio.sleep(retry_delay)
                    continue

                logger.info(f"[Client] Registered successfully as '{self.client_name}'")
                retry_delay = 2  # Reset retry delay on successful connection

                # Start background heartbeat
                heartbeat_task = asyncio.create_task(self._heartbeat_loop(writer))

                # Listen for commands from master
                try:
                    while self.is_running:
                        msg = await read_json(reader)
                        if msg is None:
                            logger.info("[Client] Master disconnected.")
                            break

                        msg_type = msg.get("type")
                        if msg_type == "open_tunnel":
                            token = msg.get("token")
                            target_host = msg.get("target_host", "127.0.0.1")
                            target_port = int(msg.get("target_port", 0))
                            # Dispatch tunnel bridge in background
                            asyncio.create_task(self._handle_bridge(token, target_host, target_port))
                        elif msg_type == "pong":
                            pass  # Keep-alive acknowledged
                finally:
                    heartbeat_task.cancel()

            except (ConnectionRefusedError, OSError) as e:
                logger.warning(f"[Client] Connection failed: {e}. Retrying in {retry_delay}s...")
            except Exception as e:
                logger.error(f"[Client] Unexpected error: {e}. Retrying in {retry_delay}s...")
            finally:
                if self._control_writer:
                    try:
                        self._control_writer.close()
                    except Exception:
                        pass
                    self._control_writer = None

            if self.is_running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 15)

    async def _heartbeat_loop(self, writer: asyncio.StreamWriter, interval: int = 20) -> None:
        """Periodically send ping message to keep connection alive."""
        try:
            while self.is_running:
                await asyncio.sleep(interval)
                await send_json(writer, {"type": "ping"})
        except (asyncio.CancelledError, Exception):
            pass

    async def _handle_bridge(self, token: str, target_host: str, target_port: int) -> None:
        """Connect to master bridge endpoint and local service, then relay data."""
        logger.info(f"[Client] Opening reverse tunnel for token {token[:8]} -> {target_host}:{target_port}")
        m_writer = None
        t_writer = None

        try:
            # 1. Connect to master server for the data channel
            m_reader, m_writer = await asyncio.open_connection(self.master_host, self.master_port)
            # Authenticate this connection as a bridge channel with token
            await send_json(m_writer, {"type": "bridge", "token": token})

            # 2. Connect to local target service
            t_reader, t_writer = await asyncio.open_connection(target_host, target_port)

            # 3. Bi-directional relay
            await relay_streams(m_reader, m_writer, t_reader, t_writer)
            logger.debug(f"[Client] Tunnel closed for token {token[:8]}")

        except Exception as e:
            logger.warning(f"[Client] Tunnel bridge error ({target_host}:{target_port}): {e}")
            if m_writer:
                try:
                    m_writer.close()
                except Exception:
                    pass
            if t_writer:
                try:
                    t_writer.close()
                except Exception:
                    pass

    def stop(self) -> None:
        """Signal client to terminate."""
        self.is_running = False
        if self._control_writer:
            try:
                self._control_writer.close()
            except Exception:
                pass

