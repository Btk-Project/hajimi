import asyncio
import json
import logging
import os
import urllib.parse
from typing import Tuple, Dict, Any, Callable, Coroutine

logger = logging.getLogger("hajimi")


class WebHandler:
    """Lightweight HTTP server handling WebUI and REST APIs with zero dependencies."""

    def __init__(self, server_context):
        # server_context will be an instance of ProxyServer
        self.server = server_context
        self.static_dir = os.path.join(os.path.dirname(__file__), "static")

    async def handle_http(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        initial_bytes: bytes = b"",
    ) -> None:
        """Parse incoming HTTP request and dispatch to corresponding handler."""
        try:
            # Read complete HTTP headers
            header_data = initial_bytes
            while b"\r\n\r\n" not in header_data and b"\n\n" not in header_data:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                header_data += chunk

            if not header_data:
                return

            delimiter = b"\r\n\r\n" if b"\r\n\r\n" in header_data else b"\n\n"
            header_part, remaining_body = header_data.split(delimiter, 1)

            lines = header_part.decode("utf-8", errors="replace").splitlines()
            if not lines:
                return

            # Parse request line: METHOD PATH VERSION
            request_line = lines[0].strip()
            parts = request_line.split()
            if len(parts) < 2:
                return
            method = parts[0].upper()
            full_path = parts[1]

            # Parse headers
            headers: Dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    key, val = line.split(":", 1)
                    headers[key.strip().lower()] = val.strip()

            # Read remaining body if Content-Length specified
            content_length = int(headers.get("content-length", 0))
            body_bytes = remaining_body
            bytes_needed = content_length - len(body_bytes)
            if bytes_needed > 0:
                body_bytes += await reader.readexactly(bytes_needed)

            # Parse URL path and query parameters
            parsed_url = urllib.parse.urlparse(full_path)
            path = parsed_url.path
            query_params = urllib.parse.parse_qs(parsed_url.query)

            # Route handling
            await self._dispatch_route(writer, method, path, query_params, body_bytes)

        except Exception as e:
            logger.debug(f"HTTP handling error: {e}")
        finally:
            try:
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch_route(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        path: str,
        query: Dict[str, list],
        body: bytes,
    ) -> None:
        """Route HTTP requests to API endpoints or static assets."""
        # Handle CORS preflight
        if method == "OPTIONS":
            self._send_response(writer, 204, "No Content", b"", "text/plain")
            return

        if path == "/api/status" and method == "GET":
            status_data = self.server.get_status()
            self._send_json(writer, 200, status_data)
            return

        if path == "/api/rules" and method == "POST":
            try:
                payload = json.loads(body.decode("utf-8")) if body else {}
                listen_port = int(payload.get("listen_port", 0))
                client_name = str(payload.get("client_name", "")).strip()
                target_port = int(payload.get("target_port", 0))
                target_host = str(payload.get("target_host", "127.0.0.1")).strip() or "127.0.0.1"

                if not listen_port or not client_name or not target_port:
                    self._send_json(writer, 400, {"ok": False, "error": "Missing required fields"})
                    return

                res = await self.server.add_rule(listen_port, client_name, target_host, target_port)
                self._send_json(writer, 200 if res.get("ok") else 400, res)
            except Exception as e:
                self._send_json(writer, 400, {"ok": False, "error": str(e)})
            return

        if path == "/api/rules" and method in ("DELETE", "POST"):
            try:
                # Support listen_port from JSON body or query param
                listen_port = 0
                if body:
                    try:
                        payload = json.loads(body.decode("utf-8"))
                        listen_port = int(payload.get("listen_port", 0))
                    except Exception:
                        pass
                if not listen_port and "port" in query:
                    listen_port = int(query["port"][0])

                if not listen_port:
                    self._send_json(writer, 400, {"ok": False, "error": "Invalid or missing listen_port"})
                    return

                res = await self.server.remove_rule(listen_port)
                self._send_json(writer, 200 if res.get("ok") else 400, res)
            except Exception as e:
                self._send_json(writer, 400, {"ok": False, "error": str(e)})
            return

        # Serve WebUI HTML
        if path in ("/", "/index.html"):
            html_file = os.path.join(self.static_dir, "index.html")
            if os.path.exists(html_file):
                with open(html_file, "rb") as f:
                    content = f.read()
                self._send_response(writer, 200, "OK", content, "text/html; charset=utf-8")
            else:
                self._send_response(writer, 200, "OK", b"<h1>Hajimi Reverse Proxy Server</h1>", "text/html")
            return

        self._send_response(writer, 404, "Not Found", b"404 Not Found", "text/plain")

    def _send_json(self, writer: asyncio.StreamWriter, status_code: int, data: dict) -> None:
        """Helper to send JSON response."""
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self._send_response(writer, status_code, "OK" if status_code == 200 else "Error", body, "application/json; charset=utf-8")

    def _send_response(
        self,
        writer: asyncio.StreamWriter,
        code: int,
        reason: str,
        body: bytes,
        content_type: str,
    ) -> None:
        """Format and write HTTP/1.1 response."""
        headers = [
            f"HTTP/1.1 {code} {reason}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            "Connection: close",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers: Content-Type",
            "",
            "",
        ]
        response_header = "\r\n".join(headers).encode("utf-8")
        writer.write(response_header + body)

