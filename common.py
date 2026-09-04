import asyncio
import json
import logging
import re
from typing import Tuple, Optional

# Configure standard logging format
logger = logging.getLogger("hajimi")


import ipaddress

def parse_address(addr_str: str, default_host: Optional[str] = None, default_port: int = 8000) -> Tuple[Optional[str], int]:
    """
    Parse an address string into (host, port).
    Supports IPv4, IPv6 (with or without brackets), dual-stack, and port-only formats:
      - '[::1]:8000', '[::]:8000', '[fe80::1]:8000'
      - '::1', '::' (IPv6 host with default port)
      - '0.0.0.0:8000', '127.0.0.1:8000'
      - 'master::8000' or 'master:8000'
      - ':8000', '::8000', '8000'
    If host is omitted or None, default_host is returned (None binds dual-stack IPv4/IPv6 in asyncio).
    """
    if not addr_str:
        return default_host, default_port

    s = addr_str.strip().strip("'\"")

    # 1. Bracketed IPv6 notation: [host]:port or [host]
    bracket_match = re.match(r"^\[([a-fA-F0-9:]+)\](?:::|:)?(\d+)?$", s)
    if bracket_match:
        host = bracket_match.group(1)
        port = int(bracket_match.group(2)) if bracket_match.group(2) else default_port
        return host, port

    # 2. Standalone pure IPv6 address without port (e.g. '::1', '::', '2001:db8::1')
    try:
        ip = ipaddress.ip_address(s)
        if ip.version == 6:
            return s, default_port
    except ValueError:
        pass

    # 3. Port-only formats: ':8000', '::8000', '8000'
    port_match = re.match(r"^:+(\d+)$", s)
    if port_match:
        return default_host, int(port_match.group(1))
    if s.isdigit():
        return default_host, int(s)

    # 4. 'host::port' (tolerating double colons for host and port)
    if "::" in s:
        parts = s.rsplit("::", 1)
        if parts[1].isdigit():
            return parts[0] or default_host, int(parts[1])

    # 5. Standard 'host:port'
    if ":" in s:
        parts = s.rsplit(":", 1)
        if parts[1].isdigit():
            return parts[0] or default_host, int(parts[1])

    # Fallback: Treat entire string as host
    return s, default_port


async def send_json(writer: asyncio.StreamWriter, data: dict) -> None:
    """Send a JSON payload terminated by newline."""
    raw = json.dumps(data).encode("utf-8") + b"\n"
    writer.write(raw)
    await writer.drain()


async def read_json(reader: asyncio.StreamReader) -> Optional[dict]:
    """Read a single line from the stream and parse as JSON."""
    line = await reader.readline()
    if not line:
        return None
    try:
        return json.loads(line.decode("utf-8").strip())
    except Exception as e:
        logger.warning(f"Failed to parse JSON message: {e}")
        return None


async def pipe_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, buffer_size: int = 65536) -> None:
    """Relay data from reader to writer until EOF or error."""
    try:
        while True:
            data = await reader.read(buffer_size)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        logger.debug(f"Pipe stream error: {e}")
    finally:
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception:
            pass


async def relay_streams(
    r1: asyncio.StreamReader,
    w1: asyncio.StreamWriter,
    r2: asyncio.StreamReader,
    w2: asyncio.StreamWriter,
) -> None:
    """Concurrently relay two duplex streams in both directions."""
    task1 = asyncio.create_task(pipe_stream(r1, w2))
    task2 = asyncio.create_task(pipe_stream(r2, w1))
    # Wait for either direction to close
    done, pending = await asyncio.wait([task1, task2], return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    # Ensure both sides are closed
    for w in (w1, w2):
        try:
            if not w.is_closing():
                w.close()
                await w.wait_closed()
        except Exception:
            pass

