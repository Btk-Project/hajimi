import asyncio
import json
import logging
import sys

from common import parse_address
from server import ProxyServer
from client import ProxyClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


async def run_echo_server(host: str, port: int):
    """Simple TCP echo server simulating local client service."""
    async def handle_echo(reader, writer):
        while True:
            data = await reader.read(4096)
            if not data:
                break
            writer.write(b"ECHO:" + data)
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_echo, host, port)
    return server


async def async_http_request(method: str, host: str, port: int, path: str, data: dict = None) -> dict:
    """Async HTTP request helper using asyncio streams without blocking loop."""
    r, w = await asyncio.open_connection(host, port)
    body = json.dumps(data).encode("utf-8") if data else b""
    req_headers = (
        f"{method} {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n"
    ).encode("utf-8")
    w.write(req_headers + body)
    await w.drain()
    resp = await r.read()
    w.close()
    await w.wait_closed()
    header_part, body_part = resp.split(b"\r\n\r\n", 1)
    return json.loads(body_part.decode("utf-8"))


async def run_test():
    print("=== Step 1: Testing IPv4 & IPv6 Address Parsing ===")
    assert parse_address("master:8000") == ("master", 8000)
    assert parse_address("master::8000") == ("master", 8000)
    assert parse_address("8000") == (None, 8000)
    assert parse_address(":8000") == (None, 8000)
    assert parse_address("'master::8000'") == ("master", 8000)
    # IPv6 tests
    assert parse_address("[::1]:8000") == ("::1", 8000)
    assert parse_address("[::]:8000") == ("::", 8000)
    assert parse_address("::1") == ("::1", 8000)
    assert parse_address("::") == ("::", 8000)
    print("[OK] All address parsing tests (including IPv6) passed!")

    print("\n=== Step 2: Testing Dedicated WebUI Port (--webui) ===")
    master_port = 28000
    webui_port = 29000
    local_service_port = 21145
    proxy_port = 20666

    # Start Proxy Server with dedicated WebUI port
    server = ProxyServer("127.0.0.1", master_port, webui_addr=f"127.0.0.1:{webui_port}")
    await server.start()
    print(f"[OK] Proxy server master started on 127.0.0.1:{master_port}")
    print(f"[OK] Proxy server dedicated WebUI started on 127.0.0.1:{webui_port}")

    # Start Proxy Client
    client = ProxyClient("127.0.0.1", master_port, "mypc")
    client_task = asyncio.create_task(client.start())
    await asyncio.sleep(0.5)
    print("[OK] Client registered with master")

    # Verify dedicated WebUI returns status
    status_data = await async_http_request("GET", "127.0.0.1", webui_port, "/api/status")
    assert status_data["master_port"] == master_port
    assert status_data["webui_port"] == webui_port
    assert len(status_data["clients"]) == 1
    assert status_data["clients"][0]["name"] == "mypc"
    print(f"[OK] Dedicated WebUI on port {webui_port} correctly reports master {master_port} and client 'mypc'")

    print("\n=== Step 3: Testing Dynamic Port Forwarding via Web API ===")
    echo_server = await run_echo_server("127.0.0.1", local_service_port)
    print(f"[OK] Echo server started on 127.0.0.1:{local_service_port}")

    # Add rule via dedicated WebUI API
    rule_res = await async_http_request("POST", "127.0.0.1", webui_port, "/api/rules", {
        "listen_port": proxy_port,
        "client_name": "mypc",
        "target_host": "127.0.0.1",
        "target_port": local_service_port,
    })
    assert rule_res.get("ok") is True
    print(f"[OK] Proxy rule added via WebUI API: :{proxy_port} -> mypc:{local_service_port}")

    # Connect visitor
    v_reader, v_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    test_msg = b"Hello IPv4 Hajimi Reverse Proxy!"
    v_writer.write(test_msg)
    await v_writer.drain()
    received = await v_reader.read(4096)
    assert received == b"ECHO:" + test_msg
    print("[OK] IPv4 forward verified!")
    v_writer.close()
    await v_writer.wait_closed()

    print("\n=== Step 4: Testing IPv6 Forwarding (Dual-Stack) ===")
    ipv6_echo_port = 21146
    ipv6_proxy_port = 20667
    ipv6_echo_server = await run_echo_server("::1", ipv6_echo_port)
    print(f"[OK] IPv6 Echo server started on [::1]:{ipv6_echo_port}")

    # Add IPv6 forwarding rule (target_host is IPv6 ::1)
    rule_res_v6 = await async_http_request("POST", "127.0.0.1", webui_port, "/api/rules", {
        "listen_port": ipv6_proxy_port,
        "client_name": "mypc",
        "target_host": "::1",
        "target_port": ipv6_echo_port,
    })
    assert rule_res_v6.get("ok") is True
    print(f"[OK] IPv6 rule added: :{ipv6_proxy_port} -> mypc:[::1]:{ipv6_echo_port}")

    # Visitor connects to proxy port using IPv6 [::1]
    v6_reader, v6_writer = await asyncio.open_connection("::1", ipv6_proxy_port)
    v6_msg = b"Hello IPv6 Hajimi Reverse Proxy!"
    v6_writer.write(v6_msg)
    await v6_writer.drain()
    received_v6 = await v6_reader.read(4096)
    assert received_v6 == b"ECHO:" + v6_msg
    print("[OK] IPv6 reverse proxy forwarding verified successfully!")
    v6_writer.close()
    await v6_writer.wait_closed()

    print("\n=== Teardown ===")
    client.stop()
    client_task.cancel()
    await server.stop()
    echo_server.close()
    await echo_server.wait_closed()
    ipv6_echo_server.close()
    await ipv6_echo_server.wait_closed()
    print("[OK] All components stopped cleanly.")
    print("\n=== ALL TESTS (INCLUDING WEBUI & IPV6) PASSED! ===")


if __name__ == "__main__":
    asyncio.run(run_test())
