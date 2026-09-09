import asyncio
import json
import logging
import sys
import urllib.parse
from typing import Tuple, List

from common import parse_address
from server import ProxyServer
from client import ProxyClient
from duckdns import classify_address, DuckDNSUpdater

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


async def run_mock_duckdns_server(port: int) -> Tuple[asyncio.Server, List[str]]:
    """Mock DuckDNS server to verify query parameters locally and quickly."""
    received_requests: List[str] = []

    async def handle_request(reader, writer):
        data = await reader.read(4096)
        if data:
            first_line = data.decode("utf-8", errors="ignore").splitlines()[0]
            received_requests.append(first_line)
        body = b"OK\n127.0.0.1\n::1\nUPDATED"
        headers = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        resp = headers + body
        writer.write(resp)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_request, "127.0.0.1", port)
    return server, received_requests


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
    print("=== Step 1: Testing IPv4, IPv6 & Domain Address Parsing ===")
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
    # Domain name tests
    assert parse_address("myhome.duckdns.org:8000") == ("myhome.duckdns.org", 8000)
    assert parse_address("myhome.duckdns.org::8000") == ("myhome.duckdns.org", 8000)
    assert parse_address("https://myhome.duckdns.org:8000/") == ("myhome.duckdns.org", 8000)
    assert parse_address("myhome.duckdns.org") == ("myhome.duckdns.org", 8000)
    print("[OK] All address parsing tests (IPv4, IPv6, Domain, URL) passed!")

    print("\n=== Step 2: Testing DuckDNS Logic (IPv4 vs IPv6 rules) ===")
    assert classify_address("0.0.0.0") == "ipv4"
    assert classify_address("127.0.0.1") == "ipv4"
    assert classify_address("192.168.1.100") == "ipv4"
    assert classify_address("::") == "ipv6"
    assert classify_address("[::]") == "ipv6"
    assert classify_address("::1") == "ipv6"
    assert classify_address("[::1]") == "ipv6"
    assert classify_address("240e:466::1") == "ipv6"
    assert classify_address(None) == "dual"

    mock_duck_port = 29999
    mock_server, requests_log = await run_mock_duckdns_server(mock_duck_port)
    mock_url = f"http://127.0.0.1:{mock_duck_port}/update"

    # Test IPv4 bound DuckDNS update
    dd_v4 = DuckDNSUpdater("mytest.duckdns.org", "test-token-4", server_host="0.0.0.0", base_url=mock_url)
    assert dd_v4.domain == "mytest"
    ok4, _ = await dd_v4.update_once()
    assert ok4 is True
    assert "domains=mytest" in requests_log[0]
    assert "token=test-token-4" in requests_log[0]
    assert "ipv6=" not in requests_log[0]  # IPv4 binding MUST NOT send ipv6
    print("[OK] IPv4 binding correctly updates IPv4 and excludes IPv6!")

    # Test IPv6 bound DuckDNS update
    dd_v6 = DuckDNSUpdater("mytest6", "test-token-6", server_host="::", base_url=mock_url)
    assert dd_v6.domain == "mytest6"
    ok6, _ = await dd_v6.update_once()
    assert ok6 is True
    assert "domains=mytest6" in requests_log[1]
    assert "token=test-token-6" in requests_log[1]
    assert "ipv6=" in requests_log[1]  # IPv6 binding MUST send ipv6
    print("[OK] IPv6 binding correctly detects and sends IPv6 update!")

    print("\n=== Step 3: Testing Master Server, Dedicated WebUI & Domain Client Connection ===")
    master_port = 28000
    webui_port = 29000
    local_service_port = 21145
    proxy_port = 20666

    # Start Proxy Server (dual-stack host=None) with dedicated WebUI and DuckDNS
    server = ProxyServer(
        host=None,
        port=master_port,
        webui_addr=f"127.0.0.1:{webui_port}",
        duckdns_domain="hajimi-test",
        duckdns_token="test-token-uuid",
        duckdns_base_url=mock_url,
    )
    await server.start()
    print(f"[OK] Proxy server master started dual-stack on port {master_port}")
    print(f"[OK] Proxy server dedicated WebUI started on 127.0.0.1:{webui_port}")

    # Start Proxy Client connecting via DOMAIN NAME 'localhost'
    client = ProxyClient("localhost", master_port, "mypc")
    client_task = asyncio.create_task(client.start())
    await asyncio.sleep(0.5)
    print("[OK] Client connected to master using domain name 'localhost'")

    # Query WebUI status
    status_data = await async_http_request("GET", "127.0.0.1", webui_port, "/api/status")
    assert status_data["master_port"] == master_port
    assert status_data["webui_port"] == webui_port
    assert len(status_data["clients"]) == 1
    assert status_data["clients"][0]["name"] == "mypc"
    assert status_data["duckdns"] is not None
    assert status_data["duckdns"]["domain"] == "hajimi-test.duckdns.org"
    print(f"[OK] Dedicated WebUI correctly reports master {master_port}, client 'mypc', and DuckDNS status!")

    print("\n=== Step 4: Testing Dynamic Port Forwarding via Web API ===")
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

    # Connect visitor via domain name 'localhost'
    v_reader, v_writer = await asyncio.open_connection("localhost", proxy_port)
    test_msg = b"Hello Hajimi Reverse Proxy via Domain!"
    v_writer.write(test_msg)
    await v_writer.drain()
    received = await v_reader.read(4096)
    assert received == b"ECHO:" + test_msg
    print("[OK] Domain-based visitor reverse proxy forwarding verified!")
    v_writer.close()
    await v_writer.wait_closed()

    print("\n=== Step 5: Testing IPv6 Forwarding (Dual-Stack) ===")
    ipv6_echo_port = 21146
    ipv6_proxy_port = 20667
    ipv6_echo_server = await run_echo_server("::1", ipv6_echo_port)
    print(f"[OK] IPv6 Echo server started on [::1]:{ipv6_echo_port}")

    # Add IPv6 forwarding rule
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
    mock_server.close()
    await mock_server.wait_closed()
    echo_server.close()
    await echo_server.wait_closed()
    ipv6_echo_server.close()
    await ipv6_echo_server.wait_closed()
    print("[OK] All components stopped cleanly.")
    print("\n=== ALL TESTS (INCLUDING DUCKDNS & DOMAIN SUPPORT) PASSED! ===")


if __name__ == "__main__":
    asyncio.run(run_test())
