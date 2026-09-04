import argparse
import asyncio
import logging
import sys

from common import parse_address
from server import ProxyServer
from client import ProxyClient

# Setup logging output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("hajimi")


async def run_server(listen_addr: str, webui_addr: str = None) -> None:
    """Run master server instance."""
    host, port = parse_address(listen_addr, default_host="0.0.0.0", default_port=8000)
    server = ProxyServer(host=host, port=port, webui_addr=webui_addr)
    await server.start()

    try:
        # Keep running until cancelled
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await server.stop()


async def run_client(connect_addr: str, name: str) -> None:
    """Run client proxy agent instance."""
    host, port = parse_address(connect_addr, default_host="127.0.0.1", default_port=8000)
    # Clean client name from possible quotes
    clean_name = name.strip("'\"") if name else "client-default"
    client = ProxyClient(master_host=host, master_port=port, client_name=clean_name)

    try:
        await client.start()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        client.stop()


def main() -> None:
    """Parse command line arguments and execute appropriate mode."""
    parser = argparse.ArgumentParser(description="Hajimi - Asyncio Reverse Proxy")
    parser.add_argument(
        "--listen",
        type=str,
        help="Run as server: listen address e.g. '0.0.0.0:8000', '[::]:8000', or '8000'",
    )
    parser.add_argument(
        "--webui",
        type=str,
        default=None,
        help="Optional dedicated WebUI address e.g. '0.0.0.0:9000', '127.0.0.1:8080', or '9000'",
    )
    parser.add_argument(
        "--connect",
        type=str,
        help="Run as client: master address e.g. '127.0.0.1:8000', '[::1]:8000', or 'master::8000'",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="mypc",
        help="Client name (used when running in client mode)",
    )

    args = parser.parse_args()

    if args.listen:
        try:
            asyncio.run(run_server(args.listen, args.webui))
        except KeyboardInterrupt:
            logger.info("Server exiting on user interrupt.")
    elif args.connect:
        try:
            asyncio.run(run_client(args.connect, args.name))
        except KeyboardInterrupt:
            logger.info("Client exiting on user interrupt.")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

