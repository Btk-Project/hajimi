import asyncio
import ipaddress
import logging
import socket
import time
import urllib.parse
import urllib.request
from typing import Optional, Tuple

logger = logging.getLogger("hajimi")


def get_local_global_ipv6() -> Optional[str]:
    """Find a global unicast IPv6 address on local network interfaces."""
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET6)
        for info in infos:
            ip_str = info[4][0]
            try:
                ip_obj = ipaddress.ip_address(ip_str)
                # Check for public global unicast IPv6 address
                if ip_obj.is_global and not ip_obj.is_link_local and not ip_obj.is_loopback:
                    return ip_str
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[DuckDNS] Failed to query local IPv6: {e}")
    return None


def fetch_public_ipv6_remote(timeout: int = 5) -> Optional[str]:
    """Fallback: query public IPv6 via remote API."""
    endpoints = ["https://api6.ipify.org", "https://v6.ident.me"]
    for url in endpoints:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hajimi-proxy/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ip_text = resp.read().decode("utf-8").strip()
                if ipaddress.ip_address(ip_text).version == 6:
                    return ip_text
        except Exception:
            continue
    return None


def classify_address(host: Optional[str]) -> str:
    """Classify address as 'ipv6', 'ipv4', or 'dual'."""
    if not host or host in ("*", ""):
        return "dual"
    clean = host.strip("[]")
    if clean in ("::", "::1") or ":" in clean:
        return "ipv6"
    return "ipv4"


class DuckDNSUpdater:
    """Background DDNS updater for DuckDNS supporting IPv4 and IPv6."""

    def __init__(
        self,
        domain: str,
        token: str,
        server_host: Optional[str] = None,
        interval: int = 300,
        base_url: str = "https://www.duckdns.org/update",
    ):
        # Normalize domain: strip .duckdns.org suffix if provided
        clean_domain = domain.strip().lower()
        if clean_domain.endswith(".duckdns.org"):
            clean_domain = clean_domain[:-len(".duckdns.org")]
        self.domain = clean_domain
        self.token = token.strip()
        self.server_host = server_host
        self.interval = max(interval, 60)  # Minimum 60 seconds interval
        self.base_url = base_url
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

        # Status tracking
        self.last_update_time: Optional[float] = None
        self.last_status: str = "initialized"
        self.last_ipv4: Optional[str] = None
        self.last_ipv6: Optional[str] = None

    async def start(self) -> None:
        """Start the periodic DuckDNS update background task."""
        self.is_running = True
        self._task = asyncio.create_task(self._update_loop())
        logger.info(f"[DuckDNS] DDNS updater started for '{self.domain}.duckdns.org' (interval: {self.interval}s)")

    async def stop(self) -> None:
        """Stop the periodic update task."""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[DuckDNS] DDNS updater stopped.")

    async def update_once(self) -> Tuple[bool, str]:
        """Perform a single DuckDNS update request according to bound IP family."""
        family = classify_address(self.server_host)
        target_v4: Optional[str] = None
        target_v6: Optional[str] = None

        # Determine target IPv4 and/or IPv6 based on server binding
        if family == "ipv6":
            # Bound to IPv6: update IPv6 only
            clean_host = (self.server_host or "").strip("[]")
            try:
                ip_obj = ipaddress.ip_address(clean_host)
                if ip_obj.is_global and not ip_obj.is_loopback:
                    target_v6 = clean_host
            except ValueError:
                pass

            if not target_v6:
                # Find local public IPv6 or query remote
                target_v6 = get_local_global_ipv6()
                if not target_v6:
                    target_v6 = await asyncio.to_thread(fetch_public_ipv6_remote)

            if not target_v6:
                msg = "No global IPv6 address detected on host"
                logger.warning(f"[DuckDNS] {msg}")
                self.last_status = f"error: {msg}"
                return False, msg

        elif family == "ipv4":
            # Bound to IPv4: update IPv4 only
            clean_host = self.server_host or ""
            try:
                ip_obj = ipaddress.ip_address(clean_host)
                if ip_obj.is_global and not ip_obj.is_loopback:
                    target_v4 = clean_host
            except ValueError:
                pass
            # If server_host is 0.0.0.0 or private, target_v4 remains None and DuckDNS auto-detects caller IPv4

        else:
            # Dual-stack: update both v4 and v6
            target_v6 = get_local_global_ipv6() or await asyncio.to_thread(fetch_public_ipv6_remote)

        # Build DuckDNS update query
        params = {
            "domains": self.domain,
            "token": self.token,
            "verbose": "true",
        }
        if target_v4:
            params["ip"] = target_v4
        if target_v6:
            params["ipv6"] = target_v6

        query_str = urllib.parse.urlencode(params)
        sep = "&" if "?" in self.base_url else "?"
        url = f"{self.base_url}{sep}{query_str}"

        # Send HTTPS request in worker thread to avoid blocking event loop
        def _do_request() -> Tuple[bool, str]:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "hajimi-proxy/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp_data = resp.read().decode("utf-8").strip()
                    if resp_data.startswith("OK"):
                        return True, resp_data
                    else:
                        return False, resp_data
            except Exception as e:
                return False, str(e)

        ok, text = await asyncio.to_thread(_do_request)
        self.last_update_time = time.time()

        if ok:
            lines = text.splitlines()
            self.last_status = "OK"
            self.last_ipv4 = target_v4 or (lines[1] if len(lines) > 1 else None)
            self.last_ipv6 = target_v6 or (lines[2] if len(lines) > 2 else None)
            logger.info(
                f"[DuckDNS] Update successful for '{self.domain}.duckdns.org': "
                f"IPv4={self.last_ipv4 or 'auto'}, IPv6={self.last_ipv6 or 'none'}"
            )
            return True, text
        else:
            self.last_status = f"error: {text}"
            logger.warning(f"[DuckDNS] Update failed for '{self.domain}.duckdns.org': {text}")
            return False, text

    async def _update_loop(self) -> None:
        """Periodic background update task loop."""
        while self.is_running:
            try:
                await self.update_once()
            except Exception as e:
                logger.error(f"[DuckDNS] Unexpected update error: {e}")

            # Sleep for interval seconds
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break

    def get_status(self) -> dict:
        """Return status dictionary for WebUI and API reporting."""
        return {
            "enabled": True,
            "domain": f"{self.domain}.duckdns.org",
            "last_update": int(self.last_update_time) if self.last_update_time else None,
            "status": self.last_status,
            "ipv4": self.last_ipv4,
            "ipv6": self.last_ipv6,
            "interval_seconds": self.interval,
        }
