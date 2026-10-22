#!/usr/bin/env python3
"""Measure Linux loopback proxy throughput and CPU time without extra packages."""

import argparse
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time


def listener():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    sock.settimeout(15)
    return sock


def free_port():
    with listener() as sock:
        return sock.getsockname()[1]


def api(port, method="GET", body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        conn.request(method, "/api/status" if body is None else "/api/rules",
                     None if body is None else json.dumps(body),
                     {"Connection": "close", "Content-Type": "application/json"})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        return json.loads(response.read())
    finally:
        conn.close()


def cpu_times(processes):
    ticks = os.sysconf("SC_CLK_TCK")
    result = []
    for proc in processes:
        fields = Path(f"/proc/{proc.pid}/stat").read_text().rsplit(")", 1)[1].split()
        result.append((int(fields[11]) + int(fields[12])) / ticks)
    return result


def udp_counters():
    lines = Path("/proc/net/snmp").read_text().splitlines()
    for header, values in zip(lines, lines[1:]):
        if header.startswith("Udp:") and values.startswith("Udp:"):
            return dict(zip(header.split()[1:], map(int, values.split()[1:])))
    return {}


def transfer(sender, receiver, processes, seconds, warmup):
    stopped = threading.Event()
    errors = []

    def write():
        payload = bytes(64 * 1024)
        try:
            while not stopped.is_set():
                sender.sendall(payload)
        except OSError as error:
            if not stopped.is_set():
                errors.append(error)

    sender.settimeout(15)
    receiver.settimeout(15)
    thread = threading.Thread(target=write)
    thread.start()
    buffer = bytearray(64 * 1024)
    total = 0
    try:
        warmup_end = time.monotonic() + warmup
        while time.monotonic() < warmup_end:
            if not receiver.recv_into(buffer):
                raise RuntimeError("EOF during warmup")
        cpu_start = cpu_times(processes)
        udp_start = udp_counters()
        harness_start = time.process_time()
        start = time.monotonic()
        while time.monotonic() - start < seconds:
            size = receiver.recv_into(buffer)
            if not size:
                raise RuntimeError("EOF during measurement")
            total += size
        elapsed = time.monotonic() - start
        cpu = [end - begin for begin, end in zip(cpu_start, cpu_times(processes))]
        harness = time.process_time() - harness_start
        udp = {key: value - udp_start[key] for key, value in udp_counters().items()}
    finally:
        stopped.set()
        for sock in (sender, receiver):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        thread.join(timeout=16)
    if errors:
        raise errors[0]
    return dict(bytes=total, seconds=elapsed, MiBps=total / 2**20 / elapsed,
                cpuSeconds=cpu, cpuPercent=sum(cpu) / elapsed * 100,
                cpuSecondsPerGiB=sum(cpu) / (total / 2**30),
                harnessCpuSeconds=harness, udpCounters=udp)


def run(args, transport, direction, round_index):
    processes = []
    logs = []
    try:
        with listener() as target:
            if transport == "direct":
                source = socket.create_connection(target.getsockname(), timeout=15)
            else:
                hub, web, forwarded = free_port(), free_port(), free_port()
                commands = [
                    [args.binary, "--listen", f"127.0.0.1:{hub}", "--webui", f"127.0.0.1:{web}",
                     "--cert-file", str(args.certs / "localhost.pem"),
                     "--key-file", str(args.certs / "localhost-key.pem")],
                    [args.binary, "--connect", f"127.0.0.1:{hub}", "--name", "linux-bench",
                     "--transport", transport, "--ca-file", str(args.certs / "localhost.pem")],
                ]
                for index, command in enumerate(commands):
                    if index:
                        deadline = time.monotonic() + 15
                        while True:
                            try:
                                api(web)
                                break
                            except (OSError, http.client.HTTPException):
                                if time.monotonic() > deadline:
                                    raise RuntimeError("Server did not start")
                                time.sleep(0.05)
                    log = (args.output.parent / f"{transport}-{direction}-{round_index}-{index}.log").open("w")
                    logs.append(log)
                    processes.append(subprocess.Popen(["stdbuf", "-oL", "-eL", *command], stdout=log, stderr=log))
                deadline = time.monotonic() + 20
                while True:
                    codes = [proc.poll() for proc in processes]
                    if any(code is not None for code in codes) or time.monotonic() > deadline:
                        raise RuntimeError(f"Client did not register (exit codes {codes}); inspect process logs")
                    try:
                        if api(web)["clients"]:
                            break
                    except (OSError, http.client.HTTPException):
                        pass
                    time.sleep(0.05)
                reply = api(web, "POST", dict(listen_port=forwarded, client_name="linux-bench",
                                             target_host="127.0.0.1", target_port=target.getsockname()[1]))
                if not reply["ok"]:
                    raise RuntimeError(reply)
                deadline = time.monotonic() + 15
                while True:
                    try:
                        source = socket.create_connection(("127.0.0.1", forwarded), timeout=1)
                        break
                    except ConnectionRefusedError:
                        if time.monotonic() > deadline or any(proc.poll() is not None for proc in processes):
                            raise
                        time.sleep(0.05)
            with source:
                peer, _ = target.accept()
                with peer:
                    sender, receiver = (peer, source) if direction == "client-to-server" else (source, peer)
                    result = transfer(sender, receiver, processes, args.seconds, args.warmup)
        return dict(transport=transport, direction=direction, round=round_index, **result)
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda value: str(Path(value).resolve()))
    parser.add_argument("--certs", type=Path, default=Path("tests/certs"))
    parser.add_argument("--output", type=Path, default=Path("build/linux-proxy-results.json"))
    parser.add_argument("--seconds", type=float, default=8)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--transports", nargs="+", choices=["direct", "tcp", "quic"], default=["direct", "tcp", "quic"])
    args = parser.parse_args()
    if args.seconds <= 0 or args.warmup < 0 or args.rounds < 1:
        parser.error("seconds and rounds must be positive; warmup must be nonnegative")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    results = []
    for index in range(1, args.rounds + 1):
        for direction in ("client-to-server", "server-to-client"):
            for transport in args.transports if index % 2 else reversed(args.transports):
                result = run(args, transport, direction, index)
                results.append(result)
                args.output.write_text(json.dumps(results, indent=2) + "\n")
                print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
