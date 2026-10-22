# Async QUIC wrapper

The wrapper uses ilias, ngtcp2 1.25.0 and OpenSSL 3.5+ with
`ngtcp2_crypto_ossl`. It provides QUIC v1 bidirectional byte streams.
It currently uses fixed peer addresses, with no migration, 0-RTT, HTTP/3,
or unreliable DATAGRAM API.

Enable the optional build and its tests:

```sh
xmake f -m debug --quic=y
xmake
xmake test
```

Disable it with `xmake f --quic=n`. When disabled, the QUIC sources and
dependencies are not included. When enabled, the proxy defaults to TCP and QUIC
on the server and QUIC-first connections on the client. See the root README for
`--transport` and `--quic-psk-file`.

`QuicContext` owns one UDP socket and drives all its connections in `run()`.
Use each context and its handles on the same ilias executor. The context can
connect to multiple peers or accept multiple connections, according to its role.
Both `connect()` and `accept()` return after the TLS handshake completes.
`connect("host:port")` resolves DNS on each call, deduplicates and interleaves
IPv4/IPv6 candidates, then races their handshakes with a 250 ms stagger.
An explicit failure starts the next attempt immediately. The first successful,
verified handshake wins; other attempts are canceled and joined before returning.
The original hostname is used for SNI and certificate verification. Numeric
addresses such as `127.0.0.1:1145` and `[::1]:1145` use IP SAN verification.
Bind a client context to `[::]:0` to race both families; an IPv4-only context
resolves IPv4 candidates only. The endpoint-plus-server-name overload remains
available when the caller already has a specific address.
The client verifies the certificate chain and the supplied DNS name or IP address
by default. Set `caFile` for a private CA; the server requires `certFile` and
`keyFile` in certificate mode. Both sides must use the same ALPN.

For certificate-free TLS 1.3 external PSK authentication, set `Config::psk` to
the same 32 random bytes on both peers. `pskIdentity` must also match (default
`hajimi/shared/v1`). PSK mode is mutually exclusive with `certFile`, `keyFile`,
and `caFile`; `verifyPeer` applies only to certificate mode. It enforces PSK
selection and an ephemeral key exchange and never falls back to certificates.
The server does not need credentials on disk, and `connect()` accepts an empty
server name in PSK mode. Session tickets and 0-RTT remain disabled.

A client operation can be scoped like this:

```cpp
auto request(ilias::IPEndpoint remote) -> ilias::IoTask<void> {
    QuicContext::Config config;
    config.caFile = "ca.pem";
    ILIAS_CO_TRY(auto context, co_await QuicContext::bind(
        ilias::IPEndpoint::fromString("0.0.0.0:0").value(), config));

    co_return co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> ilias::IoTask<void> {
        scope.spawn(context.run());
        auto exchange = [&]() -> ilias::IoTask<void> {
            ILIAS_CO_TRY(auto connection, co_await context.connect(remote, "server.example"));
            ILIAS_CO_TRY(auto stream, co_await connection.openStream());
            ILIAS_CO_TRYV(co_await stream.writeString("hello"));
            ILIAS_CO_TRYV(co_await stream.shutdown());
            std::array<char, 5> reply;
            ILIAS_CO_TRYV(co_await stream.readAll(ilias::makeBuffer(reply)));
            co_return {};
        };
        auto result = co_await exchange();
        context.stop();
        co_return result;
    });
}
```

Servers bind with `Role::Server`, run the context concurrently, and use
`accept()` followed by `acceptStream()`. A locally opened stream becomes visible
to its peer after its first data or FIN, so write or shut it down before waiting
for the peer to accept it.

`QuicStream` implements `ilias::Stream` and can be moved into `ilias::DynStream`.
`write()` may accept fewer bytes than requested and copies those bytes into an
internal queue. Caller memory can be reused when it returns. Use `writeAll()`
to accept an entire buffer; the wrapper retains unacknowledged data for
retransmission. Per-stream and per-connection limits bound these queues.
Receive credit is returned when data is consumed or discarded.

Stream RX/TX data uses `SkBufferPool`, with writes split into at most 16 KiB
chunks. Buffers retain their pool if a stream outlives its connection or context.
Set `Config::memoryResource` to customize or track buffer pool allocations; it
defaults to `std::pmr::get_default_resource()` and must not be null. The resource
must outlive the context and all its connections and streams. This setting covers
the buffer pool, not ngtcp2/TLS allocations or other wrapper containers. The proxy
server passes its allocation tracker so these buffers contribute to WebUI memory usage.
Connection lists and iteration snapshots remain vectors of handles.

Path MTU Discovery is enabled. Packets start at 1200 bytes and can grow to
1452 bytes of UDP payload after successful probes. Windows and Linux sockets
use probe mode to avoid fragmentation and stale kernel path MTU limits.
Rejected oversized probes are treated as packet loss, allowing ngtcp2 to keep
using the smaller working size.

`flush()` waits for accepted data to complete its first UDP transmission, without
waiting for an ACK or application receipt. `shutdown()` sends FIN after all
accepted data while allowing further reads. A nonempty `read()` returns zero
at normal EOF; a reset returns `QuicError::StreamReset`. `appError()` exposes
the last application error code from a local or remote reset/close.
Application error codes are QUIC varints; `reset()` clamps values to that range,
and `close()` rejects values outside it.

Keep the connection alive while using its streams. Destruction of an unfinished
stream requests RESET_STREAM and STOP_SENDING; destruction of a connection
requests CONNECTION_CLOSE. Stop the context and await `run()` before releasing
its execution scope. Stopping wakes pending connection and stream operations.
Canceling `run()` or calling `stop()` sends a best-effort CONNECTION_CLOSE to
each active peer before closing UDP. This cleanup is protected from cancellation
and has a total 250 ms send budget; it does not wait for peer acknowledgement.
Explicitly closed connections cache their close packet for retransmission when
the peer continues sending during the closing period.
Cancellation removes a pending waiter, but does not retract bytes already
accepted by a write. A canceled handshake requests closure of its partial connection.

Tests use random loopback ports and public test-only certificates. They cover
TLS/ALPN validation, multiple connections and streams, backpressure, FIN/reset,
cancellation, idle/handshake timeouts, IPv6, and relayed loss/reordering.

Performance design draft: [Batched UDP send/receive](../../docs/quic-udp-batching-draft.md).
