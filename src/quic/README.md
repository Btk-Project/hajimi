# Async QUIC wrapper

The wrapper uses ilias, ngtcp2 1.25.0 and OpenSSL 3.6.3 with
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
dependencies are not included. Enabling the wrapper does not change the
proxy application's TCP transport.

`QuicContext` owns one UDP socket and drives all its connections in `run()`.
Use each context and its handles on the same ilias executor. The context can
connect to multiple peers or accept multiple connections, according to its role.
Both `connect()` and `accept()` return after the TLS handshake completes.
The client verifies the certificate chain and the supplied DNS name or IP address
by default. Set `caFile` for a private CA; the server requires `certFile` and
`keyFile`. Both sides must use the same ALPN.

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
Cancellation removes a pending waiter, but does not retract bytes already
accepted by a write. A canceled handshake requests closure of its partial connection.

Tests use random loopback ports and public test-only certificates. They cover
TLS/ALPN validation, multiple connections and streams, backpressure, FIN/reset,
cancellation, idle/handshake timeouts, IPv6, and relayed loss/reordering.
