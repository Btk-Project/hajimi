#ifdef HAJIMI_QUIC

#include <ilias/testing.hpp>
#include <ilias/sync.hpp>
#include <gtest/gtest.h>
#include <array>
#include <vector>
#include "../src/quic/quic.hpp"

using namespace std::literals;
using ilias::Err;
using ilias::IoTask;
using ilias::IPEndpoint;

namespace {

auto serverConfig() -> QuicContext::Config {
    return {
        .role = QuicContext::Role::Server,
        .certFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
        .keyFile = HAJIMI_QUIC_TEST_DIR "/localhost-key.pem",
        .handshakeTimeout = 2000ms,
    };
}

auto clientConfig() -> QuicContext::Config {
    return {
        .caFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
        .handshakeTimeout = 2000ms,
    };
}

auto echo(QuicStream stream) -> IoTask<void> {
    std::array<std::byte, 4096> buffer;
    while (true) {
        ILIAS_CO_TRY(auto size, co_await stream.read(ilias::makeBuffer(buffer)));
        if (size == 0) {
            break;
        }
        ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(buffer.data(), size)));
    }
    co_return co_await stream.shutdown();
}

auto exchange(QuicConnection &connection, size_t size, char value) -> IoTask<void> {
    ILIAS_CO_TRY(auto stream, co_await connection.openStream());
    std::string payload(size, value);
    std::string received(size, '\0');
    auto send = [&]() -> IoTask<void> {
        ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(payload)));
        co_return co_await stream.shutdown();
    };
    auto recv = [&]() -> IoTask<void> {
        ILIAS_CO_TRYV(co_await stream.readAll(ilias::makeBuffer(received)));
        EXPECT_EQ(payload, received);
        std::byte byte;
        ILIAS_CO_TRY(auto eof, co_await stream.read(ilias::makeBuffer(&byte, 1)));
        EXPECT_EQ(eof, 0);
        co_return {};
    };
    auto [sent, read] = co_await ilias::whenAll(send(), recv());
    ILIAS_CO_TRYV(std::move(sent));
    co_return std::move(read);
}

template <typename Fn>
auto withContexts(QuicContext::Config serverConfig, QuicContext::Config clientConfig, Fn fn,
    IPEndpoint bind = IPEndpoint::fromString("127.0.0.1:0").value()) -> IoTask<void> {
    ILIAS_CO_TRY(auto server, co_await QuicContext::bind(bind, std::move(serverConfig)));
    ILIAS_CO_TRY(auto client, co_await QuicContext::bind(bind, std::move(clientConfig)));
    co_return co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
        scope.spawn(server.run());
        scope.spawn(client.run());
        auto result = co_await ilias::timeout(fn(server, client), 8s);
        server.stop();
        client.stop();
        if (!result) {
            co_return Err(std::make_error_code(std::errc::timed_out));
        }
        co_return std::move(*result);
    });
}

template <typename Fn>
auto withConnections(Fn fn, QuicContext::Config server = serverConfig(),
    QuicContext::Config client = clientConfig()) -> IoTask<void> {
    co_return co_await withContexts(std::move(server), std::move(client),
        [&](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto [connected, accepted] = co_await ilias::whenAll(
                client.connect(server.localEndpoint().value(), "localhost"), server.accept());
            ILIAS_CO_TRY(auto local, std::move(connected));
            ILIAS_CO_TRY(auto peer, std::move(accepted));
            co_return co_await fn(local, peer);
        });
}

} // namespace

ILIAS_TEST(Quic, DuplexAndFin) {
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto stream, co_await local.openStream());
        ILIAS_CO_TRYV(co_await stream.writeString("Hello, world!"));
        ILIAS_CO_TRYV(co_await stream.shutdown());
        ILIAS_CO_TRY(auto accepted, co_await peer.acceptStream());
        EXPECT_EQ(stream.id(), accepted.id());
        std::array<char, 13> buffer;
        ILIAS_CO_TRYV(co_await accepted.readAll(ilias::makeBuffer(buffer)));
        EXPECT_EQ(std::string_view(buffer.data(), buffer.size()), "Hello, world!");
        ILIAS_CO_TRY(auto eof, co_await accepted.read(ilias::makeBuffer(buffer)));
        EXPECT_EQ(eof, 0);
        ILIAS_CO_TRYV(co_await accepted.writeString("Hello, world!"));
        ILIAS_CO_TRYV(co_await accepted.shutdown());
        auto failed = co_await stream.writeString("closed");
        EXPECT_FALSE(failed);
        ilias::DynStream dynamic{std::move(stream)};
        ILIAS_CO_TRYV(co_await dynamic.readAll(ilias::makeBuffer(buffer)));
        ILIAS_CO_TRY(eof, co_await dynamic.read(ilias::makeBuffer(buffer)));
        EXPECT_EQ(eof, 0);
        co_return {};
    });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, ConcurrentStreamsAndBackpressure) {
    auto server = serverConfig();
    auto client = clientConfig();
    for (auto config : {&server, &client}) {
        config->streamWindow = 8192;
        config->connectionWindow = 32768;
        config->sendBufferLimit = 8192;
        config->connectionSendBufferLimit = 32768;
    }
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        auto send = [&]() -> IoTask<void> {
            ilias::TaskGroup<ilias::IoResult<void>> group;
            for (size_t i = 0; i < 8; ++i) {
                group.spawn(exchange(local, 256 * 1024, static_cast<char>('A' + i)));
            }
            for (auto &result : co_await group.waitAll()) {
                ILIAS_CO_TRYV(std::move(result));
            }
            co_return {};
        };
        auto recv = [&]() -> IoTask<void> {
            ilias::TaskGroup<ilias::IoResult<void>> group;
            for (size_t i = 0; i < 8; ++i) {
                ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
                group.spawn(echo(std::move(stream)));
            }
            for (auto &result : co_await group.waitAll()) {
                ILIAS_CO_TRYV(std::move(result));
            }
            co_return {};
        };
        auto [sent, read] = co_await ilias::whenAll(send(), recv());
        ILIAS_CO_TRYV(std::move(sent));
        co_return std::move(read);
    }, server, client);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, BlockedStreamDoesNotBlockAnother) {
    auto server = serverConfig();
    auto client = clientConfig();
    server.streamWindow = 4096;
    server.connectionWindow = 32768;
    client.sendBufferLimit = 4096;
    client.connectionSendBufferLimit = 32768;
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto slow, co_await local.openStream());
        std::string payload(64 * 1024, 'x');
        EXPECT_FALSE(co_await ilias::timeout(slow.writeAll(ilias::makeBuffer(payload)), 50ms));
        ILIAS_CO_TRY(auto stalled, co_await peer.acceptStream());
        ILIAS_CO_TRY(auto fast, co_await local.openStream());
        ILIAS_CO_TRYV(co_await fast.writeString("fast"));
        ILIAS_CO_TRYV(co_await fast.flush());
        ILIAS_CO_TRY(auto accepted, co_await peer.acceptStream());
        std::array<char, 4> buffer;
        ILIAS_CO_TRYV(co_await accepted.readAll(ilias::makeBuffer(buffer)));
        EXPECT_EQ(std::string_view(buffer.data(), buffer.size()), "fast");
        co_return {};
    }, server, client);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, ReadCancellationAndReset) {
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto stream, co_await local.openStream());
        ILIAS_CO_TRYV(co_await stream.writeString("a"));
        ILIAS_CO_TRY(auto accepted, co_await peer.acceptStream());
        char byte;
        ILIAS_CO_TRYV(co_await accepted.readAll(ilias::makeBuffer(&byte, 1)));
        EXPECT_FALSE(co_await ilias::timeout(stream.read(ilias::makeBuffer(&byte, 1)), 20ms));
        ILIAS_CO_TRYV(co_await accepted.writeString("b"));
        ILIAS_CO_TRYV(co_await stream.readAll(ilias::makeBuffer(&byte, 1)));
        EXPECT_EQ(byte, 'b');
        stream.reset(42);
        auto reset = co_await accepted.read(ilias::makeBuffer(&byte, 1));
        EXPECT_FALSE(reset);
        if (!reset) { EXPECT_EQ(reset.error(), make_error_code(QuicError::StreamReset)); }
        EXPECT_EQ(accepted.appError(), 42);
        ILIAS_CO_TRY(auto next, co_await local.openStream());
        ILIAS_CO_TRYV(co_await next.writeString("c"));
        ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
        ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(&byte, 1)));
        EXPECT_EQ(byte, 'c');
        co_return {};
    });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, StreamLimitAndEmptyFin) {
    auto server = serverConfig();
    server.maxStreams = 1;
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto stream, co_await local.openStream());
        EXPECT_FALSE(co_await ilias::timeout(local.openStream(), 20ms));
        ILIAS_CO_TRYV(co_await stream.shutdown());
        ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
        char byte;
        ILIAS_CO_TRY(auto eof, co_await incoming.read(ilias::makeBuffer(&byte, 1)));
        EXPECT_EQ(eof, 0);
        ILIAS_CO_TRYV(co_await incoming.shutdown());
        ILIAS_CO_TRY(eof, co_await stream.read(ilias::makeBuffer(&byte, 1)));
        EXPECT_EQ(eof, 0);
        ILIAS_CO_TRY(auto next, co_await local.openStream());
        EXPECT_GT(next.id(), stream.id());
        co_return {};
    }, server);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, ConnectionCloseWakesReader) {
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto stream, co_await local.openStream());
        ILIAS_CO_TRYV(co_await stream.writeString("a"));
        ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
        char byte;
        ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(&byte, 1)));
        auto [closed, read] = co_await ilias::whenAll(local.close(77), incoming.read(ilias::makeBuffer(&byte, 1)));
        ILIAS_CO_TRYV(std::move(closed));
        EXPECT_FALSE(read);
        EXPECT_EQ(peer.appError(), 77);
        co_return {};
    });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, AcceptCancellationAndContextStop) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            EXPECT_FALSE(co_await ilias::timeout(server.accept(), 20ms));
            auto [connected, accepted] = co_await ilias::whenAll(
                client.connect(server.localEndpoint().value(), "localhost"), server.accept());
            ILIAS_CO_TRY(auto local, std::move(connected));
            ILIAS_CO_TRY(auto peer, std::move(accepted));
            auto pending = ilias::spawn(peer.acceptStream());
            auto accept = ilias::spawn(server.accept());
            co_await ilias::sleep(5ms);
            server.stop();
            auto pendingResult = co_await std::move(pending);
            auto acceptResult = co_await std::move(accept);
            EXPECT_TRUE(pendingResult);
            EXPECT_TRUE(acceptResult);
            if (pendingResult) { EXPECT_FALSE(*pendingResult); }
            if (acceptResult) { EXPECT_FALSE(*acceptResult); }
            co_return {};
        });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, CertificateAndAlpnFailures) {
    for (int failure = 0; failure < 3; ++failure) {
        auto client = clientConfig();
        if (failure == 1) { client.alpn = "another/1"; }
        if (failure == 2) { client.caFile.clear(); }
        auto result = co_await withContexts(serverConfig(), client,
            [failure](QuicContext &server, QuicContext &client) -> IoTask<void> {
                auto result = co_await client.connect(server.localEndpoint().value(),
                    failure == 0 ? "invalid.example" : "localhost");
                EXPECT_FALSE(result);
                if (!result) { EXPECT_EQ(result.error(), make_error_code(QuicError::Tls)); }
                co_return {};
            });
        EXPECT_TRUE(result) << result.error().message();
    }
    co_return;
}

ILIAS_TEST(Quic, HandshakeTimeout) {
    auto config = clientConfig();
    config.handshakeTimeout = 100ms;
    auto result = co_await withContexts(serverConfig(), config,
        [](QuicContext &, QuicContext &client) -> IoTask<void> {
            ILIAS_CO_TRY(auto blackhole, co_await ilias::UdpSocket::bind(IPEndpoint::fromString("127.0.0.1:0").value()));
            auto result = co_await client.connect(blackhole.localEndpoint().value(), "localhost");
            EXPECT_FALSE(result);
            if (!result) { EXPECT_EQ(result.error(), std::make_error_code(std::errc::timed_out)); }
            co_return {};
        });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, MultipleConnectionsAndUdpNoise) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            ILIAS_CO_TRY(auto noise, co_await ilias::UdpSocket::bind(IPEndpoint::fromString("127.0.0.1:0").value()));
            ILIAS_CO_TRYV(co_await noise.sendto(ilias::makeBuffer("not QUIC"), server.localEndpoint().value()));
            std::vector<QuicConnection> locals;
            std::vector<QuicConnection> peers;
            for (size_t i = 0; i < 3; ++i) {
                auto [connected, accepted] = co_await ilias::whenAll(
                    client.connect(server.localEndpoint().value(), "localhost"), server.accept());
                ILIAS_CO_TRY(auto local, std::move(connected));
                ILIAS_CO_TRY(auto peer, std::move(accepted));
                locals.push_back(std::move(local));
                peers.push_back(std::move(peer));
            }
            for (size_t i = 0; i < 3; ++i) {
                ILIAS_CO_TRY(auto stream, co_await locals[i].openStream());
                ILIAS_CO_TRYV(co_await stream.writeString("ok"));
                ILIAS_CO_TRY(auto incoming, co_await peers[i].acceptStream());
                std::array<char, 2> buffer;
                ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(buffer)));
                EXPECT_EQ(std::string_view(buffer.data(), 2), "ok");
            }
            co_return {};
        });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, DiscardedDataReturnsConnectionCredit) {
    auto server = serverConfig();
    server.connectionWindow = 1024;
    server.streamWindow = 1024;
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        for (size_t i = 0; i < 4; ++i) {
            ILIAS_CO_TRY(auto stream, co_await local.openStream());
            ILIAS_CO_TRYV(co_await stream.writeString(std::string(1024, 'x')));
            ILIAS_CO_TRYV(co_await stream.shutdown());
            ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
            ILIAS_CO_TRYV(co_await incoming.shutdown());
            char byte;
            ILIAS_CO_TRY(auto eof, co_await stream.read(ilias::makeBuffer(&byte, 1)));
            EXPECT_EQ(eof, 0);
            // Destroy a gracefully finished stream without reading its payload.
        }
        co_return {};
    }, server);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, IdleTimeout) {
    auto server = serverConfig();
    auto client = clientConfig();
    server.idleTimeout = client.idleTimeout = 100ms;
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &) -> IoTask<void> {
        auto accepted = co_await local.acceptStream();
        EXPECT_FALSE(accepted);
        if (!accepted) { EXPECT_EQ(accepted.error(), std::make_error_code(std::errc::timed_out)); }
        co_return {};
    }, server, client);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, Ipv6) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto [connected, accepted] = co_await ilias::whenAll(
                client.connect(server.localEndpoint().value(), "::1"), server.accept());
            ILIAS_CO_TRY(auto local, std::move(connected));
            ILIAS_CO_TRY(auto peer, std::move(accepted));
            auto serve = [&]() -> IoTask<void> {
                ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
                co_return co_await echo(std::move(stream));
            };
            auto [sent, read] = co_await ilias::whenAll(exchange(local, 4096, 'v'), serve());
            ILIAS_CO_TRYV(std::move(sent));
            co_return std::move(read);
        }, IPEndpoint::fromString("[::1]:0").value());
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, LossAndReordering) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto bind = IPEndpoint::fromString("127.0.0.1:0").value();
            ILIAS_CO_TRY(auto front, co_await ilias::UdpSocket::bind(bind));
            ILIAS_CO_TRY(auto back, co_await ilias::UdpSocket::bind(bind));
            auto clientAddress = client.localEndpoint().value();
            auto serverAddress = server.localEndpoint().value();
            size_t dropped = 0;
            size_t reordered = 0;
            auto relay = [&](ilias::UdpSocket &input, ilias::UdpSocket &output, IPEndpoint target) -> IoTask<void> {
                std::array<std::byte, 65536> buffer;
                std::vector<std::byte> held;
                size_t count = 0;
                while (true) {
                    ILIAS_CO_TRY(auto packet, co_await input.recvfrom(ilias::makeBuffer(buffer)));
                    auto size = packet.first;
                    if (++count % 11 == 0) {
                        ++dropped;
                        continue;
                    }
                    if (held.empty() && count % 5 == 0) {
                        held.assign(buffer.begin(), buffer.begin() + size);
                        continue;
                    }
                    ILIAS_CO_TRYV(co_await output.sendto(ilias::makeBuffer(buffer.data(), size), target));
                    if (!held.empty()) {
                        ILIAS_CO_TRYV(co_await output.sendto(ilias::makeBuffer(held), target));
                        held.clear();
                        ++reordered;
                    }
                }
            };
            co_return co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
                scope.spawn(relay(front, back, serverAddress));
                scope.spawn(relay(back, front, clientAddress));
                auto [connected, accepted] = co_await ilias::whenAll(
                    client.connect(front.localEndpoint().value(), "localhost"), server.accept());
                ILIAS_CO_TRY(auto local, std::move(connected));
                ILIAS_CO_TRY(auto peer, std::move(accepted));
                auto serve = [&]() -> IoTask<void> {
                    ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
                    co_return co_await echo(std::move(stream));
                };
                auto [sent, read] = co_await ilias::whenAll(exchange(local, 256 * 1024, 'r'), serve());
                ILIAS_CO_TRYV(std::move(sent));
                ILIAS_CO_TRYV(std::move(read));
                EXPECT_GT(dropped, 0);
                EXPECT_GT(reordered, 0);
                scope.stop();
                co_return {};
            });
        });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, InvalidConfiguration) {
    auto config = clientConfig();
    config.alpn.clear();
    auto result = co_await QuicContext::bind(IPEndpoint::fromString("127.0.0.1:0").value(), config);
    EXPECT_FALSE(result);
    if (!result) { EXPECT_EQ(result.error(), std::make_error_code(std::errc::invalid_argument)); }
    config = serverConfig();
    config.keyFile.clear();
    result = co_await QuicContext::bind(IPEndpoint::fromString("127.0.0.1:0").value(), config);
    EXPECT_FALSE(result);
    co_return;
}

ILIAS_TEST(Quic, RunStopsNormally) {
    auto bound = co_await QuicContext::bind(IPEndpoint::fromString("127.0.0.1:0").value(), clientConfig());
    EXPECT_TRUE(bound);
    if (!bound) { co_return; }
    auto context = std::move(*bound);
    auto driver = ilias::spawn(context.run());
    co_await ilias::sleep(5ms);
    context.stop();
    auto result = co_await std::move(driver);
    EXPECT_TRUE(result);
    if (result) { EXPECT_TRUE(*result); }
    co_return;
}

#endif // HAJIMI_QUIC
