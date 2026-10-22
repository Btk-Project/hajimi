#ifdef HAJIMI_QUIC

#include <ilias/testing.hpp>
#include <ilias/sync.hpp>
#include <gtest/gtest.h>
#include <algorithm>
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
    IPEndpoint bind = IPEndpoint::fromString("127.0.0.1:0").value(),
    std::optional<IPEndpoint> clientBind = {}) -> IoTask<void> {
    ILIAS_CO_TRY(auto server, co_await QuicContext::bind(bind, std::move(serverConfig)));
    ILIAS_CO_TRY(auto client, co_await QuicContext::bind(clientBind.value_or(bind), std::move(clientConfig)));
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

ILIAS_TEST(Quic, BulkTransferWithBatchingAndFallback) {
    for (bool batching : {false, true}) {
        auto serverCfg = serverConfig();
        auto clientCfg = clientConfig();
        serverCfg.udpBatching = batching;
        clientCfg.udpBatching = batching;
        auto result = co_await withContexts(serverCfg, clientCfg,
            [&](QuicContext &server, QuicContext &client) -> IoTask<void> {
                auto [connected, accepted] = co_await ilias::whenAll(
                    client.connect(server.localEndpoint().value(), "localhost"), server.accept());
                ILIAS_CO_TRY(auto local, std::move(connected));
                ILIAS_CO_TRY(auto peer, std::move(accepted));
                ILIAS_CO_TRY(auto stream, co_await local.openStream());
                std::string payload(4 * 1024 * 1024, 'b');
                for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i % 251);
                auto write = [&]() -> IoTask<void> {
                    ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(payload)));
                    ILIAS_CO_TRYV(co_await stream.flush());
                    co_return co_await stream.shutdown();
                };
                auto read = [&]() -> IoTask<void> {
                    ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
                    std::array<char, 2577> bytes;
                    size_t offset = 0;
                    while (true) {
                        ILIAS_CO_TRY(auto size, co_await incoming.read(ilias::makeBuffer(bytes)));
                        if (!size) break;
                        if (offset + size > payload.size()) co_return Err(std::make_error_code(std::errc::bad_message));
                        EXPECT_EQ(std::string_view(bytes.data(), size), std::string_view(payload).substr(offset, size));
                        offset += size;
                    }
                    EXPECT_EQ(offset, payload.size());
                    co_return {};
                };
                auto [written, readResult] = co_await ilias::whenAll(write(), read());
                ILIAS_CO_TRYV(std::move(written));
                ILIAS_CO_TRYV(std::move(readResult));
                auto stats = client.ioStats();
                EXPECT_GT(stats.txDatagrams, 1000);
                if (batching && stats.sendSegmentation) EXPECT_GT(stats.txSegmentedDatagrams, 0);
                if (!batching) EXPECT_EQ(stats.maxTxBatch, 1);
                if (stats.txSegmentedDatagrams) EXPECT_LT(stats.txSubmissions, stats.txDatagrams / 2);
                auto prefix = batching ? "batch_" : "single_";
                ::testing::Test::RecordProperty(std::string(prefix) + "tx_datagrams", std::to_string(stats.txDatagrams));
                ::testing::Test::RecordProperty(std::string(prefix) + "tx_submissions", std::to_string(stats.txSubmissions));
                ::testing::Test::RecordProperty(std::string(prefix) + "tx_max_batch", std::to_string(stats.maxTxBatch));
                co_return {};
            });
        EXPECT_TRUE(result) << result.error().message();
    }
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

ILIAS_TEST(Quic, ExternalPskWithoutCertificates) {
    QuicContext::Config server;
    server.role = QuicContext::Role::Server;
    server.psk.emplace().fill(0x42);
    QuicContext::Config client;
    client.psk = server.psk;
    auto result = co_await withConnections([](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        auto serve = [&]() -> IoTask<void> {
            ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
            co_return co_await echo(std::move(stream));
        };
        auto [sent, received] = co_await ilias::whenAll(exchange(local, 300000, 'p'), serve());
        ILIAS_CO_TRYV(std::move(sent));
        co_return std::move(received);
    }, server, client);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, ExternalPskFailures) {
    for (int failure = 0; failure < 4; ++failure) {
        QuicContext::Config server;
        server.role = QuicContext::Role::Server;
        server.psk.emplace().fill(0x42);
        QuicContext::Config client;
        client.psk = server.psk;
        if (failure == 0) { client.psk->fill(0x43); }
        if (failure == 1) { client.pskIdentity = "unknown"; }
        if (failure == 2) { client.alpn = "another/1"; }
        if (failure == 3) { client.psk.reset(); client.verifyPeer = false; }
        auto result = co_await withContexts(server, client,
            [](QuicContext &server, QuicContext &client) -> IoTask<void> {
                auto connected = co_await client.connect(server.localEndpoint().value(), "localhost");
                EXPECT_FALSE(connected);
                if (!connected) { EXPECT_EQ(connected.error(), make_error_code(QuicError::Tls)); }
                co_return {};
            });
        EXPECT_TRUE(result) << result.error().message();
    }
    co_return;
}

ILIAS_TEST(Quic, PskClientRejectsCertificateFallback) {
    QuicContext::Config client;
    client.psk.emplace().fill(0x42);
    auto result = co_await withContexts(serverConfig(), client,
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto connected = co_await client.connect(server.localEndpoint().value(), "localhost");
            EXPECT_FALSE(connected);
            if (!connected) { EXPECT_EQ(connected.error(), make_error_code(QuicError::Tls)); }
            co_return {};
        });
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(Quic, PskAndCertificateConfigurationAreExclusive) {
    for (int failure = 0; failure < 4; ++failure) {
        QuicContext::Config config;
        config.psk.emplace().fill(0x42);
        if (failure == 0) { config.certFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem"; }
        if (failure == 1) { config.keyFile = HAJIMI_QUIC_TEST_DIR "/localhost-key.pem"; }
        if (failure == 2) { config.caFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem"; }
        if (failure == 3) { config.pskIdentity.clear(); }
        auto result = co_await QuicContext::bind("127.0.0.1:0", config);
        EXPECT_FALSE(result);
        if (!result) { EXPECT_EQ(result.error(), std::errc::invalid_argument); }
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

ILIAS_TEST(Quic, ContextShutdownNotifiesAllPeers) {
    for (int mode = 0; mode < 4; ++mode) {
        auto serverBound = co_await QuicContext::bind("127.0.0.1:0", serverConfig());
        auto clientBound = co_await QuicContext::bind("127.0.0.1:0", clientConfig());
        EXPECT_TRUE(serverBound);
        EXPECT_TRUE(clientBound);
        if (!serverBound || !clientBound) { co_return; }
        auto server = std::move(*serverBound);
        auto client = std::move(*clientBound);
        auto result = co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
            auto serverDriver = scope.spawn(server.run());
            auto clientDriver = scope.spawn(client.run());
            std::vector<QuicConnection> locals, peers;
            std::vector<QuicStream> localStreams, peerStreams;
            for (int i = 0; i < 3; ++i) {
                auto [connected, accepted] = co_await ilias::whenAll(
                    client.connect(server.localEndpoint().value(), "localhost"), server.accept());
                ILIAS_CO_TRY(auto local, std::move(connected));
                ILIAS_CO_TRY(auto peer, std::move(accepted));
                ILIAS_CO_TRY(auto outgoing, co_await local.openStream());
                ILIAS_CO_TRYV(co_await outgoing.writeString("a"));
                ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
                char byte;
                ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(&byte, 1)));
                locals.push_back(std::move(local));
                peers.push_back(std::move(peer));
                localStreams.push_back(std::move(outgoing));
                peerStreams.push_back(std::move(incoming));
            }
            bool stopClient = mode < 2;
            if (mode % 2) {
                (stopClient ? clientDriver : serverDriver).stop();
            }
            else {
                (stopClient ? client : server).stop();
            }
            auto &connections = stopClient ? peers : locals;
            auto &streams = stopClient ? peerStreams : localStreams;
            for (size_t i = 0; i < streams.size(); ++i) {
                char byte;
                auto read = co_await ilias::timeout(streams[i].read(ilias::makeBuffer(&byte, 1)), 500ms);
                EXPECT_TRUE(read) << "Peer did not receive CONNECTION_CLOSE, mode " << mode;
                if (read) {
                    EXPECT_FALSE(*read);
                    if (!*read) { EXPECT_EQ(read->error(), make_error_code(QuicError::ConnectionClosed)); }
                }
                EXPECT_EQ(connections[i].appError(), 0);
            }
            scope.stop();
            co_return {};
        });
        EXPECT_TRUE(result) << result.error().message();
    }
}

ILIAS_TEST(Quic, LostClosePacketIsRetransmitted) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            ILIAS_CO_TRY(auto front, co_await ilias::UdpSocket::bind("127.0.0.1:0"));
            ILIAS_CO_TRY(auto back, co_await ilias::UdpSocket::bind("127.0.0.1:0"));
            bool dropClose = false;
            bool dropped = false;
            auto relay = [&](ilias::UdpSocket &input, ilias::UdpSocket &output,
                IPEndpoint target, bool fromClient) -> IoTask<void> {
                std::array<std::byte, 65536> buffer;
                while (true) {
                    ILIAS_CO_TRY(auto packet, co_await input.recvfrom(ilias::makeBuffer(buffer)));
                    if (fromClient && std::exchange(dropClose, false)) {
                        dropped = true;
                        continue;
                    }
                    ILIAS_CO_TRYV(co_await output.sendto(ilias::makeBuffer(buffer.data(), packet.first), target));
                }
            };
            co_return co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
                scope.spawn(relay(front, back, server.localEndpoint().value(), true));
                scope.spawn(relay(back, front, client.localEndpoint().value(), false));
                auto [connected, accepted] = co_await ilias::whenAll(
                    client.connect(front.localEndpoint().value(), "localhost"), server.accept());
                ILIAS_CO_TRY(auto local, std::move(connected));
                ILIAS_CO_TRY(auto peer, std::move(accepted));
                co_await ilias::sleep(30ms); // Drain handshake ACKs before dropping the close.
                dropClose = true;
                ILIAS_CO_TRYV(co_await local.close(77));
                ILIAS_CO_TRY(auto probe, co_await peer.openStream());
                ILIAS_CO_TRYV(co_await probe.writeString("still connected?"));
                auto closed = co_await ilias::timeout(peer.acceptStream(), 1s);
                EXPECT_TRUE(closed);
                if (closed) {
                    EXPECT_FALSE(*closed);
                    if (!*closed) { EXPECT_EQ(closed->error(), make_error_code(QuicError::ConnectionClosed)); }
                }
                EXPECT_TRUE(dropped);
                EXPECT_EQ(peer.appError(), 77);
                scope.stop();
                co_return {};
            });
        });
    EXPECT_TRUE(result) << result.error().message();
}

ILIAS_TEST(Quic, BufferedStreamsOutliveContext) {
    std::optional<QuicStream> sender, receiver;
    auto result = co_await withConnections([&](QuicConnection &local, QuicConnection &peer) -> IoTask<void> {
        ILIAS_CO_TRY(auto outgoing, co_await local.openStream());
        ILIAS_CO_TRYV(co_await outgoing.writeString(std::string(16384, 'x')));
        ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
        char byte;
        ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(&byte, 1)));
        sender.emplace(std::move(outgoing));
        receiver.emplace(std::move(incoming));
        co_return {};
    });
    EXPECT_TRUE(result) << result.error().message();
    // Destroy pooled RX/TX data after both contexts and connections are gone.
    sender.reset();
    receiver.reset();
}

ILIAS_TEST(Quic, PmtudProbesAndSmallerPath) {
    for (size_t limit : {1452, 1200}) {
        auto result = co_await withContexts(serverConfig(), clientConfig(),
            [limit](QuicContext &server, QuicContext &client) -> IoTask<void> {
                ILIAS_CO_TRY(auto front, co_await ilias::UdpSocket::bind("127.0.0.1:0"));
                ILIAS_CO_TRY(auto back, co_await ilias::UdpSocket::bind("127.0.0.1:0"));
                size_t largest = 0;
                size_t dropped = 0;
                size_t largePackets = 0;
                auto relay = [&](ilias::UdpSocket &input, ilias::UdpSocket &output, IPEndpoint target) -> IoTask<void> {
                    std::array<std::byte, 65536> buffer;
                    while (true) {
                        ILIAS_CO_TRY(auto packet, co_await input.recvfrom(ilias::makeBuffer(buffer)));
                        auto size = packet.first;
                        largest = std::max(largest, size);
                        if (size > limit) {
                            ++dropped;
                            continue;
                        }
                        if (size > 1200) { ++largePackets; }
                        ILIAS_CO_TRYV(co_await output.sendto(ilias::makeBuffer(buffer.data(), size), target));
                    }
                };
                co_return co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
                    scope.spawn(relay(front, back, server.localEndpoint().value()));
                    scope.spawn(relay(back, front, client.localEndpoint().value()));
                    auto [connected, accepted] = co_await ilias::whenAll(
                        client.connect(front.localEndpoint().value(), "localhost"), server.accept());
                    ILIAS_CO_TRY(auto local, std::move(connected));
                    ILIAS_CO_TRY(auto peer, std::move(accepted));
                    auto serve = [&]() -> IoTask<void> {
                        ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
                        co_return co_await echo(std::move(stream));
                    };
                    auto [sent, read] = co_await ilias::whenAll(exchange(local, 512 * 1024, 'm'), serve());
                    ILIAS_CO_TRYV(std::move(sent));
                    ILIAS_CO_TRYV(std::move(read));
                    EXPECT_GT(largest, 1200);
                    EXPECT_LE(largest, 1452);
                    if (limit == 1200) { EXPECT_GT(dropped, 0); }
                    else { EXPECT_GT(largePackets, 16); }
                    scope.stop();
                    co_return {};
                });
            });
        EXPECT_TRUE(result) << result.error().message();
    }
}

ILIAS_TEST(Quic, HostnameAndNumericAddresses) {
    for (bool dualStack : {false, true}) {
        auto result = co_await withContexts(serverConfig(), clientConfig(),
            [dualStack](QuicContext &server, QuicContext &client) -> IoTask<void> {
                std::vector<std::string> hosts{"localhost", "127.0.0.1"};
                if (dualStack) { hosts.push_back("[::1]"); }
                for (const auto &host : hosts) {
                    auto [connected, accepted] = co_await ilias::whenAll(
                        client.connect(std::format("{}:{}", host, server.localEndpoint().value().port())),
                        server.accept());
                    ILIAS_CO_TRY(auto local, std::move(connected));
                    ILIAS_CO_TRY(auto peer, std::move(accepted));
                    auto serve = [&]() -> IoTask<void> {
                        ILIAS_CO_TRY(auto stream, co_await peer.acceptStream());
                        co_return co_await echo(std::move(stream));
                    };
                    auto [sent, read] = co_await ilias::whenAll(exchange(local, 4096, 'h'), serve());
                    ILIAS_CO_TRYV(std::move(sent));
                    ILIAS_CO_TRYV(std::move(read));
                }
                // No duplicate connection is left queued for a fast successful handshake.
                EXPECT_FALSE(co_await ilias::timeout(server.accept(), 30ms));
                co_return {};
            }, "[::]:0", IPEndpoint{dualStack ? "[::]:0" : "127.0.0.1:0"});
        EXPECT_TRUE(result) << result.error().message();
    }
}

ILIAS_TEST(Quic, HostnameRaceSkipsUnresponsiveFamily) {
    ilias::addrinfo_t hints{};
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    hints.ai_flags = AI_NUMERICSERV;
    auto resolved = co_await ilias::AddressInfo::lookup("localhost:1", hints);
    EXPECT_TRUE(resolved);
    if (!resolved) { co_return; }
    auto endpoints = resolved->endpoints();
    bool hasV4 = std::ranges::any_of(endpoints, [](auto ep) { return ep.family() == AF_INET; });
    bool hasV6 = std::ranges::any_of(endpoints, [](auto ep) { return ep.family() == AF_INET6; });
    EXPECT_TRUE(hasV4 && hasV6);
    if (!hasV4 || !hasV6) { co_return; }
    bool preferV6 = endpoints.front().family() == AF_INET6;
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [preferV6](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto port = server.localEndpoint().value().port();
            auto first = std::format("{}:{}", preferV6 ? "[::1]" : "127.0.0.1", port);
            ILIAS_CO_TRY(auto blackhole, co_await ilias::UdpSocket::bind(IPEndpoint{first}));
            // The first family stays silent; the other must win before its 2s timeout.
            auto connected = co_await ilias::timeout(client.connect(std::format("localhost:{}", port)), 1s);
            EXPECT_TRUE(connected);
            if (!connected) { co_return Err(std::make_error_code(std::errc::timed_out)); }
            ILIAS_CO_TRY(auto local, std::move(*connected));
            ILIAS_CO_TRY(auto peer, co_await server.accept());
            std::array<std::byte, 65536> packet;
            EXPECT_TRUE(co_await ilias::timeout(blackhole.recvfrom(ilias::makeBuffer(packet)), 100ms));
            ILIAS_CO_TRY(auto stream, co_await local.openStream());
            ILIAS_CO_TRYV(co_await stream.writeString("winner"));
            ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
            std::array<char, 6> bytes;
            ILIAS_CO_TRYV(co_await incoming.readAll(ilias::makeBuffer(bytes)));
            EXPECT_EQ(std::string_view(bytes.data(), bytes.size()), "winner");
            co_return {};
        }, IPEndpoint{preferV6 ? "127.0.0.1:0" : "[::1]:0"}, IPEndpoint{"[::]:0"});
    EXPECT_TRUE(result) << result.error().message();
}

ILIAS_TEST(Quic, HostnameConnectRejectsInvalidAddresses) {
    auto bound = co_await QuicContext::bind("127.0.0.1:0", clientConfig());
    EXPECT_TRUE(bound);
    if (!bound) { co_return; }
    for (const auto *address : {"", "localhost", ":1145", "localhost:", "[]:1145", "[::1:1145",
        "::1:1145", "localhost:0", "localhost:-1", "localhost:65536", "localhost:1145junk"}) {
        auto result = co_await bound->connect(address);
        EXPECT_FALSE(result) << address;
        if (!result) { EXPECT_EQ(result.error(), std::errc::invalid_argument) << address; }
    }
}

ILIAS_TEST(Quic, HostnameConnectPreservesTlsVerification) {
    auto config = clientConfig();
    config.caFile.clear();
    auto result = co_await withContexts(serverConfig(), config,
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            auto connected = co_await client.connect(std::format("localhost:{}", server.localEndpoint().value().port()));
            EXPECT_FALSE(connected);
            if (!connected) { EXPECT_EQ(connected.error(), make_error_code(QuicError::Tls)); }
            co_return {};
        }, "[::]:0");
    EXPECT_TRUE(result) << result.error().message();
}

ILIAS_TEST(Quic, HostnameConnectCancellation) {
    auto result = co_await withContexts(serverConfig(), clientConfig(),
        [](QuicContext &server, QuicContext &client) -> IoTask<void> {
            ILIAS_CO_TRY(auto blackhole, co_await ilias::UdpSocket::bind("127.0.0.1:0"));
            ilias::TaskGroup<ilias::IoResult<QuicConnection>> pending;
            auto handle = pending.spawn(client.connect(blackhole.localEndpoint()->toString()));
            std::array<std::byte, 65536> packet;
            auto initial = co_await ilias::timeout(blackhole.recvfrom(ilias::makeBuffer(packet)), 500ms);
            EXPECT_TRUE(initial);
            // Drain any additional Initial packets before canceling the handshake.
            while (co_await ilias::timeout(blackhole.recvfrom(ilias::makeBuffer(packet)), 5ms)) {}
            handle.stop();
            EXPECT_FALSE(co_await pending.next());
            EXPECT_TRUE(co_await ilias::timeout(blackhole.recvfrom(ilias::makeBuffer(packet)), 500ms));
            co_await pending.shutdown();
            auto [connected, accepted] = co_await ilias::whenAll(
                client.connect(server.localEndpoint()->toString()), server.accept());
            ILIAS_CO_TRY(auto local, std::move(connected));
            ILIAS_CO_TRY(auto peer, std::move(accepted));
            co_return {};
        });
    EXPECT_TRUE(result) << result.error().message();
}

#endif // HAJIMI_QUIC
