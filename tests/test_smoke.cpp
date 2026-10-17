#include <ilias/testing.hpp>
#include <ilias/net.hpp>
#include <gtest/gtest.h>

#include "../src/client.hpp"
#include "../src/server.hpp"

#ifdef HAJIMI_QUIC
    #include "../src/quic/quic.hpp"
    #include "../src/protocol.hpp"
#endif // HAJIMI_QUIC

using namespace std::literals;
using ilias::TcpListener;
using ilias::TcpStream;
using ilias::IoTask;

// Smoke test for the client and server
ILIAS_TEST(Hajimi, Smoke) {
    ProxyServer server {
        ProxyServer::Config {
            .listen = "127.0.0.1:8848",
            .webui = "127.0.0.1:0", // Don't care
#ifdef HAJIMI_QUIC
            .certFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
            .keyFile = HAJIMI_QUIC_TEST_DIR "/localhost-key.pem",
#endif // HAJIMI_QUIC
        }
    };
    ProxyClient client {
        ProxyClient::Config {
            .master = "127.0.0.1:8848",
            .name = "test-client",

#ifdef HAJIMI_QUIC
            .transport = "quic",
            .caFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
            .verify = true,
#endif // HAJIMI_QUIC
        }
    };

    auto echo = [](auto &stream, size_t count) -> IoTask<void> {
        char buf[13]; // "Hello, world!"
        for (size_t i = 0; i < count; ++i) {
            EXPECT_TRUE(co_await stream.writeString("Hello, world!"));
        }
        for (size_t i = 0; i < count; ++i) {
            EXPECT_TRUE(co_await stream.readAll(ilias::makeBuffer(buf)));            
        }
        EXPECT_EQ(std::string_view(buf, 13), "Hello, world!");
        co_return {};
    };

    auto testDuplex = [&] -> IoTask<void> {
        auto ok = server.addRule(ProxyServer::Rule {
            .listenPort = 8850,
            .clientName = "test-client",
            .targetHost = "127.0.0.1",
            .targetPort = 9000,
        });
        EXPECT_TRUE(ok);

        // Forward 127.0.0.1:8850 to 127.0.0.1:9000
        co_await ilias::sleep(100ms); // Wait the server to apply the rule

        ILIAS_CO_TRY(auto listener, co_await TcpListener::bind("127.0.0.1:9000"));
        ILIAS_CO_TRY(auto local, co_await TcpStream::connect("127.0.0.1:8850"));
        ILIAS_CO_TRY(auto peer, co_await listener.accept(nullptr));

        // Exchange the data
        co_await ilias::whenAll(
            echo(local, 1024),
            echo(peer, 1024)
        );
        co_return {};
    };

    auto [s, c, result] = co_await ilias::whenAny(
        server.run(),
        client.run(),
        testDuplex()
    );
    if (result) {
        EXPECT_TRUE(result->has_value()); // No-err
    }
    EXPECT_TRUE(result);
    co_return;
}

#ifdef HAJIMI_QUIC
ILIAS_TEST(Hajimi, ClientKeepsIdleQuicConnectionAlive) {
    auto bound = co_await QuicContext::bind("127.0.0.1:0", {
        .role = QuicContext::Role::Server,
        .certFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
        .keyFile = HAJIMI_QUIC_TEST_DIR "/localhost-key.pem",
        .idleTimeout = 15s,
    });
    EXPECT_TRUE(bound) << bound.error().message();
    if (!bound) {
        co_return;
    }
    auto server = std::move(*bound);
    ProxyClient client{{
        .master = std::format("{}", server.localEndpoint().value()),
        .name = "idle-client",
        .transport = "quic",
        .caFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
    }};
    auto serve = [&]() -> IoTask<void> {
        ILIAS_CO_TRY(auto connection, co_await server.accept());
        ILIAS_CO_TRY(auto stream, co_await connection.acceptStream());
        SkBufferPool pool{HAJIMI_POOL_CONFIG};
        MessageReader reader{stream, pool};
        MessageWriter writer{stream, pool};
        ILIAS_CO_TRY(auto hello, co_await reader.readMessage());
        EXPECT_EQ(hello.type(), MessageType::Hello);
        ILIAS_CO_TRYV(co_await writer.writeMessage(HelloAck{}));
        // This peer sends no periodic pings; the client must keep it alive.
        for (int i = 0; i < 3; ++i) {
            ILIAS_CO_TRY(auto ping, co_await reader.readMessage());
            EXPECT_EQ(ping.type(), MessageType::Ping);
            ILIAS_CO_TRYV(co_await writer.writeMessage(Pong{}));
        }
        co_await ilias::sleep(5s);
        ILIAS_CO_TRYV(co_await writer.writeMessage(Ping{}));
        ILIAS_CO_TRY(auto pong, co_await reader.readMessage());
        EXPECT_EQ(pong.type(), MessageType::Pong);
        co_return {};
    };
    co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> ilias::Task<void> {
        scope.spawn(server.run());
        scope.spawn(client.run());
        auto result = co_await ilias::timeout(serve(), 40s);
        EXPECT_TRUE(result);
        if (result) {
            EXPECT_TRUE(*result) << result->error().message();
        }
        scope.stop();
    });
}
#endif // HAJIMI_QUIC
