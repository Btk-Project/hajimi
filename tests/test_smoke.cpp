#include <ilias/testing.hpp>
#include <ilias/net.hpp>
#include <gtest/gtest.h>

#include "../src/client.hpp"
#include "../src/server.hpp"

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
            .caFile = HAJIMI_QUIC_TEST_DIR "/localhost.pem",
            .verify = false,
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