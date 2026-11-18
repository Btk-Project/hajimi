#include <ilias/testing.hpp>
#include <ilias/io.hpp>
#include <gtest/gtest.h>
#include <algorithm>
#include <print>
#include <vector>

#include "../src/transport.hpp"
#include "../src/skbuf.hpp"
#include <ilias/buffer.hpp>

using ilias::DuplexStream;
using ilias::IoTask;

// Send hello world and expected receive it back
auto onTestExchangeHelloWorld(HajimiStream &stream) -> IoTask<void> {
    using namespace ilias::literals;
    auto payload = "HelloWorld"_bin;

    ILIAS_CO_TRYV(co_await stream.write(SkBuffer {
        payload,
        stream.pool()
    }));
    ILIAS_CO_TRYV(co_await stream.shutdown()); // Test Half close

    ILIAS_CO_TRY(auto buf, co_await stream.read());
    EXPECT_EQ(buf.size(), payload.size()) << "Expected to receive the same size: " << buf.size();
    if (buf.size() == payload.size()) {
        EXPECT_EQ(std::memcmp(buf.data().data(), payload.data(), buf.size()), 0) << "Expected to receive the same data";
    }

    // The next read should return an empty buffer
    ILIAS_CO_TRY(buf, co_await stream.read());
    EXPECT_EQ(buf.size(), 0) << "Expected to receive an empty buffer";
    co_return {};
};

auto onServer(HajimiConnection &con) -> IoTask<void> {
    ILIAS_CO_TRY(auto msg, co_await con.readMessage());

    ILIAS_CO_TRY(auto hello, msg.cast<Hello>());
    EXPECT_EQ(hello.version, 1145);
    EXPECT_EQ(hello.name, "test");
    std::println("Server got message: {}", hello.name);

    // Give back an ack
    ILIAS_CO_TRYV(co_await con.writeMessage(HelloAck {}));

    // Test Server open a new stream
    {
        ILIAS_CO_TRY(auto stream, co_await con.openTunnel(OpenTunnel {
            .addr = "42",
            .port = 42
        }));
        ILIAS_CO_TRYV(co_await onTestExchangeHelloWorld(stream));
    }
    // Test accept from client
    {
        ILIAS_CO_TRY(auto stream, co_await con.acceptTunnel());
        EXPECT_EQ(stream.first.addr, "420");
        EXPECT_EQ(stream.first.port, 420);

        // Test exchange
        ILIAS_CO_TRYV(co_await onTestExchangeHelloWorld(stream.second));
    }

    co_return {};
}

auto onClient(HajimiConnection &con) -> IoTask<void> {
    ILIAS_CO_TRYV(co_await con.writeMessage(Hello {
        .version = 1145,
        .name = "test"
    }));
    ILIAS_CO_TRY(auto msg, co_await con.readMessage());
    EXPECT_EQ(msg.type(), MessageType::HelloAck);
    std::println("Client got ack");

    {
        // Test accept from server
        ILIAS_CO_TRY(auto stream, co_await con.acceptTunnel());
        EXPECT_EQ(stream.first.addr, "42");
        EXPECT_EQ(stream.first.port, 42);

        // Test exchange
        ILIAS_CO_TRYV(co_await onTestExchangeHelloWorld(stream.second));
    }

    {
        // Test open a new stream
        ILIAS_CO_TRY(auto stream, co_await con.openTunnel(OpenTunnel {
            .addr = "420",
            .port = 420
        }));
        ILIAS_CO_TRYV(co_await onTestExchangeHelloWorld(stream));
    }
    co_return {};
}

ILIAS_TEST(Transport, Handshake) {
    SkBufferPool pool{HAJIMI_POOL_CONFIG};
    auto [a, b] = DuplexStream::make(1024 * 16);
    auto client = co_await HajimiConnection::fromClient(pool, std::move(a));
    auto server = co_await HajimiConnection::fromServer(pool, std::move(b));
    if (!client || !server) {
        EXPECT_TRUE(client) << "client failed: " << client.error().message();
        EXPECT_TRUE(server) << "server failed: " << server.error().message();
        co_return;
    }

    auto [gp1, gp2] = co_await ilias::whenAny(
        ilias::whenAll(
            onServer(*server),
            onClient(*client)
        ),
        ilias::whenAll(
            server->run(), // Should run forever
            client->run()
        )
    );
    if (gp1) {
        auto [s, c] = *gp1;
        EXPECT_TRUE(s) << "onServer failed" << s.error().message();
        EXPECT_TRUE(c) << "onClient failed" << c.error().message();
    }
    co_return;
}

ILIAS_TEST(Transport, ReplenishesReceiveWindow) {
    SkBufferPool pool{HAJIMI_POOL_CONFIG};
    auto [left, right] = DuplexStream::make(1024 * 16);
    auto client = co_await HajimiConnection::fromClient(pool, std::move(left));
    auto server = co_await HajimiConnection::fromServer(pool, std::move(right));
    EXPECT_TRUE(client);
    EXPECT_TRUE(server);
    if (!client || !server) co_return;

    // Send the data more then the receive window size
    std::vector<std::byte> payload(2 * HAJIMI_TCP_INIT_WINDOW_SIZE + 17, std::byte{0x42});
    auto send = [&]() -> IoTask<void> {
        ILIAS_CO_TRY(auto tunnel, co_await client->openTunnel(OpenTunnel{"localhost", 42}));
        ILIAS_CO_TRY(auto written, co_await tunnel.write(SkBuffer{payload, pool}));
        EXPECT_EQ(written, payload.size());
        co_return co_await tunnel.shutdown();
    };
    auto receive = [&]() -> IoTask<void> {
        ILIAS_CO_TRY(auto tunnel, co_await server->acceptTunnel());
        auto remaining = std::span<const std::byte>{payload};
        while (true) {
            ILIAS_CO_TRY(auto buf, co_await tunnel.second.read());
            if (buf.empty()) break;
            if (buf.size() > remaining.size()) {
                ADD_FAILURE() << "Received more data than sent";
                co_return {};
            }
            EXPECT_TRUE(std::ranges::equal(buf.data(), remaining.first(buf.size())));
            remaining = remaining.subspan(buf.size());
        }
        EXPECT_TRUE(remaining.empty());
        co_return {};
    };
    
    auto [transfer, clientRun, serverRun] = co_await ilias::whenAny(
        ilias::whenAll(send(), receive()), client->run(), server->run()
    );

    EXPECT_FALSE(clientRun);
    EXPECT_FALSE(serverRun);
    EXPECT_TRUE(transfer);
    if (transfer) {
        auto &[sent, received] = *transfer;
        EXPECT_TRUE(sent) << sent.error().message();
        EXPECT_TRUE(received) << received.error().message();
    }
    co_return;
}
