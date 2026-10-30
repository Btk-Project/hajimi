#ifdef HAJIMI_QUIC

#include <ilias/testing.hpp>
#include <gtest/gtest.h>
#include <algorithm>
#include "../src/quic/udp_batch.hpp"

using namespace std::chrono_literals;
using ilias::Err;
using ilias::IoTask;
using ilias::IPEndpoint;

namespace {

auto bindUdp(SkBufferPool &pool, bool batching, int family = AF_INET, bool noChecksum = false) -> ilias::IoResult<quic::UdpBatchSocket> {
    ILIAS_TRY(auto socket, ilias::Socket::make(family, SOCK_DGRAM, IPPROTO_UDP));
    if (family == AF_INET6) ILIAS_TRYV(socket.setOption(ilias::sockopt::Ipv6Only{false}));
    ILIAS_TRYV(socket.bind(IPEndpoint::fromString(family == AF_INET6 ? "[::]:0" : "127.0.0.1:0").value()));
#if defined(__linux__)
    if (noChecksum) {
        int enabled = 1;
        ILIAS_TRYV(socket.setOption(SOL_SOCKET, SO_NO_CHECK, &enabled, sizeof(enabled)));
    }
#endif
    return quic::UdpBatchSocket::from(std::move(socket), pool, batching);
}

auto transfer(bool sendBatching, bool receiveBatching, int family = AF_INET, bool noChecksum = false) -> IoTask<void> {
    SkBufferPool pool{{{65536, 2}}};
    ILIAS_CO_TRY(auto sender, bindUdp(pool, sendBatching, family, noChecksum));
    ILIAS_CO_TRY(auto receiver, bindUdp(pool, receiveBatching));
    constexpr size_t segment = 1200;
    constexpr size_t count = 40;
    std::vector<std::byte> payload(segment * (count - 1) + 57);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = std::byte((i / segment) + 1);
    auto read = [&]() -> IoTask<void> {
        size_t offset = 0;
        size_t datagrams = 0;
        while (offset < payload.size()) {
            ILIAS_CO_TRY(auto received, co_await receiver.receive());
            for (auto &message : received) {
                EXPECT_EQ(message.ecn, 2);
                auto bytes = message.bytes;
                do {
                    auto size = message.segmentSize ? std::min(message.segmentSize, bytes.size()) : bytes.size();
                    if (offset + size > payload.size()) co_return Err(std::make_error_code(std::errc::bad_message));
                    EXPECT_EQ(size, std::min(segment, payload.size() - offset));
                    EXPECT_TRUE(std::ranges::equal(bytes.first(size), std::span{payload}.subspan(offset, size)));
                    offset += size;
                    bytes = bytes.subspan(size);
                    ++datagrams;
                } while (!bytes.empty());
            }
        }
        EXPECT_EQ(datagrams, count);
        co_return {};
    };
    auto write = [&]() -> IoTask<void> {
        auto remote = receiver.localEndpoint().value();
        if (family == AF_INET6) remote = {ilias::IPAddress6::fromV4Mapped(remote.address4()), remote.port()};
        auto sent = co_await sender.send(payload, remote, segment, 2);
        EXPECT_EQ(sent.datagrams, count);
        if (sent.error) co_return Err(sent.error);
        co_return {};
    };
    auto result = co_await ilias::timeout(ilias::whenAll(write(), read()), 3s);
    if (!result) co_return Err(std::make_error_code(std::errc::timed_out));
    ILIAS_CO_TRYV(std::move(std::get<0>(*result)));
    ILIAS_CO_TRYV(std::move(std::get<1>(*result)));
    auto tx = sender.stats();
    auto rx = receiver.stats();
    EXPECT_EQ(tx.txDatagrams, count);
    EXPECT_EQ(rx.rxDatagrams, count);
    EXPECT_EQ(tx.txBytes, payload.size());
    EXPECT_EQ(rx.rxBytes, payload.size());
    EXPECT_EQ(rx.droppedReceives, 0);
    if (!sendBatching) EXPECT_EQ(tx.txSegmentedDatagrams, 0);
    if (tx.txSegmentedDatagrams) EXPECT_LT(tx.txSubmissions, count);
    if (noChecksum) EXPECT_EQ(tx.segmentationFallbacks, 1);
    ::testing::Test::RecordProperty("tx_submissions", std::to_string(tx.txSubmissions));
    ::testing::Test::RecordProperty("tx_segmented_datagrams", std::to_string(tx.txSegmentedDatagrams));
    ::testing::Test::RecordProperty("rx_coalesced_datagrams", std::to_string(rx.rxCoalescedDatagrams));
    ::testing::Test::RecordProperty("receive_coalescing", rx.receiveCoalescing ? "true" : "false");
    co_return {};
}

} // namespace

ILIAS_TEST(UdpBatch, DatagramBoundariesAndShortTail) {
    auto result = co_await transfer(true, true);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(UdpBatch, SegmentedSenderWithSinglePacketReceiver) {
    auto result = co_await transfer(true, false);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(UdpBatch, SinglePacketSenderWithCoalescingReceiver) {
    auto result = co_await transfer(false, true);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(UdpBatch, SinglePacketFallback) {
    auto result = co_await transfer(false, false);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

ILIAS_TEST(UdpBatch, DualStackSegmentedIPv4) {
    auto result = co_await transfer(true, true, AF_INET6);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}

#if defined(__linux__)
ILIAS_TEST(UdpBatch, SegmentationFailureFallsBackToMmsg) {
    // Linux GSO requires checksums; this forces the real fallback path.
    auto result = co_await transfer(true, true, AF_INET, true);
    EXPECT_TRUE(result) << result.error().message();
    co_return;
}
#endif

ILIAS_TEST(UdpBatch, InvalidBatchAndReceiveCancellation) {
    SkBufferPool pool{{{65536, 2}}};
    auto bound = bindUdp(pool, true);
    EXPECT_TRUE(bound);
    if (!bound) co_return;
    auto &sender = *bound;
    std::array<std::byte, 8> bytes;
    auto remote = sender.localEndpoint().value();
    EXPECT_TRUE((co_await sender.send(bytes, remote, 0)).error);
    EXPECT_TRUE((co_await sender.send(bytes, remote, bytes.size() + 1)).error);
    EXPECT_EQ(sender.stats().txSubmissions, 0);
    EXPECT_FALSE(co_await ilias::timeout(sender.receive(), 10ms));
    co_return;
}

#endif
