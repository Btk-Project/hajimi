#pragma once

#include "quic.hpp"
#include <array>
#include <span>
#include <vector>

namespace quic {

// One owner registers the socket with ilias; all I/O stays on that executor.
class UdpBatchSocket {
public:
    static constexpr size_t sendCapacity = 60 * 1024;
    static constexpr size_t maxSegments = 64;
    static constexpr size_t receiveSlots = 16;

    struct Received {
        ilias::Buffer bytes;
        ilias::IPEndpoint remote;
        size_t segmentSize = 0;
        unsigned int ecn = 0;
    };

    struct Sent {
        size_t datagrams = 0; // Only the successfully completed prefix.
        std::error_code error;
    };

    static auto from(ilias::Socket socket, bool batching) -> ilias::IoResult<UdpBatchSocket>;
    auto send(ilias::Buffer data, const ilias::IPEndpoint &remote, size_t segmentSize,
        unsigned int ecn = 0) -> ilias::Task<Sent>;
    auto receive() -> ilias::IoTask<std::span<const Received>>;
    auto localEndpoint() const { return mHandle.fd().localEndpoint<ilias::IPEndpoint>(); }
    auto close() -> void { mHandle.close(); }
    auto stats() const noexcept -> QuicContext::IoStats { return mStats; }

private:
    struct ReceiveSlot {
        std::array<std::byte, 65536> data;
        ilias::IPEndpoint remote;
        alignas(size_t) std::array<std::byte, 128> control;
    };

    ilias::IoHandle<ilias::Socket> mHandle;
    std::vector<ReceiveSlot> mReceive;
    std::array<Received, receiveSlots> mReceived;
    QuicContext::IoStats mStats;
    bool mBatching = false;
};

} // namespace quic
