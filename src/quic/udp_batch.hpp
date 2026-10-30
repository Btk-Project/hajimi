#pragma once

#include "quic.hpp"
#include "../skbuf.hpp"
#include <ilias/net/cmsg.hpp>
#include <array>
#include <span>
#include <vector>

namespace quic {

/**
 * @brief A UDP socket with reusable pooled receive buffers and optional batching.
 * @note The pool passed to from() must outlive the socket and its pending I/O.
 *       One owner registers the socket with ilias; all I/O stays on that executor.
 * @note At most one send() and one receive() may be active; they may run concurrently.
 */
class UdpBatchSocket {
public:
    static constexpr size_t sendCapacity = 60 * 1024;
    static constexpr size_t receiveCapacity = 64 * 1024;
    static constexpr size_t maxSegments = 64;
    static constexpr size_t receiveSlots = 16;

    struct Received {
        ilias::Buffer bytes;
        IPEndpoint remote;
        size_t segmentSize = 0;
        unsigned int ecn = 0;
    };

    struct Sent {
        size_t datagrams = 0; // Only the successfully completed prefix.
        std::error_code error;
    };

    /// @brief Register a bound socket and allocate reusable receive slots from pool.
    static auto from(ilias::Socket socket, SkBufferPool &pool, bool batching) -> IoResult<UdpBatchSocket>;

    auto send(ilias::Buffer data, const IPEndpoint &remote, size_t segmentSize,
        unsigned int ecn = 0) -> Task<Sent>;

    /// @brief Receive borrowed views valid until the next receive() or socket destruction.
    auto receive() -> IoTask<std::span<const Received>>;

    auto localEndpoint() const { return mSocket.localEndpoint(); }
    auto close() -> void { mSocket.close(); }
    auto stats() const noexcept -> QuicContext::IoStats { return mStats; }

private:
    struct ReceiveSlot {
        SkBuffer bytes;
        IPEndpoint remote;
        ilias::CmsgSpace control;
    };

    ilias::UdpSocket mSocket;
    ilias::CmsgSpace mSendControl;
    std::vector<ReceiveSlot> mReceive;
    std::array<Received, receiveSlots> mReceived;
    QuicContext::IoStats mStats;
    bool mBatching = false;
};

} // namespace quic
