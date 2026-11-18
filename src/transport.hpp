#pragma once

// Transport layer for Hajimi
#include <ilias/io.hpp>
#include <memory>
#include <utility>
#include <variant>

#include "protocol.hpp"
#include "skbuf.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif // HAJIMI_QUIC

// Import types
using ilias::DynStream;
using ilias::IoTask;
using ilias::Task;
using ilias::Err;

// Forward declaration
class HajimiStream;

/**
 * @brief The Connection of the hajimi (wrapping DynStream or QuicConnection)
 * 
 *  HajimiConnection ->
 *      ControlStream 0
 *      HajimiStream  1 <-> Remote HajimiStream 1
 *      HajimiStream  2 <-> Remote HajimiStream 2
 */
class HajimiConnection {
public:
    struct Status {
        std::size_t bytesReceived = 0;
        std::size_t bytesSent = 0;
    };

    struct Impl;

    HajimiConnection(const HajimiConnection &) = delete; // Move only
    HajimiConnection(HajimiConnection &&) = default;
    ~HajimiConnection();

    // Control (one reader, writes are serialized internally)
    auto readMessage() -> IoTask<ControlMessage>;
    auto writeMessage(ControlMessage msg) -> IoTask<void>;

    // Stream
    auto acceptTunnel() -> IoTask<std::pair<OpenTunnel, HajimiStream>>;
    auto openTunnel(OpenTunnel tunnel) -> IoTask<HajimiStream>;

    // Run once alongside the session, until error or cancellation. Pool must outlive all handles and buffers.
    auto run() -> IoTask<void>;

    // Get the pool associated with this connection
    auto pool() const -> SkBufferPool &;

    // Get the snapshot of the connection status
    auto status() const -> Status;

    // Build the connection for it
    static auto fromClient(SkBufferPool &pool, DynStream stream) -> IoTask<HajimiConnection>;
    static auto fromServer(SkBufferPool &pool, DynStream stream) -> IoTask<HajimiConnection>;

#ifdef HAJIMI_QUIC
    static auto fromClient(SkBufferPool &pool, QuicConnection stream) -> IoTask<HajimiConnection>;
    static auto fromServer(SkBufferPool &pool, QuicConnection stream) -> IoTask<HajimiConnection>;
#endif // HAJIMI_QUIC

private:
    HajimiConnection() = default;
    std::shared_ptr<Impl> d;
friend class HajimiStream;
};

/**
 * @brief The bytes stream of the hajimi, 
 * 
 */
class HajimiStream {
public:
    struct Impl;

    HajimiStream(const HajimiStream &) = delete; // Move only
    HajimiStream(HajimiStream &&) noexcept;
    ~HajimiStream();

    /**
     * @brief Read a buffer from the stream
     * 
     * @return IoTask<SkBuffer> (EOF on empty SkBuffer)
     */
    auto read() -> IoTask<SkBuffer>;

    /**
     * @brief Write a whole buffer to the stream, this function ensure that the buffer is fully written or an error is returned
     * 
     * @param buf The buffer to write (empty on no-op), preferably with `HAJIMI_HEADER_ROOM` headroom
     * @note One reader and one writer may run concurrently. Serialize write() and shutdown().
     *       Cancellation during write() or shutdown() resets this stream.
     * @return IoTask<size_t> The number of bytes written, 0 on EOF
     */
    auto write(SkBuffer buf) -> IoTask<size_t>;

    /**
     * @brief Shutdown the send part after flushing earlier data (idempotent)
     * 
     * @return IoTask<void> 
     */
    auto shutdown() -> IoTask<void>;

    // Abort both directions without waiting for the peer. Also called by the destructor.
    auto reset() noexcept -> void;

    // Get the pool associated with this connection
    auto pool() const -> SkBufferPool &;
private:
    HajimiStream();
    
    std::shared_ptr<HajimiConnection::Impl> mConn;
    std::unique_ptr<Impl>                   d;
friend class HajimiConnection;
};

// Helper functions
inline auto copyBidirectional(HajimiStream &stream, ilias::StreamView other) -> Task<void> {
    auto read = [&](HajimiStream &reader, ilias::StreamView writer) -> IoTask<void> {
        while (true) {
            auto buf = co_await reader.read();
            if (!buf || buf->empty()) {
                break;
            }

            auto n = co_await writer.writeAll(buf->data());
            if (!n || n == 0) {
                break;
            }
        }
        // Shutdown the writer
        ILIAS_CO_TRYV(co_await writer.shutdown());
        co_return {};
    };
    auto writer = [&](HajimiStream &writer, ilias::StreamView reader) -> IoTask<void> {
        while (true) {
            auto buf = writer.pool().allocate(HAJIMI_MAX_DATA_EXCHANGE + HAJIMI_HEADER_ROOM);
            auto space = buf.prepareBack(HAJIMI_MAX_DATA_EXCHANGE);
            auto n = co_await reader.read(space);
            if (!n || n == 0) {
                break;
            }

            buf.commitBack(*n);

            // Write to peer
            auto ok = co_await writer.write(std::move(buf));
            if (!ok) {
                break;
            }
        }
        ILIAS_CO_TRYV(co_await writer.shutdown());
        co_return {};
    };
    auto _ = co_await ilias::whenAll(
        read(stream, other),
        writer(stream, other)
    );
}

// Zero copy version
inline auto copyBidirectional(HajimiStream &stream, HajimiStream &other) -> Task<void> {
    auto read = [&](HajimiStream &reader, HajimiStream &writer) -> IoTask<void> {
        while (true) {
            auto buf = co_await reader.read();
            if (!buf || buf->empty()) {
                break;
            }

            auto n = co_await writer.write(std::move(*buf));
            if (!n || n == 0) {
                break;
            }
        }
        // Shutdown the writer
        ILIAS_CO_TRYV(co_await writer.shutdown());
        co_return {};
    };
    auto writer = [&](HajimiStream &writer, HajimiStream &reader) -> IoTask<void> {
        while (true) {
            auto buf = co_await reader.read();
            if (!buf || buf->empty()) {
                break;
            }

            auto n = co_await writer.write(std::move(*buf));
            if (!n || n == 0) {
                break;
            }
        }
        ILIAS_CO_TRYV(co_await writer.shutdown());
        co_return {};
    };
    auto _ = co_await ilias::whenAll(
        read(stream, other),
        writer(stream, other)
    );
}