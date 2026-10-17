#pragma once

#include <ilias/io.hpp>
#include <ilias/net.hpp>
#include <chrono>
#include <array>
#include <memory>
#include <optional>
#include <string>

namespace quic {
struct ContextState;
struct ConnectionState;
struct StreamState;
}

class QuicConnection;
class QuicStream;

enum class QuicError {
    Tls = 1,
    AlpnMismatch,
    StreamReset,
    ConnectionClosed,
};

auto make_error_code(QuicError error) noexcept -> std::error_code;

/**
 * @brief A UDP endpoint and its QUIC connections.
 * @note Use all handles on the same ilias executor. Run run() concurrently
 *       with application tasks, and await its completion before leaving scope.
 */
class QuicContext {
public:
    enum class Role { Client, Server };

    struct Config {
        Role role = Role::Client;
        std::string alpn = "hajimi/1";
        std::string certFile;
        std::string keyFile;
        std::string caFile;
        bool verifyPeer = true; // Client-side certificate verification.
        std::chrono::milliseconds handshakeTimeout{10000};
        std::chrono::milliseconds idleTimeout{30000};
        size_t maxConnections = 128;
        size_t maxStreams = 128;
        size_t streamWindow = 256 * 1024;
        size_t connectionWindow = 4 * 1024 * 1024;
        size_t sendBufferLimit = 256 * 1024; // Per stream, including unacknowledged data.
        size_t connectionSendBufferLimit = 4 * 1024 * 1024;
        std::optional<std::array<unsigned char, 32>> psk;
        std::string pskIdentity = "hajimi/shared/v1";
    };

    QuicContext(const QuicContext &) = delete;
    QuicContext(QuicContext &&) noexcept;
    auto operator =(QuicContext &&) noexcept -> QuicContext &;
    ~QuicContext();

    static auto bind(ilias::IPEndpoint local, Config config) -> ilias::IoTask<QuicContext>;

    auto run() -> ilias::IoTask<void>;
    auto stop() noexcept -> void;
    auto connect(ilias::IPEndpoint remote, std::string serverName) -> ilias::IoTask<QuicConnection>;
    auto accept() -> ilias::IoTask<QuicConnection>;
    auto localEndpoint() const -> ilias::IoResult<ilias::IPEndpoint>;
private:
    explicit QuicContext(std::shared_ptr<quic::ContextState> state);
    std::shared_ptr<quic::ContextState> mState;
};

/**
 * @brief A completed QUIC handshake and its bidirectional streams.
 * @note Keep the connection alive while using its streams. Destruction
 *       aborts the connection; close() sends an application CONNECTION_CLOSE.
 */
class QuicConnection {
public:
    QuicConnection(const QuicConnection &) = delete;
    QuicConnection(QuicConnection &&) noexcept;
    auto operator =(QuicConnection &&) noexcept -> QuicConnection &;
    ~QuicConnection();

    // The peer observes a new stream after the first write or FIN.
    auto openStream() -> ilias::IoTask<QuicStream>;
    auto acceptStream() -> ilias::IoTask<QuicStream>;
    auto close(uint64_t appError = 0) -> ilias::IoTask<void>;
    auto remoteEndpoint() const -> ilias::IPEndpoint;
    auto appError() const noexcept -> std::optional<uint64_t>;
private:
    explicit QuicConnection(std::shared_ptr<quic::ConnectionState> state);
    std::shared_ptr<quic::ConnectionState> mState;
friend class QuicContext;
};

/** @brief An ordered byte stream, compatible with ilias::DynStream. */
class QuicStream : public ilias::StreamExt<QuicStream> {
public:
    QuicStream(const QuicStream &) = delete;
    QuicStream(QuicStream &&) noexcept;
    auto operator =(QuicStream &&) noexcept -> QuicStream &;
    ~QuicStream();

    auto read(ilias::MutableBuffer buffer) -> ilias::IoTask<size_t>;
    // Copy accepted bytes into a bounded queue; caller memory is reusable on return.
    auto write(ilias::Buffer buffer) -> ilias::IoTask<size_t>;
    // Wait for previously accepted bytes to be sent, without waiting for ACKs.
    auto flush() -> ilias::IoTask<void>;
    // Send FIN after previously accepted bytes; the read direction stays open.
    auto shutdown() -> ilias::IoTask<void>;
    auto reset(uint64_t appError = 0) noexcept -> void;
    auto id() const noexcept -> int64_t;
    auto appError() const noexcept -> std::optional<uint64_t>;
private:
    explicit QuicStream(std::shared_ptr<quic::StreamState> state);
    std::shared_ptr<quic::StreamState> mState;
friend class QuicConnection;
};

static_assert(ilias::Stream<QuicStream>);
