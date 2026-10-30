#pragma once

#include <ilias/io.hpp>
#include <ilias/net.hpp>
#include <ilias/task.hpp>
#include <array>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>

// Import types
using ilias::Task;
using ilias::IoTask;
using ilias::IoResult;
using ilias::IPEndpoint;

namespace quic {
struct ContextState;
struct ConnectionState;
struct StreamState;
} // namespace quic

class QuicConnection;
class QuicStream;

/** @brief Wrapper errors; transport errors also use the QUIC error category. */
enum class QuicError {
    Tls = 1,          ///< TLS handshake or peer authentication failed.
    AlpnMismatch,     ///< The negotiated application protocol does not match.
    StreamReset,      ///< A stream direction was reset locally or by its peer.
    ConnectionClosed, ///< The connection was closed locally or by its peer.
};

/** @brief Convert a wrapper error to the QUIC error category. */
auto make_error_code(QuicError error) noexcept -> std::error_code;

/**
 * @brief Own one UDP endpoint and drive its QUIC v1 connections.
 * @note The context, connections and streams must use the same ilias executor;
 *       these handles are not thread-safe. Keep handles alive during their operations.
 * @note Run run() concurrently with application tasks. Stop and await it before
 *       leaving its execution scope. A stopped context cannot be restarted.
 * @note Cancellation propagates through ilias and removes pending waiters.
 */
class QuicContext {
public:
    /** @brief Select whether this endpoint initiates or accepts connections. */
    enum class Role {
        Client, ///< Initiate connections with connect().
        Server, ///< Accept connections with accept().
    };

    /**
     * @brief Cumulative UDP I/O counters and currently enabled capabilities.
     * @note Submissions count API operations, not measured system calls.
     *       Datagram counts refer to the original UDP boundaries, before coalescing.
     */
    struct IoStats {
        uint64_t txDatagrams = 0;          ///< Successfully sent datagrams.
        uint64_t txBytes = 0;              ///< Successfully sent UDP payload bytes.
        uint64_t txSubmissions = 0;        ///< Send API submissions, including retries.
        uint64_t txSegmentedDatagrams = 0; ///< Datagrams sent through UDP segmentation.

        uint64_t rxDatagrams = 0;          ///< Accepted datagrams after splitting coalesced receives.
        uint64_t rxBytes = 0;              ///< Accepted UDP payload bytes.
        uint64_t rxSubmissions = 0;        ///< Receive API submissions, including retries.
        uint64_t rxCoalescedDatagrams = 0; ///< Datagrams delivered in coalesced receives.

        uint64_t droppedReceives = 0;       ///< Truncated receives or invalid ancillary metadata.
        uint64_t segmentationFallbacks = 0; ///< Segmentation failures that enabled datagram fallback.

        size_t maxTxBatch = 0; ///< Largest completed send batch, in datagrams.
        size_t maxRxBatch = 0; ///< Largest accepted receive batch, in datagrams.

        bool sendSegmentation = false;  ///< Segmentation is enabled and has not been rejected.
        bool receiveCoalescing = false; ///< Coalescing is enabled; actual use depends on the path.
    };

    /**
     * @brief Transport, authentication and buffer settings fixed at bind().
     * @pre Timeouts, connection/stream limits and buffer sizes are positive.
     *      Protocol values must fit their QUIC bounds; bind() validates the settings.
     */
    struct Config {
        /// @brief Select outgoing connections or incoming connections.
        Role role = Role::Client;

        /// @brief Application protocol; both peers must use the same 1 to 255 bytes.
        std::string alpn = "hajimi/1";

        /// @brief PEM certificate chain; required for servers in certificate mode.
        std::string certFile;

        /// @brief PEM private key matching certFile; required for certificate servers.
        std::string keyFile;

        /// @brief Client trust file; an empty path uses the system trust sources.
        std::string caFile;

        /// @brief Verify the server certificate and name; ignored for servers and PSK.
        bool verifyPeer = true;

        /// @brief Maximum duration allowed for each TLS handshake.
        std::chrono::milliseconds handshakeTimeout{10000};

        /// @brief Local idle timeout; the effective timeout also depends on the peer.
        std::chrono::milliseconds idleTimeout{30000};

        /// @brief Maximum retained connections, including handshakes and closing peers.
        size_t maxConnections = 128;

        /// @brief Initial number of bidirectional streams the peer may open.
        size_t maxStreams = 128;

        /// @brief Initial receive credit per stream, in bytes.
        size_t streamWindow = 256 * 1024;

        /// @brief Initial receive credit shared by all connection streams, in bytes.
        size_t connectionWindow = 4 * 1024 * 1024;

        /// @brief Queued byte limit per stream, including unacknowledged data.
        size_t sendBufferLimit = 256 * 1024;

        /// @brief Queued byte limit per connection, including unacknowledged data.
        size_t connectionSendBufferLimit = 4 * 1024 * 1024;

        /// @brief Optional 32-byte external PSK shared by both peers.
        /// @note PSK mode excludes certFile, keyFile and caFile and requires ephemeral
        ///       key exchange. It never falls back to certificate authentication.
        std::optional<std::array<unsigned char, 32>> psk;

        /// @brief PSK identity; both peers must use the same 1 to 128 bytes.
        std::string pskIdentity = "hajimi/shared/v1";

        /// @brief Enable UDP batching with fallback when segmentation is unsupported.
        bool udpBatching = true;

        /// @brief Non-null upstream resource for stream and packet buffer pools.
        /// @note Must outlive the context and all its connections and streams.
        ///       It does not control TLS, ngtcp2 or other container allocations.
        std::pmr::memory_resource *memoryResource = std::pmr::get_default_resource();
    };

    /// @brief Context ownership cannot be copied.
    QuicContext(const QuicContext &) = delete;

    /// @brief Transfer the handle, leaving other empty.
    QuicContext(QuicContext &&other) noexcept;

    /// @brief Stop the current context and transfer other's handle, leaving it empty.
    auto operator =(QuicContext &&other) noexcept -> QuicContext &;

    /// @brief Request stop() without waiting for run() to finish.
    ~QuicContext();

    /**
     * @brief Bind a UDP endpoint and initialize its TLS configuration.
     * @param local IPv4 or IPv6 address; port zero selects an ephemeral port.
     * @param config Settings retained by the context for its entire lifetime.
     * @return A bound context, or a configuration, TLS or socket error.
     * @note IPv6 sockets allow IPv4-mapped peers. Call run() to start processing I/O.
     */
    static auto bind(IPEndpoint local, Config config) -> IoTask<QuicContext>;

    /**
     * @brief Drive packet I/O, timers and connection handshakes until stopped.
     * @pre This context has not been stopped and no other run() is active.
     * @return Success on stop(), or an I/O error from a worker.
     * @note Stop and cancellation attempt CONNECTION_CLOSE for all active peers
     *       before closing UDP. Cleanup has a total 250 ms budget and ignores
     *       cancellation; it does not wait for peer acknowledgements.
     */
    auto run() -> IoTask<void>;

    /**
     * @brief Request permanent shutdown and wake pending connection/stream operations.
     * @note Idempotent; does not wait for cleanup. Await the active run() to finish it.
     */
    auto stop() noexcept -> void;

    /**
     * @brief Resolve an address and return the first completed, authenticated handshake.
     * @param address DNS name or numeric IP with a nonzero port; bracket IPv6 literals.
     * @pre The context has Role::Client and run() is active concurrently.
     * @note IPv4/IPv6 candidates race with a 250 ms stagger; an explicit failure
     *       starts the next attempt immediately. An IPv4 context resolves IPv4 only.
     *       The original host is used for SNI or IP SAN verification.
     * @note Losing attempts are canceled and joined before returning. Cancellation
     *       also requests closure of any unfinished connections.
     */
    auto connect(std::string address) -> IoTask<QuicConnection>;

    /**
     * @brief Connect to a specific peer and wait for its authenticated handshake.
     * @param remote Peer address compatible with the bound socket's address family.
     * @param serverName DNS name for SNI/verification, or an IP for SAN verification.
     * @pre The context has Role::Client and run() is active concurrently.
     * @note serverName may be empty in PSK mode or when certificate verification
     *       is disabled. Cancellation requests closure of the unfinished connection.
     */
    auto connect(IPEndpoint remote, std::string serverName) -> IoTask<QuicConnection>;

    /**
     * @brief Wait for an incoming connection whose TLS handshake has completed.
     * @pre The context has Role::Server and run() is active concurrently.
     * @note Cancellation removes only this waiter; stop() wakes it with an error.
     */
    auto accept() -> IoTask<QuicConnection>;

    /**
     * @brief Return the bound endpoint, including the actual port chosen by bind().
     * @return The endpoint, or bad_file_descriptor for an empty handle.
     */
    auto localEndpoint() const -> IoResult<IPEndpoint>;

    /// @brief Return a snapshot of UDP statistics, or zeroed statistics for an empty handle.
    auto ioStats() const noexcept -> IoStats;

private:
    explicit QuicContext(std::shared_ptr<quic::ContextState> state);
    std::shared_ptr<quic::ContextState> mState;
};

/**
 * @brief Own a completed QUIC handshake and its bidirectional byte streams.
 * @note Use this handle on its context's executor and keep it alive while using
 *       its streams. The context's run() must remain active to make I/O progress.
 * @note Destruction requests CONNECTION_CLOSE with application error 1.
 */
class QuicConnection {
public:
    /// @brief Connection ownership cannot be copied.
    QuicConnection(const QuicConnection &) = delete;

    /// @brief Transfer the handle, leaving other empty.
    QuicConnection(QuicConnection &&other) noexcept;

    /// @brief Abandon the current connection and transfer other's handle, leaving it empty.
    auto operator =(QuicConnection &&other) noexcept -> QuicConnection &;

    /// @brief Request connection closure without waiting for transmission.
    ~QuicConnection();

    /**
     * @brief Open a bidirectional stream, waiting for peer stream credit if needed.
     * @note The peer observes it only after its first data or FIN. Write or shut
     *       down the stream before waiting for the peer to accept it.
     */
    auto openStream() -> IoTask<QuicStream>;

    /**
     * @brief Wait for a bidirectional stream opened by the peer.
     * @note Cancellation removes the waiter without closing the connection.
     */
    auto acceptStream() -> IoTask<QuicStream>;

    /**
     * @brief Request application closure and wait for CONNECTION_CLOSE transmission.
     * @param appError QUIC application error in [0, 2^62 - 1]; zero means no error.
     * @note Pending stream operations fail immediately. This does not flush stream
     *       data or wait for a peer acknowledgement; cancellation does not undo closure.
     */
    auto close(uint64_t appError = 0) -> IoTask<void>;

    /// @brief Return the fixed peer address, or an empty endpoint for an empty handle.
    auto remoteEndpoint() const -> IPEndpoint;

    /// @brief Return the recorded local/remote application close code, if any.
    auto appError() const noexcept -> std::optional<uint64_t>;

private:
    explicit QuicConnection(std::shared_ptr<quic::ConnectionState> state);
    std::shared_ptr<quic::ConnectionState> mState;
    friend class QuicContext;
};

/**
 * @brief Own an ordered, bidirectional byte stream compatible with ilias::DynStream.
 * @note Use the connection's executor and keep the connection alive during I/O.
 *       Reads are serialized; write(), flush() and shutdown() share a separate lock,
 *       allowing one read and one write operation to make progress concurrently.
 * @note Cancellation removes pending waits without retracting bytes already accepted.
 *       Destruction resets an unfinished stream with application error 1.
 */
class QuicStream : public ilias::StreamExt<QuicStream> {
public:
    /// @brief Stream ownership cannot be copied.
    QuicStream(const QuicStream &) = delete;

    /// @brief Transfer the handle, leaving other empty.
    QuicStream(QuicStream &&other) noexcept;

    /// @brief Reset the current stream and transfer other's handle, leaving it empty.
    auto operator =(QuicStream &&other) noexcept -> QuicStream &;

    /// @brief Reset an unfinished stream without waiting for transmission.
    ~QuicStream();

    /**
     * @brief Read available bytes, waiting for data, FIN or an error when necessary.
     * @param buffer Destination memory; keep it valid until the operation finishes.
     * @return Bytes copied; zero means EOF for a nonempty buffer. An empty buffer
     *         returns zero immediately. A reset reports QuicError::StreamReset.
     */
    auto read(ilias::MutableBuffer buffer) -> IoTask<size_t>;

    /**
     * @brief Copy bytes into a bounded send queue, waiting for space when necessary.
     * @param buffer Source memory; keep it valid until the operation finishes.
     * @return Bytes accepted, possibly fewer than requested; zero for an empty buffer.
     * @note Caller memory is reusable on return. Use writeAll() for the entire buffer;
     *       accepted bytes remain queued until acknowledged or the stream is closed.
     */
    auto write(ilias::Buffer buffer) -> IoTask<size_t>;

    /**
     * @brief Wait until all accepted bytes complete their first UDP transmission.
     * @note Does not wait for an ACK or application receipt, and does not send FIN.
     */
    auto flush() -> IoTask<void>;

    /**
     * @brief Send FIN after all accepted bytes and wait for its UDP transmission.
     * @note Further writes fail; reads remain available. Cancellation does not undo
     *       an already requested FIN. Repeated calls wait for the same FIN.
     */
    auto shutdown() -> IoTask<void>;

    /**
     * @brief Abort both stream directions with RESET_STREAM and STOP_SENDING.
     * @param appError Application error, clamped to the QUIC range [0, 2^62 - 1].
     * @note Does not wait for transmission. Repeated resets preserve the first code;
     *       a finished stream only discards any unread data.
     */
    auto reset(uint64_t appError = 0) noexcept -> void;

    /// @brief Return the QUIC stream ID, or -1 for an empty handle.
    auto id() const noexcept -> int64_t;

    /// @brief Return the recorded local/remote application reset code, if any.
    auto appError() const noexcept -> std::optional<uint64_t>;

private:
    explicit QuicStream(std::shared_ptr<quic::StreamState> state);
    std::shared_ptr<quic::StreamState> mState;
    friend class QuicConnection;
};

static_assert(ilias::Stream<QuicStream>);
