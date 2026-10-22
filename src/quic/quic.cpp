#include "../utils/scope_exit.hpp"
#include "../skbuf.hpp"
#include "crypto.hpp"
#include "quic.hpp"
#include "udp_batch.hpp"

#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <deque>
#include <map>
#include <print>
#include <stop_token>
#include <utility>

using ilias::Err;
using ilias::IoResult;
using ilias::IoTask;
using ilias::IPEndpoint;

namespace {

class QuicErrorCategory final : public std::error_category {
public:
    auto name() const noexcept -> const char * override { return "quic"; }
    auto message(int value) const -> std::string override {
        if (value < 0) {
            return ngtcp2_strerror(value);
        }
        switch (static_cast<QuicError>(value)) {
            case QuicError::Tls: return "QUIC TLS handshake failed";
            case QuicError::AlpnMismatch: return "QUIC ALPN mismatch";
            case QuicError::StreamReset: return "QUIC stream reset";
            case QuicError::ConnectionClosed: return "QUIC connection closed";
        }
        return "Unknown QUIC error";
    }
};

const QuicErrorCategory quicErrorCategory;
constexpr size_t cidLength = 16;
constexpr size_t packetSize = 1452;
constexpr size_t streamChunkSize = 16 * 1024;
constexpr uint64_t abandonedError = 1;

auto timestamp() -> ngtcp2_tstamp {
    return static_cast<ngtcp2_tstamp>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

auto transportError(int value) -> std::error_code {
    if (value == NGTCP2_ERR_IDLE_CLOSE || value == NGTCP2_ERR_HANDSHAKE_TIMEOUT) {
        return std::make_error_code(std::errc::timed_out);
    }
    if (value == NGTCP2_ERR_CRYPTO) {
        return make_error_code(QuicError::Tls);
    }
    return {value, quicErrorCategory};
}

auto cidKey(const uint8_t *data, size_t size) -> std::string {
    return {reinterpret_cast<const char *>(data), size};
}

auto randomCid(ngtcp2_cid &cid) -> bool {
    cid.datalen = cidLength;
    return RAND_bytes(cid.data, static_cast<int>(cid.datalen)) == 1;
}

auto address(IPEndpoint &endpoint) -> ngtcp2_addr {
    return {static_cast<ngtcp2_sockaddr *>(endpoint.data()), endpoint.length()};
}

// Never propagate C++ exceptions across a C callback boundary.
template <typename Fn>
auto callback(Fn fn) noexcept -> int {
    try {
        return fn();
    }
    catch (...) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
}

auto waitChanged(ilias::Event &event, std::stop_token token) -> IoTask<void> {
    event.clear();
    if (!token.stop_requested()) {
        co_await event.wait();
    }
    if (token.stop_requested()) {
        co_return Err(std::make_error_code(std::errc::operation_canceled));
    }
    co_return {};
}

} // namespace

auto make_error_code(QuicError error) noexcept -> std::error_code {
    return {static_cast<int>(error), quicErrorCategory};
}

namespace quic {

struct StreamState {
    struct Chunk {
        SkBuffer bytes;
        uint64_t offset = 0;
    };

    // Streams can outlive the context and still retain received data.
    std::shared_ptr<SkBufferPool> pool;
    std::weak_ptr<ConnectionState> connection;
    int64_t id = -1;
    ilias::Event changed;
    ilias::Mutex reader;
    ilias::Mutex writer; // Also serializes flush and FIN.

    std::deque<Chunk> rx;
    std::deque<Chunk> tx;
    size_t buffered = 0;
    uint64_t received = 0;
    uint64_t consumed = 0;
    uint64_t credit = 0;
    uint64_t accepted = 0;
    uint64_t submitted = 0;
    uint64_t sent = 0;
    bool readFin = false;
    bool writeFin = false;
    bool finSubmitted = false;
    bool finSent = false;
    bool closed = false;
    bool resetApplied = false;
    std::optional<uint64_t> resetRequested;
    std::optional<uint64_t> appError;
    std::error_code readError;
    std::error_code writeError;

    auto discardReceived(ConnectionState &conn, uint64_t finalSize) -> void;
    auto reset(uint64_t error) noexcept -> void;
};

struct PacketBatch {
    struct Receipt {
        std::shared_ptr<StreamState> stream;
        uint64_t offset = 0;
        bool fin = false;
    };
    std::array<Receipt, UdpBatchSocket::maxSegments> receipts;
    size_t count = 0;
    bool padding = false;
};

struct ConnectionState : std::enable_shared_from_this<ConnectionState> {
    std::shared_ptr<SkBufferPool> pool;
    std::weak_ptr<ContextState> context;
    IPEndpoint local;
    IPEndpoint remote;
    ngtcp2_conn *conn = nullptr;
    std::unique_ptr<TlsSession> tls;
    ilias::Event changed;
    std::map<int64_t, std::shared_ptr<StreamState>> streams;
    std::deque<std::shared_ptr<StreamState>> incoming;
    std::vector<std::shared_ptr<StreamState>> closedStreams;
    size_t buffered = 0;
    size_t streamCredit = 0;
    uint64_t credit = 0;
    int64_t nextStream = -1;
    PacketBatch *writingBatch = nullptr; // Only set during synchronous packet generation.
    bool server = false;
    bool handshake = false;
    bool queued = false;
    bool closeRequested = false;
    bool closeSent = false;
    bool draining = false;
    ngtcp2_tstamp retireAt = 0;
    ngtcp2_ccerr closeReason{};
    SkBuffer closePacket;
    std::optional<uint64_t> appError;
    std::error_code error;

    ~ConnectionState() {
        tls.reset(); // Release TLS references before deleting ngtcp2 buffers.
        ngtcp2_conn_del(conn);
    }

    auto ensureStream(int64_t id) -> std::shared_ptr<StreamState>;
    auto notify() -> void;
    auto fail(std::error_code value) -> void;
    auto handleError(int value) -> void;
    auto close(uint64_t value) noexcept -> void;
    auto wake() noexcept -> void;
};

struct ContextState {
    explicit ContextState(QuicContext::Config value) : config(std::move(value)) {}

    QuicContext::Config config;
    std::shared_ptr<SkBufferPool> pool = std::make_shared<SkBufferPool>(
        std::initializer_list<SkBufferPool::ClassInfo>{{256, 128}, {2048, 32}, {16384, 8}, {65536, 2}},
        config.memoryResource);
    UdpBatchSocket socket;
    IPEndpoint local;
    TlsContext tls;
    ilias::Event wake;
    ilias::Event changed;
    ilias::Event timerChanged;
    ngtcp2_tstamp deadline = UINT64_MAX;
    ngtcp2_tstamp armedDeadline = 0;
    std::vector<std::shared_ptr<ConnectionState>> connections;
    std::deque<std::shared_ptr<ConnectionState>> incoming;
    std::map<std::string, std::weak_ptr<ConnectionState>> routes;
    bool running = false;
    bool stopped = false;
    std::error_code error;

    auto stop(std::error_code value) noexcept -> void {
        if (stopped) {
            return;
        }
        stopped = true;
        error = value;
        for (auto &connection : connections) {
            if (!connection->error) {
                connection->closeRequested = true;
                connection->appError = 0;
                ngtcp2_ccerr_set_application_error(&connection->closeReason, 0, nullptr, 0);
            }
            connection->fail(value);
            connection->notify();
        }
        changed.set();
        wake.set();
    }

    auto notify() -> void {
        for (auto &connection : connections) {
            if (connection->server && connection->handshake && !connection->queued && !connection->error) {
                incoming.push_back(connection);
                connection->queued = true;
                changed.set();
            }
            connection->notify();
        }
        std::erase_if(incoming, [](auto &connection) { return bool(connection->error); });
    }

    auto removeExpired(ngtcp2_tstamp now) -> void {
        std::erase_if(connections, [now](auto &connection) {
            return connection->retireAt != 0 && connection->retireAt <= now;
        });
        std::erase_if(routes, [now](auto &entry) {
            auto connection = entry.second.lock();
            return !connection || (connection->retireAt != 0 && connection->retireAt <= now);
        });
    }
};

auto ConnectionState::wake() noexcept -> void {
    if (auto owner = context.lock()) {
        owner->wake.set();
    }
}

auto StreamState::discardReceived(ConnectionState &conn, uint64_t finalSize) -> void {
    conn.credit += finalSize - consumed;
    consumed = finalSize;
    rx.clear();
}

auto StreamState::reset(uint64_t error) noexcept -> void {
    if (closed || (readFin && finSent)) {
        if (auto owner = connection.lock()) {
            discardReceived(*owner, received);
            owner->wake();
        }
        return;
    }
    if (resetRequested) {
        return;
    }
    resetRequested = error;
    appError = error;
    readError = make_error_code(QuicError::StreamReset);
    writeError = readError;
    if (auto owner = connection.lock()) {
        discardReceived(*owner, received);
        owner->wake();
    }
    changed.set();
}

auto ConnectionState::ensureStream(int64_t id) -> std::shared_ptr<StreamState> {
    if (auto it = streams.find(id); it != streams.end()) {
        return it->second;
    }
    auto stream = std::make_shared<StreamState>();
    stream->pool = pool;
    stream->connection = shared_from_this();
    stream->id = id;
    streams.emplace(id, stream);
    if ((id & 1) != static_cast<int>(server)) {
        incoming.push_back(stream);
    }
    return stream;
}

auto ConnectionState::notify() -> void {
    changed.set();
    for (auto &[id, stream] : streams) {
        stream->changed.set();
    }
    for (auto &stream : closedStreams) {
        stream->changed.set();
    }
    closedStreams.clear();
    std::erase_if(incoming, [](auto &stream) { return stream->closed && bool(stream->readError); });
}

auto ConnectionState::fail(std::error_code value) -> void {
    if (error) {
        return;
    }
    error = value;
    for (auto &[id, stream] : streams) {
        stream->readError = value;
        stream->writeError = value;
    }
}

auto ConnectionState::close(uint64_t value) noexcept -> void {
    if (error) {
        return;
    }
    appError = value;
    closeRequested = true;
    ngtcp2_ccerr_set_application_error(&closeReason, value, nullptr, 0);
    fail(make_error_code(QuicError::ConnectionClosed));
    notify();
    wake();
}

auto ConnectionState::handleError(int value) -> void {
    if (value == 0) {
        return;
    }
    if (value == NGTCP2_ERR_DRAINING) {
        draining = true;
        auto reason = ngtcp2_conn_get_ccerr2(conn);
        if (reason->type == NGTCP2_CCERR_TYPE_APPLICATION) {
            appError = reason->error_code;
        }
        auto tlsError = reason->type == NGTCP2_CCERR_TYPE_TRANSPORT &&
            reason->error_code >= NGTCP2_CRYPTO_ERROR && reason->error_code < NGTCP2_CRYPTO_ERROR + 256;
        fail(make_error_code(tlsError ? QuicError::Tls : QuicError::ConnectionClosed));
    }
    else {
        if (value == NGTCP2_ERR_CRYPTO && tls && tls->ssl) {
            auto result = SSL_get_verify_result(tls->ssl);
            if (result != X509_V_OK) {
                std::println(stderr, "[QUIC] Certificate verification failed: {}",
                    X509_verify_cert_error_string(result));
            }
        }
        fail(transportError(value));
        if (value != NGTCP2_ERR_IDLE_CLOSE && value != NGTCP2_ERR_DROP_CONN &&
            value != NGTCP2_ERR_RETRY) {
            closeRequested = true;
            if (value == NGTCP2_ERR_CRYPTO) {
                ngtcp2_ccerr_set_tls_alert(&closeReason, ngtcp2_conn_get_tls_alert2(conn), nullptr, 0);
            }
            else {
                ngtcp2_ccerr_set_liberr(&closeReason, value, nullptr, 0);
            }
        }
    }
    retireAt = timestamp() + 3 * ngtcp2_conn_get_pto2(conn);
}

static auto callbacks() -> ngtcp2_callbacks {
    ngtcp2_callbacks cb{};
    cb.client_initial = ngtcp2_crypto_client_initial_cb;
    cb.recv_client_initial = ngtcp2_crypto_recv_client_initial_cb;
    cb.recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb;
    cb.encrypt = ngtcp2_crypto_encrypt_cb;
    cb.decrypt = ngtcp2_crypto_decrypt_cb;
    cb.hp_mask = ngtcp2_crypto_hp_mask_cb;
    cb.recv_retry = ngtcp2_crypto_recv_retry_cb;
    cb.update_key = ngtcp2_crypto_update_key_cb;
    cb.delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb;
    cb.delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb;
    cb.version_negotiation = ngtcp2_crypto_version_negotiation_cb;
    cb.get_path_challenge_data2 = ngtcp2_crypto_get_path_challenge_data2_cb;
    cb.rand = [](uint8_t *dest, size_t size, const ngtcp2_rand_ctx *) {
        if (RAND_bytes(dest, static_cast<int>(size)) != 1) {
            std::terminate();
        }
    };
    cb.get_new_connection_id2 = [](ngtcp2_conn *, ngtcp2_cid *cid,
        ngtcp2_stateless_reset_token *token, size_t size, void *data) -> int {
        return callback([&]() {
            auto &self = *static_cast<ConnectionState *>(data);
            cid->datalen = size;
            if (RAND_bytes(cid->data, static_cast<int>(size)) != 1 ||
                RAND_bytes(token->data, sizeof(token->data)) != 1) {
                return NGTCP2_ERR_CALLBACK_FAILURE;
            }
            if (auto owner = self.context.lock()) {
                owner->routes.emplace(cidKey(cid->data, size), self.shared_from_this());
            }
            return 0;
        });
    };
    cb.remove_connection_id = [](ngtcp2_conn *, const ngtcp2_cid *cid, void *data) -> int {
        return callback([&]() {
            if (auto owner = static_cast<ConnectionState *>(data)->context.lock()) {
                owner->routes.erase(cidKey(cid->data, cid->datalen));
            }
            return 0;
        });
    };
    cb.handshake_completed = [](ngtcp2_conn *, void *data) -> int {
        auto &self = *static_cast<ConnectionState *>(data);
        auto owner = self.context.lock();
        const unsigned char *alpn = nullptr;
        unsigned int size = 0;
        SSL_get0_alpn_selected(self.tls->ssl, &alpn, &size);
        if (!owner || size != owner->config.alpn.size() ||
            std::memcmp(alpn, owner->config.alpn.data(), size) != 0) {
            self.fail(make_error_code(QuicError::AlpnMismatch));
            return NGTCP2_ERR_CALLBACK_FAILURE;
        }
        if (owner->config.psk && (SSL_session_reused(self.tls->ssl) != 1 ||
            SSL_get_negotiated_group(self.tls->ssl) == 0)) {
            self.fail(make_error_code(QuicError::Tls));
            return NGTCP2_ERR_CALLBACK_FAILURE;
        }
        self.handshake = true;
        return 0;
    };
    cb.stream_open = [](ngtcp2_conn *, int64_t id, void *data) -> int {
        return callback([&]() {
            static_cast<ConnectionState *>(data)->ensureStream(id);
            return 0;
        });
    };
    cb.recv_stream_data = [](ngtcp2_conn *, uint32_t flags, int64_t id, uint64_t offset,
        const uint8_t *bytes, size_t size, void *data, void *) -> int {
        return callback([&]() {
            auto stream = static_cast<ConnectionState *>(data)->ensureStream(id);
            if (offset != stream->received) {
                return NGTCP2_ERR_CALLBACK_FAILURE;
            }
            if (size) {
                auto buffer = stream->pool->allocate(size);
                buffer.append(std::as_bytes(std::span{bytes, size}));
                stream->rx.push_back({std::move(buffer), offset});
                stream->received += size;
            }
            stream->readFin |= (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0;
            return 0;
        });
    };
    cb.acked_stream_data_offset = [](ngtcp2_conn *, int64_t id, uint64_t offset,
        uint64_t size, void *data, void *) -> int {
        auto &self = *static_cast<ConnectionState *>(data);
        auto it = self.streams.find(id);
        if (it == self.streams.end()) {
            return 0;
        }
        auto &stream = *it->second;
        while (!stream.tx.empty() && stream.tx.front().offset + stream.tx.front().bytes.size() <= offset + size) {
            auto count = stream.tx.front().bytes.size();
            stream.buffered -= count;
            self.buffered -= count;
            stream.tx.pop_front();
        }
        return 0;
    };
    cb.stream_reset = [](ngtcp2_conn *, int64_t id, uint64_t,
        uint64_t error, void *data, void *) -> int {
        return callback([&]() {
            auto &self = *static_cast<ConnectionState *>(data);
            auto stream = self.ensureStream(id);
            stream->appError = error;
            stream->readError = make_error_code(QuicError::StreamReset);
            // ngtcp2 returns credit for bytes never delivered to the application.
            stream->discardReceived(self, stream->received);
            return 0;
        });
    };
    cb.recv_stop_sending = [](ngtcp2_conn *, int64_t id, uint64_t error, void *data, void *) -> int {
        auto &self = *static_cast<ConnectionState *>(data);
        if (auto it = self.streams.find(id); it != self.streams.end()) {
            it->second->appError = error;
            it->second->writeError = make_error_code(QuicError::StreamReset);
        }
        return 0;
    };
    cb.stream_close2 = [](ngtcp2_conn *, uint32_t flags, int64_t id, uint64_t rxError,
        uint64_t txError, void *data, void *) -> int {
        return callback([&]() {
            auto &self = *static_cast<ConnectionState *>(data);
            auto it = self.streams.find(id);
            if (it == self.streams.end()) {
                return 0;
            }
            auto stream = it->second;
            if (flags & NGTCP2_STREAM_CLOSE2_FLAG_RX_APP_ERROR_CODE_SET) {
                stream->readError = make_error_code(QuicError::StreamReset);
                stream->appError = rxError;
                stream->discardReceived(self, std::max(stream->received, stream->consumed));
            }
            if (flags & NGTCP2_STREAM_CLOSE2_FLAG_TX_APP_ERROR_CODE_SET) {
                stream->writeError = make_error_code(QuicError::StreamReset);
                stream->appError = txError;
            }
            stream->closed = true;
            self.buffered -= stream->buffered;
            stream->buffered = 0;
            stream->tx.clear();
            self.closedStreams.push_back(stream);
            self.streams.erase(it);
            if ((id & 1) != static_cast<int>(self.server)) {
                ++self.streamCredit;
            }
            return 0;
        });
    };
    return cb;
}

static auto makeConnection(const std::shared_ptr<ContextState> &context, IPEndpoint remote,
    const std::string &serverName, const ngtcp2_pkt_hd *initial = nullptr)
    -> IoResult<std::shared_ptr<ConnectionState>> {
    if (context->connections.size() >= context->config.maxConnections) {
        return Err(std::make_error_code(std::errc::no_buffer_space));
    }
    auto state = std::make_shared<ConnectionState>();
    state->pool = context->pool;
    state->context = context;
    state->local = context->local;
    state->remote = remote;
    state->server = initial != nullptr;
    ngtcp2_ccerr_default(&state->closeReason);
    ngtcp2_cid scid{}, dcid{};
    if (!randomCid(scid) || !randomCid(dcid)) {
        return Err(make_error_code(QuicError::Tls));
    }
    if (initial) {
        dcid = initial->scid;
    }
    auto &config = context->config;
    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = timestamp();
    settings.handshake_timeout = std::chrono::duration_cast<std::chrono::nanoseconds>(config.handshakeTimeout).count();
    settings.max_tx_udp_payload_size = packetSize;
    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_data = config.connectionWindow;
    params.initial_max_stream_data_bidi_local = config.streamWindow;
    params.initial_max_stream_data_bidi_remote = config.streamWindow;
    params.initial_max_streams_bidi = config.maxStreams;
    params.max_idle_timeout = std::chrono::duration_cast<std::chrono::nanoseconds>(config.idleTimeout).count();
    params.active_connection_id_limit = 4;
    params.disable_active_migration = 1;
    if (initial) {
        params.original_dcid = initial->dcid;
        params.original_dcid_present = 1;
        params.stateless_reset_token_present = 1;
        if (RAND_bytes(params.stateless_reset_token, sizeof(params.stateless_reset_token)) != 1) {
            return Err(make_error_code(QuicError::Tls));
        }
    }
    ngtcp2_path path{
        .local = address(state->local),
        .remote = address(state->remote),
    };
    auto cb = callbacks();
    auto rv = initial ? ngtcp2_conn_server_new(&state->conn, &dcid, &scid, &path,
        initial->version, &cb, &settings, &params, nullptr, state.get()) :
        ngtcp2_conn_client_new(&state->conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1,
            &cb, &settings, &params, nullptr, state.get());
    if (rv != 0) {
        return Err(transportError(rv));
    }
    state->tls = std::make_unique<TlsSession>();
    ILIAS_TRYV(state->tls->init(context->tls.get(), state->conn, config, serverName));
    context->routes.emplace(cidKey(scid.data, scid.datalen), state);
    if (initial) {
        context->routes.emplace(cidKey(initial->dcid.data, initial->dcid.datalen), state);
    }
    context->connections.push_back(state);
    return state;
}

static auto receive(const std::shared_ptr<ContextState> &context, IPEndpoint remote,
    const uint8_t *packet, size_t size, ngtcp2_tstamp now, unsigned int ecn) -> void {
    ngtcp2_version_cid cid{};
    if (ngtcp2_pkt_decode_version_cid(&cid, packet, size, cidLength) != 0) {
        return; // Ignore unrelated UDP traffic and malformed headers.
    }
    std::shared_ptr<ConnectionState> state;
    if (auto it = context->routes.find(cidKey(cid.dcid, cid.dcidlen)); it != context->routes.end()) {
        state = it->second.lock();
    }
    if (!state && context->config.role == QuicContext::Role::Server) {
        ngtcp2_pkt_hd initial{};
        if (ngtcp2_accept(&initial, packet, size) != 0 || initial.version != NGTCP2_PROTO_VER_V1) {
            return;
        }
        auto created = makeConnection(context, remote, {}, &initial);
        if (!created) {
            return;
        }
        state = std::move(*created);
    }
    if (!state || state->draining || remote != state->remote) {
        return;
    }
    if (state->closeSent) {
        state->closeRequested = true; // Retransmit CONNECTION_CLOSE on incoming traffic.
        return;
    }
    ngtcp2_path path{
        .local = address(state->local),
        .remote = address(remote),
    };
    ngtcp2_pkt_info pi{};
    pi.ecn = ecn;
    now = std::max(now, ngtcp2_conn_get_timestamp(state->conn));
    state->handleError(ngtcp2_conn_read_pkt(state->conn, &path, &pi, packet, size, now));
}

static auto applyStreamChanges(ConnectionState &state) -> void {
    if (state.credit) {
        ngtcp2_conn_extend_max_offset(state.conn, std::exchange(state.credit, 0));
    }
    if (state.streamCredit) {
        ngtcp2_conn_extend_max_streams_bidi(state.conn, std::exchange(state.streamCredit, 0));
    }
    // RESET may close and erase a stream synchronously.
    std::vector<std::shared_ptr<StreamState>> streams;
    for (auto &[id, stream] : state.streams) {
        streams.push_back(stream);
    }
    for (auto &stream : streams) {
        if (stream->credit) {
            auto rv = ngtcp2_conn_extend_max_stream_offset(state.conn, stream->id,
                std::exchange(stream->credit, 0));
            if (rv != 0) {
                state.handleError(rv);
                return;
            }
        }
        if (stream->resetRequested && !stream->resetApplied && !stream->closed) {
            stream->resetApplied = true;
            state.handleError(ngtcp2_conn_shutdown_stream(state.conn, 0, stream->id, *stream->resetRequested));
        }
    }
}

static auto writePacket(ngtcp2_conn *, ngtcp2_path *path, ngtcp2_pkt_info *pi,
    uint8_t *packet, size_t capacity, ngtcp2_tstamp now, void *data) noexcept -> ngtcp2_ssize {
    auto &state = *static_cast<ConnectionState *>(data);
    auto &batch = *state.writingBatch;
    ngtcp2_ssize n = 0;
    ngtcp2_ssize dataSize = -1;
    std::shared_ptr<StreamState> selected;
    auto it = state.streams.upper_bound(state.nextStream);
    for (size_t checked = 0; checked < state.streams.size(); ++checked) {
        if (it == state.streams.end()) it = state.streams.begin();
        auto stream = (it++)->second;
        state.nextStream = stream->id;
        if (stream->writeError || stream->closed || stream->finSubmitted ||
            (stream->submitted == stream->accepted && !stream->writeFin)) continue;

        std::array<ngtcp2_vec, 16> vectors{};
        size_t count = 0;
        size_t available = 0;
        for (auto &chunk : stream->tx) {
            if (chunk.offset + chunk.bytes.size() <= stream->submitted) continue;
            auto offset = stream->submitted > chunk.offset ? static_cast<size_t>(stream->submitted - chunk.offset) : 0;
            vectors[count++] = {reinterpret_cast<uint8_t *>(chunk.bytes.mutableData().data()) + offset,
                chunk.bytes.size() - offset};
            available += chunk.bytes.size() - offset;
            if (count == vectors.size()) break;
        }
        auto fin = stream->writeFin && stream->submitted + available == stream->accepted;
        uint32_t flags = fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0;
        if (batch.padding && available >= 1024) flags |= NGTCP2_WRITE_STREAM_FLAG_PADDING;
        n = ngtcp2_conn_writev_stream(state.conn, path, pi, packet, capacity, &dataSize,
            flags, stream->id, count ? vectors.data() : nullptr, count, now);
        if (n == NGTCP2_ERR_STREAM_DATA_BLOCKED) continue;
        if (n == NGTCP2_ERR_STREAM_SHUT_WR || n == NGTCP2_ERR_STREAM_NOT_FOUND) {
            stream->writeError = make_error_code(QuicError::StreamReset);
            continue;
        }
        selected = stream;
        if (dataSize >= 0) {
            stream->submitted += dataSize;
            stream->finSubmitted = fin && static_cast<size_t>(dataSize) == available;
        }
        break;
    }
    if (!selected) {
        n = ngtcp2_conn_writev_stream(state.conn, path, pi, packet, capacity, nullptr, 0, -1, nullptr, 0, now);
    }
    if (n > 0) {
        auto &receipt = batch.receipts[batch.count++];
        if (selected && dataSize >= 0) receipt = {selected, selected->submitted, selected->finSubmitted};
    }
    return n;
}

static auto sendPackets(const std::shared_ptr<ContextState> &context,
    const std::shared_ptr<ConnectionState> &state) -> IoTask<void> {
    if (state->closeRequested && !state->draining) {
        if (state->closePacket.empty()) {
            auto buffer = state->pool->allocate(packetSize);
            auto data = buffer.prepareBack(packetSize);
            auto n = ngtcp2_conn_write_connection_close(state->conn, nullptr, nullptr,
                reinterpret_cast<uint8_t *>(data.data()), data.size(), &state->closeReason, timestamp());
            if (n < 0) {
                co_return Err(transportError(static_cast<int>(n)));
            }
            if (n == 0) {
                state->closeRequested = false;
                co_return {};
            }
            buffer.commitBack(n);
            state->closePacket = std::move(buffer);
        }
        // ngtcp2 enters closing after encoding once; retransmit the cached packet.
        auto closed = co_await context->socket.send(ilias::makeBuffer(state->closePacket.data()), state->remote, state->closePacket.size());
        if (closed.error) co_return Err(closed.error);
        state->closeRequested = false;
        state->closeSent = true;
        if (!state->retireAt) {
            state->retireAt = timestamp() + 3 * ngtcp2_conn_get_pto2(state->conn);
        }
        state->notify();
    }
    if (state->error) {
        co_return {};
    }
    applyStreamChanges(*state);
    if (state->error) co_return {};
    // One bounded quantum per connection; all buffers survive asynchronous I/O.
    auto buffer = state->pool->allocate(UdpBatchSocket::sendCapacity);
    auto packet = buffer.prepareBack(UdpBatchSocket::sendCapacity);
    PacketBatch batch;
    batch.padding = context->config.udpBatching;
    state->writingBatch = &batch;
    auto limit = context->config.udpBatching ? std::clamp(ngtcp2_conn_get_send_quantum2(state->conn),
        packetSize, UdpBatchSocket::sendCapacity) : packetSize;
    size_t segment = 0;
    ngtcp2_pkt_info pi{};
    auto n = ngtcp2_conn_write_aggregate_pkt2(state->conn, nullptr, &pi,
        reinterpret_cast<uint8_t *>(packet.data()), limit, &segment, writePacket,
        context->config.udpBatching ? UdpBatchSocket::maxSegments : 1, timestamp());
    state->writingBatch = nullptr;
    if (n < 0) {
        state->handleError(static_cast<int>(n));
        co_return {};
    }
    if (n == 0) co_return {};
    auto sent = co_await context->socket.send(ilias::makeBuffer(packet.data(), n), state->remote, segment, pi.ecn);
    for (size_t i = 0; i < sent.datagrams; ++i) {
        auto &receipt = batch.receipts[i];
        if (receipt.stream) {
            receipt.stream->sent = std::max(receipt.stream->sent, receipt.offset);
            receipt.stream->finSent |= receipt.fin;
        }
    }
    // A rejected MTU probe is a lost packet, not a failed UDP endpoint.
    if (sent.error && (sent.error != ilias::SystemError::MessageTooLarge ||
        batch.count != 1 || batch.receipts[0].stream || n <= NGTCP2_MAX_UDP_PAYLOAD_SIZE)) {
        state->notify();
        co_return Err(sent.error);
    }
    ngtcp2_conn_update_pkt_tx_time(state->conn, timestamp());
    state->notify();
    co_return {};
}

static auto receiveWorker(std::shared_ptr<ContextState> state) -> IoTask<void> {
    auto token = co_await ilias::this_coro::stopToken();
    size_t handled = 0;
    auto turn = timestamp();
    while (!state->stopped && !token.stop_requested()) {
        ILIAS_CO_TRY(auto received, co_await state->socket.receive());
        if (state->stopped || token.stop_requested()) {
            break;
        }
        auto now = timestamp();
        size_t count = 0;
        for (auto &message : received) {
            auto remaining = message.bytes;
            do {
                auto size = message.segmentSize ? std::min(message.segmentSize, remaining.size()) : remaining.size();
                receive(state, message.remote, reinterpret_cast<const uint8_t *>(remaining.data()), size, now, message.ecn);
                remaining = remaining.subspan(size);
                if (++count % 32 == 0) now = timestamp();
            } while (!remaining.empty());
        }
        state->notify();
        state->wake.set();
        handled += count;
        if (handled >= 32 && (handled >= 1024 || timestamp() - turn >= 250 * NGTCP2_MICROSECONDS)) {
            co_await ilias::this_coro::yield();
            handled = 0;
            turn = timestamp();
        }
    }
    co_return {};
}

static auto sendWorker(std::shared_ptr<ContextState> state) -> IoTask<void> {
    auto token = co_await ilias::this_coro::stopToken();
    while (!state->stopped && !token.stop_requested()) {
        state->wake.clear();
        auto now = timestamp();
        state->removeExpired(now);
        // Application coroutines may add connections while sendto is suspended.
        auto connections = state->connections;
        auto turn = now;
        for (auto &connection : connections) {
            now = timestamp();
            if (!connection->error && ngtcp2_conn_get_expiry2(connection->conn) <= now) {
                connection->handleError(ngtcp2_conn_handle_expiry(connection->conn, now));
            }
            auto sent = co_await sendPackets(state, connection);
            if (!sent) {
                state->stop(sent.error());
                co_return Err(sent.error());
            }
            if (timestamp() - turn >= 250 * NGTCP2_MICROSECONDS) {
                co_await ilias::this_coro::yield();
                turn = timestamp();
            }
        }
        state->notify();
        if (state->stopped || token.stop_requested()) {
            break;
        }
        auto deadline = timestamp() + NGTCP2_SECONDS;
        for (auto &connection : state->connections) {
            deadline = std::min(deadline, connection->retireAt ? connection->retireAt :
                ngtcp2_conn_get_expiry2(connection->conn));
        }
        state->deadline = deadline;
        if (!state->armedDeadline || deadline < state->armedDeadline) state->timerChanged.set();
        co_await state->wake.wait();
    }
    co_return {};
}

static auto timerWorker(std::shared_ptr<ContextState> state) -> IoTask<void> {
    auto token = co_await ilias::this_coro::stopToken();
    while (!state->stopped && !token.stop_requested()) {
        state->timerChanged.clear();
        auto deadline = state->deadline;
        if (deadline == UINT64_MAX) {
            co_await state->timerChanged.wait();
            continue;
        }
        state->armedDeadline = deadline;
        auto now = timestamp();
        auto delay = std::chrono::nanoseconds(deadline > now ? deadline - now : 0);
        auto [changed, expired] = co_await ilias::whenAny(state->timerChanged.wait(), ilias::sleep(delay));
        if (expired && !state->stopped && !token.stop_requested()) {
            // Wait for the sender to publish a fresh deadline before arming again.
            state->armedDeadline = 0;
            state->wake.set();
            co_await state->timerChanged.wait();
        }
    }
    co_return {};
}

static auto closeConnections(std::shared_ptr<ContextState> state) -> ilias::Task<void> {
    state->stop(std::make_error_code(std::errc::operation_canceled));
    auto send = [&]() -> ilias::Task<void> {
        for (auto &connection : state->connections) {
            // Try every peer even if another connection cannot send its close.
            auto _ = co_await sendPackets(state, connection);
        }
    };
    // Finish notification even when run() was canceled, without delaying exit indefinitely.
    auto _ = co_await ilias::unstoppable(ilias::timeout(send(), std::chrono::milliseconds{250}));
}

static auto runContext(std::shared_ptr<ContextState> state) -> IoTask<void> {
    if (!state || state->running || state->stopped) {
        co_return Err(std::make_error_code(std::errc::operation_not_permitted));
    }
    state->running = true;
    ScopeExit cleanup{[&]() {
        state->stop(std::make_error_code(std::errc::operation_canceled));
        state->socket.close();
        state->incoming.clear();
        state->routes.clear();
        state->connections.clear();
        state->running = false;
    }};
    // Keep one receive alive: racing recvfrom against a wakeup can discard
    // a datagram that completes at the same time as cancellation.
    auto [received, sent, timer] = co_await (ilias::whenAny(receiveWorker(state), sendWorker(state), timerWorker(state)) |
        ilias::finally(closeConnections(state)));
    if (received && !*received) {
        state->stop(received->error());
        co_return Err(received->error());
    }
    if (sent && !*sent) {
        state->stop(sent->error());
        co_return Err(sent->error());
    }
    co_return {};
}

} // namespace quic

// MARK: Context
QuicContext::QuicContext(std::shared_ptr<quic::ContextState> state) : mState(std::move(state)) {}
QuicContext::QuicContext(QuicContext &&) noexcept = default;
QuicContext::~QuicContext() { stop(); }

auto QuicContext::operator =(QuicContext &&other) noexcept -> QuicContext & {
    if (this != &other) {
        stop();
        mState = std::move(other.mState);
    }
    return *this;
}

auto QuicContext::bind(IPEndpoint local, Config config) -> IoTask<QuicContext> {
    if ((config.role != Role::Client && config.role != Role::Server) ||
        config.alpn.empty() || config.alpn.size() > 255 || config.handshakeTimeout.count() <= 0 ||
        config.handshakeTimeout.count() > NGTCP2_MAX_VARINT / NGTCP2_MILLISECONDS ||
        config.idleTimeout.count() > NGTCP2_MAX_VARINT / NGTCP2_MILLISECONDS ||
        config.idleTimeout.count() <= 0 || config.maxConnections == 0 || config.maxStreams == 0 ||
        config.maxStreams > (NGTCP2_MAX_VARINT / 4) || config.streamWindow == 0 ||
        config.connectionWindow == 0 || config.sendBufferLimit == 0 || config.connectionSendBufferLimit == 0 ||
        config.streamWindow > NGTCP2_MAX_VARINT || config.connectionWindow > NGTCP2_MAX_VARINT ||
        !config.memoryResource) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    auto state = std::make_shared<quic::ContextState>(std::move(config));
    ILIAS_CO_TRY(state->tls, quic::makeTlsContext(state->config));
    ILIAS_CO_TRY(auto socket, ilias::Socket::make(local.family(), SOCK_DGRAM, IPPROTO_UDP));
    if (local.family() == AF_INET6) {
        ILIAS_CO_TRYV(socket.setOption(ilias::sockopt::Ipv6Only{false}));
#if defined(_WIN32) || defined(__linux__)
        // Let QUIC probe without IP fragmentation or a stale kernel path MTU.
        ILIAS_CO_TRYV(socket.setOption(ilias::sockopt::OptionT<IPPROTO_IPV6, IPV6_MTU_DISCOVER, int>{IP_PMTUDISC_PROBE}));
#elif defined(IPV6_DONTFRAG)
        ILIAS_CO_TRYV(socket.setOption(ilias::sockopt::OptionT<IPPROTO_IPV6, IPV6_DONTFRAG, int>{1}));
#endif
    }
    else {
#if defined(_WIN32) || defined(__linux__)
        ILIAS_CO_TRYV(socket.setOption(ilias::sockopt::OptionT<IPPROTO_IP, IP_MTU_DISCOVER, int>{IP_PMTUDISC_PROBE}));
#elif defined(IP_DONTFRAG)
        ILIAS_CO_TRYV(socket.setOption(ilias::sockopt::OptionT<IPPROTO_IP, IP_DONTFRAG, int>{1}));
#endif
    }
    ILIAS_CO_TRYV(socket.bind(local));
    ILIAS_CO_TRY(state->socket, quic::UdpBatchSocket::from(std::move(socket), state->config.udpBatching));
    ILIAS_CO_TRY(state->local, state->socket.localEndpoint());
    co_return QuicContext{std::move(state)};
}

auto QuicContext::run() -> IoTask<void> {
    return quic::runContext(mState);
}

auto QuicContext::stop() noexcept -> void {
    if (mState) {
        mState->stop(std::make_error_code(std::errc::operation_canceled));
    }
}

auto QuicContext::localEndpoint() const -> IoResult<IPEndpoint> {
    if (!mState) {
        return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    return mState->local;
}

auto QuicContext::ioStats() const noexcept -> IoStats {
    return mState ? mState->socket.stats() : IoStats{};
}

auto QuicContext::connect(std::string address) -> IoTask<QuicConnection> {
    using namespace std::chrono_literals;
    auto state = mState;
    if (!state || state->config.role != Role::Client) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    if (state->stopped) {
        co_return Err(state->error);
    }

    auto separator = address.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 == address.size()) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    auto host = address.substr(0, separator);
    auto service = std::string_view(address).substr(separator + 1);
    uint16_t port = 0;
    auto [end, error] = std::from_chars(service.data(), service.data() + service.size(), port);
    if (error != std::errc{} || end != service.data() + service.size() || port == 0) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    if (host.starts_with('[') && host.ends_with(']')) {
        host = host.substr(1, host.size() - 2);
    }
    else if (host.find_first_of(":[]") != std::string::npos) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    if (host.empty()) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }

    ilias::addrinfo_t hints{};
    hints.ai_family = state->local.family() == AF_INET ? AF_INET : AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    hints.ai_flags = AI_NUMERICSERV;
    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::fromHostname(host, service, hints));
    auto resolved = info.endpoints();
    if (resolved.empty()) {
        co_return Err(std::make_error_code(std::errc::host_unreachable));
    }

    // Preserve resolver preference within each family, interleaving IPv4 and IPv6.
    std::array<std::deque<IPEndpoint>, 2> families;
    for (auto endpoint : resolved) {
        auto &family = families[endpoint.family() == AF_INET6];
        if (std::find(family.begin(), family.end(), endpoint) == family.end()) {
            family.push_back(endpoint);
        }
    }
    std::vector<IPEndpoint> candidates;
    size_t family = resolved.front().family() == AF_INET6;
    while (!families[0].empty() || !families[1].empty()) {
        if (!families[family].empty()) {
            candidates.push_back(families[family].front());
            families[family].pop_front();
        }
        family ^= 1;
    }

    ilias::TaskGroup<IoResult<QuicConnection>> attempts;
    auto race = [&]() -> IoTask<QuicConnection> {
        std::error_code lastError = std::make_error_code(std::errc::host_unreachable);
        size_t next = 0;
        while (next < candidates.size() || !attempts.empty()) {
            if (!attempts.empty()) {
                std::optional<IoResult<QuicConnection>> completed;
                if (next < candidates.size()) {
                    auto result = co_await ilias::timeout(attempts.next(), 250ms);
                    if (result) { completed = std::move(*result); }
                }
                else {
                    completed = co_await attempts.next();
                }
                if (completed) {
                    if (*completed) {
                        co_return std::move(**completed);
                    }
                    lastError = completed->error();
                }
            }
            if (next < candidates.size()) {
                // Every attempt verifies the original host, not its resolved address.
                attempts.spawn(connect(candidates[next++], host));
            }
        }
        co_return Err(lastError);
    };
    // Join canceled attempts before releasing the race's state, including on cancellation.
    co_return co_await ilias::finally(race(), attempts.shutdown());
}

auto QuicContext::connect(IPEndpoint remote, std::string serverName) -> IoTask<QuicConnection> {
    auto state = mState;
    if (state && state->local.family() == AF_INET6 && remote.family() == AF_INET) {
        remote = IPEndpoint{ilias::IPAddress6::fromV4Mapped(remote.address4()), remote.port()};
    }
    if (!state || state->config.role != Role::Client || remote.family() != state->local.family()) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    if (state->stopped) {
        co_return Err(state->error);
    }
    ILIAS_CO_TRY(auto connection, quic::makeConnection(state, remote, serverName));
    bool completed = false;
    ScopeExit cleanup{[&]() {
        if (!completed) {
            connection->close(abandonedError);
        }
    }};
    state->wake.set();
    auto token = co_await ilias::this_coro::stopToken();
    while (!connection->handshake && !connection->error) {
        ILIAS_CO_TRYV(co_await waitChanged(connection->changed, token));
    }
    if (connection->error) {
        co_return Err(connection->error);
    }
    completed = true;
    co_return QuicConnection{std::move(connection)};
}

auto QuicContext::accept() -> IoTask<QuicConnection> {
    auto state = mState;
    if (!state || state->config.role != Role::Server) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    auto token = co_await ilias::this_coro::stopToken();
    while (true) {
        if (state->stopped) {
            co_return Err(state->error);
        }
        while (!state->incoming.empty()) {
            auto connection = std::move(state->incoming.front());
            state->incoming.pop_front();
            if (!connection->error) {
                co_return QuicConnection{std::move(connection)};
            }
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

// MARK: Connection
QuicConnection::QuicConnection(std::shared_ptr<quic::ConnectionState> state) : mState(std::move(state)) {}
QuicConnection::QuicConnection(QuicConnection &&) noexcept = default;
QuicConnection::~QuicConnection() { if (mState) { mState->close(abandonedError); } }

auto QuicConnection::operator =(QuicConnection &&other) noexcept -> QuicConnection & {
    if (this != &other) {
        if (mState) { mState->close(abandonedError); }
        mState = std::move(other.mState);
    }
    return *this;
}

auto QuicConnection::openStream() -> IoTask<QuicStream> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    auto token = co_await ilias::this_coro::stopToken();
    while (true) {
        if (state->error) {
            co_return Err(state->error);
        }
        int64_t id;
        auto rv = ngtcp2_conn_open_bidi_stream(state->conn, &id, nullptr);
        if (rv == 0) {
            co_return QuicStream{state->ensureStream(id)};
        }
        if (rv != NGTCP2_ERR_STREAM_ID_BLOCKED) {
            co_return Err(transportError(rv));
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

auto QuicConnection::acceptStream() -> IoTask<QuicStream> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    auto token = co_await ilias::this_coro::stopToken();
    while (true) {
        if (state->error) {
            co_return Err(state->error);
        }
        while (!state->incoming.empty()) {
            auto stream = std::move(state->incoming.front());
            state->incoming.pop_front();
            if (!stream->closed || !stream->readError) {
                co_return QuicStream{std::move(stream)};
            }
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

auto QuicConnection::close(uint64_t appError) -> IoTask<void> {
    auto state = mState;
    if (!state || appError > NGTCP2_MAX_VARINT) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    if (state->error && !state->closeRequested && !state->closeSent) {
        co_return Err(state->error);
    }
    state->close(appError);
    auto token = co_await ilias::this_coro::stopToken();
    while (!state->closeSent) {
        auto owner = state->context.lock();
        if (!owner || owner->stopped) {
            co_return Err(owner ? owner->error : make_error_code(QuicError::ConnectionClosed));
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
    co_return {};
}

auto QuicConnection::remoteEndpoint() const -> IPEndpoint {
    return mState ? mState->remote : IPEndpoint{};
}

auto QuicConnection::appError() const noexcept -> std::optional<uint64_t> {
    return mState ? mState->appError : std::nullopt;
}

// MARK: Stream
QuicStream::QuicStream(std::shared_ptr<quic::StreamState> state) : mState(std::move(state)) {}
QuicStream::QuicStream(QuicStream &&) noexcept = default;
QuicStream::~QuicStream() { reset(abandonedError); }

auto QuicStream::operator =(QuicStream &&other) noexcept -> QuicStream & {
    if (this != &other) {
        reset(abandonedError);
        mState = std::move(other.mState);
    }
    return *this;
}

auto QuicStream::read(ilias::MutableBuffer buffer) -> IoTask<size_t> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    if (buffer.empty()) {
        co_return 0;
    }
    auto token = co_await ilias::this_coro::stopToken();
    auto guard = co_await state->reader.lock();
    while (true) {
        if (token.stop_requested()) {
            co_return Err(std::make_error_code(std::errc::operation_canceled));
        }
        if (state->readError) {
            co_return Err(state->readError);
        }
        if (!state->rx.empty()) {
            auto &chunk = state->rx.front().bytes;
            auto size = std::min(buffer.size(), chunk.size());
            std::memcpy(buffer.data(), chunk.data().data(), size);
            chunk.consumeFront(size);
            state->consumed += size;
            state->credit += size;
            if (chunk.empty()) {
                state->rx.pop_front();
            }
            if (auto connection = state->connection.lock()) {
                connection->credit += size;
                connection->wake();
            }
            co_return size;
        }
        if (state->readFin) {
            co_return 0;
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

auto QuicStream::write(ilias::Buffer buffer) -> IoTask<size_t> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    if (buffer.empty()) {
        co_return 0;
    }
    auto connection = state->connection.lock();
    auto context = connection ? connection->context.lock() : nullptr;
    if (!context) {
        co_return Err(make_error_code(QuicError::ConnectionClosed));
    }
    auto token = co_await ilias::this_coro::stopToken();
    auto guard = co_await state->writer.lock();
    while (true) {
        if (token.stop_requested()) {
            co_return Err(std::make_error_code(std::errc::operation_canceled));
        }
        if (state->writeError) {
            co_return Err(state->writeError);
        }
        if (state->writeFin || state->closed) {
            co_return Err(std::make_error_code(std::errc::broken_pipe));
        }
        auto size = std::min({buffer.size(), streamChunkSize, context->config.sendBufferLimit - state->buffered,
            context->config.connectionSendBufferLimit - connection->buffered});
        if (size) {
            auto bytes = state->pool->allocate(size);
            bytes.append(buffer.first(size));
            state->tx.push_back({std::move(bytes), state->accepted});
            state->accepted += size;
            state->buffered += size;
            connection->buffered += size;
            connection->wake();
            co_return size;
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

auto QuicStream::flush() -> IoTask<void> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    auto token = co_await ilias::this_coro::stopToken();
    auto guard = co_await state->writer.lock();
    while (true) {
        if (token.stop_requested()) {
            co_return Err(std::make_error_code(std::errc::operation_canceled));
        }
        if (state->writeError) {
            co_return Err(state->writeError);
        }
        if (state->sent == state->accepted) {
            co_return {};
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
}

auto QuicStream::shutdown() -> IoTask<void> {
    auto state = mState;
    if (!state) {
        co_return Err(std::make_error_code(std::errc::bad_file_descriptor));
    }
    auto token = co_await ilias::this_coro::stopToken();
    auto guard = co_await state->writer.lock();
    if (state->writeError) {
        co_return Err(state->writeError);
    }
    state->writeFin = true;
    if (auto connection = state->connection.lock()) {
        connection->wake();
    }
    while (!state->finSent) {
        if (state->writeError) {
            co_return Err(state->writeError);
        }
        ILIAS_CO_TRYV(co_await waitChanged(state->changed, token));
    }
    co_return {};
}

auto QuicStream::reset(uint64_t appError) noexcept -> void {
    if (mState) {
        mState->reset(std::min<uint64_t>(appError, NGTCP2_MAX_VARINT));
    }
}

auto QuicStream::id() const noexcept -> int64_t { return mState ? mState->id : -1; }
auto QuicStream::appError() const noexcept -> std::optional<uint64_t> {
    return mState ? mState->appError : std::nullopt;
}
