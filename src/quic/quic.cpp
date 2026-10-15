#include "quic.hpp"
#include "crypto.hpp"
#include "../utils.hpp"

#include <ilias/sync.hpp>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <map>
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
constexpr size_t packetSize = 1200;
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
        std::vector<uint8_t> bytes;
        uint64_t offset = 0;
    };

    std::weak_ptr<ConnectionState> connection;
    int64_t id = -1;
    ilias::Event changed;
    ilias::Mutex reader;
    ilias::Mutex writer; // Also serializes flush and FIN.

    std::deque<Chunk> rx;
    std::deque<Chunk> tx;
    size_t rxHead = 0;
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

struct ConnectionState : std::enable_shared_from_this<ConnectionState> {
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
    bool server = false;
    bool handshake = false;
    bool queued = false;
    bool closeRequested = false;
    bool closeSent = false;
    bool draining = false;
    ngtcp2_tstamp retireAt = 0;
    ngtcp2_ccerr closeReason{};
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
    QuicContext::Config config;
    ilias::UdpSocket socket;
    IPEndpoint local;
    TlsContext tls;
    ilias::Event wake;
    ilias::Event changed;
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
    rxHead = 0;
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
                stream->rx.push_back({std::vector<uint8_t>(bytes, bytes + size), offset});
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
    settings.no_pmtud = 1;
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
    const uint8_t *packet, size_t size) -> void {
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
    state->handleError(ngtcp2_conn_read_pkt(state->conn, &path, nullptr, packet, size, timestamp()));
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

static auto sendPackets(const std::shared_ptr<ContextState> &context,
    const std::shared_ptr<ConnectionState> &state) -> IoTask<void> {
    std::array<uint8_t, packetSize> packet;
    if (state->closeRequested && !state->draining) {
        auto n = ngtcp2_conn_write_connection_close(state->conn, nullptr, nullptr,
            packet.data(), packet.size(), &state->closeReason, timestamp());
        state->closeRequested = false;
        if (n > 0) {
            ILIAS_CO_TRYV(co_await context->socket.sendto(ilias::makeBuffer(packet.data(), n), state->remote));
        }
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
    for (size_t count = 0; count < 64 && !context->stopped && !state->error; ++count) {
        ngtcp2_ssize n = 0;
        ngtcp2_ssize dataSize = -1;
        std::shared_ptr<StreamState> selected;
        auto it = state->streams.upper_bound(state->nextStream);
        for (size_t checked = 0; checked < state->streams.size(); ++checked) {
            if (it == state->streams.end()) {
                it = state->streams.begin();
            }
            auto stream = (it++)->second;
            state->nextStream = stream->id;
            if (stream->writeError || stream->closed || stream->finSubmitted ||
                (stream->submitted == stream->accepted && !stream->writeFin)) {
                continue;
            }
            ngtcp2_vec vec{};
            for (auto &chunk : stream->tx) {
                if (chunk.offset + chunk.bytes.size() > stream->submitted) {
                    auto offset = static_cast<size_t>(stream->submitted - chunk.offset);
                    vec.base = chunk.bytes.data() + offset;
                    vec.len = chunk.bytes.size() - offset;
                    break;
                }
            }
            auto fin = stream->writeFin && stream->submitted + vec.len == stream->accepted;
            n = ngtcp2_conn_writev_stream(state->conn, nullptr, nullptr, packet.data(), packet.size(),
                &dataSize, fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0, stream->id,
                vec.len ? &vec : nullptr, vec.len ? 1 : 0, timestamp());
            if (n == NGTCP2_ERR_STREAM_DATA_BLOCKED) {
                continue;
            }
            if (n == NGTCP2_ERR_STREAM_SHUT_WR || n == NGTCP2_ERR_STREAM_NOT_FOUND) {
                stream->writeError = make_error_code(QuicError::StreamReset);
                continue;
            }
            selected = stream;
            if (dataSize >= 0) {
                stream->submitted += dataSize;
                stream->finSubmitted = fin && static_cast<size_t>(dataSize) == vec.len;
            }
            break;
        }
        if (!selected) {
            n = ngtcp2_conn_writev_stream(state->conn, nullptr, nullptr, packet.data(), packet.size(),
                nullptr, 0, -1, nullptr, 0, timestamp());
        }
        if (n < 0) {
            state->handleError(static_cast<int>(n));
            break;
        }
        if (n == 0) {
            break;
        }
        ILIAS_CO_TRYV(co_await context->socket.sendto(ilias::makeBuffer(packet.data(), n), state->remote));
        ngtcp2_conn_update_pkt_tx_time(state->conn, timestamp());
        if (selected && dataSize >= 0) {
            selected->sent = selected->submitted;
            selected->finSent = selected->finSubmitted;
        }
        state->notify();
        if (count == 63) {
            context->wake.set(); // Continue later, allowing receives between batches.
        }
    }
    co_return {};
}

static auto receiveWorker(std::shared_ptr<ContextState> state) -> IoTask<void> {
    auto token = co_await ilias::this_coro::stopToken();
    std::array<uint8_t, 65536> packet;
    while (!state->stopped && !token.stop_requested()) {
        ILIAS_CO_TRY(auto received, co_await state->socket.recvfrom(ilias::makeBuffer(packet)));
        if (state->stopped || token.stop_requested()) {
            break;
        }
        auto [size, remote] = received;
        receive(state, remote, packet.data(), size);
        state->notify();
        state->wake.set();
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
        for (auto &connection : connections) {
            if (!connection->error && ngtcp2_conn_get_expiry2(connection->conn) <= now) {
                connection->handleError(ngtcp2_conn_handle_expiry(connection->conn, now));
            }
            auto sent = co_await sendPackets(state, connection);
            if (!sent) {
                state->stop(sent.error());
                co_return Err(sent.error());
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
        now = timestamp();
        auto delay = std::chrono::nanoseconds(deadline > now ? deadline - now : 0);
        co_await ilias::whenAny(state->wake.wait(), ilias::sleep(delay));
    }
    co_return {};
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
    auto [received, sent] = co_await ilias::whenAny(receiveWorker(state), sendWorker(state));
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
        config.streamWindow > NGTCP2_MAX_VARINT || config.connectionWindow > NGTCP2_MAX_VARINT) {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
    auto state = std::make_shared<quic::ContextState>();
    state->config = std::move(config);
    ILIAS_CO_TRY(state->tls, quic::makeTlsContext(state->config));
    ILIAS_CO_TRY(state->socket, co_await ilias::UdpSocket::bind(local));
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

auto QuicContext::connect(IPEndpoint remote, std::string serverName) -> IoTask<QuicConnection> {
    auto state = mState;
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
            auto size = std::min(buffer.size(), chunk.size() - state->rxHead);
            std::memcpy(buffer.data(), chunk.data() + state->rxHead, size);
            state->rxHead += size;
            state->consumed += size;
            state->credit += size;
            if (state->rxHead == chunk.size()) {
                state->rx.pop_front();
                state->rxHead = 0;
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
        auto size = std::min({buffer.size(), context->config.sendBufferLimit - state->buffered,
            context->config.connectionSendBufferLimit - connection->buffered});
        if (size) {
            auto bytes = reinterpret_cast<const uint8_t *>(buffer.data());
            state->tx.push_back({std::vector<uint8_t>(bytes, bytes + size), state->accepted});
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
        mState->reset(std::min(appError, NGTCP2_MAX_VARINT));
    }
}

auto QuicStream::id() const noexcept -> int64_t { return mState ? mState->id : -1; }
auto QuicStream::appError() const noexcept -> std::optional<uint64_t> {
    return mState ? mState->appError : std::nullopt;
}
