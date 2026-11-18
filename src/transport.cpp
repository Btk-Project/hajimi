#include <ilias/task.hpp>
#include <ilias/sync.hpp>
#include <ilias/net.hpp>
#include <type_traits>
#include <print>
#include <deque>
#include <map>

#include "utils/scope_exit.hpp"
#include "transport.hpp"
#include "protocol.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif // HAJIMI_QUIC

// Import types
using ilias::IoResult;
using ilias::IoTask;
using ilias::Event;

namespace mpsc = ilias::mpsc;

namespace {

constexpr auto MAX_TUNNELS = std::size_t{128};

// Deserializes a message from a buffer.
auto parseMessage(std::uint8_t type, SkBuffer &buffer) -> std::optional<Message> {
    switch (static_cast<MessageType>(type)) {
        case MessageType::Hello: {
            ILIAS_TRY(auto version, buffer.popIntBE<uint16_t>());
            ILIAS_TRY(auto name, buffer.popString());

            // std::println("[Protocol] Hello: {}, {}", version, name);
            return Message {
                Hello {
                    .version = version,
                    .name = std::move(name),
                }
            };
        }

        case MessageType::HelloAck: {
            // std::println("[Protocol] HelloAck");
            return Message { HelloAck{} };
        }

        case MessageType::Ping: {
            // std::println("[Protocol] Ping");
            return Message { Ping{} };
        }

        case MessageType::Pong: {
            // std::println("[Protocol] Pong");
            return Message { Pong{} };
        }

        case MessageType::OpenTunnel: {
            if (buffer.size() < sizeof(uint16_t)) {
                return std::nullopt;
            }
            ILIAS_TRY(auto addr, buffer.popString(buffer.size() - sizeof(uint16_t)));
            ILIAS_TRY(auto port, buffer.popIntBE<uint16_t>());
            // std::println("[Protocol] OpenTunnel {}:{}", addr, port);
            return Message {
                OpenTunnel {
                    .addr = std::move(addr),
                    .port = port,
                }
            };
        }

        case MessageType::TunnelClose: {
            // std::println("[Protocol] TunnelClose");
            return Message { TunnelClose {} };
        }

        case MessageType::TunnelShutdown: {
            // std::println("[Protocol] TunnelShutdown");
            return Message { TunnelShutdown {} };
        }

        case MessageType::DataExchange: {
            if (buffer.empty()) return std::nullopt; // Empty buffers are reserved for EOF.
            // std::println("[Protocol] DataExchange {} bytes", buffer.size());
            return Message {
                DataExchange {
                    .data = std::exchange(buffer, SkBuffer{}),
                }
            };
        }

        case MessageType::WindowUpdate: {
            if (buffer.size() != sizeof(uint32_t)) return std::nullopt;
            ILIAS_TRY(auto size, buffer.popIntBE<uint32_t>());
            // std::println("[Protocol] WindowUpdate {} bytes", size);
            return Message {
                WindowUpdate {
                    .size = size
                }
            };
        }

        case MessageType::FatalError: {
            ILIAS_TRY(auto code, buffer.popIntBE<uint32_t>());
            ILIAS_TRY(auto msg, buffer.popString());
            // std::println("[Protocol] FatalError {} => {}", code, msg);
            return Message {
                FatalError {
                    .code = code,
                    .msg = std::move(msg)
                }  
            };
        }
        default: {
            std::println(stderr, "[Message] readMessage, invalid type: {}", type);
            return std::nullopt;
        }
    }

}

// Serializes a message to a buffer.
class MessageWriter {
public:
    MessageWriter(SkBufferPool &pool) : mPool(pool) {
        mControl = mPool.allocate(1024);
    }

    auto writeMessage(Message msg) -> SkBuffer &;
private:
    SkBufferPool &mPool;
    SkBuffer      mControl;
    SkBuffer      mExchange; // Large buffer for data exchange
};

auto MessageWriter::writeMessage(Message msg) -> SkBuffer & {
    // The number of bytes of the message
    SkBuffer *cur = &mControl;
    cur->clear();
    cur->reserveHead(HAJIMI_HEADER_ROOM);

    switch (msg.type()) {
        case MessageType::Hello: {
            auto hello = msg.cast<Hello>().value();
            assert(hello.name.size() < 512 && "name too long");
            
            cur->appendIntBE(hello.version);
            cur->append(ilias::makeBuffer(hello.name));
            break;
        }
        case MessageType::HelloAck: break; // No-payload
        case MessageType::Ping: break;
        case MessageType::Pong: break;

        case MessageType::OpenTunnel: {
            auto tunnel = msg.cast<OpenTunnel>().value();
            assert(tunnel.addr.size() < 512 && "addr too long");

            cur->append(ilias::makeBuffer(tunnel.addr));
            cur->appendIntBE(tunnel.port);
            break;
        }
        case MessageType::DataExchange: {
            auto exchange = msg.cast<DataExchange>().value();
            mExchange = std::exchange(exchange.data, SkBuffer{});
            // We just use the message buffer, avoid the copy
            // len | type | data |
            assert(mExchange.headroom() >= HAJIMI_HEADER_ROOM && "This buffer is too small");
            assert(mExchange.size() <= HAJIMI_MAX_DATA_EXCHANGE && "This buffer is too large");
            cur = &mExchange;
            break;
        }

        case MessageType::TunnelShutdown:
        case MessageType::TunnelClose: {
            break;
        }

        case MessageType::WindowUpdate: {
            auto update = msg.cast<WindowUpdate>().value();
            cur->appendIntBE(update.size);
            break;
        }

        case MessageType::FatalError: {
            auto err = msg.cast<FatalError>().value();
            assert(err.msg.size() < 512 && "msg too long");

            cur->appendIntBE(err.code);
            cur->append(ilias::makeBuffer(err.msg));
            break;
        }

        default: {
            assert(false && "Unknown message type");
        }
    }
    // Store how many bytes we are used
    auto size = cur->size();

    // | Size | Type | Data |
    cur->prependIntBE(static_cast<std::uint8_t>(msg.type()));
    cur->prependIntBE(static_cast<std::uint16_t>(size));
    return *cur;
}

// MARK: Tunnel
struct HajimiTunnel {
    using Ptr = std::shared_ptr<HajimiTunnel>;

    HajimiTunnel(std::uint64_t streamId) : id(streamId) {
        auto [sender, receiver] = mpsc::channel<SkBuffer>(); // Bounded by recvWindow
        bytesSender = std::move(sender);
        bytesReceiver = std::move(receiver);
    }

    std::uint64_t id;
    std::size_t sendWindow = HAJIMI_TCP_INIT_WINDOW_SIZE;
    std::size_t recvWindow = HAJIMI_TCP_INIT_WINDOW_SIZE;
    std::uint32_t pendingCredit = 0; // Used to merge duplicate WindowUpdate

    bool readFin = false;
    bool writeFin = false;
    
    std::error_code error; // The error happened on the tunnel
    Event sendWindowUpdated{Event::AutoClear}; // Await by write

    // Channels used for receiver incoming data
    mpsc::Sender<SkBuffer> bytesSender;
    mpsc::Receiver<SkBuffer> bytesReceiver; // Await by read

    // Notify the blocked read/write task, should only called by resetTunnel and Transport::stop
    auto notifyClosed(std::error_code ec) {
        error = ec;
        readFin = true;
        writeFin = true;
        sendWindowUpdated.set(); // Wakeup the write
        bytesSender.close(); // Wakeup the read
    }
};

// MARK: TransportStream
struct HajimiTransportStream {
    struct MessagePair {
        std::uint64_t streamId;
        Message msg;
    };

    struct PendingTunnel {
        OpenTunnel request;
        HajimiTunnel::Ptr tunnel;
    };

    HajimiTransportStream(SkBufferPool &bufferPool, DynStream bytes, bool client) :
        pool(bufferPool), stream(std::move(bytes)), nextId(client ? 1 : 2) // Client starts with 1, server with 0 (0 for control messages)
    {
        auto [tunnelSender, tunnelReceiver] = mpsc::channel<PendingTunnel>();
        auto [outSender, outReceiver] = mpsc::channel<MessagePair>();
        auto [inSender, inReceiver] = mpsc::channel<ControlMessage>();
        outMessageSender = std::move(outSender);
        outMessageReceiver = std::move(outReceiver);
        inMessageSender = std::move(inSender);
        inMessageReceiver = std::move(inReceiver);
        tunnelPendingSender = std::move(tunnelSender);
        tunnelPendingReceiver = std::move(tunnelReceiver);
    }

    SkBufferPool &pool;
    DynStream     stream; // Used for multiplexing
    std::error_code error;
    Event closed;

    // Used for serializing the writeMessage
    mpsc::Sender<MessagePair> outMessageSender;
    mpsc::Receiver<MessagePair> outMessageReceiver;

    // Used for deserializing the readMessage
    mpsc::Sender<ControlMessage> inMessageSender;
    mpsc::Receiver<ControlMessage> inMessageReceiver;

    // Tunnels mapping
    std::map<std::uint64_t, HajimiTunnel::Ptr> tunnels; // tunnel still alive, not closed

    // Pending tunnels
    mpsc::Sender<PendingTunnel> tunnelPendingSender;
    mpsc::Receiver<PendingTunnel> tunnelPendingReceiver;

    // Current streamId
    std::uint64_t nextId;
    std::uint64_t lastPeerId = 0; // Used for verifying the protocl at currect state?

    // Status
    HajimiConnection::Status status;

    auto run() -> IoTask<void>;
    auto readLoop() -> IoTask<void>;
    auto writeLoop() -> IoTask<void>;
    auto postMessage(std::uint64_t id, Message msg) -> IoResult<void>;

    auto dispatchControl(Message msg) -> IoResult<void>;
    auto dispatchTunnel(std::uint64_t streamId, Message msg) -> IoResult<void>;

    auto resetTunnel(HajimiTunnel::Ptr tunnel) -> void;

    // Notify all substream, this connection is closed by ec
    auto stop(std::error_code ec) -> void;
};

#ifdef HAJIMI_QUIC
struct HajimiTransportQuic {
    SkBufferPool &pool;
    QuicConnection conn;  
};
#endif // HAJIMI_QUIC

} // namespace

// Union
struct HajimiConnection::Impl : std::variant<
    HajimiTransportStream
    // , HajimiTransportQuic
> { 
    using variant::variant;
};

struct HajimiStream::Impl : std::variant<
    HajimiTunnel::Ptr
    // , QuicStream
> { 
    using variant::variant;
};

// Hajimi over stream (tcp)
auto HajimiConnection::fromServer(SkBufferPool &pool, DynStream stream) -> IoTask<HajimiConnection> {
    HajimiConnection conn;
    conn.d = std::make_shared<Impl>(std::in_place_type<HajimiTransportStream>, pool, std::move(stream), false);
    co_return conn;
}

auto HajimiConnection::fromClient(SkBufferPool &pool, DynStream stream) -> IoTask<HajimiConnection> {
    HajimiConnection conn;
    conn.d = std::make_shared<Impl>(std::in_place_type<HajimiTransportStream>, pool, std::move(stream), true);
    co_return conn;
}

HajimiConnection::~HajimiConnection() {

}

auto HajimiConnection::readMessage() -> IoTask<ControlMessage> {
    auto &transport = std::get<HajimiTransportStream>(*d);
    if (transport.error) {
        co_return Err(transport.error);
    }

    auto msg = co_await transport.inMessageReceiver.recv();
    if (!msg) {
        co_return Err(std::make_error_code(std::errc::connection_aborted));
    }
    co_return std::move(*msg);
}

auto HajimiConnection::writeMessage(ControlMessage msg) -> IoTask<void> {
    auto conn = d;
    if (auto transport = std::get_if<HajimiTransportStream>(conn.get())) { // Stream
        co_return transport->postMessage(0, msg.toMessage());
    }
    else {
        co_return Err(std::make_error_code(std::errc::invalid_argument));
    }
}

auto HajimiConnection::acceptTunnel() -> IoTask<std::pair<OpenTunnel, HajimiStream>> {
    auto &transport = std::get<HajimiTransportStream>(*d);
    auto tun = co_await transport.tunnelPendingReceiver.recv();
    if (!tun) {
        co_return Err(std::make_error_code(std::errc::connection_aborted));
    }
    // Build it
    HajimiStream stream;
    stream.mConn = d;
    stream.d = std::make_unique<HajimiStream::Impl>(std::move(tun->tunnel));
    co_return std::pair {
        std::move(tun->request),
        std::move(stream)
    };
}

auto HajimiConnection::openTunnel(OpenTunnel open) -> IoTask<HajimiStream> {
    auto conn = d;
    auto &transport = std::get<HajimiTransportStream>(*conn);

    // Check precond
    if (transport.error) co_return Err(transport.error);
    if (open.addr.size() > HAJIMI_MAX_DATA_EXCHANGE - sizeof(open.port)) {
        co_return Err(std::make_error_code(std::errc::message_size));
    }
    if (transport.tunnels.size() >= MAX_TUNNELS) {
        co_return Err(std::make_error_code(std::errc::no_buffer_space));
    }
    if (transport.nextId >= UINT64_MAX - 1) {
        co_return Err(std::make_error_code(std::errc::value_too_large));
    }

    // Register it
    auto tunnel = std::make_shared<HajimiTunnel>(transport.nextId);
    transport.nextId += 2;
    transport.tunnels.emplace(tunnel->id, tunnel);

    // Local handle resets the tunnel if opening fails or is cancelled.
    HajimiStream stream;
    stream.mConn = conn;
    stream.d = std::make_unique<HajimiStream::Impl>(tunnel);
    // Queue Open synchronously so concurrent opens preserve stream ID order.
    ILIAS_CO_TRYV(transport.postMessage(tunnel->id, std::move(open)));
    co_return stream;
}

auto HajimiConnection::run() -> IoTask<void> {
    auto conn = d;
    auto &transport = std::get<HajimiTransportStream>(*conn);

    // Start loop
    auto [read, write] = co_await ilias::whenAny(
        transport.readLoop(), 
        transport.writeLoop()
    );
    if (read) co_return *read;
    if (write) co_return *write;
    co_return {};
}

auto HajimiConnection::status() const -> Status {
    return std::get<HajimiTransportStream>(*d).status;
}

HajimiStream::HajimiStream() = default;
HajimiStream::HajimiStream(HajimiStream &&) noexcept = default;
HajimiStream::~HajimiStream() { reset(); }

auto HajimiStream::reset() noexcept -> void {
    if (!d) return;
    auto tunnel = std::get<HajimiTunnel::Ptr>(*d);
    std::get<HajimiTransportStream>(*mConn).resetTunnel(tunnel);
}

auto HajimiStream::read() -> IoTask<SkBuffer> {
    auto &transport = std::get<HajimiTransportStream>(*mConn);
    auto tunnel = std::get<HajimiTunnel::Ptr>(*d);

    auto buf = co_await tunnel->bytesReceiver.recv();
    if (!buf) co_return SkBuffer{}; // FIN drains earlier data before EOF.

    // Return the credit back to the peer sender
    if (!tunnel->readFin) {
        auto size = static_cast<uint32_t>(buf->size());
        ILIAS_CO_TRYV(transport.postMessage(tunnel->id, WindowUpdate{size}));
        tunnel->recvWindow += size;
    }
    transport.status.bytesReceived += buf->size();
    co_return std::move(*buf);
}

auto HajimiStream::write(SkBuffer buf) -> IoTask<size_t> {
    if (buf.empty()) co_return 0;

    auto &transport = std::get<HajimiTransportStream>(*mConn);
    auto tunnel = std::get<HajimiTunnel::Ptr>(*d);
    auto bufSize = buf.size();

    // If cancel or failed, reset the stream
    ScopeExit cleanup{[&]() { 
        transport.resetTunnel(tunnel); 
    }};
    while (!buf.empty()) {
        // Waiting for the send window to updated
        while (tunnel->sendWindow == 0 && !tunnel->error && !tunnel->writeFin) {
            co_await tunnel->sendWindowUpdated.wait();
        }
        if (tunnel->error) co_return Err(tunnel->error);
        if (tunnel->writeFin) co_return 0;

        auto size = std::min({buf.size(), tunnel->sendWindow, std::size_t{HAJIMI_MAX_DATA_EXCHANGE}});

        SkBuffer part;
        if (size == buf.size() && buf.unique() && buf.headroom() >= HAJIMI_HEADER_ROOM) {
            part = std::exchange(buf, SkBuffer{}); // Fast path: prepend the frame header without copying.
        }
        else { // Copy :(
            part = transport.pool.allocate(HAJIMI_HEADER_ROOM + size);
            part.reserveHead(HAJIMI_HEADER_ROOM);
            part.append(buf.data().first(size));
            buf.consumeFront(size);
        }
        auto _ = transport.postMessage(tunnel->id, DataExchange {
            .data = std::move(part)
        });
        tunnel->sendWindow -= size;
    }
    cleanup.disarm();

    // Update the status
    transport.status.bytesSent += bufSize;
    co_return bufSize;
}

auto HajimiStream::shutdown() -> IoTask<void> {
    auto conn = mConn;
    auto &transport = std::get<HajimiTransportStream>(*mConn);
    auto tunnel = std::get<HajimiTunnel::Ptr>(*d);

    if (tunnel->error) co_return Err(tunnel->error);
    if (tunnel->writeFin) co_return {}; // Already shutdown
    
    auto _ = transport.postMessage(tunnel->id, TunnelShutdown{});
    co_return {};
}

auto HajimiStream::pool() const -> SkBufferPool & {
    assert(mConn);
    return std::get<HajimiTransportStream>(*mConn).pool;
}

// MARK: Over Stream
// Only the writer loop touches the underlying writable stream.
auto HajimiTransportStream::postMessage(std::uint64_t id, Message msg) -> IoResult<void> {
    if (error) return Err(error);
    auto _ = outMessageSender.trySend(MessagePair { 
        .streamId = id, 
        .msg = std::move(msg)
    });
    return {};
}

auto HajimiTransportStream::resetTunnel(HajimiTunnel::Ptr tunnel) -> void {
    tunnel->notifyClosed(std::make_error_code(std::errc::connection_reset));
    if (tunnels.erase(tunnel->id) == 1) { // Exists in map, send peer a TunnelClose
        auto _ = postMessage(tunnel->id, TunnelClose{});
    }
}

auto HajimiTransportStream::stop(std::error_code ec) -> void {
    error = ec;
    for (auto &[_, tunnel] : tunnels) {
        tunnel->notifyClosed(ec);
    }
    tunnelPendingSender.close();
    tunnels.clear();
    closed.set();
}

auto HajimiTransportStream::readLoop() -> IoTask<void> {
    constexpr auto HEADER_SIZE = HAJIMI_HEADER + sizeof(std::uint64_t);

    struct Header {
        std::uint64_t streamId;
        std::uint16_t size;
        std::uint8_t type;
    };
    // u64 streamId
    // u8[] message

    // message
    // u16 size
    // u8 type
    // u8[] data
    SkBuffer buf;
    while (true) {
        buf.clear();
        if (!buf) {
            buf = pool.allocate(HEADER_SIZE);
        }

        // Read the header
        auto headerBuf = buf.prepareBack(HEADER_SIZE);
        ILIAS_CO_TRYV(co_await stream.readAll(headerBuf));
        buf.commitBack(headerBuf.size());

        // Parse it
        auto header = [&]() -> std::optional<Header> {
            ILIAS_TRY(auto streamId, buf.popIntBE<std::uint64_t>());
            ILIAS_TRY(auto size, buf.popIntBE<std::uint16_t>());
            ILIAS_TRY(auto type, buf.popIntBE<std::uint8_t>());
            return Header {
                .streamId = streamId,
                .size = size,
                .type = type
            };
        }();

        if (!header) {
            co_return Err(std::make_error_code(std::errc::bad_message));
        }

        // Read the body of the message
        // Small frames should not retain a maximum-sized buffer in the receive queue.
        buf.clear();
        if (buf.capacity() < HAJIMI_HEADER_ROOM + header->size) {
            buf = pool.allocate(HAJIMI_HEADER_ROOM + header->size);
        }
        buf.reserveHead(HAJIMI_HEADER_ROOM);
        auto bodyBuf = buf.prepareBack(header->size);
        ILIAS_CO_TRYV(co_await stream.readAll(bodyBuf));
        buf.commitBack(bodyBuf.size());

        // Parse it
        auto msg = parseMessage(header->type, buf);
        if (!msg) {
            std::println(stderr, "[Transport] Invalid frame: stream={}, type={}, payload size={}", header->streamId, header->type, header->size);
            co_return Err(std::make_error_code(std::errc::bad_message));
        }

        // std::println("[Transport] Stream {} receive msg '{}'", header->streamId, static_cast<MessageType>(header->type));
        // Done
        if (header->streamId != 0) {
            ILIAS_CO_TRYV(dispatchTunnel(header->streamId, std::move(*msg)));
        }
        else {
            ILIAS_CO_TRYV(dispatchControl(std::move(*msg)));
        }
    }
}

auto HajimiTransportStream::dispatchControl(Message msg) -> IoResult<void> {
    auto type = msg.type();
    auto ctl = ControlMessage::fromMessage(std::move(msg));
    if (!ctl) {
        std::println("[Transport] Unknown control message: {}", std::to_underlying(type));
        return Err(std::make_error_code(std::errc::bad_message));
    }
    if (!inMessageSender.trySend(std::move(*ctl))) {
        return Err(std::make_error_code(std::errc::no_buffer_space));
    }
    return {};
}

auto HajimiTransportStream::dispatchTunnel(std::uint64_t streamId, Message msg) -> IoResult<void> {
    if (streamId == UINT64_MAX) return Err(std::make_error_code(std::errc::bad_message));
    // Try find
    auto it = tunnels.find(streamId);
    switch (msg.type()) {
        case MessageType::OpenTunnel: {
            ILIAS_TRY(auto open, msg.cast<OpenTunnel>());
            if (it != tunnels.end() || streamId <= lastPeerId || streamId % 2 == nextId % 2) {
                std::println("[Transport] Invalid tunnel ID: {}", streamId);
                return Err(std::make_error_code(std::errc::bad_message));
            }

            lastPeerId = streamId;
            if (tunnels.size() >= MAX_TUNNELS) {
                return postMessage(streamId, TunnelClose{});
            }
            if (!tunnelPendingSender) { // closed
                return {};
            }
            
            // Create one
            auto ptr = std::make_shared<HajimiTunnel>(streamId);
            tunnels.insert({streamId, ptr});
            auto _ = tunnelPendingSender.trySend({std::move(open), std::move(ptr)});
            return {};
        }
        case MessageType::DataExchange: {
            ILIAS_TRY(auto exchange, msg.cast<DataExchange>());
            if (it == tunnels.end()) { // Data may already be in flight when a tunnel is reset.
                return {}; // Just ignore it
            }

            // Send it
            auto [_, tunnel] = *it;
            if (tunnel->readFin || exchange.data.size() > tunnel->recvWindow) {
                std::println(
                    stderr, "[Transport] Invalid data: stream={}, size={}, receive window={}, peer FIN={}",
                    streamId, exchange.data.size(), tunnel->recvWindow, tunnel->readFin
                );
                return Err(std::make_error_code(std::errc::bad_message));
            }
           
            tunnel->recvWindow -= exchange.data.size();
            std::ignore = tunnel->bytesSender.trySend(std::move(exchange.data));
            return {};
        }
        case MessageType::WindowUpdate: {
            ILIAS_TRY(auto update, msg.cast<WindowUpdate>());
            // tunnel != end()
            if (it == tunnels.end()) {
                return {};
            }
            auto [_, tunnel] = *it;
            if (update.size == 0 || update.size > HAJIMI_TCP_INIT_WINDOW_SIZE - tunnel->sendWindow) {
                std::println(
                    stderr, "[Transport] Invalid window update: stream={}, credit={}, send window={}",
                    streamId, update.size, tunnel->sendWindow
                );
                return Err(std::make_error_code(std::errc::bad_message));
            }
            tunnel->sendWindow += update.size;
            tunnel->sendWindowUpdated.set();
            return {};
        }
        case MessageType::TunnelShutdown: {
            if (it == tunnels.end()) return {};
            auto tunnel = it->second;
            tunnel->readFin = true;
            tunnel->bytesSender = {}; // Queued data is still readable before EOF.
            return {};
        }
        case MessageType::TunnelClose: {
            if (it == tunnels.end()) return {}; // Close is idempotent.
            resetTunnel(it->second);
            return {};
        }
        default: {
            std::println("[Transport] Unexpected tunnel message: {} at stream {}", std::to_underlying(msg.type()), streamId);
            return Err(std::make_error_code(std::errc::bad_message));
        }
    }
}

auto HajimiTransportStream::writeLoop() -> IoTask<void> {
    MessageWriter writer{pool};
    while (auto pair = co_await outMessageReceiver.recv()) {
        auto &[streamId, msg] = *pair;
        auto &buf = writer.writeMessage(std::move(msg));

        // Write All
        buf.prependIntBE(streamId);
        ILIAS_CO_TRYV(co_await stream.writeAll(buf.data()));
        ILIAS_CO_TRYV(co_await stream.flush());
    }
    co_return {};
}

// MARK: Tunnel
// auto HajimiTransportStream::Tunnel::write(SkBuffer buf) -> IoTask<void> {
    
// }

// MARK: Over Quic
