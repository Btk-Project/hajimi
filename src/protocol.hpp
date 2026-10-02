#pragma once

// Wire protocol for Hajimi
#include <ilias/buffer.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <cassert>
#include <cstdint>
#include <string>
#include <print>
#include <array>
#include <span>
#include <bit>

#include "skbuf.hpp"

// MARK: Protocol
// Current used version (increase it of the protocol changes)
#define HAJIMI_VERSION (uint16_t{0x2})

// The header size 
#define HAJIMI_HEADER (sizeof(uint16_t) + sizeof(uint8_t))

// The max size of the message
#define HAJIMI_STORAGE_SIZE (UINT16_MAX + HAJIMI_HEADER)

// The max size of the payload on data exchange message
#define HAJIMI_MAX_DATA_EXCHANGE (UINT16_MAX - sizeof(uint64_t))

// The init window size of the stream
#define HAJIMI_INIT_WINDOW_SIZE (1024 * 128)

// The header room size (larger than the header size for more safe)
#define HAJIMI_HEADER_ROOM (HAJIMI_HEADER + sizeof(uint64_t) * 2)

// The config  of the buffer pool
constexpr auto HAJIMI_POOL_CONFIG = std::initializer_list<SkBufferPool::ClassInfo> {
    {   256, 128 },
    {   512,  64 },
    {  1024,  64 },
    {  2048,  32 },
    {  4096,  16 },
    {  8192,   8 },
    { 16384,   8 },
    { 32768,   4 },
    { 65600,   2 },
};

// Each message is packed by
// u16   size (payload length)
// u8    type
// u8 [] payload (whole Hello or DataExchange...)
//
// All messages (control and tunnel data) are multiplexed on the same
// TCP connection, every tunnel data frame is tagged with a u64 streamId.

// Hello from client
// u16   version
// u8 [] name
struct Hello {
    uint16_t version;
    std::string name;
};

// HelloAck from server, the registration is accepted (no payload)
struct HelloAck {};

// Ping from client
struct Ping {};
struct Pong {};

// OpenTunnel from server
// The tunnel is logical exist before the TunnelClose message
// u64   streamId
// u8 [] host:port
struct OpenTunnel {
    uint64_t streamId;
    std::string endpoint;
};

// DataExchange between client <-> server
// u64   streamId
// u8 [] data
struct DataExchange {
    uint64_t streamId;
    SkBuffer data; // The message of the data (must alloced with the headroom of HAJIMI_HEADER_ROOM)
};

// WindowUpdate between client <-> server
// u64 streamId
// u32 size
struct WindowUpdate {
    uint64_t streamId;
    uint32_t size; //< Incerase the sending window
};

// TunnelClose between client <-> server, the tunnel is gone (idempotent)
// u64   streamId
struct TunnelClose {
    uint64_t streamId;
};

// FatalError between client <-> server, if received, the connection is gone
// u8 [] msg
struct FatalError {
    std::string msg;
};

// The type of the Message
enum class MessageType : uint8_t {
    Hello         = 1, // Client -> Server, register
    HelloAck      = 2, // Server -> Client, registration accepted
    Ping          = 3, // Both, heartbeat ping
    Pong          = 4, // Both, heartbeat ack
    OpenTunnel    = 5, // Server -> Client, Try Open a tunnel
    DataExchange  = 7, // Both, tunnel data
    TunnelClose   = 8, // Both, tunnel closed
    WindowUpdate  = 9, // Both, tunnel send window update
    FatalError    = 255,
};

// MARK: Utils
template <typename ...Ts>
struct Overloads : Ts... { using Ts::operator()...; };

// The message union
struct Message {
public:
    using Storage = std::variant<
        Hello,
        HelloAck,
        Ping,
        Pong,
        OpenTunnel,
        DataExchange,
        TunnelClose,
        WindowUpdate,
        FatalError
    >;

    Message(const Message &) = delete;
    Message(Message &&) = default;

    // Construct inner
    template <typename T>
    Message(T &&v) : mStorage(std::forward<T>(v)) {}

    // Try cast to the target message, used when using macro
    template <typename T>
    auto cast() -> ilias::IoResult<T> {
        if (std::holds_alternative<T>(mStorage)) {
            return std::get<T>(std::move(mStorage));
        }
        // Doesn't expected
        return ilias::Err(std::make_error_code(std::errc::bad_message));
    }

    // Get the type of the message
    auto type() const noexcept -> MessageType {
        return std::visit(Overloads {
            [](const Hello &) { return MessageType::Hello; },
            [](const HelloAck &) { return MessageType::HelloAck; },
            [](const Ping &) { return MessageType::Ping; },
            [](const Pong &) { return MessageType::Pong; },
            [](const OpenTunnel &) { return MessageType::OpenTunnel; },
            [](const DataExchange &) { return MessageType::DataExchange; },
            [](const TunnelClose &) { return MessageType::TunnelClose; },
            [](const WindowUpdate &) { return MessageType::WindowUpdate; },
            [](const FatalError &) { return MessageType::FatalError; },
        }, mStorage);
    }

    // Operator
    auto operator =(Message &&) -> Message & = default;
private:
    Storage mStorage;
};

// MARK: Deserilize
class MessageReader {
public:
    MessageReader(ilias::ReadableView stream, SkBufferPool &pool) : mStream(stream), mPool(pool) {}
    MessageReader(MessageReader &&) = default;

    // Read the header and payload into the storage, return the Message
    auto readMessage() -> ilias::IoTask<Message>;
private:
    static auto parse(uint8_t type, SkBuffer &buffer) -> std::optional<Message>;

    ilias::ReadableView mStream;
    SkBufferPool       &mPool; // The pool for the message
    SkBuffer            mBuffer; // The current used buffer
};

inline auto MessageReader::readMessage() -> ilias::IoTask<Message> {
    // TODO: Optomize the io calls
    std::byte header[HAJIMI_HEADER] {};
    ILIAS_CO_TRYV(co_await mStream.readAll(header));

    uint16_t len = std::to_integer<uint16_t>(header[0]) << 8 | std::to_integer<uint16_t>(header[1]);
    uint8_t type = std::to_integer<uint8_t>(header[2]);

    if (len != 0) {
        if (mBuffer.capacity() < len) {
            mBuffer = mPool.allocate(len);
        }
        mBuffer.clear();
        auto span = mBuffer.prepareBack(len);
        ILIAS_CO_TRYV(co_await mStream.readAll(span));
        mBuffer.commitBack(len);
    }
    if (auto msg = parse(type, mBuffer)) {
        co_return std::move(*msg);
    }
    co_return ilias::Err(std::make_error_code(std::errc::bad_message));
}

inline auto MessageReader::parse(uint8_t type, SkBuffer &buffer) -> std::optional<Message> {
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
            ILIAS_TRY(auto streamId, buffer.popIntBE<uint64_t>());
            ILIAS_TRY(auto endpoint, buffer.popString());
            // std::println("[Protocol] OpenTunnel {} => {}", streamId, endpoint);
            return Message {
                OpenTunnel {
                    .streamId = streamId,
                    .endpoint = std::move(endpoint)
                }
            };
        }

        case MessageType::TunnelClose: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<uint64_t>());
            // std::println("[Protocol] TunnelClose {}", streamId);
            return Message {
                TunnelClose {
                    .streamId = streamId,
                }
            };
        }

        case MessageType::DataExchange: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<uint64_t>());
            // std::println("[Protocol] DataExchange {}, {} bytes", streamId, data.size());
            return Message {
                DataExchange {
                    .streamId = streamId,
                    .data = std::exchange(buffer, SkBuffer{}),
                }
            };
        }

        case MessageType::WindowUpdate: {
            ILIAS_TRY(auto streamId, buffer.popIntBE<uint64_t>());
            ILIAS_TRY(auto size, buffer.popIntBE<uint32_t>());
            // std::println("[Protocol] WindowUpdate {}, {} bytes", streamId, size);
            return Message {
                WindowUpdate {
                    .streamId = streamId,
                    .size = size
                }
            };
        }

        case MessageType::FatalError: {
            ILIAS_TRY(auto msg, buffer.popString());
            // std::println("[Protocol] FatalError {}", msg);
            return Message {
                FatalError {
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

// MARK: Serilize
class MessageWriter {
public:
    MessageWriter(ilias::WritableView stream, SkBufferPool &pool) : mStream(stream), mPool(pool) {
        mBuffer = mPool.allocate(256); // A Small buffer for tiny message
    }
    MessageWriter(MessageWriter &&) = default;

    auto writeMessage(Message msg) -> ilias::IoTask<void>;
private:
    ilias::WritableView  mStream;
    SkBufferPool        &mPool; // The pool for the message
    SkBuffer             mBuffer;
};

inline auto MessageWriter::writeMessage(Message msg) -> ilias::IoTask<void> {
    // The number of bytes of the message
    mBuffer.clear();
    mBuffer.reserveHead(HAJIMI_HEADER_ROOM);

    switch (msg.type()) {
        case MessageType::Hello: {
            auto hello = msg.cast<Hello>().value();
            mBuffer.appendIntBE(hello.version);
            mBuffer.append(ilias::makeBuffer(hello.name));
            break;
        }
        case MessageType::HelloAck: break; // No-payload
        case MessageType::Ping: break;
        case MessageType::Pong: break;

        case MessageType::OpenTunnel: {
            auto tunnel = msg.cast<OpenTunnel>().value();
            mBuffer.appendIntBE(tunnel.streamId);
            mBuffer.append(ilias::makeBuffer(tunnel.endpoint));
            break;
        }
         case MessageType::DataExchange: {
            auto exchange = msg.cast<DataExchange>().value();
            auto buffer = std::exchange(exchange.data, SkBuffer{});
            // We just use the message buffer, avoid the copy
            // len | type | streamId | data |
            assert(buffer.headroom() >= HAJIMI_HEADER_ROOM && "This buffer is too small");
            buffer.prependIntBE(exchange.streamId);
            auto size = buffer.size(); // The size only contains streamId + data

            buffer.prependIntBE(static_cast<uint8_t>(MessageType::DataExchange));
            buffer.prependIntBE(static_cast<uint16_t>(size));

            // Write it
            ILIAS_CO_TRYV(co_await mStream.writeAll(buffer.data()));
            co_return co_await mStream.flush();
        }

        case MessageType::TunnelClose: {
            auto close = msg.cast<TunnelClose>().value();
            mBuffer.appendIntBE(close.streamId);
            break;
        }

        case MessageType::WindowUpdate: {
            auto update = msg.cast<WindowUpdate>().value();
            mBuffer.appendIntBE(update.streamId);
            mBuffer.appendIntBE(update.size);
            break;
        }

        case MessageType::FatalError: {
            auto err = msg.cast<FatalError>().value();
            mBuffer.append(ilias::makeBuffer(err.msg));
            break;
        }

        default: { // Unsupported now
            assert(false && "TODO: Currently not impl");
        }
    }
    // Store how many bytes we are used
    auto size = mBuffer.size();

    // | Size | Type | Data |
    mBuffer.prependIntBE(static_cast<uint8_t>(msg.type()));
    mBuffer.prependIntBE(static_cast<uint16_t>(size));

    // Write a;;
    ILIAS_CO_TRYV(co_await mStream.writeAll(mBuffer.data()));
    ILIAS_CO_TRYV(co_await mStream.flush());
    co_return {};
}
