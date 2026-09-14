#pragma once

// Wire protocol for Hajimi
#include <ilias/buffer.hpp>
#include <ilias/io.hpp>
#include <cassert>
#include <cstdint>
#include <string>
#include <print>
#include <array>
#include <span>
#include <bit>

// MARK: Protocol
// Current used version
#define HAJIMI_VERSION (uint16_t{0x1})
#define HAJIMI_HEADER (sizeof(uint16_t) + sizeof(uint8_t))
#define HAJIMI_STORAGE_SIZE (UINT16_MAX + HAJIMI_HEADER)
#define HAJIMI_MAX_DATA_EXCHANGE (UINT16_MAX - sizeof(uint64_t))

using WriteBuffer = std::array<std::byte, HAJIMI_STORAGE_SIZE>;
using ReadBuffer = std::array<std::byte, HAJIMI_STORAGE_SIZE>;

// Each message is packed by
// u16   size (payload length)
// u8    type
// u8 [] payload (whole Hello or DataExchange...)
//
// All messages (control and tunnel data) are multiplexed on the same
// TCP connection, every tunnel data frame is tagged with a u64 token.

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
// u64   token
// u8 [] host:port
struct OpenTunnel {
    uint64_t token;
    std::string host;
};

// OpenTunnel result from the client
// u64 token
// u32 size
// u8  ok
struct OpenTunnelAck {
    uint64_t token;
    uint32_t size; //< Initialize window size
    uint8_t  ok;
};

// DataExchange between client <-> server
// u64   token
// u8 [] data
struct DataExchange {
    uint64_t token;
    ilias::Buffer data; // View into the message storage (size <= HAJIMI_MAX_DATA_EXCHANGE)
};

// WindowUpdate between client <-> server
// u64 token
// u32 size
struct WindowUpdate {
    uint64_t token;
    uint32_t size; //< Incerase the sending window
};

// TunnelClose between client <-> server, the tunnel is gone (idempotent)
// u64   token
struct TunnelClose {
    uint64_t token;
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
    OpenTunnelAck = 6, // Client -> Server, 
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
        OpenTunnelAck,
        DataExchange,
        TunnelClose,
        FatalError
    >;

    Message(const Message &) = default;
    Message(Message &&) = default;

    // Construct inner
    template <typename T>
    Message(T &&v) : mStorage(std::forward<T>(v)) {}

    // Try cast to the target message, used when using macro
    template <typename T>
    auto cast() -> ilias::IoResult<T> {
        if (std::holds_alternative<T>(mStorage)) {
            return std::get<T>(mStorage);
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
            [](const OpenTunnelAck &) { return MessageType::OpenTunnelAck; },
            [](const DataExchange &) { return MessageType::DataExchange; },
            [](const TunnelClose &) { return MessageType::TunnelClose; },
            [](const WindowUpdate &) { return MessageType::WindowUpdate; },
            [](const FatalError &) { return MessageType::FatalError; },
        }, mStorage);
    }
private:
    Storage mStorage;
};

// MARK: Deserilize
// Read the header and payload into the storage, return the Message
template <ilias::Readable T>
inline auto readMessage(T &stream, ilias::MutableBuffer storage) -> ilias::IoTask<Message> {
    assert(storage.size_bytes() >= UINT16_MAX && "Ensure the storage is bigger than the max payload size");
    // TODO: Optomize the io calls
    ILIAS_CO_TRY(auto len, co_await stream.readUint16BE());
    ILIAS_CO_TRY(auto type, co_await stream.readUint8());
    auto span = storage.subspan(0, len);
    if (len != 0) {
        ILIAS_CO_TRYV(co_await stream.readAll(span));
    }

    ilias::MemReader reader{span};
    switch (static_cast<MessageType>(type)) {
        case MessageType::Hello: {
            std::string name;
            ILIAS_CO_TRY(auto version, co_await reader.readUint16BE());
            ILIAS_CO_TRYV(co_await reader.readToEnd(name));

            std::println("[Protocol] Hello: {}, {}", version, name);
            co_return Message {
                Hello {
                    .version = version,
                    .name = std::move(name),
                }
            };
        }
        case MessageType::HelloAck: {
            std::println("[Protocol] HelloAck");
            co_return Message { HelloAck{} };
        }
        case MessageType::Ping: {
            std::println("[Protocol] Ping");
            co_return Message { Ping{} };
        }
        case MessageType::Pong: {
            std::println("[Protocol] Pong");
            co_return Message { Pong{} };
        }
        case MessageType::OpenTunnel: {
            std::string host;
            ILIAS_CO_TRY(auto token, co_await reader.readUint64BE());
            ILIAS_CO_TRYV(co_await reader.readToEnd(host));
            std::println("[Protocol] OpenTunnel {} => {}", token, host);
            co_return Message {
                OpenTunnel {
                    .token = token,
                    .host = std::move(host)
                }
            };
        }
        case MessageType::TunnelClose: {
            ILIAS_CO_TRY(auto token, co_await reader.readUint64BE());
            std::println("[Protocol] TunnelClose {}", token);
            co_return Message {
                TunnelClose {
                    .token = token,
                }
            };
        }
        case MessageType::FatalError: {
            std::string msg;
            ILIAS_CO_TRYV(co_await reader.readToEnd(msg));
            std::println("[Protocol] FatalError {}", msg);
            co_return Message {
                FatalError {
                    .msg = std::move(msg)
                }  
            };
        }
        default: {
            std::println(stderr, "[Message] readMessage, invalid type: {}", type);
            co_return ilias::Err(std::make_error_code(std::errc::bad_message));
        }
    }
}

// MARK: Serilize
template <ilias::Writable T>
inline auto writeMessage(T &stream, ilias::MutableBuffer storage, Message msg) -> ilias::IoTask<void> {
    assert(storage.size_bytes() >= UINT16_MAX + HAJIMI_HEADER && "Ensure the storage is bigger than the max payload size + header");
    // The number of bytes of the message
    std::byte *end = storage.data() + HAJIMI_HEADER;
    uint16_t len = 0;

    // Utils lambda
    auto appendBytes = [&](ilias::Buffer bytes) {
        ::memcpy(end, bytes.data(), bytes.size());
        end += bytes.size();
        len += bytes.size();
    };
    auto appendInt = [&](auto i) {
        auto be = ilias::hostToNetwork(i);
        appendBytes(std::bit_cast<std::array<std::byte, sizeof(be)>>(be));
    };

    switch (msg.type()) {
        case MessageType::Hello: {
            auto hello = msg.cast<Hello>().value();
            appendInt(hello.version);
            appendBytes(ilias::makeBuffer(hello.name));
            break;
        }
        case MessageType::HelloAck: break; // No-payload
        case MessageType::Ping: break;
        case MessageType::Pong: break;

        case MessageType::OpenTunnel: {
            auto tunnel = msg.cast<OpenTunnel>().value();
            appendInt(tunnel.token);
            appendBytes(ilias::makeBuffer(tunnel.host));
            break;
        }

        case MessageType::OpenTunnelAck: {
            auto ack = msg.cast<OpenTunnelAck>().value();
            appendInt(ack.token);
            appendInt(ack.size);
            appendInt(ack.ok);
            break;
        }

        case MessageType::TunnelClose: {
            auto close = msg.cast<TunnelClose>().value();
            appendInt(close.token);
            break;
        }

        case MessageType::FatalError: {
            auto err = msg.cast<FatalError>().value();
            appendBytes(ilias::makeBuffer(err.msg));
            break;
        }

        default: { // Unsupported now
            assert(false && "TODO: Currently not impl");
        }
    }
    // Write the payload len
    uint8_t type = static_cast<uint8_t>(msg.type());

    // Combine to storage
    auto beLen = ilias::networkToHost(len); // To be
    ::memcpy(storage.data(), &beLen, sizeof(beLen));
    storage[sizeof(len)] = std::byte{type};

    auto write = storage.subspan(0, HAJIMI_HEADER + len);
    ILIAS_CO_TRYV(co_await stream.writeAll(write));
    ILIAS_CO_TRYV(co_await stream.flush());
    co_return {};
}
