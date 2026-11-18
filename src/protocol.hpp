#pragma once

// Wire protocol for Hajimi
#include <ilias/buffer.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <cassert>
#include <cstdint>
#include <string>
#include <format>
#include <array>
#include <span>
#include <bit>

#include "utils/overloads.hpp"
#include "skbuf.hpp"

// MARK: Protocol
// Current used version (increase it of the protocol changes)
constexpr auto HAJIMI_VERSION = std::uint16_t{0x4};

// The header size 
constexpr auto HAJIMI_HEADER = sizeof(std::int16_t) + sizeof(std::uint8_t);

// The max size of the payload on data exchange message
constexpr auto HAJIMI_MAX_DATA_EXCHANGE = UINT16_MAX;

// The max size of the message in bytes
constexpr auto HAJIMI_MAX_MESSAGE_SIZE = HAJIMI_MAX_DATA_EXCHANGE + HAJIMI_HEADER;

// The init window size of the stream (only used in tcp)
constexpr auto HAJIMI_TCP_INIT_WINDOW_SIZE = std::size_t{1024} * 256;

// The header room size (larger than the header size for more safe)
constexpr auto HAJIMI_HEADER_ROOM = HAJIMI_HEADER + sizeof(std::uint64_t) * 2;

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

// Tcp Mux / Demux
// u64   streamId (UINT64_MAX for datagram, 0 for control, client -> server 1, 3, 5, ... server -> client 2, 4, 6, ...)
// u8 [] message

// Each message is packed by
// u16   size (payload length)
// u8    type
// u8 [] payload (whole Hello or DataExchange...)

// The first stream opened in the connection is the control stream

// Hello from client
// u16   version
// u8 [] name
struct Hello {
    std::uint16_t version;
    std::string   name;
};

// HelloAck from server, the registration is accepted (no payload)
struct HelloAck {};

// Ping from client
struct Ping {};
struct Pong {};

// OpenTunnel between client <-> server
// The tunnel is logical exist before the TunnelClose message
// u8 [] host
// u16   port
struct OpenTunnel {
    std::string   addr;
    std::uint16_t port;
};

// DataExchange between client <-> server
// u8 [] data
struct DataExchange {
    SkBuffer data; // The message of the data (must alloced with the headroom of HAJIMI_HEADER_ROOM if send to writeMessage)
};

// WindowUpdate between client <-> server
// u32 size
struct WindowUpdate {
    std::uint32_t size; //< Incerase the sending window
};

// TunnelClose between client <-> server, the tunnel is gone (idempotent)
struct TunnelClose {};

// TunnelShutdown between client <-> server, the send part of the tunnel is gone (half closed)
struct TunnelShutdown {};

// FatalError between client <-> server, if received, the connection is gone
// u32 code
// u8 [] msg
struct FatalError {
    std::uint32_t code;
    std::string   msg;
};

// TODO:
struct DatagramExchange {

};

// The type of the Message
enum class MessageType : uint8_t {
    // Control
    Hello         = 1,   // Client -> Server, register
    HelloAck      = 2,   // Server -> Client, registration accepted
    Ping          = 3,   // Both, heartbeat ping
    Pong          = 4,   // Both, heartbeat ack
    FatalError    = 255, // Both, fatal error

    // Tunnel
    OpenTunnel     = 10,   // Server <-> Client, Try Open a tunnel

    // Tunnel, Tcp Only
    DataExchange   = 100, // Both, tunnel data
    TunnelClose    = 101, // Both, tunnel closed
    TunnelShutdown = 102, // Both, tunnel shutdown
    WindowUpdate   = 103, // Both, tunnel send window update  
};

// The message union
struct Message {
public:
    using Storage = std::variant<
        Hello,
        HelloAck,
        Ping,
        Pong,
        FatalError,
        OpenTunnel,
        DataExchange,
        TunnelClose,
        TunnelShutdown,
        WindowUpdate
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
            [](const FatalError &) { return MessageType::FatalError; },
            [](const OpenTunnel &) { return MessageType::OpenTunnel; },
            [](const DataExchange &) { return MessageType::DataExchange; },
            [](const TunnelClose &) { return MessageType::TunnelClose; },
            [](const TunnelShutdown &) { return MessageType::TunnelShutdown; },
            [](const WindowUpdate &) { return MessageType::WindowUpdate; },
        }, mStorage);
    }

    // Visit the message
    template<class F>
    decltype(auto) visit(this auto&& self, F &&f) {
        return std::visit(
            std::forward<F>(f),
            std::forward<decltype(self)>(self).mStorage
        );
    }

    // Operator
    auto operator =(Message &&) -> Message & = default;
private:
    Storage mStorage;
};

/**
 * @brief The control message
 * 
 */
class ControlMessage {
public:
    using Storage = std::variant<
        Hello,
        HelloAck,
        Ping,
        Pong,
        FatalError
    >;

    ControlMessage(const ControlMessage &) = delete;
    ControlMessage(ControlMessage &&) = default;

    // Construct inner
    template <typename T>
    ControlMessage(T &&v) : mStorage(std::forward<T>(v)) {}

    // As same as message
    template <typename T>
    auto cast() -> ilias::IoResult<T> {
        if (std::holds_alternative<T>(mStorage)) {
            return std::get<T>(std::move(mStorage));
        }
        // Doesn't expected
        return ilias::Err(std::make_error_code(std::errc::bad_message));
    }

    // Visit the message
    template <typename T>
    auto visit(T &&visitor) {
        return std::visit(std::forward<T>(visitor), mStorage);
    }

    auto type() const noexcept -> MessageType {
        return std::visit(Overloads {
            [](const Hello &) { return MessageType::Hello; },
            [](const HelloAck &) { return MessageType::HelloAck; },
            [](const Ping &) { return MessageType::Ping; },
            [](const Pong &) { return MessageType::Pong; },
            [](const FatalError &) { return MessageType::FatalError; },
        }, mStorage);
    }

    // Operator
    auto operator =(ControlMessage &&) -> ControlMessage & = default;

    // Cast to Message, message is a superset of control message
    auto toMessage() -> Message {
        return std::visit(Overloads {
            [](Hello v) { return Message{std::move(v)}; },
            [](HelloAck v) { return Message{std::move(v)}; },
            [](Ping v) { return Message{std::move(v)}; },
            [](Pong v) { return Message{std::move(v)}; },
            [](FatalError v) { return Message{std::move(v)}; },
        }, std::move(mStorage));
    }

    static auto fromMessage(Message msg) -> std::optional<ControlMessage> {
        return std::move(msg).visit(Overloads {
            [](Hello v) -> std::optional<ControlMessage> { return std::move(v); },
            [](HelloAck v) -> std::optional<ControlMessage> { return std::move(v); },
            [](Ping v) -> std::optional<ControlMessage> { return std::move(v); },
            [](Pong v) -> std::optional<ControlMessage> { return std::move(v); },
            [](FatalError v) -> std::optional<ControlMessage> { return std::move(v); },
            [](auto &&) -> std::optional<ControlMessage> { return std::nullopt; },
        });
    }
private:
    Storage mStorage;
};

// Print for MessageType
template <>
struct std::formatter<MessageType> {
    constexpr auto parse(std::format_parse_context &ctxt) -> decltype(ctxt.begin()) {
        return ctxt.begin();
    }

    auto format(MessageType type, std::format_context &ctxt) const -> decltype(ctxt.out()) {
        auto str = [&]() -> std::string_view {
            switch (type) {
                case MessageType::Hello: return "Hello";
                case MessageType::HelloAck: return "HelloAck";
                case MessageType::Ping: return "Ping";
                case MessageType::Pong: return "Pong";
                case MessageType::FatalError: return "FatalError";
                case MessageType::OpenTunnel: return "OpenTunnel";
                case MessageType::DataExchange: return "DataExchange";
                case MessageType::TunnelClose: return "TunnelClose";
                case MessageType::TunnelShutdown: return "TunnelShutdown";
                case MessageType::WindowUpdate: return "WindowUpdate";
                default: return "Unknown";
            }
        }();
        return std::format_to(ctxt.out(), "{}", str);
    }
};