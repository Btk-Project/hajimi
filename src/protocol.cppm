// Wire protocol for Hajimi

export module hajimi.protocol;
import std;
import ilias;
import hajimi.skbuf;

// MARK: Protocol
// Current used version (increase it of the protocol changes)
export inline constexpr auto HAJIMI_VERSION = std::uint16_t{0x2};

// The header size 
export inline constexpr auto HAJIMI_HEADER = sizeof(std::uint16_t) + sizeof(std::uint8_t);

// The max size of the message
export inline constexpr auto HAJIMI_STORAGE_SIZE = std::numeric_limits<std::uint16_t>::max() + HAJIMI_HEADER;

// The max size of the payload on data exchange message
export inline constexpr auto HAJIMI_MAX_DATA_EXCHANGE = std::numeric_limits<std::uint16_t>::max() - sizeof(std::uint64_t);

// The init window size of the stream
export inline constexpr auto HAJIMI_INIT_WINDOW_SIZE = std::size_t{1024} * 128;

// The header room size (larger than the header size for more safe)
export inline constexpr auto HAJIMI_HEADER_ROOM = HAJIMI_HEADER + sizeof(std::uint64_t) * 2;

// The config  of the buffer pool
export inline constexpr auto HAJIMI_POOL_CONFIG = std::initializer_list<SkBufferPool::ClassInfo> {
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
export struct Hello {
    std::uint16_t version;
    std::string name;
};

// HelloAck from server, the registration is accepted (no payload)
export struct HelloAck {};

// Ping from client
export struct Ping {};
export struct Pong {};

// OpenTunnel from server
// The tunnel is logical exist before the TunnelClose message
// u64   streamId
// u8 [] host:port
export struct OpenTunnel {
    std::uint64_t streamId;
    std::string endpoint;
};

// DataExchange between client <-> server
// u64   streamId
// u8 [] data
export struct DataExchange {
    std::uint64_t streamId;
    SkBuffer data; // The message of the data (must alloced with the headroom of HAJIMI_HEADER_ROOM if send to writeMessage)
};

// WindowUpdate between client <-> server
// u64 streamId
// u32 size
export struct WindowUpdate {
    std::uint64_t streamId;
    std::uint32_t size; //< Incerase the sending window
};

// TunnelClose between client <-> server, the tunnel is gone (idempotent)
// u64   streamId
export struct TunnelClose {
    std::uint64_t streamId;
};

// FatalError between client <-> server, if received, the connection is gone
// u8 [] msg
export struct FatalError {
    std::string msg;
};

// The type of the Message
export enum class MessageType : std::uint8_t {
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
export struct Message {
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

    inline Message(const Message &) = delete;
    inline Message(Message &&) = default;

    // Construct inner
    template <typename T>
    inline Message(T &&v) : mStorage(std::forward<T>(v)) {}

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
    inline auto type() const noexcept -> MessageType {
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
export class MessageReader {
public:
    inline MessageReader(ilias::ReadableView stream, SkBufferPool &pool) : mStream(stream), mPool(pool) {}
    inline MessageReader(MessageReader &&) = default;

    // Read the header and payload into the storage, return the Message
    auto readMessage() -> ilias::IoTask<Message>;
private:
    static auto parse(std::uint8_t type, SkBuffer &buffer) -> std::optional<Message>;

    ilias::ReadableView mStream;
    SkBufferPool       &mPool; // The pool for the message
    SkBuffer            mBuffer; // The current used buffer
};

// MARK: Serilize
export class MessageWriter {
public:
    inline MessageWriter(ilias::WritableView stream, SkBufferPool &pool) : mStream(stream), mPool(pool) {
        mBuffer = mPool.allocate(256); // A Small buffer for tiny control message
    }
    inline MessageWriter(MessageWriter &&) = default;

    auto writeMessage(Message msg) -> ilias::IoTask<void>;
private:
    ilias::WritableView  mStream;
    SkBufferPool        &mPool; // The pool for the message
    SkBuffer             mBuffer;
};
