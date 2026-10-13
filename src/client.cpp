#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <chrono>
#include <print>
#include <map>

#include "protocol.hpp"
#include "client.hpp"
#include "tunnel.hpp"
#include "skbuf.hpp"
#include "utils.hpp"

// Import types
using ilias::DynStream;
using ilias::Buffer;
using ilias::TaskScope;
using ilias::Task;


// Internal state of the client
class ClientState {
public:
    // ServerInfo
    std::string mMaster;

    // Scope
    TaskScope &mScope;

    // Tunnel
    std::uint64_t mStreamId = 1; // Self increase
    TunnelManager mTunnels;

    // For posting message to write worker
    ilias::mpsc::Sender<Message> mMessageSender;

    // Workers
    auto readWorker(MessageReader &reader) -> IoTask<void>;
    auto writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void>;
};


// MARK: Impl
ProxyClient::ProxyClient(Config config) : mConfig(config) {

}

ProxyClient::~ProxyClient() = default;

auto ProxyClient::run() -> IoTask<void> {
    using namespace std::literals;
    using std::chrono::seconds;

    std::println("[ProxyClient] client '{}' target master {}", mConfig.name, mConfig.master);

    // Reconnect loop with backoff: 2s, x1.5 on each failure, up to 15s
    seconds retryDelay = 2s;
    while (true) {
        bool registered = false;
        if (auto res = co_await connectOnce(registered); !res) {
            std::println("[ProxyClient] Connection failed: {}", res.error().message());
        }
        // Reset the delay once we registered successfully
        retryDelay = registered ? 2s : std::min(retryDelay * 3 / 2, 15s);
        co_await ilias::sleep(retryDelay);
    }
    co_return {};
}

auto ProxyClient::connectOnce(bool &registered) -> IoTask<void> {
    using namespace std::literals;

    std::println("[ProxyClient] Connecting to master {}...", mConfig.master);
    // Resolve the master address, it may be a domain name (re-resolve on each reconnect)
    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(mConfig.master));
    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
    auto _ = stream.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm

    // Do handshake
    SkBufferPool pool{HAJIMI_POOL_CONFIG};
    MessageReader reader{stream, pool};
    MessageWriter writer{stream, pool};

    std::println("[ProxyClient] Connected to master {}", mConfig.master);

    // Register and wait for the ack
    ILIAS_CO_TRYV(co_await writer.writeMessage(Hello {
        .version = HAJIMI_VERSION,
        .name = mConfig.name,
    }));
    ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
    if (msg.type() == MessageType::FatalError) {
        std::println("[ProxyClient] Master rejected the registration, Fatal error from master: {}", msg.cast<FatalError>().value().msg);
        co_return {};
    }
    if (msg.type() != MessageType::HelloAck) {
        std::println("[ProxyClient] Invalid message from master");
        co_return {};
    }
    registered = true;
    std::println("[ProxyClient] Registered as '{}'", mConfig.name);

    // Begin the loop
    // Prepare channel
    auto [sender, receiver] = ilias::mpsc::channel<Message>(); //< Unbound
    TaskScope scope;
    ClientState state {
        .mMaster = mConfig.master,
        .mScope = scope,
        .mTunnels = TunnelManager {
            scope,
            pool,
            sender
        },
        .mMessageSender = sender
    };
    auto main = [&]() -> IoTask<void> {
        // If any error, we stop the whole scope
        auto _ = co_await ilias::whenAny(
            state.readWorker(reader),
            state.writeWorker(writer, std::move(receiver))
        );
        scope.stop();
        co_return {};
    };
    co_await ilias::whenAll(main(), scope.waitAll());
    co_return {};
}

auto ClientState::readWorker(MessageReader &reader) -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
        switch (msg.type()) {
            case MessageType::Ping: { // Reply Pong
                auto _ = mMessageSender.trySend(Pong{});
                continue;
            }
            case MessageType::OpenTunnel: { // Request to open tunnel
                ILIAS_CO_TRY(auto tunnel, msg.cast<OpenTunnel>());
                if (tunnel.streamId % 2 != 0) { // Is not even, protocol error
                    std::println("[ProxyClient] Invalid streamId '{}' from master", tunnel.streamId);
                    co_return {};
                }
                mTunnels.addTunnel(tunnel.streamId, [endpoint = tunnel.endpoint]() -> IoTask<DynStream> {
                    ILIAS_CO_TRY(auto info , co_await ilias::AddressInfo::lookup(endpoint));
                    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
                    auto _ = stream.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
                    co_return stream;
                });
                continue;
            }
            case MessageType::TunnelClose: { // The tunnel was closed
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                mTunnels.closeTunnel(close.streamId);
                continue;
            }
            case MessageType::DataExchange: { // The tunnel has data
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                mTunnels.onDataExchange(exchange.streamId,std::move(exchange.data));
                continue;
            }
            case MessageType::WindowUpdate: {
                ILIAS_CO_TRY(auto update, msg.cast<WindowUpdate>());
                mTunnels.onWindowUpdate(update.streamId, update.size);
                continue;
            }
            default: {
                std::println("[ProxyClient] Unexpected type from {}, disconnect", mMaster);
                co_return {};
            }
        }
    }
}

auto ClientState::writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void> {
    while (auto msg = co_await receiver.recv()) {
        ILIAS_CO_TRYV(co_await writer.writeMessage(std::move(*msg)));
    }
    co_return {};
}