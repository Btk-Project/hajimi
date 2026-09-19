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
#include "utils.hpp"

// Import types
using ilias::ReadableView;
using ilias::WritableView;
using ilias::Buffer;
using ilias::TaskScope;
using ilias::Task;


// Internal state of the client
class ClientState {
public:
    // Take an shortcut, send an vector :)
    struct Tunnel {
        using Ptr = std::shared_ptr<Tunnel>;

        // The send window
        size_t sendWindow = HAJIMI_INIT_WINDOW_SIZE;
        ilias::Event sendWindowUpdated{ilias::Event::AutoClear};
        ilias::Event closed{}; //< Set when closed

        // For sending the data frame
        ilias::mpsc::Sender<BytesVector> bytesSender;
        ilias::mpsc::Receiver<BytesVector> bytesReceiver;
    };

    // ServerInfo
    std::string mMaster;

    // Scope
    TaskScope *mScope = nullptr;

    // Tunnel
    std::map<uint64_t, Tunnel::Ptr> mTunnels;

    // For posting message to write worker
    ilias::mpsc::Sender<Message> mMessageSender;

    // Pool
    std::pmr::unsynchronized_pool_resource mPool;

    // Workers
    auto readWorker(ReadableView, ReadBuffer &buf) -> IoTask<void>;
    auto writeWorker(WritableView, WriteBuffer &buf, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void>;
    auto tunnelWorker(uint64_t streamId, std::string endpoint, Tunnel::Ptr tunnel) -> IoTask<void>;
};


// MARK: Impl
ProxyClient::ProxyClient(Config config) : mConfig(config) {

}

ProxyClient::~ProxyClient() = default;

auto ProxyClient::run() -> IoTask<void> {
    std::println("[ProxyClient] client '{}' target master {}", mConfig.name, mConfig.master);

    // Reconnect loop with backoff: 2s, x1.5 on each failure, up to 15s
    std::chrono::seconds retryDelay{2};
    while (true) {
        bool registered = false;
        if (auto res = co_await connectOnce(registered); !res) {
            std::println("[ProxyClient] Connection failed: {}", res.error().message());
        }
        // Reset the delay once we registered successfully
        retryDelay = registered ? std::chrono::seconds(2) : std::min(retryDelay * 3 / 2, std::chrono::seconds(15));
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

    // Do handshake
    auto writeBuffer = std::make_unique<WriteBuffer>();
    auto readBuffer = std::make_unique<ReadBuffer>();

    std::println("[ProxyClient] Connected to master {}", mConfig.master);

    // Register and wait for the ack
    ILIAS_CO_TRYV(co_await writeMessage(stream, *writeBuffer, Hello {
        .version = HAJIMI_VERSION,
        .name = mConfig.name,
    }));
    ILIAS_CO_TRY(auto msg, co_await readMessage(stream, *readBuffer));
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
    ClientState state {
        .mMaster = mConfig.master,
        .mMessageSender = sender,
    };
    co_return co_await TaskScope::enter([&](auto &scope) -> IoTask<void> {
        // If any error, we stop the whole scope
        state.mScope = &scope;
        auto _ = co_await ilias::whenAny(
            state.readWorker(stream, *readBuffer),
            state.writeWorker(stream, *writeBuffer, std::move(receiver))
        );
        scope.stop();
        co_return {};
    });
}

auto ClientState::readWorker(ReadableView stream, ReadBuffer &readBuffer) -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await readMessage(stream, readBuffer));
        switch (msg.type()) {
            case MessageType::Ping: { // Reply Pong
                auto _ = mMessageSender.trySend(Pong{});
                continue;
            }
            case MessageType::OpenTunnel: { // Request to open tunnel
                ILIAS_CO_TRY(auto tunnel, msg.cast<OpenTunnel>());
                // Synchronously create and register channel before next message can arrive
                auto [sender, receiver] = ilias::mpsc::channel<BytesVector>();
                Tunnel::Ptr tunnelPtr {
                    new Tunnel { // In place
                        .bytesSender = std::move(sender),
                        .bytesReceiver = std::move(receiver)
                    }
                };
                mTunnels.emplace(tunnel.streamId, tunnelPtr);
                mScope->spawn(tunnelWorker(tunnel.streamId, tunnel.endpoint, tunnelPtr));
                continue;
            }
            case MessageType::TunnelClose: { // The tunnel was closed
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                auto it = mTunnels.find(close.streamId);
                if (it == mTunnels.end()) {
                    continue;
                }
                it->second->closed.set();
                mTunnels.erase(it); //< Unregister it, close the sender
                continue;
            }
            case MessageType::DataExchange: { // The tunnel has data
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                auto it = mTunnels.find(exchange.streamId);
                if (it == mTunnels.end()) {
                    std::println("[ProxyClient] DataExchange for Tunnel '{}' not found", exchange.streamId);
                    continue;
                }
                auto [streamId, tunnel] = *it;
                // Copy the buffer into vector
                auto _ = co_await tunnel->bytesSender.send(std::move(exchange.data));
                continue;
            }
            case MessageType::WindowUpdate: {
                ILIAS_CO_TRY(auto update, msg.cast<WindowUpdate>());
                auto it = mTunnels.find(update.streamId);
                if (it == mTunnels.end()) {
                    std::println("[ProxyClient] WindowUpdate for Tunnel '{}' not found", update.streamId);
                    continue;
                }
                auto [streamId, tunnel] = *it;
                tunnel->sendWindow += update.size;
                tunnel->sendWindowUpdated.set();
                continue;
            }
            default: {
                std::println("[ProxyClient] Unexpected type from {}, disconnect", mMaster);
                co_return {};
            }
        }
    }
}

auto ClientState::writeWorker(WritableView stream, WriteBuffer &writeBuffer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void> {
    while (auto msg = co_await receiver.recv()) {
        ILIAS_CO_TRYV(co_await writeMessage(stream, writeBuffer, std::move(*msg)));
    }
    co_return {};
}

auto ClientState::tunnelWorker(uint64_t streamId, std::string endpoint, Tunnel::Ptr tunnel) -> IoTask<void> {
    std::println("[ProxyClient] OpenTunnel '{}' => {}", streamId, endpoint);
    
    // Prepare cleanup
    ScopeExit exit{[&, this]() {
        std::println("[ProxyClient] Tunnel '{}' => {} closed", streamId, endpoint);
        if (mTunnels.contains(streamId)) {
            mTunnels.erase(streamId);
            auto _ = mMessageSender.trySend(TunnelClose {
                .streamId = streamId
            });
        }
    }};

    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(endpoint));
    ILIAS_CO_TRY(auto local, co_await happy_eyeballs::connect(info.endpoints()));

    // Got stream, begin copy
    auto readCopyWorker = [&]() -> IoTask<void> {
        std::array<std::byte, HAJIMI_MAX_DATA_EXCHANGE> storage;
        while (true) {
            while (tunnel->sendWindow == 0) { // Waiting for the send window
                co_await tunnel->sendWindowUpdated.wait();
            }
            auto buffer = std::span{storage}.subspan(
                0,
                std::min(tunnel->sendWindow, storage.size())
            ); // Read the number of bytes in the send window
            ILIAS_CO_TRY(auto n, co_await local.read(buffer));

            if (n == 0) { // EOF
                break;
            }
            buffer = buffer.subspan(0, n);
            auto _ = mMessageSender.trySend(DataExchange {
                .streamId = streamId,
                .data = BytesVector{buffer.begin(), buffer.end()}
            });
            tunnel->sendWindow -= n;
        }
        co_return {};
    };
    auto writeCopyWorker = [&]() -> IoTask<void> {
        size_t peerSum = 0;
        while (auto bytes = co_await tunnel->bytesReceiver.recv()) {
            // Send bytes to local stream
            // std::println("[ClientState] Tunnel '{}' write {} bytes data to local", streamId, bytes->size());
            ILIAS_CO_TRYV(co_await local.writeAll(*bytes));

            // Update the peer send window
            peerSum += bytes->size();
            if (peerSum < HAJIMI_INIT_WINDOW_SIZE / 2) {
                // Wait for peer custome more
                continue;
            }
            auto _ = mMessageSender.trySend(WindowUpdate {
                .streamId = streamId,
                .size = static_cast<uint32_t>(peerSum)
            });
            peerSum = 0;
        }
        // std::println("[ClientState] Tunnel '{}' => {} request to closed by master", streamId, endpoint);
        // Peer closed?
        co_return {};
    };
    auto _ = co_await ilias::whenAny(
        readCopyWorker(),
        writeCopyWorker(),
        tunnel->closed.wait()
    );
    co_return {};
}