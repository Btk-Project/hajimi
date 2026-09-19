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

    // Stream
    StreamView mStream;

    // State
    ilias::MutableBuffer mReadBuffer;
    ilias::MutableBuffer mWriteBuffer;
    Mutex mWriteMutex;

    // Channel for the closed id
    ilias::mpsc::Sender<uint64_t> mClosedTunnelIds;

    // Tunnel
    std::map<uint64_t, Tunnel::Ptr> mTunnels;

    // Pool
    std::pmr::unsynchronized_pool_resource mPool;

    // Workers
    auto readWorker() -> IoTask<void>;
    auto tunnelWorker(uint64_t streamId, std::string endpoint, Tunnel::Ptr tunnel) -> IoTask<void>;
    auto tunnelGCWorker(ilias::mpsc::Receiver<uint64_t> receiver) -> IoTask<void>; // Collect the id of the closed channel, send it to peer
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
    WriteBuffer writeBuffer;
    ReadBuffer readBuffer;

    std::println("[ProxyClient] Connected to master {}", mConfig.master);

    // Register and wait for the ack
    ILIAS_CO_TRYV(co_await writeMessage(stream, writeBuffer, Hello {
        .version = HAJIMI_VERSION,
        .name = mConfig.name,
    }));
    ILIAS_CO_TRY(auto msg, co_await readMessage(stream, readBuffer));
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
    auto [sender, receiver] = ilias::mpsc::channel<uint64_t>(); // Unbounded
    ClientState state {
        .mMaster = mConfig.master,
        .mStream = stream,
        .mReadBuffer = readBuffer,
        .mWriteBuffer = writeBuffer,
        .mClosedTunnelIds = sender,
    };
    co_return co_await TaskScope::enter([&](auto &scope) -> IoTask<void> {
        // If any error, we stop the whole scope
        state.mScope = &scope;
        auto _ = co_await ilias::whenAny(
            state.readWorker(),
            state.tunnelGCWorker(std::move(receiver))
            // tunnelGCWorker()
        );
        scope.stop();
        co_return {};
    });
}

auto ClientState::readWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await readMessage(mStream, mReadBuffer));
        switch (msg.type()) {
            case MessageType::Ping: { // Reply Pong
                auto lock = co_await mWriteMutex.lock();
                ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, Pong{}));
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
                BytesVector vec{&mPool};
                vec.assign(exchange.data.begin(), exchange.data.end());
                auto _ = co_await tunnel->bytesSender.send(std::move(vec));
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

auto ClientState::tunnelWorker(uint64_t streamId, std::string endpoint, Tunnel::Ptr tunnel) -> IoTask<void> {
    std::println("[ProxyClient] OpenTunnel '{}' => {}", streamId, endpoint);
    
    // Prepare cleanup
    ScopeExit exit{[&, this]() {
        auto it = mTunnels.find(streamId);
        if (it == mTunnels.end()) { // Handled for peer send the TunnelClose
            return;
        }
        mTunnels.erase(it);
        auto _ = mClosedTunnelIds.trySend(streamId); // Send peer an TunnelClose
    }};

    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(endpoint));
    ILIAS_CO_TRY(auto local, co_await happy_eyeballs::connect(info.endpoints()));

    // Got stream, begin copy
    auto readCopyWorker = [&]() -> IoTask<void> {
        std::byte storage[HAJIMI_MAX_DATA_EXCHANGE]; //<  Take short cut now
        std::span buffer{storage};
        while (true) {
            ILIAS_CO_TRY(auto left, co_await local.read(buffer));
            if (left == 0) { // EOF, Close!
                break;
            }
            // std::println("[ClientState] Tunnel '{}' read {} bytes data from local", streamId, left);
            
            // Send to master
            auto data = buffer.subspan(0, left);
            while (!data.empty()) {
                auto n = std::min(tunnel->sendWindow, data.size());
                if (n == 0) { // No space, waiting for it
                    co_await tunnel->sendWindowUpdated.wait();
                    continue;
                }

                auto lock = co_await mWriteMutex.lock();
                ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, DataExchange {
                    .streamId = streamId,
                    .data = data.subspan(0, n)
                }));

                // Advance
                tunnel->sendWindow -= n;
                data = data.subspan(n);
            }

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
            auto lock = co_await mWriteMutex.lock();
            ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, WindowUpdate {
                .streamId = streamId,
                .size = static_cast<uint32_t>(peerSum)
            }));
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

auto ClientState::tunnelGCWorker(ilias::mpsc::Receiver<uint64_t> receiver) -> IoTask<void> {
    while (auto streamId = co_await receiver.recv()) {
        std::println("[ProxyClient] Tunnel '{}' was closed, sending it to master", *streamId);
        auto lock = co_await mWriteMutex.lock();
        ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, TunnelClose {
            .streamId = *streamId
        }));
    }
    co_return {};
}