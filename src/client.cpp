#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <memory>
#include <chrono>
#include <print>
#include <array>
#include <map>
#include <bit>

#include "protocol.hpp"
#include "client.hpp"
#include "utils.hpp"
#include <cstring>

// Import types
using ilias::Buffer;
using ilias::TaskScope;
using ilias::Task;

// Take an shortcut, send an vector :)
using BytesVector = std::pmr::vector<std::byte>;

// Internal state of the client
class ClientState {
public:
    using Tunnel = ilias::mpsc::Sender<BytesVector>; // Send data to the tunnel

    // ServerInfo
    std::string mMaster;

    // Scope
    TaskScope &mScope;

    // Stream
    StreamView mStream;

    // State
    ilias::MutableBuffer mReadBuffer;
    ilias::MutableBuffer mWriteBuffer;
    Mutex mWriteMutex;

    // Channel for the closed id
    ilias::mpsc::Sender<uint64_t> mClosedTunnelIds;

    // Tunnel
    std::map<uint64_t, Tunnel> mTunnels;

    // Workers
    auto readWorker() -> IoTask<void>;
    auto tunnelWorker(uint64_t token, std::string host) -> IoTask<void>;
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
    co_return co_await TaskScope::enter([&](auto &scope) -> IoTask<void> {
        // Prepare channel
        auto [sender, receiver] = ilias::mpsc::channel<uint64_t>(); // Unbounded

        // If any error, we stop the whole scope
        ClientState state {
            .mMaster = mConfig.master,
            .mScope = scope,
            .mStream = stream,
            .mReadBuffer = readBuffer,
            .mWriteBuffer = writeBuffer,
            .mClosedTunnelIds = sender,
        };

        // Do it
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
                mScope.spawn(tunnelWorker(tunnel.token, tunnel.host)); // Add to dynmaic scope
                continue;
            }
            case MessageType::TunnelClose: { // The tunnel was closed
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                auto it = mTunnels.find(close.token);
                if (it == mTunnels.end()) {
                    continue;
                }
                mTunnels.erase(it); //< Unregister it, close the sender
                continue;
            }
            case MessageType::DataExchange: { // The tunnel has data
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                auto it = mTunnels.find(exchange.token);
                if (it == mTunnels.end()) {
                    continue;
                }
                auto [token, tunnel] = *it;
                // Copy the buffer into vector
                BytesVector vec{};
                vec.assign(exchange.data.begin(), exchange.data.end());
                auto _ = co_await tunnel.send(std::move(vec));
                continue;
            }
            default: {
                std::println("[ProxyClient] Unexpected type from {}, disconnect", mMaster);
                co_return {};
            }
        }
    }
}

auto ClientState::tunnelWorker(uint64_t token, std::string host) -> IoTask<void> {
    std::println("[ProxyClient] OpenTunnel '{}' => {}", token, host);
    
    // Prepare state
    auto [sender, receiver] = ilias::mpsc::channel<BytesVector>();
    mTunnels.emplace(token, std::move(sender));

    // Prepare cleanup
    ScopeExit exit{[&]() {
        auto it = mTunnels.find(token);
        if (it == mTunnels.end()) { // Handled for peer send the TunnelClose
            return;
        }
        auto _ = mClosedTunnelIds.trySend(token); // Send peer an TunnelClose
    }};

    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(host));
    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));

    // Got stream, begin copy
    auto readCopyWorker = [&]() -> IoTask<void> {
        std::byte storage[4096]; //<  Take short cut now
        std::span buffer{storage};
        while (true) {
            ILIAS_CO_TRY(auto n, co_await stream.read(buffer));
            if (n == 0) { // EOF, Close!
                break;
            }
            // Send to master
            auto lock = co_await mWriteMutex.lock();
            ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, DataExchange {
                .token = token,
                .data = buffer.subspan(0, n)
            }));
        }
        co_return {};
    };
    auto writeCopyWorker = [&]() -> IoTask<void> {
        while (auto bytes = co_await receiver.recv()) {
            // Send bytes to local stream
            ILIAS_CO_TRYV(co_await stream.writeAll(*bytes));
        }
        std::println("[ProxyServer] Tunnel '{}' => {} request to closed by master", token, host);
        // Peer closed?
        co_return {};
    };
    auto _ = co_await ilias::whenAny(
        readCopyWorker(),
        writeCopyWorker()
    );
    co_return {};
}

auto ClientState::tunnelGCWorker(ilias::mpsc::Receiver<uint64_t> receiver) -> IoTask<void> {
    while (auto token = co_await receiver.recv()) {
        std::println("[ProxyClient] Tunnel '{}' was closed, sending it to master", *token);
        auto lock = co_await mWriteMutex.lock();
        ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, TunnelClose {
            .token = *token
        }));
    }
}