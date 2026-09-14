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

using ilias::Buffer;
using ilias::TaskScope;
using ilias::Task;

// Internal state of the client
class ClientState {
public:
    struct Tunnel {
        ilias::Event mClosed{}; // Set to cancel the whole tunnel
    };

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
    ilias::mpsc::Sender<uint64_t> mSender;

    // Tunnel
    std::map<uint64_t, Tunnel *> mTunnels;

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
    if (msg.type() != MessageType::HelloAck) {
        std::println("[ProxyClient] Master rejected the registration.");
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
            .mSender = sender,
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
                if (it != mTunnels.end()) {
                    auto [_, tunnel] = *it;
                    tunnel->mClosed.set(); //< Noify it quiting!!!
                    mTunnels.erase(it); //< Unregister it
                }
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
    Tunnel tunnel {

    };
    mTunnels.emplace(token, &tunnel);
    ScopeExit exit{[&]() {
        auto it = mTunnels.find(token);
        if (it == mTunnels.end()) { // Handled for peer send the TunnelClose
            return;
        }
        auto _ = mSender.trySend(token); // Send peer an TunnelClose
    }};

    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(host));
    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
    // Got stream, begin copy
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