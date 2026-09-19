#include <nlohmann/json.hpp>
#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <print>

#include "protocol.hpp"
#include "duckdns.hpp"
#include "server.hpp"
#include "utils.hpp"
#include "web.hpp"

using ilias::MutableBuffer;
using ilias::TcpListener;
using ilias::StreamView;
using ilias::IPEndpoint;
using ilias::TaskScope;
using ilias::Task;
using ilias::Event;
using ilias::Mutex;

// The status of it
class ProxyStatus {
public:
    using Ptr = std::shared_ptr<ProxyStatus>;

    size_t activeConnections = 0;
    size_t totalConnections = 0;
};

// The actived proxy rule
class ProxyRule {
public:
    ProxyServer     &mServer;
    
    // Config
    IPEndpoint       mEndpoint;
    std::string      mClientName; //< Target machine forward to
    std::string      mTargetHost;
    uint16_t         mTargetPort;

    // State
    std::stop_source mStopSource; //< Used to stop whole rule
    ProxyStatus::Ptr mStatus;

    // Worker
    auto run() -> IoTask<void>;
};

// The state of the client
class ClientSession {
public:
    struct Tunnel {
        using Ptr = std::shared_ptr<Tunnel>;

        // The send window
        size_t sendWindow = HAJIMI_INIT_WINDOW_SIZE;
        ilias::Event sendWindowUpdated{ilias::Event::AutoClear};
        ilias::Event closed{}; //< Set when closed

        // For sending the data frame
        ilias::mpsc::Sender<BytesVector> bytesSender;
    };

    // Scope
    TaskScope  *mScope = nullptr;

    // Client info
    std::string mName;
    std::size_t mSendBytes = 0;
    std::size_t mRecvBytes = 0;
    IPEndpoint  mRemoteEndpoint;
    std::chrono::steady_clock::time_point mConnectedAt;

    // Connection
    StreamView mStream;

    // State
    ReadBuffer  mReadBuffer;
    WriteBuffer mWriteBuffer;
    Mutex       mWriteMutex;

    // Ping timer
    Event       mPongArrive {Event::AutoClear};

    // Tunnels
    uint64_t    mStreamId = 0; // Self increase
    std::map<uint64_t, Tunnel::Ptr> mTunnels;

    // Pool
    std::pmr::unsynchronized_pool_resource mPool;

    // Sub worker
    auto readWorker() -> IoTask<void>;
    auto pingWorker() -> IoTask<void>;
    auto tunnelWorker(ProxyStatus::Ptr status, TcpStream local, std::string host, uint16_t port) -> IoTask<void>;
};

ProxyServer::ProxyServer(Config config) : mConfig(config) {

}

ProxyServer::~ProxyServer() {
    
}

auto ProxyServer::run() -> IoTask<void> {
    std::println("[ProxyServer] listen on {}", mConfig.listen);

    ILIAS_CO_TRY(auto listener, co_await ilias::TcpBuilder{mConfig.listen.family()}
        .option(ilias::sockopt::ReuseAddress{true})
        .bind(mConfig.listen)
    );
    mStartTime = std::chrono::steady_clock::now();

    // Make duckdns
    std::optional<DuckDnsUpdater> duckdns;
    if (!mConfig.duckdnsDomain.empty()) {
        duckdns.emplace(mConfig.duckdnsDomain, mConfig.duckdnsToken, mConfig.duckdnsInterval);
    }

    // Make an webui
    WebUi ui{*this, mConfig.webui};

    // Handle incoming connection
    auto main = TaskScope::enter([&](auto &scope) -> Task<void> {
        mScope = &scope;
        if (duckdns) {
            scope.spawn(duckdns->run());
        }
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyServer] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
            // auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true});
            scope.spawn(handleSession(std::move(sock)));
        }
    });
    auto [_, uiErr] = co_await ilias::whenAny(std::move(main), ui.run());
    if (uiErr && !*uiErr) { // Failed to start webui?
        co_return ilias::Err(uiErr->error());
    }
    // Should be unreachable, waiting for cancel
    co_return {};
}

auto ProxyServer::handleSession(TcpStream stream) -> IoTask<void> {
    ClientSession session {}; // The session of it
    co_return co_await TaskScope::enter([&](TaskScope &scope) -> IoTask<void> {
        session.mScope = &scope;
        std::span readBuffer{session.mReadBuffer};
        std::span writeBuffer{session.mWriteBuffer};

        // First, read and parse hello
        Hello hello{};
        {
            ILIAS_CO_TRY(auto msg, co_await readMessage(stream, readBuffer));
            ILIAS_CO_TRY(hello, msg.cast<Hello>());
            if (hello.version != HAJIMI_VERSION) {
                std::println("[ProxyServer] Unexpected version {}, expected {}", hello.version, HAJIMI_VERSION);
                auto _ = co_await writeMessage(stream, writeBuffer, FatalError {
                    .msg = std::format("Version mismatch {}, expected: {}", hello.version, HAJIMI_VERSION)
                });
                co_return {};
            }
        }
        // Register it
        {
            session.mName = hello.name;
            session.mStream = stream;
            session.mConnectedAt = std::chrono::steady_clock::now();
            ILIAS_CO_TRY(session.mRemoteEndpoint, stream.remoteEndpoint());

            // Try emplace
            auto [it, emplace] = mSessions.emplace(hello.name, &session);
            if (!emplace) {
                std::println("[ProxyServer] Failed to register '{}', already exists?", hello.name);
                auto _ = co_await writeMessage(stream, writeBuffer, FatalError { .msg = "Name already exists" });
                co_return {};
            }
        }
        std::println("[ProxyServer] New Client '{}' from {}", hello.name, stream.remoteEndpoint().value());

        // Add RAII guard remove when disconnect
        ScopeExit exit{[&]() {
            std::println("[ProxyServer] Client '{}' disconnect", session.mName);
            mSessions.erase(session.mName);
        }};
        // Reply with Ack
        ILIAS_CO_TRYV(co_await writeMessage(stream, writeBuffer, HelloAck{}));

        // Do the main loop
        auto _ = co_await ilias::whenAny(
            session.readWorker(),
            session.pingWorker()
        );
        co_return {};
    });
}

// Get the status
auto ProxyServer::status() const -> std::string {
    using namespace std::chrono;
    using nlohmann::json;

    // uptime
    auto uptime = duration_cast<seconds>(steady_clock::now() - mStartTime);

    // clients
    // "name": self.name,
    // "remote_ip": f"{self.remote_addr[0]}:{self.remote_addr[1]}",
    // "connected_at": int(self.connected_at),
    // "uptime_seconds": int(time.time() - self.connected_at),
    auto clients = json::array();
    for (const auto &[name, client] : mSessions) {
        clients.push_back(json {
            {"name", name},
            {"remote_ip", client->mRemoteEndpoint.toString()},
            {"connected_at", 0}, // TODO:
            {"uptime_seconds", duration_cast<seconds>(steady_clock::now() - client->mConnectedAt).count()}
        });
    }

    // rules
    // "listen_port": self.listen_port,
    // "client_name": self.client_name,
    // "target_host": self.target_host,
    // "target_port": self.target_port,
    // "active_connections": self.active_connections,
    // "total_connections": self.total_connections,
    // "created_at": int(self.created_at),
    auto rules = json::array();
    for (const auto &[port, rule] : mRules) {
        rules.push_back(json {
            {"listen_port", rule->mEndpoint.port()},
            {"client_name", rule->mClientName},
            {"target_host", rule->mTargetHost},
            {"target_port", rule->mTargetPort},
            {"active_connections", rule->mStatus->activeConnections},
            {"total_connections", rule->mStatus->totalConnections},
            {"created_at", 0} // TODO:
        });
    }

    json js {
        {"uptime_seconds", uptime.count()},
        {"master_port", mConfig.listen.port()},
        {"master_host", mConfig.listen.address().toString()},
        {"webui_port", mConfig.webui.port()},
        {"webui_host", mConfig.webui.address().toString()},
        {"clients", std::move(clients)},
        {"rules", std::move(rules)}
    };

    // return {
    //     "uptime_seconds": int(time.time() - self.start_time),
    //     "master_port": self.port,
    //     "master_host": self.host or "*",
    //     "webui_port": self.webui_port,
    //     "webui_host": self.webui_host or "*",
    //     "clients": [c.to_dict() for c in self.clients.values()],
    //     "rules": [r.to_dict() for r in self.rules.values()],
    //     "duckdns": self.duckdns_updater.get_status() if self.duckdns_updater else None,
    // }


    return js.dump(0);
    // return js.dump(4);
}

auto ProxyServer::addRule(std::string_view ruleJson) -> Result<void, std::string> try {
    using nlohmann::json;
    if (!mScope) {
        return ilias::Err("Server is not fully started yet");
    }

    // Parse it
    auto rule = json::parse(ruleJson);

    // {"listen_port":666,"client_name":"hajimi-client","target_host":"127.0.0.1","target_port":555}
    auto listenPort = rule["listen_port"].get<uint16_t>();
    auto clientName=  rule["client_name"].get<std::string>();
    auto targetHost = rule["target_host"].get<std::string>();
    auto targetPort = rule["target_port"].get<uint16_t>();
    if (mRules.contains(listenPort)) {
        return ilias::Err("Port already used");
    }

    std::println("[ProxyServer] addRule from 0:{} => {}({}: {})", listenPort, clientName, targetHost, targetPort);

    // Create it
    std::unique_ptr<ProxyRule> ruleWorker {
        new ProxyRule { //< Inplace
            .mServer = *this,
            .mEndpoint = IPEndpoint{"[::0]:" + std::to_string(listenPort)},
            .mClientName = clientName,
            .mTargetHost = targetHost,
            .mTargetPort = targetPort,
            .mStatus = std::make_shared<ProxyStatus>(),
        }
    };
    mScope->spawn([this, r = std::move(ruleWorker)]() -> IoTask<void> {
        // Register to it
        mRules.emplace(r->mEndpoint.port(), r.get());
        ScopeExit exit{[&]() {
            mRules.erase(r->mEndpoint.port());
            std::println("[ProxyServer] '{}' => '{}' => '{}:{}' Rules was removed, {} left", r->mEndpoint, r->mClientName, r->mTargetHost, r->mTargetPort, mRules.size());
        }};
        co_return co_await r->run();
    });
    return {};
}
catch (std::exception &exp) {
    return ilias::Err(exp.what());
}

auto ProxyServer::removeRule(std::string_view ruleJson) -> Result<void, std::string> try {
    // {"listen_port":666}
    using nlohmann::json;

    auto port = json::parse(ruleJson)["listen_port"].get<uint16_t>();
    auto it = mRules.find(port);
    if (it == mRules.end()) {
        return ilias::Err("Rule not found");
    }
    auto &[_, rulePtr] = *it;
    rulePtr->mStopSource.request_stop(); // Request to stop, it will remove self from the map
    return {};
}
catch (std::exception &exp) {
    return ilias::Err(exp.what());
}

// MARK: ClientSession
auto ClientSession::readWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await readMessage(mStream, mReadBuffer));
        switch (msg.type()) {
            case MessageType::Pong: {
                std::println("[ProxyServer] Get pong from '{}'", mName);
                mPongArrive.set();
                continue;
            }
            case MessageType::Ping: { // Reply Pong
                auto lock = co_await mWriteMutex.lock();
                ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, Pong{}));
                continue;
            }
            case MessageType::DataExchange: {
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                auto it = mTunnels.find(exchange.streamId);
                if (it == mTunnels.end()) {
                    std::println("[ClientSession] DataExchange for Tunnel '{}' not found", exchange.streamId);
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
                    std::println("[ClientSession] WindowUpdate for Tunnel '{}' not found", update.streamId);
                    continue;
                }
                auto [streamId, tunnel] = *it;
                tunnel->sendWindow += update.size;
                tunnel->sendWindowUpdated.set();
                continue;
            }
            case MessageType::TunnelClose: { 
                // A tunnel was closed by peer, remove it
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                std::println("[ProxyServer] Tunnel {} was request to close by remote", close.streamId);
                auto it = mTunnels.find(close.streamId);
                if (it == mTunnels.end()) {
                    continue;
                }
                auto [streamId, tunnel] = *it;
                tunnel->closed.set();
                mTunnels.erase(it);
                continue;
            }
            case MessageType::FatalError: { // ERRROR!!!!!
                ILIAS_CO_TRY(auto err, msg.cast<FatalError>());
                std::println("[ProxyServer] FatalError!!!! from '{}' => {}", mName, err.msg);
                co_return {};
            }
            default: {
                std::println("[ProxyServer] Unexpected type from '{}', disconnect", mName);
                auto lock = co_await mWriteMutex.lock();
                ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, FatalError{ .msg = "WTF???, invalid msg type" }));
                co_return {};
            }
        }
    }
}

auto ClientSession::pingWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        co_await ilias::sleep(30s);

        // Send ping
        std::println("[ProxyServer] Send ping to '{}'", mName);
        auto lock = co_await mWriteMutex.lock();
        ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, Ping{}));

        auto pong = co_await ilias::timeout(mPongArrive.wait(), 30s);
        if (!pong) {
            std::println("[ProxyServer] Client '{}' pong timeout, disconnect", mName);
            co_return {};
        }
    }
}

auto ClientSession::tunnelWorker(ProxyStatus::Ptr status, TcpStream local, std::string host, uint16_t port) -> IoTask<void> {
    // Alloc streamId
    auto streamId = ++mStreamId;
    std::println("[ClientSession] {} request to open tunnel to {}:{}", mName, host, port);

    // Register it
    auto [sender, receiver] = ilias::mpsc::channel<BytesVector>();
    Tunnel::Ptr tunnel {
        new Tunnel { // In place
            .bytesSender = std::move(sender),
        }
    };
    mTunnels.emplace(streamId, tunnel);

    // RAII guard to cleanup
    status->activeConnections += 1;
    status->totalConnections += 1;
    ScopeExit exit{[this, streamId, status]() {
        std::println("[ClientSession] Tunnel {} closed", streamId);
        status->activeConnections -= 1;
        mTunnels.erase(streamId);
    }};

    // Open Tunnel
    {
        auto lock = co_await mWriteMutex.lock();
        ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, OpenTunnel {
            .streamId = streamId,
            .endpoint = host + ':' + std::to_string(port)
        }));
    }

    // Begin copy
    auto readCopyWorker = [&]() -> IoTask<void> {
        std::byte storage[HAJIMI_MAX_DATA_EXCHANGE];
        std::span buffer{storage};
        while (true) {
            ILIAS_CO_TRY(auto left, co_await local.read(buffer));
            if (left == 0) { // EOF, Close!
                break;
            }
            auto data = buffer.subspan(0, left);
            while (!data.empty()) {
                // Send to master
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
        while (auto bytes = co_await receiver.recv()) {
            // Send bytes to local stream
            // std::println("[ProxyServer] Tunnel '{}' write {} bytes data to local", streamId, bytes->size());
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
        // Peer closed?
        co_return {};
    };
    auto _ = co_await ilias::whenAny(
        readCopyWorker(),
        writeCopyWorker(),
        tunnel->closed.wait()
    );

    // Cleanup the tunnel
    if (mTunnels.contains(streamId)) {
        mTunnels.erase(streamId);
        auto lock = co_await mWriteMutex.lock();
        ILIAS_CO_TRYV(co_await writeMessage(mStream, mWriteBuffer, TunnelClose {
            .streamId = streamId
        }));
    }
    co_return {};
}

// MARK: ProxyRule
auto ProxyRule::run() -> IoTask<void> {
    std::println("[ProxyRule] Listen on {}, forward to '{}' => '{}:{}'", mEndpoint, mClientName, mTargetHost, mTargetPort);
    ILIAS_CO_TRY(auto listener, co_await ilias::TcpBuilder{mEndpoint.family()}
        .option(ilias::sockopt::ReuseAddress{true})
        .option(ilias::sockopt::Ipv6Only{false})
        .bind(mEndpoint)
    );

    // Handle incoming connection
    auto main = [&]() -> Task<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyRule] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
            // auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true});
            std::println("[ProxyRule] {}, new connection from {}", mEndpoint, addr);

            // Try find the client
            auto it = mServer.mSessions.find(mClientName);
            if (it == mServer.mSessions.end()) { // Not found
                continue;
            }
            auto session = it->second;

            // Start it in scope
            session->mScope->spawn(
                session->tunnelWorker(mStatus, std::move(sock), mTargetHost, mTargetPort)
            );
        }
    };
    co_await ilias::whenAny(main(), mStopSource.get_token()); // Wait for stop request
    co_return {};
}