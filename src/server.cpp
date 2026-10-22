#include <nlohmann/json.hpp>
#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <atomic>
#include <print>

#include "utils/scope_exit.hpp"
#include "protocol.hpp"
#include "duckdns.hpp"
#include "tunnel.hpp"
#include "server.hpp"
#include "web.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif // HAJIMI_QUIC

using ilias::MutableBuffer;
using ilias::TcpListener;
using ilias::IPEndpoint;
using ilias::TaskScope;
using ilias::Task;
using ilias::Event;

// The status of it
class ProxyStatus final {
public:
    using Ptr = std::shared_ptr<ProxyStatus>;

    size_t activeConnections = 0;
    size_t totalConnections = 0;
};

// The actived proxy rule
class ProxyRule final {
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
class ClientSession final {
public:
    using Ptr = std::shared_ptr<ClientSession>;

    ProxyServer &mServer;

    // Client info
    std::string mName;
    std::string mTag;
    std::size_t mSendBytes = 0;
    std::size_t mRecvBytes = 0;
    IPEndpoint  mRemoteEndpoint;
    std::chrono::steady_clock::time_point mConnectedAt;

    // Traffic statistics
    std::size_t mUploadBytes = 0;   // Client -> Server
    std::size_t mDownloadBytes = 0; // Server -> Client
    std::size_t mUploadRate = 0;    // Bytes/sec
    std::size_t mDownloadRate = 0;  // Bytes/sec

    // Latency
    std::int32_t mLatencyMs = -1;
    std::chrono::steady_clock::time_point mLastPingSent;

    // For posting message to write worker
    ilias::mpsc::Sender<Message> mMessageSender;

    // Ping timer
    Event mPongArrive {Event::AutoClear};

    // Tunnels
    std::uint64_t mStreamId = 0; // Self increase
    TunnelManager mTunnels;

    // Sub worker
    auto pingWorker() -> IoTask<void>;
    auto rateWorker() -> IoTask<void>;
    auto readWorker(MessageReader &reader) -> IoTask<void>;
    auto writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void>;
    auto openTunnel(ProxyStatus::Ptr status, DynStream local, std::string host, uint16_t port) -> void;
    auto onPeerOpenTunnel(uint64_t id, std::string targetEndpoint) -> bool;
};


// MARK: ProxyServer
ProxyServer::ProxyServer(Config config) : mConfig(config) {

}

ProxyServer::~ProxyServer() {
    
}

auto ProxyServer::run() -> IoTask<void> {
    mStartTime = std::chrono::steady_clock::now();

    // Make duckdns
    if (!mConfig.duckdnsDomain.empty()) {
        mDuckDns.emplace(mConfig.duckdnsDomain, mConfig.duckdnsToken, mConfig.duckdnsInterval);
    }

    // Make an webui
    WebUi ui{*this, mConfig.webui};

    // Handle incoming connection
    co_return co_await TaskScope::enter([&](auto &scope) -> IoTask<void> {
        mScope = &scope;
        if (mDuckDns) {
            scope.spawn(mDuckDns->run());
        }
        std::vector<IoTask<void>> tasks;
        tasks.emplace_back(ui.run());
        tasks.emplace_back(runTcp());

#if defined(HAJIMI_QUIC)
        tasks.emplace_back(runQuic());
#endif // HAJIMI_QUIC

        co_return co_await ilias::whenAny(std::move(tasks));
    });
}

auto ProxyServer::runTcp() -> IoTask<void> {
    std::println("[ProxyServer] Tcp listen on {}", mConfig.listen);

    ILIAS_CO_TRY(auto listener, co_await ilias::TcpBuilder{mConfig.listen.family()}
        .option(ilias::sockopt::ReuseAddress{true})
        .bind(mConfig.listen)
    );

    while (true) {
        auto incoming = co_await listener.accept();
        if (!incoming) {
            std::println("[ProxyServer] error at accept {}", incoming.error().message());
            continue;
        }
        auto &[sock, addr] = *incoming;
        auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
        std::println("[ProxyServer] New tcp connection from {}", addr);
        mScope->spawn(handleSession(std::move(sock), addr, "TCP"));
    }
}

#if defined(HAJIMI_QUIC)
auto ProxyServer::runQuic() -> IoTask<void> {
    using namespace std::chrono_literals;
    if (mConfig.certFile.empty() || mConfig.keyFile.empty()) {
        std::println("[ProxyServer] Cert or key file is empty, disable quic");
        while (true) {
            co_await ilias::sleep(1h); // Wait for cancel
        }
    }

    std::println("[ProxyServer] Quic listen on {}", mConfig.listen);
    ILIAS_CO_TRY(auto listener, co_await QuicContext::bind(
        mConfig.listen,
        QuicContext::Config {
            .role = QuicContext::Role::Server,
            .certFile = mConfig.certFile,
            .keyFile = mConfig.keyFile,
            .memoryResource = &mTracker,
        }
    ));

    auto worker = [&](QuicConnection conn) -> IoTask<void> { // Take the lifetime of the connection
        ILIAS_CO_TRY(auto stream, co_await conn.acceptStream());
        co_return co_await handleSession(std::move(stream), conn.remoteEndpoint(), "QUIC");
    };

    // Accept each connections
    auto loop = [&]() -> Task<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyServer] error at accept {}", incoming.error().message());
                continue;
            }
            std::println("ProxyServer] New quic connection from {}", incoming->remoteEndpoint());
            mScope->spawn(worker(std::move(*incoming)));
        }
    };
    auto _ = co_await ilias::whenAll(loop(), listener.run());
    co_return {};
}
#endif // HAJIMI_QUIC

auto ProxyServer::handleSession(DynStream stream, IPEndpoint remoteEndpoint, std::string tag) -> IoTask<void> {
    SkBufferPool pool{HAJIMI_POOL_CONFIG, &mTracker};
    MessageReader reader{stream, pool};
    MessageWriter writer{stream, pool};

    // First, read and parse hello
    Hello hello{};
    {
        ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
        ILIAS_CO_TRY(hello, msg.cast<Hello>());
        if (hello.version != HAJIMI_VERSION) {
            std::println("[ProxyServer] Unexpected version {}, expected {}", hello.version, HAJIMI_VERSION);
            auto _ = co_await writer.writeMessage(FatalError {
                .msg = std::format("Version mismatch {}, expected: {}", hello.version, HAJIMI_VERSION)
            });
            co_return {};
        }
    }

    // Register it
    auto [sender, receiver] = ilias::mpsc::channel<Message>(); // Unbound
    TaskScope scope;
    ClientSession::Ptr session {
        new ClientSession { // Pin
            .mServer = *this,
            .mName = hello.name,
            .mTag = tag,
            .mRemoteEndpoint = remoteEndpoint,
            .mConnectedAt = std::chrono::steady_clock::now(),
            .mMessageSender = sender,
            .mTunnels = TunnelManager {
                scope,
                pool,
                sender,
            }
        }
    };

    {
        // Try emplace
        auto [it, emplace] = mSessions.emplace(hello.name, session);
        if (!emplace) {
            std::println("[ProxyServer] Failed to register '{}', already exists?", hello.name);
            auto _ = co_await writer.writeMessage(FatalError { .msg = "Name already exists" });
            co_return {};
        }
    }
    std::println("[ProxyServer] New Client '{}' from {}", hello.name, remoteEndpoint);

    // Add RAII guard remove when disconnect
    ScopeExit exit{[this, session, &scope]() {
        std::println("[ProxyServer] Client '{}' disconnect", session->mName);
        mSessions.erase(session->mName);
        scope.stop();
    }};
    // Reply with Ack
    ILIAS_CO_TRYV(co_await writer.writeMessage(HelloAck{}));

    // Do the main loop
    auto main = [&]() -> Task<void> {
        auto _ = co_await ilias::whenAny(
            session->readWorker(reader),
            session->writeWorker(writer, std::move(receiver)),
            session->pingWorker(),
            session->rateWorker()
        );
        scope.stop();
    };
    scope.spawn(main());
    co_await scope.waitAll();
    co_return {};
}

// Get the status
auto ProxyServer::status() const -> std::string {
    using namespace std::chrono;
    using nlohmann::json;

    // uptime
    auto uptime = duration_cast<seconds>(steady_clock::now() - mStartTime);

    // clients
    auto clients = json::array();
    for (const auto &[name, client] : mSessions) {
        clients.push_back(json {
            {"name", name},
            {"tag", client->mTag},
            {"remote_ip", client->mRemoteEndpoint.toString()},
            {"uptime_seconds", duration_cast<seconds>(steady_clock::now() - client->mConnectedAt).count()},
            {"ping_ms", client->mLatencyMs},
            {"upload_speed", client->mUploadRate},
            {"download_speed", client->mDownloadRate},
            {"upload_total", client->mUploadBytes},
            {"download_total", client->mDownloadBytes}
        });
    }

    // rules
    // "listen_port": self.listen_port,
    // "client_name": self.client_name,
    // "target_host": self.target_host,
    // "target_port": self.target_port,
    // "active_connections": self.active_connections,
    // "total_connections": self.total_connections,
    auto rules = json::array();
    for (const auto &[port, rule] : mRules) {
        rules.push_back(json {
            {"listen_port", rule->mEndpoint.port()},
            {"client_name", rule->mClientName},
            {"target_host", rule->mTargetHost},
            {"target_port", rule->mTargetPort},
            {"active_connections", rule->mStatus->activeConnections},
            {"total_connections", rule->mStatus->totalConnections},
        });
    }

    // duckdns
    // "enabled": True,
    // "domain": f"{self.domain}.duckdns.org",
    // "last_update": int(self.last_update_time) if self.last_update_time else None,
    // "status": self.last_status,
    // "ipv4": self.last_ipv4,
    // "ipv6": self.last_ipv6,
    auto duckdns = json{};
    if (mDuckDns) {
        duckdns = json {
            {"enabled", true},
            {"domain", std::format("{}.duckdns.org", mDuckDns->domain())},
            {"status", mDuckDns->status()},
            {"ipv4", mDuckDns->ipv4()},
            {"ipv6", mDuckDns->ipv6()},
        };
    }

    json js {
        {"uptime_seconds", uptime.count()},
        {"master_port", mConfig.listen.port()},
        {"master_host", mConfig.listen.address().toString()},
        {"webui_port", mConfig.webui.port()},
        {"webui_host", mConfig.webui.address().toString()},
        {"clients", std::move(clients)},
        {"rules", std::move(rules)},
        {"duckdns", std::move(duckdns)},
        {"memory_usage", mTracker.allocated()}
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

auto ProxyServer::addRule(Rule rule) -> Result<void, std::string> try {
    // {"listen_port":666,"client_name":"hajimi-client","target_host":"127.0.0.1","target_port":555}
    if (mRules.contains(rule.listenPort)) {
        return ilias::Err("Port already used");
    }

    std::println("[ProxyServer] addRule from 0:{} => {}({}: {})", rule.listenPort, rule.clientName, rule.targetHost, rule.targetPort);

    // Create it
    std::shared_ptr<ProxyRule> ruleWorker {
        new ProxyRule { //< Inplace
            .mServer = *this,
            .mEndpoint = IPEndpoint{"[::0]:" + std::to_string(rule.listenPort)},
            .mClientName = rule.clientName,
            .mTargetHost = rule.targetHost,
            .mTargetPort = rule.targetPort,
            .mStatus = std::make_shared<ProxyStatus>(),
        }
    };
    mScope->spawn([this, r = std::move(ruleWorker)]() -> IoTask<void> {
        // Register to it
        mRules.emplace(r->mEndpoint.port(), r);
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

auto ProxyServer::removeRule(std::uint16_t port) -> Result<void, std::string> {
    // {"listen_port":666}
    auto it = mRules.find(port);
    if (it == mRules.end()) {
        return ilias::Err("Rule not found");
    }
    auto &[_, rulePtr] = *it;
    rulePtr->mStopSource.request_stop(); // Request to stop, it will remove self from the map
    return {};
}

// MARK: ClientSession
auto ClientSession::readWorker(MessageReader &reader) -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
        switch (msg.type()) {
            case MessageType::Pong: {
                auto now = std::chrono::steady_clock::now();
                auto rtt = std::chrono::duration_cast<std::chrono::milliseconds>(now - mLastPingSent).count();
                mLatencyMs = rtt;
                mPongArrive.set();
                continue;
            }
            case MessageType::Ping: { // Reply Pong
                auto _ = mMessageSender.trySend(Pong{});
                continue;
            }
            case MessageType::DataExchange: {
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                // Count uploaded bytes from client
                mUploadBytes += exchange.data.size();
                mTunnels.onDataExchange(exchange.streamId, std::move(exchange.data));
                continue;
            }
            case MessageType::WindowUpdate: {
                ILIAS_CO_TRY(auto update, msg.cast<WindowUpdate>());
                mTunnels.onWindowUpdate(update.streamId, update.size);
                continue;
            }
            case MessageType::TunnelClose: { 
                // A tunnel was closed by peer, remove it
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                mTunnels.closeTunnel(close.streamId);
                continue;
            }
            case MessageType::FatalError: { // ERRROR!!!!!
                ILIAS_CO_TRY(auto err, msg.cast<FatalError>());
                std::println("[ProxyServer] FatalError!!!! from '{}' => {}", mName, err.msg);
                co_return {};
            }
            case MessageType::OpenTunnel: { // From client to server, is forward
                ILIAS_CO_TRY(auto open, msg.cast<OpenTunnel>());
                if (open.streamId % 2 == 0) { // Should not be even
                    std::println("[ProxyServer] Invalid streamId '{}' from client '{}'", open.streamId, mName);
                    co_return {};
                }
                if (!onPeerOpenTunnel(open.streamId, open.endpoint)) {
                    auto _ = mMessageSender.trySend(TunnelClose {
                        .streamId = open.streamId,
                    });
                }
                continue;
            }
            default: {
                std::println("[ProxyServer] Unexpected type from '{}', disconnect", mName);
                co_return {};
            }
        }
    }
}

auto ClientSession::writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void> {
    while (auto msg = co_await receiver.recv()) {
        msg->visit(Overloads {
            [&](const auto &_) {}, // Ignore
            [&](const DataExchange &exchange) { // Count downloaded bytes from client
                mDownloadBytes += exchange.data.size();
            }
        });
        ILIAS_CO_TRYV(co_await writer.writeMessage(std::move(*msg)));
    }
    co_return {};
}

auto ClientSession::pingWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        // Send ping and measure latency
        mLastPingSent = std::chrono::steady_clock::now();
        auto _ = mMessageSender.trySend(Ping{});
        auto pong = co_await ilias::timeout(mPongArrive.wait(), 10s);
        if (!pong) {
            std::println("[ProxyServer] Client '{}' pong timeout, disconnect", mName);
            co_return {};
        }
        co_await ilias::sleep(10s); // Leave room before QUIC's 30s idle timeout.
    }
}

auto ClientSession::rateWorker() -> IoTask<void> {
    // Refresh the Traffic statistics
    using namespace std::chrono_literals;
    auto lastTime = std::chrono::steady_clock::now();
    auto lastUp = mUploadBytes;
    auto lastDown = mDownloadBytes;
    while (true) {
        co_await ilias::sleep(1s);
        auto now = std::chrono::steady_clock::now();
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastTime).count();
        if (elapsedMs > 0) {
            auto curUp = mUploadBytes;
            auto curDown = mDownloadBytes;
            mUploadRate = ((curUp - lastUp) * 1000 / elapsedMs);
            mDownloadRate = ((curDown - lastDown) * 1000 / elapsedMs);
            lastUp = curUp;
            lastDown = curDown;
            lastTime = now;
        }
    }
    co_return {};
}

auto ClientSession::openTunnel(ProxyStatus::Ptr status, DynStream local, std::string host, uint16_t port) -> void {
    // Alloc streamId
    auto streamId = mStreamId;
    mStreamId += 2;
    std::println("[ClientSession] {} request to open tunnel to {}:{}", mName, host, port);

    mTunnels.addTunnel(streamId, [this, streamId, status, local = std::move(local), host, port]() mutable -> IoTask<DynStream> {
        // Send Open Tunnel to peer
        {
            auto _ = mMessageSender.trySend(OpenTunnel {
                .streamId = streamId,
                .endpoint = host + ':' + std::to_string(port)
            });
        }

        // Wrapper stream to track the active connections
        struct Stream {
            Stream(ProxyStatus::Ptr status, DynStream stream) : status(status), stream(std::move(stream)) {
                if (status) {
                    status->activeConnections += 1;
                    status->totalConnections += 1;
                }
            }

            Stream(Stream &&) = default;
            
            ~Stream() {
                if (status) {
                    status->activeConnections -= 1;
                }
            }

            // Impl readable
            auto read(ilias::MutableBuffer buffer) { return stream.read(buffer); }

            // Impl writable
            auto write(ilias::Buffer buffer) { return stream.write(buffer); }
            auto flush() { return stream.flush(); }
            auto shutdown() { return stream.shutdown(); }

            ProxyStatus::Ptr status;
            DynStream  stream;
        };
        co_return Stream {
            status,
            std::move(local)
        };
    });
}

auto ClientSession::onPeerOpenTunnel(uint64_t streamId, std::string endpoint) -> bool {
    std::println("[ProxyServer] OpenTunnel from '{}' => {} '{}'", mName, endpoint, streamId);
    auto vecs = endpoint | std::views::split(':') | std::ranges::to<std::vector<std::string>>();
    if (vecs.size() != 2) {
        std::println("[ProxyServer] Invalid endpoint '{}'", endpoint);
        return false;
    }
    auto &target = vecs[0];
    auto &portStr = vecs[1];
    auto it = mServer.mSessions.find(target);
    if (it == mServer.mSessions.end()) {
        std::println("[ProxyServer] Client '{}' not found", target);
        return false;
    }
    auto [_, wanted] = *it;
    uint16_t port = 0;
    if (std::from_chars(portStr.data(), portStr.data() + portStr.size(), port).ec != std::errc{}) {
        std::println("[ProxyServer] Invalid port '{}'", portStr);
        return false;
    }

    // Create a tunnel from client to client
    auto [a, b] = ilias::DuplexStream::make(HAJIMI_MAX_DATA_EXCHANGE);
    wanted->openTunnel(nullptr, std::move(b), "localhost", port);
    mTunnels.addTunnel(streamId, [a = std::move(a)]() mutable -> IoTask<DynStream> {
        co_return std::move(a);
    });
    return true;
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
            auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
            std::println("[ProxyRule] {}, new connection from {}", mEndpoint, addr);

            // Try find the client
            auto it = mServer.mSessions.find(mClientName);
            if (it == mServer.mSessions.end()) { // Not found
                continue;
            }
            auto session = it->second;

            // Start it in scope
            session->openTunnel(mStatus, std::move(sock), mTargetHost, mTargetPort);
        }
    };
    co_await ilias::whenAny(main(), mStopSource.get_token()); // Wait for stop request
    co_return {};
}
