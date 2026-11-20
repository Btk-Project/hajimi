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
#include "transport.hpp"
#include "protocol.hpp"
#include "duckdns.hpp"
#include "server.hpp"
#include "web.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif // HAJIMI_QUIC

using ilias::MutableBuffer;
using ilias::TcpListener;
using ilias::IPEndpoint;
using ilias::TaskScope;
using ilias::Err;
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
    HajimiConnection &mConn;
    TaskScope &mScope;

    // Client info
    std::string mName;
    std::string mTag;
    IPEndpoint  mRemoteEndpoint;
    std::chrono::steady_clock::time_point mConnectedAt;

    // Traffic statistics
    std::size_t mUploadRate = 0;    // Bytes/sec
    std::size_t mDownloadRate = 0;  // Bytes/sec

    // Latency
    std::int32_t mLatencyMs = -1;
    std::chrono::steady_clock::time_point mLastPingSent;

    // Ping timer
    Event mPongArrive {Event::AutoClear};

    // Sub worker
    auto pingWorker() -> IoTask<void>;
    auto rateWorker() -> IoTask<void>;
    auto readWorker() -> IoTask<void>;
    auto acceptWorker() -> IoTask<void>;
    auto openTunnel(ProxyStatus::Ptr status, DynStream local, std::string host, uint16_t port) -> void;
};


// MARK: ProxyServer
ProxyServer::ProxyServer(Config config) : mConfig(config), mPool(HAJIMI_POOL_CONFIG, &mTracker) {

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
    ScopeExit resetScope{[&]() { mScope = nullptr; }};
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

        auto result = co_await ilias::whenAny(std::move(tasks));
        scope.stop(); // enter() joins children; it does not stop them on a normal error return.
        co_return result;
    });
}

auto ProxyServer::runTcp() -> IoTask<void> {
    using namespace std::chrono_literals;
    std::println("[ProxyServer] Tcp listen on {}", mConfig.listen);

    ILIAS_CO_TRY(auto listener, co_await ilias::TcpBuilder{mConfig.listen.family()}
        .option(ilias::sockopt::ReuseAddress{true})
        .bind(mConfig.listen)
    );

    while (true) {
        auto incoming = co_await listener.accept();
        if (!incoming) {
            std::println("[ProxyServer] error at accept tcp: {}", incoming.error().message());
            co_await ilias::sleep(1s);
            continue;
        }
        auto &[sock, addr] = *incoming;
        auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
        std::println("[ProxyServer] New tcp connection from {}", addr);

        mScope->spawn([this, s = std::move(sock), addr]() mutable -> IoTask<void>{
            ILIAS_CO_TRY(auto conn, co_await HajimiConnection::fromServer(mPool, std::move(s)));
            co_return co_await handleSession(conn, addr, "TCP");
        });
    }
}

#if defined(HAJIMI_QUIC)
auto ProxyServer::runQuic() -> IoTask<void> {
    using namespace std::chrono_literals;
    if (mConfig.certFile.empty() || mConfig.keyFile.empty()) {
        std::println("[ProxyServer] Cert or key file is empty, disable quic");
        co_await ilias::this_coro::suspendForever(); // Wait for cancel
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

    auto worker = [&](QuicConnection qconn) -> IoTask<void> { // Take the lifetime of the connection
        auto endpoint = qconn.remoteEndpoint();
        ILIAS_CO_TRY(auto conn, co_await HajimiConnection::fromServer(mPool, std::move(qconn)));
        co_return co_await handleSession(conn, endpoint, "QUIC");
    };

    TaskScope connections;
    auto loop = [&]() -> IoTask<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                auto error = incoming.error();
                std::println(stderr, "[ProxyServer/QUIC] accept failed: {} ({}:{})",
                    error.message(), error.category().name(), error.value());
                co_return Err(error);
            }
            std::println("[ProxyServer] New quic connection from {}", incoming->remoteEndpoint());
            connections.spawn(worker(std::move(*incoming)));
        }
    };
    auto serve = [&]() -> IoTask<void> {
        // whenAny cancels and joins the other task, including run()'s cleanup.
        auto [accepted, driven] = co_await ilias::whenAny(loop(), listener.run());
        if (accepted) co_return std::move(*accepted);
        co_return std::move(*driven);
    };
    // Join sessions before destroying their coroutine closure or listener.
    co_return co_await ilias::finally(serve(), connections.shutdown());
}
#endif // HAJIMI_QUIC

auto ProxyServer::handleSession(HajimiConnection &conn, IPEndpoint remoteEndpoint, std::string tag) -> IoTask<void> {
    auto [a, b] = co_await ilias::whenAny(
        handleSessionImpl(conn, remoteEndpoint, tag),
        conn.run() // Drive the connection
    );
    if (a) co_return *a;
    if (b) co_return *b;
    co_return {};
}

auto ProxyServer::handleSessionImpl(HajimiConnection &conn, IPEndpoint remoteEndpoint, std::string tag) -> IoTask<void> {
    // First, read and parse hello
    Hello hello{};
    {
        ILIAS_CO_TRY(auto msg, co_await conn.readMessage());
        ILIAS_CO_TRY(hello, msg.cast<Hello>());
        if (hello.version != HAJIMI_VERSION) {
            std::println("[ProxyServer] Unexpected version {}, expected {}", hello.version, HAJIMI_VERSION);
            auto _ = co_await conn.writeMessage(FatalError {
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
            .mConn = conn,
            .mScope = scope,
            .mName = hello.name,
            .mTag = tag,
            .mRemoteEndpoint = remoteEndpoint,
            .mConnectedAt = std::chrono::steady_clock::now(),
        }
    };

    {
        // Try emplace
        auto [it, emplace] = mSessions.emplace(hello.name, session);
        if (!emplace) {
            std::println("[ProxyServer] Failed to register '{}', already exists?", hello.name);
            auto _ = co_await conn.writeMessage(FatalError { .msg = "Name already exists" });
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
    ILIAS_CO_TRYV(co_await conn.writeMessage(HelloAck{}));

    // Do the main loop
    auto main = [&]() -> Task<void> {
        auto _ = co_await ilias::whenAny(
            session->readWorker(),
            session->pingWorker(),
            session->rateWorker(),
            session->acceptWorker()
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
        auto status = client->mConn.status();
        clients.push_back(json {
            {"name", name},
            {"tag", client->mTag},
            {"remote_ip", client->mRemoteEndpoint.toString()},
            {"uptime_seconds", duration_cast<seconds>(steady_clock::now() - client->mConnectedAt).count()},
            {"ping_ms", client->mLatencyMs},
            {"upload_speed", client->mUploadRate},
            {"download_speed", client->mDownloadRate},
            {"upload_total", status.bytesReceived},    // Client -> Server
            {"download_total", status.bytesSent}       // Client <- Server
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
auto ClientSession::readWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await mConn.readMessage());
        switch (msg.type()) {
            case MessageType::Pong: {
                auto now = std::chrono::steady_clock::now();
                auto rtt = std::chrono::duration_cast<std::chrono::milliseconds>(now - mLastPingSent).count();
                mLatencyMs = rtt;
                mPongArrive.set();
                continue;
            }
            case MessageType::Ping: { // Reply Pong
                auto _ = co_await mConn.writeMessage(Pong{});
                continue;
            }
            case MessageType::FatalError: { // ERRROR!!!!!
                ILIAS_CO_TRY(auto err, msg.cast<FatalError>());
                std::println("[ProxyServer] FatalError!!!! from '{}' => {}", mName, err.msg);
                co_return {};
            }
            default: {
                std::println("[ProxyServer] Unexpected type from '{}', disconnect", mName);
                co_return {};
            }
        }
    }
}

auto ClientSession::pingWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        // Send ping and measure latency
        mLastPingSent = std::chrono::steady_clock::now();
        auto _ = co_await mConn.writeMessage(Ping{});
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
    auto status = mConn.status();
    auto lastUp = status.bytesReceived; // Client -> Server
    auto lastDown = status.bytesSent;

    while (true) {
        co_await ilias::sleep(1s);
        auto now = std::chrono::steady_clock::now();
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastTime).count();
        if (elapsedMs > 0) {
            status = mConn.status();
            auto curUp = status.bytesReceived;
            auto curDown = status.bytesSent;
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
    mScope.spawn([this, status, local = std::move(local), host, port]() mutable -> IoTask<void> {
        status->totalConnections += 1;
        status->activeConnections += 1;
        ScopeExit exit{[&]() {
            status->activeConnections -= 1;
        }};

        std::println("[ClientSession] {} request to open tunnel to {}:{}", mName, host, port);
        ILIAS_CO_TRY(auto peer, co_await mConn.openTunnel(OpenTunnel {
            .addr = host,
            .port = port
        }));
        co_await copyBidirectional(peer, local);
        co_return {};
    });
}

auto ClientSession::acceptWorker() -> IoTask<void> {
    while (true) {
        ILIAS_CO_TRY(auto tunnel, co_await mConn.acceptTunnel());
        auto &name = tunnel.first.addr;
        auto &port = tunnel.first.port;
        auto &stream = tunnel.second;

        std::println("[ClientSession] {} accept tunnel from peer, open tunnel to {}:{}", mName, name, port);
        auto it = mServer.mSessions.find(name);
        if (it == mServer.mSessions.end()) {
            std::println("[ClientSession] Client '{}' not found", name);
            continue;
        }

        // Start a tunnel to the client, and copy bidirectional
        mScope.spawn([this, session = it->second, local = std::move(stream), port]() mutable -> IoTask<void> {
            ILIAS_CO_TRY(auto peer, co_await session->mConn.openTunnel(OpenTunnel {
                .addr = "localhost",
                .port = port
            }));
            co_await copyBidirectional(peer, local);
            co_return {};
        });
    }
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
