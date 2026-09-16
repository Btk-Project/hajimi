#include <nlohmann/json.hpp>
#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <print>
#include "protocol.hpp"
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

// The state of the client
class ClientSession {
public:
    // Client info
    std::string mName;
    IPEndpoint  mRemoteEndpoint;
    std::chrono::steady_clock::time_point mConnectedAt;

    // Connection
    StreamView mStream;

    // State
    ReadBuffer  mReadBuffer;
    WriteBuffer mWriteBuffer;
    Mutex       mWriteMutex;

    // Tunnels
    uint64_t    mToken = 0; // Self increase

    // Ping timer
    Event       mPongArrive {Event::AutoClear};

    // Sub worker
    auto readWorker() -> IoTask<void>;
    auto pingWorker() -> IoTask<void>;
};

// The actived proxy rules
class ProxyRule {
public:
    ProxyServer     &mServer;
    
    // Config
    IPEndpoint       mEndpoint;
    std::string      mClientName; //< Target machine forward to
    std::string      mTargetHost;
    uint16_t         mTargetPort;

    // State
    size_t           mActiveConnections = 0;
    size_t           mTotalConnections = 0;
    std::stop_source mStopSource; //< Used to stop

    auto run() -> IoTask<void>;
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
    WebUi ui{*this, mConfig.webui}; // Make an webui

    // Handle incoming connection
    auto main = TaskScope::enter([&](auto &scope) -> Task<void> {
        mScope = &scope;
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyServer] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
            auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true});
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
    ClientSession session; // The session of it
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
            {"active_connections", rule->mActiveConnections},
            {"total_connections", rule->mTotalConnections},
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
            .mEndpoint = IPEndpoint{"0.0.0.0:" + std::to_string(listenPort)},
            .mClientName = clientName,
            .mTargetHost = targetHost,
            .mTargetPort = targetPort,
        }
    };
    mScope->spawn([this, r = std::move(ruleWorker)]() -> ilias::IoTask<void> {
        // Register to it
        mRules.emplace(r->mEndpoint.port(), r.get());
        ScopeExit exit{[&]() {
            mRules.erase(r->mEndpoint.port());
            std::println("[ProxyServer] '{}' => '{}' => '{}: {}' Rules was removed, {} left", r->mEndpoint, r->mClientName, r->mTargetHost, r->mTargetPort, mRules.size());
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
auto ClientSession::readWorker() -> ilias::IoTask<void> {
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
            case MessageType::OpenTunnelAck : {
                continue;   
            }
            case MessageType::DataExchange: {
                ILIAS_CO_TRY(auto data, msg.cast<DataExchange>());
                continue;
            }
            case MessageType::TunnelClose: { // A tunnel was closed by peer, remove it
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

auto ClientSession::pingWorker() -> ilias::IoTask<void> {
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

// MARK: ProxyRule
auto ProxyRule::run() -> ilias::IoTask<void> {
    std::println("[ProxyRule] Listen on {}, forward to '{}' => '{}: {}'", mEndpoint, mClientName, mTargetHost, mTargetPort);
    ILIAS_CO_TRY(auto listener, co_await TcpListener::bind(mEndpoint));

    // Handle incoming connection
    auto main = TaskScope::enter([&](auto &scope) -> Task<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyRule] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
            auto _ = sock.setOption(ilias::sockopt::TcpNoDelay{true});
            // Todo:
        }
    });
    co_await ilias::whenAny(std::move(main), mStopSource.get_token()); // Wait for stop request
    co_return {};
}
