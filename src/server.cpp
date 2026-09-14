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
using ilias::TaskScope;
using ilias::Task;
using ilias::Event;
using ilias::Mutex;

// The state of the client
class ClientSession {
public:
    // Client info
    std::string mName;

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
    co_await TaskScope::enter([&](auto &scope) -> Task<void> {
        mScope = &scope;
        scope.spawn(ui.run()); // Start the webui
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
            co_return {};
        }
    }
    // Register it
    {
        session.mName = hello.name;
        session.mStream = stream;
        auto [it, emplace] = mSessions.emplace(hello.name, &session);
        if (!emplace) {
            std::println("[ProxyServer] Failed to register '{}', already exists?", hello.name);
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

    auto diff = steady_clock::now() - mStartTime;
    auto s = duration_cast<seconds>(diff);

    json js {
        {"uptime_seconds", s.count()},
        {"master_port", mConfig.listen.port()},
        {"master_host", mConfig.listen.address().toString()},
        {"webui_port", mConfig.webui.port()},
        {"webui_host", mConfig.webui.address().toString()},
        {"clients", json::array()},
        {"rules", json::array()}
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


    return js.dump(4);
}

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