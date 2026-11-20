#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <memory_resource>
#include <chrono>
#include <print>
#include <map>

#include "utils/scope_exit.hpp"
#include "utils/color.hpp"
#include "utils/he2.hpp"
#include "transport.hpp"
#include "protocol.hpp"
#include "client.hpp"
#include "skbuf.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif

// Import types
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

    // Conn
    HajimiConnection &mConn;

    // Workers
    auto readWorker() -> IoTask<void>;
    auto pingWorker() -> IoTask<void>;
    auto acceptWorker() -> IoTask<void>;
    auto forwardWorker(std::span<const std::pair<IPEndpoint, std::string>> forwards) -> IoTask<void>;
};


// MARK: Impl
ProxyClient::ProxyClient(Config config) : mConfig(config) {

}

ProxyClient::~ProxyClient() = default;

auto ProxyClient::run() -> IoTask<void> {
    using namespace std::literals;
    using std::chrono::seconds;

    std::println("[ProxyClient] client '{}' target master {}", mConfig.name, mConfig.master);

    std::vector<IoTask<void>> tasks;
    void *quicContext = nullptr;

#if defined(HAJIMI_QUIC)
    // Prepare quic context
    auto ctxt = co_await QuicContext::bind(
        "[::0]:0",
        QuicContext::Config {
            .role = QuicContext::Role::Client,
            .caFile = mConfig.caFile,
            .verifyPeer = mConfig.verify,
        }
    );
    if (ctxt) { // Quic initialized
        quicContext = &*ctxt;
        tasks.emplace_back(ctxt->run()); // Add loop job
    }
    else {
        std::println("[ProxyClient] QUIC initialization failed: {}", ctxt.error().message());
        if (mConfig.transport == "quic") {
            co_return ilias::Err(ctxt.error());
        }
    }
#endif // HAJIMI_QUIC

    // Main body
    auto main = [&] -> IoTask<void> {
        // Reconnect loop with backoff: 2s, x1.5 on each failure, up to 15s
        seconds retryDelay = 2s;
        while (true) {
            bool registered = false;
            if (auto res = co_await connectOnce(registered, quicContext); !res) {
                std::println("[ProxyClient] Connection failed: {}", res.error().message());
            }
            // Reset the delay once we registered successfully
            retryDelay = registered ? 2s : std::min(retryDelay * 3 / 2, 15s);
            co_await ilias::sleep(retryDelay);
        }
        co_return {};
    };
    tasks.emplace_back(main());
    co_return co_await ilias::whenAny(std::move(tasks));
}

auto ProxyClient::connectOnce(bool &registered, void *quicContext) -> IoTask<void> {
    using namespace std::literals;
    SkBufferPool pool{HAJIMI_POOL_CONFIG};
#ifdef HAJIMI_QUIC
    // Try quic first, other wise, fallback to tcp
    std::error_code lastErr;
    do {
        if (mConfig.transport == "tcp" || !quicContext) {
            break;
        }
        auto ctxt = static_cast<QuicContext *>(quicContext);
        std::println("[ProxyClient] Connecting to master {} using QUIC...", mConfig.master);
        auto qconn = co_await ctxt->connect(mConfig.master);
        if (!qconn) { // Failed
            lastErr = qconn.error();
            break;
        }
        ILIAS_CO_TRY(auto conn, co_await HajimiConnection::fromClient(pool, std::move(*qconn)));
        co_return co_await runOnce(registered, conn);
    }
    while (false);
    if (mConfig.transport == "quic") { // Didn't allow fallback
        co_return ilias::Err(lastErr);
    }
    std::println("[ProxyClient] Fallback to TCP... reason: {}", lastErr.message());
#endif // HAJIMI_QUIC
    
    std::println("[ProxyClient] Connecting to master {} using TCP...", mConfig.master);
    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(mConfig.master));
    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
    auto _ = stream.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
    
    ILIAS_CO_TRY(auto conn, co_await HajimiConnection::fromClient(pool, std::move(stream)));
    co_return co_await runOnce(registered, conn);
}

auto ProxyClient::runOnce(bool &registered, HajimiConnection &conn) -> IoTask<void> {
    auto [a, b] = co_await ilias::whenAny(
        runImpl(registered, conn),
        conn.run() // Drive the connection
    );
    if (a) co_return *a;
    if (b) co_return *b;
    co_return {};
}

auto ProxyClient::runImpl(bool &registered, HajimiConnection &conn) -> IoTask<void> {
    std::println("[ProxyClient] Connected to master {}", mConfig.master);
    
    // Do handshake
    // Register and wait for the ack
    ILIAS_CO_TRYV(co_await conn.writeMessage(Hello {
        .version = HAJIMI_VERSION,
        .name = mConfig.name,
    }));
    ILIAS_CO_TRY(auto msg, co_await conn.readMessage());
    if (msg.type() == MessageType::FatalError) {
        std::println("[ProxyClient] Master rejected the registration, Fatal error from master: {}", msg.cast<FatalError>().value().msg);
        co_return {};
    }
    if (msg.type() != MessageType::HelloAck) {
        std::println("[ProxyClient] Invalid message from master");
        co_return {};
    }
    registered = true;
    std::println("[ProxyClient] {} as '{}'", styled("Registered", Color::Green), mConfig.name);
    
    // Begin the loop
    // Prepare channel
    TaskScope scope;
    ClientState state {
        .mMaster = mConfig.master,
        .mScope = scope,
        .mConn = conn,
    };
    auto main = [&]() -> IoTask<void> {
        // If any error, we stop the whole scope
        auto [read, accept, forward, ping] = co_await ilias::whenAny(
            state.readWorker(),
            state.acceptWorker(),
            state.forwardWorker(mConfig.forwards),
            state.pingWorker()
        );
        for (const auto res : {read, accept, forward, ping}) {
            if (res && !res->has_value()) {
                std::println("[ProxyClient] Session {}: {}", styled("ended", Color::Red), res->error().message());
                break;
            }
        }
        scope.stop();
        co_return {};
    };
    scope.spawn(main());
    co_await scope.waitAll();
    co_return {};
}

auto ClientState::readWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await mConn.readMessage());
        switch (msg.type()) {
            case MessageType::Ping: { // Reply Pong
                auto _ = co_await mConn.writeMessage(Pong {});
                continue;
            }
            case MessageType::Pong: {
                continue;
            }
            default: {
                std::println("[ProxyClient] Unexpected type from {}, disconnect", mMaster);
                co_return {};
            }
        }
    }
}

auto ClientState::pingWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        // Stay below QUIC's 30s idle timeout, including with older servers.
        co_await ilias::sleep(10s);
        if (!co_await mConn.writeMessage(Ping {})) {
            co_return {};
        }
    }
}

auto ClientState::acceptWorker() -> IoTask<void> {
    while (true) {
        ILIAS_CO_TRY(auto tunnel, co_await mConn.acceptTunnel());
        mScope.spawn([tunnel = std::move(tunnel)]() mutable -> IoTask<void> {
            auto &[req, stream] = tunnel;
            ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::fromHostname(req.addr, std::to_string(req.port)));
            ILIAS_CO_TRY(auto local, co_await happy_eyeballs::connect(info.endpoints()));
            // Begin copy
            co_await copyBidirectional(stream, local);
            co_return {};
        });
    }
}

auto ClientState::forwardWorker(std::span<const std::pair<IPEndpoint, std::string>> forwards) -> IoTask<void> {
    using namespace std::chrono_literals;
    if (forwards.empty()) {
        co_await ilias::this_coro::suspendForever();
    }

    // Start each worker
    std::vector<IoTask<void>> subworkers;

    auto copyWorker = [&](ilias::TcpStream local, std::string addr, std::uint16_t port) -> IoTask<void> {
        ILIAS_CO_TRY(auto peer, co_await mConn.openTunnel(OpenTunnel {
            .addr = addr,
            .port = port,
        }));
        co_await copyBidirectional(peer, local);
        co_return {};
    };

    auto listenWorker = [&](IPEndpoint bind, std::string addr, std::uint16_t port) -> IoTask<void> {
        std::println("[ProxyClient] Listening on '{}', forwarding to '{}:{}'", bind, addr, port);
        ILIAS_CO_TRY(auto listener, co_await ilias::TcpListener::bind(bind));
        while (true) {
            auto conn = co_await listener.accept();
            if (!conn) {
                continue;
            }
            auto &[stream, endpoint] = *conn;
            std::println("[ProxyClient] Accepted connection from '{}'", endpoint);
            mScope.spawn(copyWorker(std::move(stream), addr, port));
        }
    };

    for (const auto &[listen, target] : forwards) {
        // Parse the target
        const auto compoments = target
            | std::views::split(':')
            | std::ranges::to<std::vector<std::string>>();
        if (compoments.size() != 2) {
            std::println("[ProxyClient] Invalid forward target '{}'", target);
            throw std::invalid_argument{"Invalid forward target"};
        }
        const auto &addr = compoments[0];
        const auto &portStr = compoments[1];
        std::uint16_t port = 0;
        if (std::from_chars(portStr.data(), portStr.data() + portStr.size(), port).ec != std::errc{}) {
            std::println("[ProxyClient] Invalid forward target '{}'", target);
            throw std::invalid_argument{"Invalid forward target"};
        }

        // Ok start it
        subworkers.push_back(listenWorker(listen, addr, port));
    }
    co_return co_await ilias::whenAny(std::move(subworkers));
    co_return {};
}
