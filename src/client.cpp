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
#include "tunnel.hpp"
#include "skbuf.hpp"
#include "utils.hpp"

#ifdef HAJIMI_QUIC
    #include "quic/quic.hpp"
#endif

// Import types
using ilias::DynStream;
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

    // Tunnel
    std::uint64_t mStreamId = 1; // Self increase
    TunnelManager mTunnels;

    // For posting message to write worker
    ilias::mpsc::Sender<Message> mMessageSender;

    // Workers
    auto readWorker(MessageReader &reader) -> IoTask<void>;
    auto writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void>;
    auto pingWorker() -> IoTask<void>;
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

    // Resolve the master address, it may be a domain name (re-resolve on each reconnect)
    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::lookup(mConfig.master));

#ifdef HAJIMI_QUIC
    // Try quic first, other wise, fallback to tcp
    std::error_code lastErr;
    do {
        if (mConfig.transport == "tcp" || !quicContext || info.endpoints().empty()) {
            break;
        }
        auto ctxt = static_cast<QuicContext *>(quicContext);
        auto name = mConfig.master;
        if (name.starts_with('[')) {
            name = name.substr(1, name.find(']') - 1); // Unwrap an IPv6 literal.
        }
        else if (auto pos = name.rfind(':'); pos != std::string::npos) {
            name.resize(pos); // Trim port
        }

        // TODO: HE2? for QUIC?, i use a silly way here
        std::println("[ProxyClient] Connecting to master {} using QUIC...", name);
        auto conn = co_await ctxt->connect(info.endpoints()[0], name);
        if (!conn) { // Failed
            lastErr = conn.error();
            break;
        }
        auto stream = co_await conn->openStream();
        if (!stream) { // Failed
            lastErr = stream.error();
            break;
        }
        co_return co_await runOnce(registered, *stream);
    }
    while (false);
    if (mConfig.transport == "quic") { // Didn't allow fallback
        co_return ilias::Err(lastErr);
    }
    std::println("[ProxyClient] Fallback to TCP... reason: {}", lastErr.message());
#endif // HAJIMI_QUIC
    
    std::println("[ProxyClient] Connecting to master {} using TCP...", mConfig.master);
    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
    auto _ = stream.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
    co_return co_await runOnce(registered, stream);
}

auto ProxyClient::runOnce(bool &registered, StreamView stream) -> IoTask<void> {
    std::println("[ProxyClient] Connected to master {}", mConfig.master);
    
    // Do handshake
    SkBufferPool pool{HAJIMI_POOL_CONFIG};
    MessageReader reader{stream, pool};
    MessageWriter writer{stream, pool};

    // Register and wait for the ack
    ILIAS_CO_TRYV(co_await writer.writeMessage(Hello {
        .version = HAJIMI_VERSION,
        .name = mConfig.name,
    }));
    ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
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
    TaskScope scope;
    ClientState state {
        .mMaster = mConfig.master,
        .mScope = scope,
        .mTunnels = TunnelManager {
            scope,
            pool,
            sender
        },
        .mMessageSender = sender
    };
    auto main = [&]() -> IoTask<void> {
        // If any error, we stop the whole scope
        auto [read, write, forward, ping] = co_await ilias::whenAny(
            state.readWorker(reader),
            state.writeWorker(writer, std::move(receiver)),
            state.forwardWorker(mConfig.forwards),
            state.pingWorker()
        );
        for (const auto res : {read, write, forward, ping}) {
            if (res && !res->has_value()) {
                std::println("[ProxyClient] Session ended: {}", res->error().message());
            }
        }
        scope.stop();
        co_return {};
    };
    scope.spawn(main());
    co_await scope.waitAll();
    co_return {};
}

auto ClientState::readWorker(MessageReader &reader) -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        ILIAS_CO_TRY(auto msg, co_await reader.readMessage());
        switch (msg.type()) {
            case MessageType::Ping: { // Reply Pong
                auto _ = mMessageSender.trySend(Pong{});
                continue;
            }
            case MessageType::Pong: {
                continue;
            }
            case MessageType::OpenTunnel: { // Request to open tunnel
                ILIAS_CO_TRY(auto tunnel, msg.cast<OpenTunnel>());
                if (tunnel.streamId % 2 != 0) { // Is not even, protocol error
                    std::println("[ProxyClient] Invalid streamId '{}' from master", tunnel.streamId);
                    co_return {};
                }
                mTunnels.addTunnel(tunnel.streamId, [endpoint = tunnel.endpoint]() -> IoTask<DynStream> {
                    ILIAS_CO_TRY(auto info , co_await ilias::AddressInfo::lookup(endpoint));
                    ILIAS_CO_TRY(auto stream, co_await happy_eyeballs::connect(info.endpoints()));
                    auto _ = stream.setOption(ilias::sockopt::TcpNoDelay{true}); // Disable Nagle algorithm
                    co_return stream;
                });
                continue;
            }
            case MessageType::TunnelClose: { // The tunnel was closed
                ILIAS_CO_TRY(auto close, msg.cast<TunnelClose>());
                mTunnels.closeTunnel(close.streamId);
                continue;
            }
            case MessageType::DataExchange: { // The tunnel has data
                ILIAS_CO_TRY(auto exchange, msg.cast<DataExchange>());
                mTunnels.onDataExchange(exchange.streamId,std::move(exchange.data));
                continue;
            }
            case MessageType::WindowUpdate: {
                ILIAS_CO_TRY(auto update, msg.cast<WindowUpdate>());
                mTunnels.onWindowUpdate(update.streamId, update.size);
                continue;
            }
            default: {
                std::println("[ProxyClient] Unexpected type from {}, disconnect", mMaster);
                co_return {};
            }
        }
    }
}

auto ClientState::writeWorker(MessageWriter &writer, ilias::mpsc::Receiver<Message> receiver) -> IoTask<void> {
    while (auto msg = co_await receiver.recv()) {
        ILIAS_CO_TRYV(co_await writer.writeMessage(std::move(*msg)));
    }
    co_return {};
}

auto ClientState::pingWorker() -> IoTask<void> {
    using namespace std::chrono_literals;
    while (true) {
        // Stay below QUIC's 30s idle timeout, including with older servers.
        co_await ilias::sleep(10s);
        if (!mMessageSender.trySend(Ping{})) {
            co_return {};
        }
    }
}

auto ClientState::forwardWorker(std::span<const std::pair<IPEndpoint, std::string>> forwards) -> IoTask<void> {
    using namespace std::chrono_literals;
    if (forwards.empty()) {
        while (true) { // Suspend forever
            co_await ilias::sleep(1h);
        }
    }

    // Start each worker
    std::vector<IoTask<void>> subworkers;

    auto addTunnel = [&](ilias::TcpStream stream, std::string target) {
        // Alloc Id
        auto id = mStreamId;
        mStreamId += 2;

        // Send server OpenTunnel
        {
            auto _ = mMessageSender.trySend(OpenTunnel {
                .streamId = id,
                .endpoint = target
            });
        }
        mTunnels.addTunnel(id, [s = std::move(stream)]() mutable -> IoTask<DynStream> {
            co_return std::move(s);
        });
    };
    auto listenWorker = [&](IPEndpoint bind, std::string target) -> IoTask<void> {
        std::println("[ProxyClient] Listening on '{}', forwarding to '{}'", bind, target);
        ILIAS_CO_TRY(auto listener, co_await ilias::TcpListener::bind(bind));
        while (true) {
            auto conn = co_await listener.accept();
            if (!conn) {
                continue;
            }
            auto &[stream, endpoint] = *conn;
            std::println("[ProxyClient] Accepted connection from '{}'", endpoint);
            addTunnel(std::move(stream), target);
        }
    };

    for (const auto &[listen, target] : forwards) {
        subworkers.push_back(listenWorker(listen, target));
    }
    co_return co_await ilias::whenAny(std::move(subworkers));
}
