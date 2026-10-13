#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <cstdint>
#include <print>

#include "tunnel.hpp"
#include "utils.hpp"

using ilias::IoTask;
using ilias::Task;

struct TunnelManager::Tunnel {
    using Ptr = std::shared_ptr<Tunnel>;

    // The send window
    size_t sendWindow = HAJIMI_INIT_WINDOW_SIZE;
    ilias::Event sendWindowUpdated{ilias::Event::AutoClear};
    ilias::Event closed{}; //< Set when closed

    // For manager sending the data frame
    ilias::mpsc::Sender<SkBuffer> bytesSender;
};

auto TunnelManager::addTunnel(uint64_t id, Entry entry) -> void {
    auto [sender, receiver] = ilias::mpsc::channel<SkBuffer>();
    Tunnel::Ptr tunnel {
        new Tunnel { // In place
            .bytesSender = std::move(sender),
        }
    };

    // Register it
    auto [it, emplace] = mTunnels.emplace(id, tunnel);
    if (!emplace) {
        return;
    }

    // Start the worker
    mScope.spawn([this, id, tunnel, entry = std::move(entry), receiver = std::move(receiver)]() mutable -> IoTask<void> {
        // Prepare cleanup
        ScopeExit exit{[&, this]() {
            std::println("[TunnelManager] Tunnel '{}' closed", id);
            if (mTunnels.contains(id)) {
                mTunnels.erase(id);
                auto _ = mMessageSender.trySend(TunnelClose {
                    .streamId = id
                });
            }
        }};
        std::println("[TunnelManager] Tunnel '{}' opened", id);

        // Get the stream
        ILIAS_CO_TRY(auto stream, co_await entry());

        // Got stream, begin copy
        auto readCopyWorker = [&]() -> IoTask<void> {
            while (true) {
                while (tunnel->sendWindow == 0) { // Waiting for the send window
                    co_await tunnel->sendWindowUpdated.wait();
                }
                auto bufsize = std::min(HAJIMI_MAX_DATA_EXCHANGE, tunnel->sendWindow);
                auto skbuf = mPool.allocate(HAJIMI_HEADER_ROOM + bufsize);
                skbuf.reserveHead(HAJIMI_HEADER_ROOM);

                ILIAS_CO_TRY(auto n, co_await stream.read(skbuf.prepareBack(bufsize)));
                skbuf.commitBack(n);
                if (n == 0) { // EOF
                    break;
                }

                // Send this buffer
                auto _ = mMessageSender.trySend(DataExchange {
                    .streamId = id,
                    .data = std::move(skbuf)
                });
                tunnel->sendWindow -= n;
            }
            co_return {};
        };
        auto writeCopyWorker = [&]() -> IoTask<void> {
            size_t peerSum = 0;
            while (auto bytes = co_await receiver.recv()) {
                // Send bytes to local stream
                // std::println("[ClientState] Tunnel '{}' write {} bytes data to local", streamId, bytes->size());
                ILIAS_CO_TRYV(co_await stream.writeAll(bytes->data()));

                // Update the peer send window
                peerSum += bytes->size();
                if (peerSum < HAJIMI_INIT_WINDOW_SIZE / 2) {
                    // Wait for peer custome more
                    continue;
                }
                auto _ = mMessageSender.trySend(WindowUpdate {
                    .streamId = id,
                    .size = static_cast<uint32_t>(peerSum)
                });
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
    });
}

auto TunnelManager::closeTunnel(uint64_t id) -> void {
    std::println("[TunnelManager] Request to close tunnel '{}'", id);
    if (auto it = mTunnels.find(id); it != mTunnels.end()) {
        auto tunnel = it->second;
        tunnel->closed.set();
        mTunnels.erase(it);
    }
}

auto TunnelManager::onWindowUpdate(uint64_t id, size_t size) -> void {
    auto it = mTunnels.find(id);
    if (it == mTunnels.end()) {
        std::println("[TunnelManager] WindowUpdate for Tunnel '{}' not found", id);
        return;
    }
    auto [streamId, tunnel] = *it;
    tunnel->sendWindow += size;
    tunnel->sendWindowUpdated.set();
}

auto TunnelManager::onDataExchange(uint64_t id, SkBuffer data) -> void {
    auto it = mTunnels.find(id);
    if (it == mTunnels.end()) {
        std::println("[TunnelManager] DataExchange for Tunnel '{}' not found", id);
        return;
    }
    auto [_, tunnel] = *it;
    auto mustOk = tunnel->bytesSender.trySend(std::move(data));
    assert(mustOk);
}