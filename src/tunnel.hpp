#pragma once

#include <ilias/sync/mpsc.hpp>
#include <ilias/task.hpp>
#include <ilias/io.hpp>
#include <functional>
#include <memory>
#include <map>

#include "protocol.hpp"
#include "skbuf.hpp"

using ilias::DynStream;
using ilias::TaskScope;
using ilias::IoTask;
using ilias::Task;

/**
 * @brief A set of the logic channels
 * 
 */
class TunnelManager {
public:
    using Entry = std::move_only_function<IoTask<DynStream> ()>; // Create an dyn stream and we will start copy data bidirectionally
    using MessageOutput = ilias::mpsc::Sender<Message>;


    TunnelManager(TaskScope &scope, SkBufferPool &pool, MessageOutput sender) : mScope(scope), mPool(pool), mMessageSender(sender) {}
    TunnelManager(const TunnelManager &) = delete; // Pin
    ~TunnelManager() = default;

    // onMessage
    auto addTunnel(uint64_t id, Entry entry) -> void; // Peer send the open message
    auto closeTunnel(uint64_t id) -> void; // Peer send the close message
    auto onWindowUpdate(uint64_t id, size_t window) -> void; // Peer send the window update message
    auto onDataExchange(uint64_t id, SkBuffer buffer) -> void;
private:
    struct Tunnel;

    TaskScope    &mScope;
    SkBufferPool &mPool;
    MessageOutput mMessageSender; // Output the message
    std::map<uint64_t, std::shared_ptr<Tunnel>> mTunnels; // The tunnels mapping
};
