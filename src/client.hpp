#pragma once

#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <map>

#include "protocol.hpp"

// Import
using ilias::IPEndpoint;
using ilias::StreamView;
using ilias::Mutex;
using ilias::IoTask;

// Forward
class ProxyClient;

/**
 * @brief The proxy client
 *
 * Connects to the master server and bridges reverse proxy connections.
 * All the control messages and tunnel data are multiplexed on this
 * single TCP connection (see protocol.hpp).
 */
class ProxyClient {
public:
    struct Config {
        std::string master; // Master address, "host:port", a domain:port or "[ipv6]:port"
        std::string name; // Client name
    };

    ProxyClient(Config config);
    ~ProxyClient();

    /**
     * @brief Run the client, keep reconnecting to the master
     *
     * @return IoTask<void>
     */
    auto run() -> IoTask<void>;
private:
    // Connect, register, dispatch messages until the connection breaks
    auto connectOnce(bool &registered) -> IoTask<void>;
    Config mConfig;
};
