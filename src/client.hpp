#pragma once

#include <ilias/sync.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <vector>
#include <string>

// Import
using ilias::IPEndpoint;
using ilias::StreamView;
using ilias::IoTask;
using ilias::Mutex;

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
        std::string transport = "auto"; // The transport protocol, "tcp" or "quic" or "auto"
        std::vector<std::pair<IPEndpoint, std::string>> forwards; // The local forward connections

        // Quic specific
        std::string caFile; // CA certificate file
        bool verify = true; // Verify the master's certificate
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
    auto connectOnce(bool &registered, void *quicContext) -> IoTask<void>;
    auto runOnce(bool &registered, StreamView stream) -> IoTask<void>;

    Config mConfig;
};
