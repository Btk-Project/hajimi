#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>

// Import
using ilias::IPEndpoint;
using ilias::TcpStream;
using ilias::IoTask;

/**
 * @brief The Proxy server 
 * 
 */
class ProxyServer {
public:
    struct Config {
        IPEndpoint listen; // Bind on which address
        IPEndpoint webui; // The webui address
    };

    ProxyServer(Config config);
    ~ProxyServer();

    /**
     * @brief Start the server
     * 
     * @return IoTask<void> 
     */
    auto run() -> IoTask<void>;
private:
    Config mConfig;
};