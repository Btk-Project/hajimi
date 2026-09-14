#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <string>
#include <memory>
#include <chrono>
#include <map>

// Import
using ilias::IPEndpoint;
using ilias::TcpStream;
using ilias::BufStream;
using ilias::IoTask;

// Forward
class ClientSession;

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

    /**
     * @brief Get the status, in json format
     * 
     * @return std::string 
     */
    auto status() const -> std::string;

    // Must call in run
private:
    auto handleSession(TcpStream stream) -> IoTask<void>;
    auto handleRule() -> IoTask<void>;

    Config mConfig;
    ilias::TaskScope *mScope = nullptr;
    std::chrono::steady_clock::time_point mStartTime; // timestamp of inited
    std::map<std::string, ClientSession *> mSessions; // name -> session
};