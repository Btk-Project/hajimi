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
using ilias::Result;

// Forward
class ClientSession;
class ProxyRule;

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

    // Must call in durling the run
    /**
     * @brief Get the status, in json format
     * 
     * @return std::string 
     */
    auto status() const -> std::string;

    /**
     * @brief Add an rule
     * 
     * @param json The json string of the request
     * @return Result<void, std::string> 
     */
    auto addRule(std::string_view json) -> Result<void, std::string>;

    /**
     * @brief Remove an rule
     * 
     * @param json The json string of the request
     * @return Result<void, std::string> 
     */
    auto removeRule(std::string_view json) -> Result<void, std::string>;
private:
    auto handleSession(TcpStream stream) -> IoTask<void>;

    Config mConfig;
    ilias::TaskScope *mScope = nullptr;
    std::chrono::steady_clock::time_point mStartTime; // timestamp of inited
    std::map<std::string, ClientSession *> mSessions; // name -> session
    std::map<std::uint16_t, ProxyRule *>   mRules; // The rules of proxy [port: rule]
};