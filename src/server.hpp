#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <optional>
#include <string>
#include <chrono>
#include <memory>
#include <map>

#include "duckdns.hpp"

// Import
using ilias::IPEndpoint;
using ilias::TaskScope;
using ilias::DynStream;
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

        // Quic specific
        std::string certFile;
        std::string keyFile;
        
        // DuckDns
        std::string duckdnsDomain;
        std::string duckdnsToken;
        int duckdnsInterval = 300;
    };

    struct Rule {
        std::uint16_t listenPort; // The port the server should listen on
        std::string   clientName; // The name of the client forward to
        std::string   targetHost; // The host of the target
        std::uint16_t targetPort; // The port of the target
    };

    // TODO:
    struct Status {
        
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
    auto addRule(Rule rule) -> Result<void, std::string>;

    /**
     * @brief Remove an rule
     * 
     * @param json The json string of the request
     * @return false if the rule not found
     */
    auto removeRule(std::uint16_t port) -> Result<void, std::string>;
private:
    auto runTcp() -> IoTask<void>;
    auto runQuic() -> IoTask<void>;
    auto handleSession(DynStream stream, IPEndpoint remoteEndpoint) -> IoTask<void>;

    Config mConfig;
    ilias::TaskScope *mScope = nullptr;
    std::optional<DuckDnsUpdater> mDuckDns; // DuckDns updater, nullopt if ddns is disabled
    std::chrono::steady_clock::time_point mStartTime; // timestamp of inited
    std::map<std::string, std::shared_ptr<ClientSession> > mSessions; // name -> session
    std::map<std::uint16_t, std::shared_ptr<ProxyRule> >   mRules; // The rules of proxy [port: rule]
friend class ProxyRule;
friend class ClientSession;
};