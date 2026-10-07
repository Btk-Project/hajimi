export module hajimi.server;
import hajimi.duckdns;
import ilias;
import std;

// Forward
class ClientSession;
class ProxyRule;

/**
 * @brief The Proxy server 
 * 
 */
export class ProxyServer {
public:
    struct Config {
        ilias::IPEndpoint listen; // Bind on which address
        ilias::IPEndpoint webui; // The webui address
        
        // DuckDns
        std::string duckdnsDomain;
        std::string duckdnsToken;
        int duckdnsInterval = 300;
    };

    ProxyServer(Config config);
    ~ProxyServer();

    /**
     * @brief Start the server
     * 
     * @return ilias::IoTask<void> 
     */
    auto run() -> ilias::IoTask<void>;

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
     * @return ilias::Result<void, std::string> 
     */
    auto addRule(std::string_view json) -> ilias::Result<void, std::string>;

    /**
     * @brief Remove an rule
     * 
     * @param json The json string of the request
     * @return ilias::Result<void, std::string> 
     */
    auto removeRule(std::string_view json) -> ilias::Result<void, std::string>;
private:
    auto handleSession(ilias::TcpStream stream) -> ilias::IoTask<void>;

    Config mConfig;
    ilias::TaskScope *mScope = nullptr;
    std::optional<DuckDnsUpdater> mDuckDns; // DuckDns updater, nullopt if ddns is disabled
    std::chrono::steady_clock::time_point mStartTime; // timestamp of inited
    std::map<std::string, std::shared_ptr<ClientSession> > mSessions; // name -> session
    std::map<std::uint16_t, std::shared_ptr<ProxyRule> >   mRules; // The rules of proxy [port: rule]
friend class ProxyRule;
};
