export module hajimi.client;
import std;
import ilias;

/**
 * @brief The proxy client
 *
 * Connects to the master server and bridges reverse proxy connections.
 * All the control messages and tunnel data are multiplexed on this
 * single TCP connection (see hajimi.protocol).
 */
export class ProxyClient {
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
     * @return ilias::IoTask<void>
     */
    auto run() -> ilias::IoTask<void>;
private:
    // Connect, register, dispatch messages until the connection breaks
    auto connectOnce(bool &registered) -> ilias::IoTask<void>;
    Config mConfig;
};
