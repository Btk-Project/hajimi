export module hajimi.duckdns;
import std;
import ilias;

/**
 * @brief Updater for duckdns
 * 
 */
export class DuckDnsUpdater {
public:
    explicit DuckDnsUpdater(std::string_view domain, std::string_view token, int interval);
    DuckDnsUpdater(const DuckDnsUpdater &) = delete;
    ~DuckDnsUpdater();

    // Start the update loop
    auto run() -> ilias::Task<void>;

    // Get the domain update for
    inline auto domain() const -> std::string_view { return mDomain; }

    // Get the detected IPv6 address
    inline auto ipv6() const -> std::string_view { return mIPV6; }

    // Get the detected IPv4 address
    inline auto ipv4() const -> std::string_view { return mIPV4; }

    // Get current status message
    inline auto status() const -> std::string_view { return mStatus; }

    // Get the interval between updates
    inline auto interval() const -> std::chrono::seconds { return mInterval; }
private:
    auto updateOnce() -> ilias::IoTask<void>;
    auto probeIPV6() -> ilias::IoTask<ilias::IPAddress6>;
    auto httpsGet(std::string_view host, std::string_view path) -> ilias::IoTask<std::string>;

    std::string mDomain;
    std::string mToken;
    std::string mIPV6; //< Currently detected IPv6 address
    std::string mIPV4; //< Currently detected IPv4 address
    std::string mStatus; // < Last status message
    std::chrono::seconds mInterval; //< Update interval in seconds
    ilias::TlsContext mCtxt;
};
