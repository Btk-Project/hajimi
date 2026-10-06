#pragma once

#include <ilias/net.hpp>
#include <ilias/tls.hpp>
#include <chrono>
#include <string>

using ilias::Task;
using ilias::IoTask;
using ilias::IoResult;
using ilias::IPAddress6;

/**
 * @brief Updater for duckdns
 * 
 */
class DuckDnsUpdater {
public:
    explicit DuckDnsUpdater(std::string_view domain, std::string_view token, int interval);
    DuckDnsUpdater(const DuckDnsUpdater &) = delete;
    ~DuckDnsUpdater();

    // Start the update loop
    auto run() -> Task<void>;

    // Get the domain update for
    auto domain() const -> std::string_view { return mDomain; }

    // Get the detected IPv6 address
    auto ipv6() const -> std::string_view { return mIPV6; }

    // Get the detected IPv4 address
    auto ipv4() const -> std::string_view { return mIPV4; }

    // Get current status message
    auto status() const -> std::string_view { return mStatus; }

    // Get the interval between updates
    auto interval() const -> std::chrono::seconds { return mInterval; }
private:
    auto updateOnce() -> IoTask<void>;
    auto probeIPV6() -> IoTask<IPAddress6>;
    auto httpsGet(std::string_view host, std::string_view path) -> IoTask<std::string>;

    std::string mDomain;
    std::string mToken;
    std::string mIPV6; //< Currently detected IPv6 address
    std::string mIPV4; //< Currently detected IPv4 address
    std::string mStatus; // < Last status message
    std::chrono::seconds mInterval; //< Update interval in seconds
    ilias::TlsContext mCtxt;
};