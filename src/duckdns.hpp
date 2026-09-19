#pragma once

#include <ilias/net.hpp>
#include <ilias/tls.hpp>
#include <chrono>
#include <string>

using ilias::Task;
using ilias::IoTask;
using ilias::IoResult;
using ilias::IPAddress;

/**
 * @brief Updater for duckdns
 * 
 */
class DuckDnsUpdater {
public:
    explicit DuckDnsUpdater(std::string_view domain, std::string_view token, int interval);
    DuckDnsUpdater(const DuckDnsUpdater &) = delete;
    ~DuckDnsUpdater();

    auto run() -> Task<void>;
private:
    auto updateOnce() -> IoTask<void>;
    auto probeIPV6() -> IoTask<IPAddress>;

    std::string mDomain;
    std::string mToken;
    std::chrono::seconds mInterval; //< Update interval in seconds
    ilias::TlsContext mCtxt;
};