#include <ilias/task.hpp>
#include <ilias/net.hpp>

#include <chrono>
#include <format>
#include <print>

#include "duckdns.hpp"
#include "utils.hpp"

using ilias::Task;
using ilias::IoTask;
using ilias::IoResult;
using ilias::IPEndpoint;

DuckDnsUpdater::DuckDnsUpdater(std::string_view domain, std::string_view token, int interval): 
    mDomain(domain),
    mToken(token),
    mInterval(std::max(interval, 60))
{
    // Strip .duckdns.org suffix if user provided full domain
    if (mDomain.ends_with(".duckdns.org")) {
        mDomain.resize(mDomain.size() - 12);
    }
}

DuckDnsUpdater::~DuckDnsUpdater() = default;

auto DuckDnsUpdater::run() -> Task<void> {
    while (true) {
        auto res = co_await updateOnce();
        if (!res) {
            std::println("[DuckDns] Update failed => {}", res.error().message());
        }
        std::println("[DuckDns] Waiting for {}s", mInterval.count());
        co_await ilias::sleep(mInterval);
    }
}

auto DuckDnsUpdater::probeIPV6() -> IoTask<IPAddress> {
    // Try resolving local host to discover global unicast IPv6
    char hostnameArray[256] = {0};
    if (::gethostname(hostnameArray, sizeof(hostnameArray)) != 0) {
        co_return ilias::Err(std::make_error_code(std::errc::address_not_available));
    }
    ILIAS_CO_TRY(auto info, co_await ilias::AddressInfo::fromHostname(hostnameArray, "80"));
    for (auto endpoint : info.endpoints()) {
        if (endpoint.family() != AF_INET6) {
            continue;
        }
        auto addr6 = endpoint.address6();
        if ((addr6.s6_addr[0] & 0xE0) == 0x20) {
            co_return addr6;
        }
    }
    co_return ilias::Err(std::make_error_code(std::errc::address_not_available));
}

auto DuckDnsUpdater::updateOnce() -> IoTask<void> {
    using namespace ilias;
    std::println("[DuckDns] Updating domain '{}'", mDomain);

    // Try to probe for a global IPv6 address
    std::string ipv6Param;
    if (auto v6 = co_await probeIPV6(); v6) {
        std::println("[DuckDns] Probed IPv6 address: {}", *v6);
        ipv6Param = "&ipv6=" + v6->toString();
    }

    // Connect to DuckDNS update endpoint
    ILIAS_CO_TRY(auto info, co_await AddressInfo::lookup("www.duckdns.org:443"));
    ILIAS_CO_TRY(auto tcp, co_await happy_eyeballs::connect(info.endpoints()));

    TlsStream stream{mCtxt, std::move(tcp)};
    stream.setHostname("www.duckdns.org");
    ILIAS_CO_TRYV(co_await stream.handshake(TlsRole::Client));

    // DuckDNS format: /update?domains={domain}&token={token}[&ipv6={ipv6}]
    // DuckDNS automatically detects public IPv4 if omitted
    std::string request = std::format(
        "GET /update?domains={}&token={}{} HTTP/1.1\r\n"
        "Host: www.duckdns.org\r\n"
        "User-Agent: curl/8.0.0\r\n"
        "Connection: close\r\n"
        "\r\n",
        mDomain,
        mToken,
        ipv6Param
    );
    ILIAS_CO_TRYV(co_await stream.writeString(request));
    ILIAS_CO_TRYV(co_await stream.flush());

    // Wait for reply
    ILIAS_CO_TRY(auto reply, co_await stream.readTo<std::string>());

    // Verify HTTP response body
    auto pos = reply.find("\r\n\r\n");
    if (pos == std::string::npos) {
        std::println("[DuckDns] Invalid response from server");
        co_return ilias::Err(std::make_error_code(std::errc::bad_message));
    }

    auto body = std::string_view{reply}.substr(pos + 4);
    if (body.contains("OK")) {
        std::println("[DuckDns] Updated domain '{}' successfully", mDomain);
    }
    else {
        std::println("[DuckDns] Update domain '{}' failed, response: {}", mDomain, body);
        co_return ilias::Err(std::make_error_code(std::errc::connection_refused));
    }
    co_return {};
}