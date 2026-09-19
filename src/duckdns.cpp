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
    mInterval(interval)
{

}

DuckDnsUpdater::~DuckDnsUpdater() = default;

auto DuckDnsUpdater::run() -> Task<void> {
    while (true) {
        auto res = co_await updateOnce();
        if (!res) {
            std::println("[DuckDns] Update failed => {}", res.error().message());
        }
        std::println("[DuckDns] Waiting for {}", mInterval);
        co_await ilias::sleep(mInterval);
    }
}

auto DuckDnsUpdater::probeIPV6() -> IoResult<IPAddress> {
    ILIAS_TRY(auto info, ilias::AddressInfo::fromHostnameBlocking("", "80"));
    for (auto endpoint : info.endpoints()) {
        if (endpoint.family() != AF_INET6) {
            continue;
        }
        auto addr6 = endpoint.address6();
        if ((addr6.s6_addr[0] & 0xE0) == 0x20) { // Global
            return addr6;
        }
    }

    return ilias::Err(std::make_error_code(std::errc::address_not_available));
}

auto DuckDnsUpdater::updateOnce() -> IoTask<void> {
    using namespace ilias;
    std::println("[DuckDns] Update for domain '{}'", mDomain);

    // Try Get V6
    std::string ipv6;
    if (auto v6 = probeIPV6(); v6) {
        std::println("[DuckDns] Probe v6 address {}", *v6);
        ipv6 = "&ipv6=" + v6->toString();
    }

    // Connect to server
    ILIAS_CO_TRY(auto info, co_await AddressInfo::lookup("www.duckdns.org:443"));
    ILIAS_CO_TRY(auto tcp, co_await happy_eyeballs::connect(info.endpoints()));

    // Get https://www.duckdns.org/update?domains={YOURVALUE}&token={YOURVALUE}[&ip={YOURVALUE}][&ipv6={YOURVALUE}][&verbose=true][&clear=true]
    std::println("[DuckDns] Tcp connected, do handshake");
    TlsStream stream{mCtxt, std::move(tcp)};
    stream.setHostname("www.duckdns.org");
    ILIAS_CO_TRYV(co_await stream.handshake(TlsRole::Client));

    std::string request = std::format(
        "GET /update?domains={}&token={}&ip={} HTTP/1.1\r\n"
        "Host: www.duckdns.org\r\n"
        "User-Agent: curl/8.0.0\r\n"
        "Connection: close\r\n"
        "\r\n",
        mDomain,
        mToken,
        ipv6
    );
    // std::println("[DuckDns] Request {}", request);
    ILIAS_CO_TRYV(co_await stream.writeString(request));
    ILIAS_CO_TRYV(co_await stream.flush());

    // Waiting reply
    ILIAS_CO_TRY(auto reply, co_await stream.readTo<std::string>());

    // We just check the reply body contains the OK?
    auto pos = reply.find("\r\n\r\n");
    auto body = std::string{reply}.substr(pos + 4);
    // std::println("[DuckDns] body {}", body);
    if (body.contains("OK")) {
        std::println("[DuckDns] Update the domain '{}' successfully", mDomain);
    }
    co_return {};
}