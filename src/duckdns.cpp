#include <ilias/macros.hpp>

module hajimi.duckdns;
import std;
import ilias;
import hajimi.utils;

using ilias::Task;
using ilias::IoTask;
using ilias::IoResult;
using ilias::IPAddress6;

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
            mStatus = std::format("Update failed: {}", res.error().message());
        }
        else {
            mStatus = "OK";
        }
        std::println("[DuckDns] Waiting for {}s", mInterval.count());
        co_await ilias::sleep(mInterval);
    }
}

auto DuckDnsUpdater::updateOnce() -> IoTask<void> {
    mStatus = "Updating";
    // Get public IP address
    auto v6addr = co_await probeIPV6();

    // Update DNS record
    auto path = std::format("/update?domains={}&token={}", mDomain, mToken);
    if (v6addr) {
        path += std::format("&ipv6={}", *v6addr);
    }
    ILIAS_CO_TRY(auto reply, co_await httpsGet("www.duckdns.org", path));
    if (!reply.contains("OK")) {
        co_return ilias::Err(std::make_error_code(std::errc::bad_message));
    }
    std::println("[DuckDns] Update successful");
    co_return {};
}

auto DuckDnsUpdater::probeIPV6() -> IoTask<IPAddress6> {
    // Try resolving local host to discover global unicast IPv6
    // https://api6.ipify.org/?format=text
    ILIAS_CO_TRY(auto ip, co_await httpsGet("api6.ipify.org", "/?format=text"));
    auto addr = IPAddress6::fromString(ip);
    if (!addr) { // Bad IP address
        co_return ilias::Err(std::make_error_code(std::errc::invalid_argument));
    }
    mIPV6 = ip;
    std::println("[DuckDns] Probe Address {}", *addr);
    co_return *addr;
}

auto DuckDnsUpdater::httpsGet(std::string_view host, std::string_view path) -> IoTask<std::string> {
    using namespace ilias;
    std::println("[DuckDns] Sending GET to https://{}{}", host, path);

    // Connect to target
    ILIAS_CO_TRY(auto info, co_await AddressInfo::fromHostname(host, "443"));
    ILIAS_CO_TRY(auto tcp, co_await happy_eyeballs::connect(info.endpoints()));

    TlsStream stream{mCtxt, std::move(tcp)};
    stream.setHostname(host);
    ILIAS_CO_TRYV(co_await stream.handshake(TlsRole::Client));

    // DuckDNS format: /update?domains={domain}&token={token}[&ipv6={ipv6}]
    // DuckDNS automatically detects public IPv4 if omitted
    std::string request = std::format(
        "GET {} HTTP/1.1\r\n"
        "Host: {}\r\n"
        "User-Agent: curl/8.0.0\r\n"
        "Connection: close\r\n"
        "\r\n",
        path,
        host
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
    co_return std::string{body};
}
