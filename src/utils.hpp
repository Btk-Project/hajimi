#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <algorithm>
#include <vector>
#include <print>

// Bytes vector...
using BytesVector = std::pmr::vector<std::byte>;

// ScopeExit
template <typename Fn>
class ScopeExit {
public:
    ScopeExit(Fn fn) : mFn(fn) {}
    ~ScopeExit() { mFn(); }
private:
    Fn mFn;
};

// Use HE2.0
namespace happy_eyeballs {

inline auto connect(std::vector<ilias::IPEndpoint> endpoints) -> ilias::IoTask<ilias::TcpStream> {
    using namespace std::chrono_literals;
    using namespace ilias;

    // Deduplicate
    auto newEnd = std::unique(endpoints.begin(), endpoints.end());
    endpoints.erase(newEnd, endpoints.end());

#if __cpp_lib_format_ranges
    std::println("[HE2] Connect {}", endpoints);
#endif // __cpp_lib_format_ranges

    if (endpoints.empty()) {
        co_return Err(std::make_error_code(std::errc::host_unreachable));
    }

    // TODO: Impl real HE2.0
    TaskGroup<IoResult<TcpStream> > group;
    for (auto &endpoint : endpoints) {
        group.spawn(TcpStream::connect(endpoint));
    }

    std::error_code errc;
    while (!group.empty()) {
        auto res = (co_await group.next()).value(); // We are not cancel, just call .value()
        if (res) {
            std::println("[HE2] Connect OK");
            co_return std::move(*res);
        }
        errc = res.error();
    }
    // All failed;
    std::println("[HE2] Connect all failed {}", errc.message());
    co_return ilias::Err(errc);
}

} // happy_eyeballs