#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>

#include <algorithm>
#include <vector>
#include <print>


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
    std::error_code lastErr;

    // Not all added
    std::ranges::reverse(endpoints);

    while (!endpoints.empty() || !group.empty()) {
        if (!group.empty()) {
            // Try wait 
            auto res = co_await timeout(group.next(), 250ms);
            if (res) { // Has result
                auto &stream = res->value();
                if (stream) { // Done
                    std::println("[HE2] Connect OK, Using '{}'", stream->remoteEndpoint().value());
                    co_await group.shutdown(); // Shutdown the another tasks
                    co_return std::move(*stream);
                }
                std::println("[HE2] Connect failed by {}, {} pending, {} left", stream.error().message(), group.size(), endpoints.size());
                lastErr = stream.error();
            }
        }
        if (!endpoints.empty()) { // Start more
            std::println("[HE2] Connect To '{}', {} pending, {} left", endpoints.back(), group.size(), endpoints.size() - 1);
            group.spawn(TcpStream::connect(endpoints.back()));
            endpoints.pop_back();
        }
    }

    // All failed, none connected
    co_return Err(lastErr);
}

} // happy_eyeballs