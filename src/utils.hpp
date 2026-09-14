#pragma once

#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <algorithm>
#include <vector>
#include <print>

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

    auto lastError = std::make_error_code(std::errc::host_unreachable);
    std::println("[HE2] Connect {}", endpoints);
    if (endpoints.empty()) {
        co_return Err(lastError);
    }

    TaskGroup<IoResult<TcpStream>> group{};
    size_t nextIdx = 0;
    size_t inFlight = 0;

    // Add one
    std::println("[HE2] Try connect {}, {} left", endpoints[nextIdx], endpoints.size() - 1);
    group.spawn(TcpStream::connect(endpoints[nextIdx++]));
    ++inFlight;

    while (inFlight > 0) {
        if (nextIdx < endpoints.size()) {
            // Has more
            auto tryWait = co_await ilias::timeout(group.next(), 250ms);

            if (tryWait) {
                --inFlight;
                auto &conn = tryWait->value(); // We didn't cancel, just unwrap
                if (conn) {
                    // Ok
                    co_return std::move(*conn);
                }

                // Save it, try more
                lastError = conn.error();
            }
            // Add more
            std::println("[HE2] Try connect {}, {} left", endpoints[nextIdx], endpoints.size() - nextIdx - 1);
            group.spawn(TcpStream::connect(endpoints[nextIdx++]));
            ++inFlight;
        }
        else {
            // All submit
            auto conn = (co_await group.next()).value(); // We didn't cancel, just unwrap
            --inFlight;

            if (conn) {
                co_return std::move(*conn);
            }
            lastError = conn.error();
        }
    }

    // All failed
    co_return Err(lastError);}

} // happy_eyeballs