export module hajimi.utils;

import ilias;
import std;

// Scope Exit helper
export template <typename Fn>
class ScopeExit {
public:
    inline ScopeExit(Fn fn) : mFn(fn) {}
    inline ~ScopeExit() { mFn(); }
private:
    Fn mFn;
};


// HE2
namespace happy_eyeballs {

export auto connect(std::vector<ilias::IPEndpoint> endpoints) -> ilias::IoTask<ilias::TcpStream> {
    using namespace std::chrono_literals;
    using namespace ilias;

    // Deduplicate
    auto newEnd = std::unique(endpoints.begin(), endpoints.end());
    endpoints.erase(newEnd, endpoints.end());

#if defined(__cpp_lib_format_ranges)
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

} // namespace happy_eyeballs