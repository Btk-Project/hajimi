#pragma once

// ScopeExit
template <typename Fn>
class ScopeExit {
public:
    ScopeExit(Fn fn) : mFn(fn) {}
    ~ScopeExit() { if (!mDisarm) mFn(); }

    auto disarm() -> void { mDisarm = true; }
private:
    Fn mFn;
    bool mDisarm = false;
};
