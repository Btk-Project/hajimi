#pragma once

// ScopeExit
template <typename Fn>
class ScopeExit {
public:
    ScopeExit(Fn fn) : mFn(fn) {}
    ~ScopeExit() { mFn(); }
private:
    Fn mFn;
};
