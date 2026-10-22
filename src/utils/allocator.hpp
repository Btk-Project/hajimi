#pragma once

#include <memory_resource>
#include <cstddef>

// AllocationTracker
class AllocationTracker final  : public std::pmr::memory_resource {
public:
    explicit AllocationTracker(std::pmr::memory_resource *upstream = std::pmr::get_default_resource()) : mUpstream(upstream) {}
    AllocationTracker(const AllocationTracker &) = delete;

    auto do_allocate(std::size_t bytes, std::size_t alignment) -> void * override {
        auto res = mUpstream->allocate(bytes, alignment);
        mAllocated += bytes;
        return res;
    }

    auto do_deallocate(void *p, std::size_t bytes, std::size_t alignment) -> void override {
        mUpstream->deallocate(p, bytes, alignment);
        mAllocated -= bytes;
    }

    auto do_is_equal(const std::pmr::memory_resource &other) const noexcept -> bool override {
        return this == &other;
    }

    // Get allocated bytes
    auto allocated() const noexcept -> std::size_t { return mAllocated; }

    // Get the upstream resource
    auto upstream() const noexcept -> std::pmr::memory_resource * { return mUpstream; }
private:
    std::pmr::memory_resource *mUpstream;
    std::size_t mAllocated = 0;
};