#pragma once

// Wire protocol for Hajimi
#include <cstdint>
#include <span>

struct Hello {
    uint32_t version;
};