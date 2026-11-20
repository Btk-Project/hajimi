#pragma once

#include <utility>
#include <string>
#include <format>

enum class Color : int {
    Black   = 30,
    Red     = 31,
    Green   = 32,
    Yellow  = 33,
    Blue    = 34,
    Magenta = 35,
    Cyan    = 36,
    White   = 37,

    BrightBlack   = 90,
    BrightRed     = 91,
    BrightGreen   = 92,
    BrightYellow  = 93,
    BrightBlue    = 94,
    BrightMagenta = 95,
    BrightCyan    = 96,
    BrightWhite   = 97,
};

template <typename T>
[[nodiscard]] 
inline auto styled(T&& value, Color c) {
    return std::format(
        "\033[{}m{}\033[0m",
        static_cast<int>(c),
        std::forward<T>(value)
    );
}