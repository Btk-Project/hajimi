#pragma once

template <typename ...Ts>
struct Overloads : Ts... { using Ts::operator()...; };
