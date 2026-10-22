#include <ilias/platform.hpp>
#include <ilias/task.hpp>
#include <array>
#include <charconv>
#include <ctime>
#include <print>
#include "../src/quic/quic.hpp"

using ilias::Err;
using ilias::IoTask;

auto cpuTime() -> double {
#ifdef _WIN32
    FILETIME created{}, exited{}, kernel{}, user{};
    GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user);
    auto ticks = [](FILETIME value) { return (uint64_t(value.dwHighDateTime) << 32) | value.dwLowDateTime; };
    return double(ticks(kernel) + ticks(user)) / 10000000;
#else
    return double(std::clock()) / CLOCKS_PER_SEC;
#endif
}

int ilias_main(int argc, char **argv) {
    if (argc > 1 && std::string_view(argv[1]) != "single" && std::string_view(argv[1]) != "batch") co_return 2;
    bool batching = argc < 2 || std::string_view(argv[1]) != "single";
    size_t mebibytes = 512;
    if (argc > 2) {
        auto value = std::string_view(argv[2]);
        auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), mebibytes);
        if (error != std::errc{} || end != value.data() + value.size() || !mebibytes || mebibytes > 16384) co_return 2;
    }
    std::array<unsigned char, 32> psk;
    psk.fill(0x42); // Local benchmark credentials only.
    QuicContext::Config config;
    config.psk = psk;
    config.udpBatching = batching;
    auto clientResult = co_await QuicContext::bind("127.0.0.1:0", config);
    config.role = QuicContext::Role::Server;
    auto serverResult = co_await QuicContext::bind("127.0.0.1:0", config);
    if (!clientResult || !serverResult) co_return 1;
    auto &client = *clientResult;
    auto &server = *serverResult;
    auto run = [&]() -> IoTask<void> {
        auto [connected, accepted] = co_await ilias::whenAll(
            client.connect(server.localEndpoint().value(), ""), server.accept());
        ILIAS_CO_TRY(auto local, std::move(connected));
        ILIAS_CO_TRY(auto peer, std::move(accepted));
        ILIAS_CO_TRY(auto stream, co_await local.openStream());
        constexpr size_t chunk = 64 * 1024;
        std::array<std::byte, chunk> payload{};
        size_t total = mebibytes * 1024 * 1024;
        size_t received = 0;
        auto write = [&]() -> IoTask<void> {
            for (size_t bytes = 0; bytes < total; bytes += chunk) {
                ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(payload)));
            }
            ILIAS_CO_TRYV(co_await stream.flush());
            co_return co_await stream.shutdown();
        };
        auto read = [&]() -> IoTask<void> {
            ILIAS_CO_TRY(auto incoming, co_await peer.acceptStream());
            std::array<std::byte, chunk> buffer;
            while (true) {
                ILIAS_CO_TRY(auto size, co_await incoming.read(ilias::makeBuffer(buffer)));
                if (!size) break;
                received += size;
            }
            co_return received == total ? ilias::IoResult<void>{} :
                ilias::IoResult<void>{Err(std::make_error_code(std::errc::bad_message))};
        };
        auto start = std::chrono::steady_clock::now();
        auto cpuStart = cpuTime();
#ifdef _WIN32
        ULONG64 cyclesStart = 0;
        QueryProcessCycleTime(GetCurrentProcess(), &cyclesStart);
#endif
        auto [written, readResult] = co_await ilias::whenAll(write(), read());
        ILIAS_CO_TRYV(std::move(written));
        ILIAS_CO_TRYV(std::move(readResult));
        auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        auto cpu = cpuTime() - cpuStart;
        double cyclesPerByte = 0;
#ifdef _WIN32
        ULONG64 cyclesEnd = 0;
        QueryProcessCycleTime(GetCurrentProcess(), &cyclesEnd);
        cyclesPerByte = double(cyclesEnd - cyclesStart) / total;
#endif
        auto tx = client.ioStats();
        auto rx = server.ioStats();
        std::println("{{\"batching\":{},\"MiB\":{},\"seconds\":{:.4f},\"MiBps\":{:.2f},"
            "\"cpuSeconds\":{:.4f},\"cyclesPerByte\":{:.2f},\"txDatagrams\":{},\"txSubmissions\":{},"
            "\"maxTxBatch\":{},\"rxDatagrams\":{},\"rxSubmissions\":{},\"rxCoalescedDatagrams\":{},"
            "\"txSegmentedDatagrams\":{},\"segmentationFallbacks\":{},\"maxRxBatch\":{},\"droppedReceives\":{}}}",
            batching, mebibytes, seconds, mebibytes / seconds, cpu, cyclesPerByte,
            tx.txDatagrams, tx.txSubmissions, tx.maxTxBatch, rx.rxDatagrams, rx.rxSubmissions, rx.rxCoalescedDatagrams,
            tx.txSegmentedDatagrams, tx.segmentationFallbacks, rx.maxRxBatch, rx.droppedReceives);
        co_return {};
    };
    auto result = co_await ilias::TaskScope::enter([&](ilias::TaskScope &scope) -> IoTask<void> {
        scope.spawn(client.run());
        scope.spawn(server.run());
        auto result = co_await run();
        client.stop();
        server.stop();
        co_return result;
    });
    if (!result) std::println(stderr, "{}", result.error().message());
    co_return result ? 0 : 1;
}
