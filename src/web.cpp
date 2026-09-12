#include <nlohmann/json.hpp>
#include <ilias/macros.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <ranges>
#include <string>
#include <format>
#include <print>
#include "web.hpp"

// Import types
using nlohmann::json;

using ilias::TcpListener;
using ilias::TcpStream;
using ilias::BufStream;
using ilias::TaskScope;
using ilias::Task;

// Static
extern "C" {
    extern const char _binary_index_html_start[];
    extern const char _binary_index_html_end[];
}

// Impl
WebUi::WebUi() {

}

auto WebUi::run(IPEndpoint endpoint) -> IoTask<void> {
    std::println("[WebUi] listen on {}", endpoint);
    ILIAS_CO_TRY(auto listener, co_await TcpListener::bind(endpoint));
    
    // Handle incoming connection
    co_await TaskScope::enter([&](auto &scope) -> Task<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[WebUi] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
            scope.spawn(handleIncoming(std::move(sock)));
        }
    });
    co_return {};
}

auto WebUi::handleIncoming(Stream stream) -> IoTask<void> {
    // Accept each http request and respond
    // GET PATH HTTP1.1
    // HEADER
    // \r\n
    bool keepAlive = true;
    while (true) {
        std::string initLine;
        std::string line;
        ILIAS_CO_TRY(initLine, co_await stream.getline("\r\n"));
        if (!initLine.ends_with("HTTP/1.1")) {
            break;
        }

        // Parse the method and the path
        auto compoments = 
            initLine | 
            std::views::split(' ') |
            std::ranges::to<std::vector<std::string>>()
        ;
        if (compoments.size() != 3) {
            break;
        }
        auto &method = compoments[0];
        auto &path = compoments[1];

        // Parse the headers
        while (true) {
            ILIAS_CO_TRY(line, co_await stream.getline("\r\n"));
            if (line.empty()) {
                break;
            }
            if (line.starts_with("Connection:") && line.ends_with("close")) {
                keepAlive = false;
            }
        }

        // Route
        ILIAS_CO_TRYV(co_await dispatch(stream, method, path));
        ILIAS_CO_TRYV(co_await stream.flush());
        if (!keepAlive) {
            break;
        }
    }
    co_return {};
}

auto WebUi::dispatch(Stream &stream, std::string_view method, std::string_view path) -> IoTask<void> {
    auto reply = [&](int code, std::string reason, std::string_view content, std::string_view type) -> IoTask<void> {
        auto headers = std::format(
            "HTTP/1.1 {} {}\r\n"
            "Content-Type: {}\r\n"
            "Content-Length: {}\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type\r\n"
            "\r\n",
            code, reason,
            type,
            content.size()
        );
        ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(headers)));
        if (!content.empty()) {
            ILIAS_CO_TRYV(co_await stream.writeAll(ilias::makeBuffer(content)));
        }
        ILIAS_CO_TRYV(co_await stream.writeString("\r\n")); // End 
        co_return {};
    };

    std::println("[WebUi] {}:{}", method, path);

    // Serve WebUI HTML
    if (path == "/" || path == "/index.html") {
        std::string_view html {
            _binary_index_html_start,
            _binary_index_html_end
        };
        co_return co_await reply(200, "OK", html, "text/html; charset=utf-8");
    }
    co_return co_await reply(404, "Not Found", "404 Not Found", "text/plain");
}
