#include <nlohmann/json.hpp>
#include <ilias/macros.hpp>
#include <ilias/task.hpp>
#include <ilias/net.hpp>
#include <ilias/io.hpp>
#include <ranges>
#include <string>
#include <format>
#include <print>
#include "server.hpp"
#include "web.hpp"

// Import types
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
WebUi::WebUi(ProxyServer &server, IPEndpoint endpoint) : mServer(server), mEndpoint(endpoint) {

}

auto WebUi::run() -> IoTask<void> {
    ILIAS_CO_TRY(auto listener, co_await TcpListener::bind(mEndpoint));
    std::println("[WebUi] listen on {}", listener.localEndpoint().value());
    
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
        std::string content;
        std::optional<size_t> contentLength;
        while (true) {
            ILIAS_CO_TRY(line, co_await stream.getline("\r\n"));
            if (line.empty()) {
                break;
            }
            if (line.starts_with("Connection: ") && line.ends_with("close")) {
                keepAlive = false;
            }
            else if (line.starts_with("Content-Length: ")) {
                auto numStr = std::string_view{line}.substr(16);
                auto len = size_t{0};
                if (std::from_chars(numStr.data(), numStr.data() + numStr.size(), len).ec != std::errc{}) {
                    break;
                }
                contentLength = len;
            }
        }

        // Read the content
        if (contentLength && contentLength != 0) {
            content.resize(*contentLength);
            ILIAS_CO_TRYV(co_await stream.readAll(ilias::makeBuffer(content)));
        }

        // Route
        ILIAS_CO_TRYV(co_await dispatch(stream, method, path, content));
        ILIAS_CO_TRYV(co_await stream.flush());
        if (!keepAlive) {
            break;
        }
    }
    co_return {};
}

auto WebUi::dispatch(Stream &stream, std::string_view method, std::string_view path, std::string_view content) -> IoTask<void> {
    auto reply = [&](int code, std::string reason, std::string_view content, std::string_view type) -> IoTask<void> {
        auto headers = std::format(
            "HTTP/1.1 {} {}\r\n"
            "Content-Type: {}\r\n"
            "Content-Length: {}\r\n"
            "Connection: keep-alive\r\n"
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

    using namespace std::chrono;
    using nlohmann::json;

    // std::println("[WebUi] {}:{}, contentLength: {}", method, path, content.size());

    // Serve WebUI HTML
    if (path == "/" || path == "/index.html") {
        std::string_view html {
            _binary_index_html_start,
            _binary_index_html_end
        };
        co_return co_await reply(200, "OK", html, "text/html; charset=utf-8");
    }
    // Status
    if (path == "/api/status" && method == "GET") {
        auto status = mServer.status();
        co_return co_await reply(200, "OK", status, "application/json; charset=utf-8");
    }
    if (path == "/api/rules" && (method == "POST" || method == "DELETE")) {
        json js = json::object();

        // Fill the reply
        auto res = [&]() {
            if (method == "POST") { // add rule
                std::println("[WebUi] add rule: {}", content);
                return mServer.addRule(content);
            }
            else {
                std::println("[WebUi] remove rule: {}", content);
                return mServer.removeRule(content);
            }
        }();
        js["ok"] = res.has_value();
        if (!res) {
            js["error"] = res.error();
        }
        auto code = res ? 200 : 400;
        auto reason = res ? "OK" : "BAD REQUEST";
        co_return co_await reply(code, reason, js.dump(), "application/json; charset=utf-8");
    }
    co_return co_await reply(404, "Not Found", "404 Not Found", "text/plain");
}
