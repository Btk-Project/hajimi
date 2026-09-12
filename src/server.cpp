#include <ilias/macros.hpp>
#include <ilias/sync.hpp>
#include <ilias/net.hpp>
#include <print>
#include "protocol.hpp"
#include "server.hpp"

using ilias::TcpListener;
using ilias::TaskScope;
using ilias::Task;

ProxyServer::ProxyServer(Config config) : mConfig(config) {

}

ProxyServer::~ProxyServer() {
    
}

auto ProxyServer::run() -> IoTask<void> {
    std::println("[ProxyServer] listen on {}", mConfig.listen);
    ILIAS_CO_TRY(auto listener, co_await TcpListener::bind(mConfig.listen));
    
    // Handle incoming connection
    co_await TaskScope::enter([&](auto &scope) -> Task<void> {
        while (true) {
            auto incoming = co_await listener.accept();
            if (!incoming) {
                std::println("[ProxyServer] error at accept {}", incoming.error().message());
                continue;
            }
            auto &[sock, addr] = *incoming;
        }
    });
    co_return {};
}