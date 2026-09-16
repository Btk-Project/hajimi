#include <argparse/argparse.hpp>
#include <ilias/platform.hpp>
#include <ilias/signal.hpp>
#include <ilias/task.hpp>
#include <iostream>
#include "server.hpp"
#include "client.hpp"

int ilias_main(int argc, char **argv) try {
    argparse::ArgumentParser parser{"hajimi"};

    parser.add_description("Hajimi - Asyncio Reverse Proxy");

    // Server options
    parser.add_argument("--listen")
        .help("Run as server: listen address e.g. '0.0.0.0:8000', '[::]:8000'");

    parser.add_argument("--webui")
        .help("Optional dedicated WebUI address e.g. '0.0.0.0:9000', '127.0.0.1:8080'");

    parser.add_argument("--duckdns")
        .help("DuckDNS configuration in 'domain:token' format (e.g. 'myhome:a7c4d0ad-...')");

    parser.add_argument("--duckdns-domain")
        .help("DuckDNS domain/subdomain e.g. 'myhome' or 'myhome.duckdns.org'");

    parser.add_argument("--duckdns-token")
        .help("DuckDNS account token");

    parser.add_argument("--duckdns-interval")
        .scan<'d', int>()
        .default_value(300)
        .help("DuckDNS update interval in seconds (default: 300)");

    // Client options
    parser.add_argument("--connect")
        .help("Run as client: master address e.g. 'myhome.duckdns.org:8000', '127.0.0.1:8000', or '[::1]:8000'");

    parser.add_argument("--name")
        .help("Client name (used when running in client mode)");

    // Parse it
    parser.parse_args(argc, argv);

    // Is server ?
    if (auto listen = parser.present("--listen"); listen) {
        auto webui = parser.present("--webui").value_or("127.0.0.1:0"); // Choose a random port

        ProxyServer server {
            ProxyServer::Config {
                .listen = IPEndpoint::fromString(listen.value()).value(),
                .webui = IPEndpoint::fromString(webui).value()
            }
        };
        auto [err, ctrlC] = co_await ilias::whenAny(
            server.run(),
            ilias::signal::ctrlC()
        );
        if (err && !*err) {
            std::println("[App] Server failed to start => {}", (*err).error().message());
        }
        if (ctrlC) {
            std::println("[App] Ctrl-C, Quiting");
        }
        co_return 0;
    }
    else if (auto connect = parser.present("--connect"); connect) {
        ProxyClient client {
            ProxyClient::Config {
                .master = connect.value(),
                .name = parser.present("--name").value_or("hajimi-client")
            }
        };
        auto _ = co_await ilias::whenAny(
            client.run(),
            ilias::signal::ctrlC()
        );
        co_return 0;
    }
    else {
        std::cerr << parser;
        co_return 0;
    }
}
catch (std::exception &e) {
    std::cerr << e.what();
    co_return 0;
}