#include <argparse/argparse.hpp>
#include <ilias/platform.hpp>
#include <ilias/signal.hpp>
#include <ilias/task.hpp>

#ifndef NDEBUG
    #include <gtest/gtest.h>
#endif // NDEBUG

#include <sstream>
#include <ranges>
#include <print>

#include "server.hpp"
#include "client.hpp"

int ilias_main(int argc, char **argv) try {
    argparse::ArgumentParser parser{"hajimi"};

    parser.add_description("Hajimi - Asyncio Reverse Proxy, ilias runtime version: " ILIAS_VERSION_STRING);

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

    // Server Quic
    parser.add_argument("--cert-file")
        .help("Path to the certificate file");

    parser.add_argument("--key-file")
        .help("Path to the private key file");

    // Client options
    parser.add_argument("--connect")
        .help("Run as client: master address e.g. 'myhome.duckdns.org:8000', '127.0.0.1:8000', or '[::1]:8000'");

    parser.add_argument("--name")
        .help("Client name (used when running in client mode)");

    parser.add_argument("--transport")
        .default_value(std::string{"auto"})
        .help("Transport: auto (QUIC first, then plain TCP), tcp or quic");

    parser.add_argument("--forward")
        .help("The tunnel to the target serve e.g. '127.0.0.1:8848@client-name:8000'");

    // Client Quic
    parser.add_argument("--ca-file")
        .help("The server certificate to trust");

    parser.add_argument("--insecure")
        .flag()
        .help("Disable server certificate verification");

    // Unit Test
#ifndef NDEBUG
    parser.add_argument("--test")
        .flag()
        .help("Run tests instead of the application");
#endif // NDEBUG

    // Parse it
    parser.parse_args(argc, argv);

    // Is server ?
    if (auto listen = parser.present("--listen"); listen) {
        auto webui = parser.present("--webui").value_or("127.0.0.1:0"); // Choose a random port

        // Parse it
        auto duckdnsDomain = parser.present("--duckdns-domain").value_or("");
        auto duckdnsToken = parser.present("--duckdns-token").value_or("");
        if (auto duckdns = parser.present("--duckdns"); duckdns) {
            auto pos = duckdns->find(":");
            if (pos == std::string::npos) {
                throw std::runtime_error{"Bad string in --duckdns, did you missing :?"};
            }
            duckdnsDomain = duckdns->substr(0, pos);
            duckdnsToken = duckdns->substr(pos + 1);
        }

        ProxyServer server {
            ProxyServer::Config {
                .listen = IPEndpoint::fromString(*listen).value(),
                .webui = IPEndpoint::fromString(webui).value(),

                // Quic
                .certFile = parser.present("--cert-file").value_or(""),
                .keyFile = parser.present("--key-file").value_or(""),

                // DuckDns
                .duckdnsDomain = duckdnsDomain,
                .duckdnsToken = duckdnsToken,
                .duckdnsInterval = parser.get<int>("--duckdns-interval"),
            }
        };
        auto [err, ctrlC] = co_await ilias::whenAny(
            server.run(),
            ilias::signal::ctrlC()
        );
        if (err && !*err) {
            std::println("[App] Server failed to start => {}", (*err).error().message());
            co_return 1;
        }
        if (ctrlC) {
            std::println("[App] Ctrl-C, Quiting");
        }
        co_return 0;
    }
    else if (auto connect = parser.present("--connect"); connect) {
        std::vector<std::pair<IPEndpoint, std::string>> forwards; // e.g. 127.0.0.1:8848@client-name:8000
        // Parse forwards 
        if (auto forward = parser.present("--forward"); forward) {
            for (auto s : *forward | std::views::split(',')) {
                std::string_view f{s.begin(), s.end()};
                auto pos = f.find('@');
                if (pos == std::string::npos) {
                    throw std::runtime_error{"Bad string in --forward, did you missing @?"};
                }
                auto target = f.substr(0, pos); // e.g. 127.0.0.1:8848
                auto name = f.substr(pos + 1); // e.g. client-name:8000
                forwards.emplace_back(IPEndpoint::fromString(target).value(), name);
            }
        }

        ProxyClient client {
            ProxyClient::Config {
                .master = connect.value(),
                .name = parser.present("--name").value_or("hajimi-client"),
                .transport = parser.get<std::string>("--transport"),
                .forwards = std::move(forwards),

                // Quic
                .caFile = parser.present("--ca-file").value_or(""),
                .verify = !parser.get<bool>("--insecure"),
            }
        };
        auto [result, ctrlC] = co_await ilias::whenAny(
            client.run(),
            ilias::signal::ctrlC()
        );
        if (result && !*result) {
            std::println(stderr, "[App] Client failed => {}", result->error().message());
            co_return 1;
        }
        co_return 0;
    }
#ifndef NDEBUG
    else if (parser.get<bool>("--test")) {
        ::testing::InitGoogleTest();
        co_return RUN_ALL_TESTS();
    }
#endif // NDEBUG
    else {
        std::stringstream ss;
        ss << parser;
        std::println("{}", ss.str());
        co_return 0;
    }
}
catch (std::exception &e) {
    std::println(stderr, "{}", e.what());
    co_return 1;
}
