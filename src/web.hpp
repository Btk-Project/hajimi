#pragma once

#include <ilias/net.hpp>
#include <ilias/io.hpp>

using ilias::TcpStream;
using ilias::BufStream;
using ilias::IPEndpoint;
using ilias::IoTask;

// Forward
class ProxyServer;

/**
 * @brief The webui
 * 
 */
class WebUi {
public:
    // Borrow the server
    WebUi(ProxyServer &server, IPEndpoint endpoint);
    WebUi(const WebUi &) = delete;

    /**
     * @brief Bind to the endpoint and start the webui server
     * 
     * @return IoTask<void>
     */
    auto run() -> IoTask<void>;
private:
    using Stream = BufStream<TcpStream>;
    
    auto handleIncoming(Stream stream) -> IoTask<void>;
    auto dispatch(Stream &stream, std::string_view method, std::string_view path) -> IoTask<void>;

    ProxyServer &mServer;
    IPEndpoint   mEndpoint;
};