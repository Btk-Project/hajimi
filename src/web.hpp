#pragma once

#include <ilias/net.hpp>
#include <ilias/io.hpp>

using ilias::TcpStream;
using ilias::BufStream;
using ilias::IPEndpoint;
using ilias::IoTask;

/**
 * @brief The webui
 * 
 */
class WebUi {
public:
    WebUi();
    WebUi(const WebUi &) = delete;

    /**
     * @brief Bind to the endpoint and start the webui server
     * 
     * @param endpoint 
     * @return IoTask<void>
     */
    auto run(IPEndpoint endpoint) -> IoTask<void>;
private:
    using Stream = BufStream<TcpStream>;
    
    auto handleIncoming(Stream stream) -> IoTask<void>;
    auto dispatch(Stream &stream, std::string_view method, std::string_view path) -> IoTask<void>;
};