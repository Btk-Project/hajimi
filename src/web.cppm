export module hajimi.web;
import hajimi.server;
import ilias;
import std;

/**
 * @brief The webui
 * 
 */
export class WebUi {
public:
    // Borrow the server
    WebUi(ProxyServer &server, ilias::IPEndpoint endpoint);
    WebUi(const WebUi &) = delete;

    /**
     * @brief Bind to the endpoint and start the webui server
     * 
     * @return ilias::IoTask<void>
     */
    auto run() -> ilias::IoTask<void>;
private:
    using Stream = ilias::BufStream<ilias::TcpStream>;
    
    auto handleIncoming(Stream stream) -> ilias::IoTask<void>;
    auto dispatch(Stream &stream, std::string_view method, std::string_view path, std::string_view content) -> ilias::IoTask<void>;

    ProxyServer &mServer;
    ilias::IPEndpoint   mEndpoint;
};
