#pragma once

#include "quic.hpp"
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#include <openssl/ssl.h>

namespace quic {

using TlsContext = std::shared_ptr<SSL_CTX>;

struct TlsSession {
    SSL *ssl = nullptr;
    ngtcp2_crypto_ossl_ctx *handle = nullptr;
    ngtcp2_crypto_conn_ref ref{};

    TlsSession() = default;
    TlsSession(const TlsSession &) = delete;
    ~TlsSession();

    auto init(SSL_CTX *context, ngtcp2_conn *conn, const QuicContext::Config &config,
              const std::string &serverName) -> ilias::IoResult<void>;
};

auto makeTlsContext(const QuicContext::Config &config) -> ilias::IoResult<TlsContext>;

} // namespace quic
