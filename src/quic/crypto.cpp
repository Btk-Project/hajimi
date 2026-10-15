#include "crypto.hpp"
#include <openssl/x509v3.h>
#include <cstring>

namespace quic {

auto makeTlsContext(const QuicContext::Config &config) -> ilias::IoResult<TlsContext> {
    using ilias::Err;
    // Initialize once, and release the helper's cached algorithms at exit.
    static struct CryptoInit {
        int result = ngtcp2_crypto_ossl_init();
        ~CryptoInit() { ngtcp2_crypto_ossl_free(); }
    } init;
    if (init.result != 0) {
        return Err(make_error_code(QuicError::Tls));
    }
    TlsContext context{SSL_CTX_new(TLS_method()), SSL_CTX_free};
    if (!context || !SSL_CTX_set_min_proto_version(context.get(), TLS1_3_VERSION) ||
        !SSL_CTX_set_max_proto_version(context.get(), TLS1_3_VERSION)) {
        return Err(make_error_code(QuicError::Tls));
    }
    SSL_CTX_set_num_tickets(context.get(), 0);
    SSL_CTX_set_session_cache_mode(context.get(), SSL_SESS_CACHE_OFF);

    if (config.role == QuicContext::Role::Server) {
        if (config.certFile.empty() || config.keyFile.empty() ||
            SSL_CTX_use_certificate_chain_file(context.get(), config.certFile.c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(context.get(), config.keyFile.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(context.get()) != 1) {
            return Err(make_error_code(QuicError::Tls));
        }
        SSL_CTX_set_alpn_select_cb(context.get(), [](SSL *, const unsigned char **out,
            unsigned char *outlen, const unsigned char *in, unsigned int inlen, void *arg) -> int {
            auto &alpn = *static_cast<std::string *>(arg);
            for (unsigned int pos = 0; pos < inlen;) {
                auto len = in[pos++];
                if (len == 0 || len > inlen - pos) {
                    return SSL_TLSEXT_ERR_ALERT_FATAL;
                }
                if (len == alpn.size() && std::memcmp(in + pos, alpn.data(), len) == 0) {
                    *out = in + pos;
                    *outlen = len;
                    return SSL_TLSEXT_ERR_OK;
                }
                pos += len;
            }
            return SSL_TLSEXT_ERR_ALERT_FATAL;
        }, const_cast<std::string *>(&config.alpn));
    }
    else if (config.verifyPeer) {
        SSL_CTX_set_verify(context.get(), SSL_VERIFY_PEER, nullptr);
        auto ok = config.caFile.empty() ? SSL_CTX_set_default_verify_paths(context.get()) :
            SSL_CTX_load_verify_locations(context.get(), config.caFile.c_str(), nullptr);
        if (ok != 1) {
            return Err(make_error_code(QuicError::Tls));
        }
    }
    return context;
}

TlsSession::~TlsSession() {
    // SSL may retain references to ngtcp2 handshake buffers.
    if (ssl) {
        SSL_set_app_data(ssl, nullptr);
        SSL_free(ssl);
    }
    ngtcp2_crypto_ossl_ctx_del(handle);
}

auto TlsSession::init(SSL_CTX *context, ngtcp2_conn *conn, const QuicContext::Config &config,
                      const std::string &serverName) -> ilias::IoResult<void> {
    using ilias::Err;
    ssl = SSL_new(context);
    if (!ssl || ngtcp2_crypto_ossl_ctx_new(&handle, ssl) != 0) {
        return Err(make_error_code(QuicError::Tls));
    }
    ref.get_conn = [](ngtcp2_crypto_conn_ref *ref) {
        return static_cast<ngtcp2_conn *>(ref->user_data);
    };
    ref.user_data = conn;
    SSL_set_app_data(ssl, &ref);
    if (config.role == QuicContext::Role::Server) {
        SSL_set_accept_state(ssl);
        if (ngtcp2_crypto_ossl_configure_server_session(ssl) != 0) {
            return Err(make_error_code(QuicError::Tls));
        }
    }
    else {
        SSL_set_connect_state(ssl);
        std::string alpn(1, static_cast<char>(config.alpn.size()));
        alpn += config.alpn;
        if (ngtcp2_crypto_ossl_configure_client_session(ssl) != 0 ||
            SSL_set_alpn_protos(ssl, reinterpret_cast<const unsigned char *>(alpn.data()),
                                static_cast<unsigned int>(alpn.size())) != 0) {
            return Err(make_error_code(QuicError::Tls));
        }
        if (!serverName.empty()) {
            // Numeric addresses use IP SAN verification and do not send SNI.
            auto param = SSL_get0_param(ssl);
            if (X509_VERIFY_PARAM_set1_ip_asc(param, serverName.c_str()) != 1) {
                if (SSL_set_tlsext_host_name(ssl, serverName.c_str()) != 1 ||
                    SSL_set1_host(ssl, serverName.c_str()) != 1) {
                    return Err(make_error_code(QuicError::Tls));
                }
            }
        }
        else if (config.verifyPeer) {
            return Err(std::make_error_code(std::errc::invalid_argument));
        }
    }
    ngtcp2_conn_set_tls_native_handle(conn, handle);
    return {};
}

} // namespace quic
