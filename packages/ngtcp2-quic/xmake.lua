-- The upstream recipe builds only the transport library, without TLS helpers.
package("ngtcp2-quic")
    set_homepage("https://github.com/ngtcp2/ngtcp2")
    set_description("ngtcp2 with the OpenSSL QUIC TLS helper")
    set_license("MIT")
    add_urls("https://github.com/ngtcp2/ngtcp2/releases/download/v$(version)/ngtcp2-$(version).tar.gz")
    add_versions("1.25.0", "1c0843076528a87b65e9a9d455100941f4cb65d44f96c5da6ae56df146043955")
    add_deps("cmake", "openssl3")
    add_links("ngtcp2_crypto_ossl", "ngtcp2")
    add_defines("NGTCP2_STATICLIB")

    on_install("windows", "mingw", "linux", "macosx", function (package)
        local configs = {
            "-DENABLE_LIB_ONLY=ON",
            "-DENABLE_OPENSSL=ON",
            "-DENABLE_SHARED_LIB=OFF",
            "-DENABLE_STATIC_LIB=ON",
            "-DBUILD_TESTING=OFF",
            -- "-DOPENSSL_USE_STATIC_LIBS=TRUE",
            "-DOPENSSL_ROOT_DIR=" .. package:dep("openssl3"):installdir(),
            "-DCMAKE_BUILD_TYPE=" .. (package:debug() and "Debug" or "Release")
        }
        import("package.tools.cmake").install(package, configs)
    end)

    on_test(function (package)
        assert(package:has_cfuncs("ngtcp2_crypto_ossl_configure_client_session",
            {includes = "ngtcp2/ngtcp2_crypto_ossl.h"}))
    end)
package_end()
