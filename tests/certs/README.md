These self-signed localhost credentials are public test fixtures, not production credentials.
The certificate includes `localhost`, `127.0.0.1`, and `::1` as subject alternative names.

To regenerate them with OpenSSL:

```sh
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -keyout localhost-key.pem -out localhost.pem
```
