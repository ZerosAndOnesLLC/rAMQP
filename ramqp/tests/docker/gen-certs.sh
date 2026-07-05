#!/usr/bin/env bash
# Generate a throwaway CA + server cert (SAN: localhost, 127.0.0.1) for the
# live TLS interop test. Output lands in ./certs/ (git-ignored).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs && cd certs

openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 \
  -subj "/CN=ramqp-test-ca" 2>/dev/null
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -out server.csr -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null

echo "wrote certs/{ca.crt,server.crt,server.key}"
