#!/usr/bin/env bash
# Qpid JMS leg of the feature matrix (run by ../matrix/run.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
QPID_JMS_VERSION="${QPID_JMS_VERSION:-2.10.0}"
CACHE="${QPID_JMS_CACHE:-$ROOT/target/interop/qpid-jms}"
LIB="$CACHE/apache-qpid-jms-$QPID_JMS_VERSION/lib"
CLASSES="$MATRIX_WORK/jms-classes"
mkdir -p "$CACHE" "$CLASSES"
# Check for the client jar itself, not the directory: CI caches target/,
# and a restored cache can hold the directory without its jars.
if ! compgen -G "$LIB/qpid-jms-client-*.jar" >/dev/null; then
    curl -fsSL -o "$CACHE/qpid-jms.tar.gz" \
        "https://repo1.maven.org/maven2/org/apache/qpid/apache-qpid-jms/$QPID_JMS_VERSION/apache-qpid-jms-$QPID_JMS_VERSION-bin.tar.gz"
    tar xzf "$CACHE/qpid-jms.tar.gz" -C "$CACHE"
fi
# JKS/PKCS12 truststore holding the fixture CA (Java does not read PEM).
TS="$MATRIX_WORK/jms-truststore.p12"
rm -f "$TS"
keytool -importcert -noprompt -alias matrix-ca -file "$MATRIX_CA" \
    -keystore "$TS" -storetype PKCS12 -storepass matrix >/dev/null 2>&1
javac -cp "$LIB/*" -d "$CLASSES" "$HERE/JmsFeatures.java"
MATRIX_TRUSTSTORE="$TS" java -cp "$CLASSES:$LIB/*" JmsFeatures
