#!/usr/bin/env bash
# Fetch Apache Qpid JMS, compile the JMS interop client, and run the (ignored)
# `jms_interop` rust test against a loopback ramqp-broker. Used both locally and
# by the `interop-jms` CI job.
#
#   ramqp-broker/tests/interop/jms/run.sh
#
# Requires: a JVM (java + javac), curl, tar, and cargo.
set -euo pipefail

QPID_JMS_VERSION="${QPID_JMS_VERSION:-2.10.0}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)" # repo root: jms -> interop -> tests -> ramqp-broker -> root
CACHE="${QPID_JMS_CACHE:-$ROOT/target/interop/qpid-jms}"
DIST="$CACHE/apache-qpid-jms-$QPID_JMS_VERSION"
LIB="$DIST/lib"
CLASSES="$CACHE/classes"

mkdir -p "$CACHE" "$CLASSES"

if [ ! -d "$LIB" ]; then
    echo ">> downloading qpid-jms $QPID_JMS_VERSION (binary distribution) ..."
    url="https://repo1.maven.org/maven2/org/apache/qpid/apache-qpid-jms/$QPID_JMS_VERSION/apache-qpid-jms-$QPID_JMS_VERSION-bin.tar.gz"
    curl -fsSL -o "$CACHE/qpid-jms.tar.gz" "$url"
    tar xzf "$CACHE/qpid-jms.tar.gz" -C "$CACHE"
fi

echo ">> compiling JmsInterop.java ..."
javac -cp "$LIB/*" -d "$CLASSES" "$HERE/JmsInterop.java"

echo ">> running the interop test (Qpid JMS -> ramqp-broker) ..."
export QPID_JMS_CP="$LIB"
export QPID_JMS_CLASSES="$CLASSES"
cd "$ROOT"
cargo test -p ramqp-broker --test jms_interop -- --ignored --nocapture
