#!/usr/bin/env python3
# Qpid Proton (Python/C) against ramqp-broker's 0.10 feature set: PLAIN
# logins, exchange addresses, amqps. Reads MATRIX_* env (../matrix/run.sh);
# prints PASS/FAIL per scenario, exits non-zero on any failure.
import os
import sys

from proton import Message, SSLDomain
from proton.utils import BlockingConnection

E = os.environ
HOST, PLAIN, TLS = E["MATRIX_HOST"], E["MATRIX_PLAIN_PORT"], E["MATRIX_TLS_PORT"]
USER, PASS, CA = E["MATRIX_USER"], E["MATRIX_PASS"], E["MATRIX_CA"]
failures = 0


def check(name, fn):
    global failures
    try:
        fn()
        print("PASS  " + name)
    except Exception as e:  # noqa: BLE001
        failures += 1
        print("FAIL  %s: %r" % (name, e))


def connect(url, user=USER, password=PASS, ssl_domain=None):
    return BlockingConnection(url, timeout=10, user=user, password=password,
                              allowed_mechs="PLAIN", ssl_domain=ssl_domain)


def round_trip(conn, queue):
    conn.create_sender(queue).send(Message(body="ping"))
    r = conn.create_receiver(queue, credit=1)
    m = r.receive(timeout=5)
    r.accept()
    assert m.body == "ping", m.body


def plain_login():
    c = connect("amqp://%s:%s" % (HOST, PLAIN))
    try:
        round_trip(c, "/queues/proton-q")
    finally:
        c.close()


def wrong_password():
    try:
        c = connect("amqp://%s:%s" % (HOST, PLAIN), password="nope")
    except Exception:  # noqa: BLE001 — refused, as expected
        return
    c.close()
    raise AssertionError("connected with a wrong password")


def exchanges():
    c = connect("amqp://%s:%s" % (HOST, PLAIN))
    try:
        c.create_sender("/exchanges/events/proton.order.created").send(Message(body="keyed"))
        c.create_sender("/exchanges/events").send(
            Message(body="by-subject", subject="proton.order.paid"))
        r = c.create_receiver("/queues/proton-orders", credit=2)
        got = set()
        for _ in range(2):
            got.add(r.receive(timeout=5).body)
            r.accept()
        assert got == {"keyed", "by-subject"}, got
    finally:
        c.close()


def unknown_exchange():
    c = connect("amqp://%s:%s" % (HOST, PLAIN))
    try:
        try:
            c.create_sender("/exchanges/missing/k").send(Message(body="x"))
        except Exception:  # noqa: BLE001 — refused, as expected
            return
        raise AssertionError("sent to an unknown exchange")
    finally:
        try:
            c.close()
        except Exception:  # noqa: BLE001
            pass


def amqps():
    domain = SSLDomain(SSLDomain.MODE_CLIENT)
    domain.set_trusted_ca_db(CA)
    domain.set_peer_authentication(SSLDomain.VERIFY_PEER_NAME)
    c = connect("amqps://localhost:%s" % TLS, ssl_domain=domain)
    try:
        round_trip(c, "/queues/proton-tls")
    finally:
        c.close()


check("PLAIN login + round trip", plain_login)
check("wrong password refused", wrong_password)
check("exchanges: key in address + key from subject", exchanges)
check("unknown exchange refused", unknown_exchange)
check("amqps (private CA, hostname verified)", amqps)
print("ALL_OK" if failures == 0 else "%d FAILED" % failures)
sys.exit(1 if failures else 0)
