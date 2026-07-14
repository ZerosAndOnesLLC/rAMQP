#!/usr/bin/env python3
# Apache Qpid Proton (Python, AMQP 1.0) interop client for ramqp-broker.
#
# A third, independent AMQP 1.0 stack (the C-based proton engine) exercising OUR
# broker, alongside the Rust fe2o3-amqp and Java Qpid JMS legs: connect, send a
# message, receive it back, verify the body. Prints "INTEROP_OK" and exits 0 on
# success; prints the failure and exits 1 otherwise.
#
# Usage: python3 proton_interop.py [amqp://host:port] [address]

import sys

from proton import Message
from proton.utils import BlockingConnection

url = sys.argv[1] if len(sys.argv) > 1 else "amqp://127.0.0.1:5680"
address = sys.argv[2] if len(sys.argv) > 2 else "/queues/proton-interop"
payload = "hello-from-qpid-proton"

conn = BlockingConnection(url, timeout=15)
try:
    # Send first so the transient queue exists and holds the message.
    sender = conn.create_sender(address)
    sender.send(Message(body=payload))

    receiver = conn.create_receiver(address, credit=1)
    msg = receiver.receive(timeout=15)
    receiver.accept()

    if msg.body != payload:
        print("INTEROP_FAIL: body mismatch: expected %r, got %r"
              % (payload, msg.body), file=sys.stderr)
        sys.exit(1)

    print("INTEROP_OK: round-tripped %r via Qpid proton" % (msg.body,))
    sys.exit(0)
except Exception as e:  # noqa: BLE001 — any failure is an interop failure
    print("INTEROP_FAIL: %s" % (e,), file=sys.stderr)
    sys.exit(1)
finally:
    try:
        conn.close()
    except Exception:  # noqa: BLE001
        pass
