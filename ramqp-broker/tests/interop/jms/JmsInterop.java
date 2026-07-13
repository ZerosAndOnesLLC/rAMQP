// Apache Qpid JMS (pure-Java, AMQP 1.0) interop client for ramqp-broker.
//
// An independent, third-party AMQP 1.0 client stack exercising OUR broker:
// connect, create a producer + consumer on a transient queue, round-trip a
// text message, and verify the body. Prints "INTEROP_OK" and exits 0 on
// success; prints the failure and exits 1 otherwise.
//
// Usage: java -cp <classes>:<qpid-jms-lib>/* JmsInterop amqp://host:port
//
// qpid-jms 2.x uses the Jakarta Messaging namespace (jakarta.jms.*).

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class JmsInterop {
    public static void main(String[] args) {
        String url = args.length > 0 ? args[0] : "amqp://127.0.0.1:5672";
        // Transient queue on our broker (auto-declared under /queues/<name>).
        String address = args.length > 1 ? args[1] : "/queues/jms-interop";
        String payload = "hello-from-qpid-jms";

        ConnectionFactory factory = new JmsConnectionFactory(url);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(address);

            // Send first so the transient queue exists and holds the message.
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(payload));

            MessageConsumer consumer = session.createConsumer(queue);
            Message received = consumer.receive(5000);
            if (received == null) {
                System.err.println("INTEROP_FAIL: no message received within 5s");
                System.exit(1);
            }
            if (!(received instanceof TextMessage)) {
                System.err.println("INTEROP_FAIL: expected TextMessage, got " + received.getClass());
                System.exit(1);
            }
            String body = ((TextMessage) received).getText();
            if (!payload.equals(body)) {
                System.err.println("INTEROP_FAIL: body mismatch: expected '" + payload
                        + "', got '" + body + "'");
                System.exit(1);
            }
            System.out.println("INTEROP_OK: round-tripped '" + body + "' via Qpid JMS");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("INTEROP_FAIL: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
