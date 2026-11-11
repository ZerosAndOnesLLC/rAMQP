// Qpid JMS (Java) against ramqp-broker's 0.10 feature set: logins, exchange
// addresses, amqps. Reads MATRIX_* env (see ../matrix/run.sh); prints
// PASS/FAIL per scenario and exits non-zero on any failure.
//
// qpid-jms maps JMSType to the AMQP properties.subject, which a keyless
// exchange address routes on.

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class JmsFeatures {
    static final String HOST = System.getenv("MATRIX_HOST");
    static final String PLAIN = System.getenv("MATRIX_PLAIN_PORT");
    static final String TLS = System.getenv("MATRIX_TLS_PORT");
    static final String USER = System.getenv("MATRIX_USER");
    static final String PASS = System.getenv("MATRIX_PASS");
    static final String USER256 = System.getenv("MATRIX_USER256");
    static final String PASS256 = System.getenv("MATRIX_PASS256");
    static final String TRUSTSTORE = System.getenv("MATRIX_TRUSTSTORE");
    static int failures = 0;

    interface Scenario { void run() throws Exception; }

    static void check(String name, Scenario s) {
        try {
            s.run();
            System.out.println("PASS  " + name);
        } catch (Throwable t) {
            failures++;
            System.out.println("FAIL  " + name + ": " + t);
        }
    }

    static Connection connect(String url, String user, String pass) throws JMSException {
        Connection c = new JmsConnectionFactory(user, pass, url).createConnection();
        c.start();
        return c;
    }

    static void roundTrip(Connection c, String queue) throws JMSException {
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = s.createProducer(s.createQueue(queue));
        p.send(s.createTextMessage("ping"));
        MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
        Message m = consumer.receive(5000);
        if (!(m instanceof TextMessage) || !"ping".equals(((TextMessage) m).getText())) {
            throw new IllegalStateException("round trip got " + m);
        }
    }

    public static void main(String[] args) {
        String plain = "amqp://" + HOST + ":" + PLAIN;

        check("PLAIN login + round trip", () -> {
            try (Connection c = connect(plain + "?amqp.saslMechanisms=PLAIN", USER, PASS)) {
                roundTrip(c, "/queues/jms-q");
            }
        });
        check("SCRAM (client's own choice: strongest offered)", () -> {
            // Must pick a mechanism both users can complete.
            try (Connection c = connect(plain, USER, PASS)) { roundTrip(c, "/queues/jms-q"); }
            try (Connection c = connect(plain, USER256, PASS256)) { roundTrip(c, "/queues/jms-q"); }
        });
        check("SCRAM-SHA-256 forced", () -> {
            try (Connection c = connect(plain + "?amqp.saslMechanisms=SCRAM-SHA-256", USER, PASS)) {
                roundTrip(c, "/queues/jms-q");
            }
        });
        check("wrong password refused", () -> {
            try (Connection c = connect(plain, USER, "nope")) {
                throw new IllegalStateException("connected with a wrong password");
            } catch (JMSException expected) {
                // refused
            }
        });
        check("exchanges: key in address + key from subject (JMSType)", () -> {
            try (Connection c = connect(plain, USER, PASS)) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer keyed = s.createProducer(s.createQueue("/exchanges/events/jms.order.created"));
                keyed.send(s.createTextMessage("keyed"));
                MessageProducer keyless = s.createProducer(s.createQueue("/exchanges/events"));
                TextMessage m = s.createTextMessage("by-subject");
                m.setJMSType("jms.order.paid");
                keyless.send(m);
                MessageConsumer orders = s.createConsumer(s.createQueue("/queues/jms-orders"));
                java.util.Set<String> got = new java.util.TreeSet<>();
                for (int i = 0; i < 2; i++) {
                    Message r = orders.receive(5000);
                    if (r == null) throw new IllegalStateException("missing routed message; got " + got);
                    got.add(((TextMessage) r).getText());
                }
                if (!got.equals(new java.util.TreeSet<>(java.util.List.of("by-subject", "keyed")))) {
                    throw new IllegalStateException("routed " + got);
                }
            }
        });
        check("unknown exchange refused", () -> {
            try (Connection c = connect(plain, USER, PASS)) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try {
                    s.createProducer(s.createQueue("/exchanges/missing/k"));
                    throw new IllegalStateException("producer attached to an unknown exchange");
                } catch (JMSException expected) {
                    // refused
                }
            }
        });
        check("amqps (private CA, hostname verified)", () -> {
            String url = "amqps://localhost:" + TLS + "?transport.trustStoreLocation=" + TRUSTSTORE
                + "&transport.trustStorePassword=matrix&transport.verifyHost=true";
            try (Connection c = connect(url, USER, PASS)) { roundTrip(c, "/queues/jms-tls"); }
        });

        System.out.println(failures == 0 ? "ALL_OK" : failures + " FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }
}
