// Azure go-amqp (Go) against ramqp-broker's 0.10 feature set: PLAIN
// logins, exchange addresses, amqps. Reads MATRIX_* env (../matrix/run.sh);
// prints PASS/FAIL per scenario, exits non-zero on any failure.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"time"

	amqp "github.com/Azure/go-amqp"
)

var (
	host     = os.Getenv("MATRIX_HOST")
	plain    = "amqp://" + host + ":" + os.Getenv("MATRIX_PLAIN_PORT")
	user     = os.Getenv("MATRIX_USER")
	pass     = os.Getenv("MATRIX_PASS")
	failures = 0
)

func check(name string, fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := fn(ctx); err != nil {
		failures++
		fmt.Printf("FAIL  %s: %v\n", name, err)
		return
	}
	fmt.Printf("PASS  %s\n", name)
}

func dial(ctx context.Context, url string, opts *amqp.ConnOptions) (*amqp.Conn, *amqp.Session, error) {
	conn, err := amqp.Dial(ctx, url, opts)
	if err != nil {
		return nil, nil, err
	}
	s, err := conn.NewSession(ctx, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, s, nil
}

func login() *amqp.ConnOptions {
	return &amqp.ConnOptions{SASLType: amqp.SASLTypePlain(user, pass)}
}

func send(ctx context.Context, s *amqp.Session, addr string, msg *amqp.Message) error {
	snd, err := s.NewSender(ctx, addr, nil)
	if err != nil {
		return err
	}
	defer snd.Close(ctx)
	return snd.Send(ctx, msg, nil)
}

func receive(ctx context.Context, s *amqp.Session, addr string, n int) ([]string, error) {
	rcv, err := s.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: int32(n)})
	if err != nil {
		return nil, err
	}
	defer rcv.Close(ctx)
	var got []string
	for len(got) < n {
		m, err := rcv.Receive(ctx, nil)
		if err != nil {
			return got, err
		}
		got = append(got, fmt.Sprint(m.Value))
		if err := rcv.AcceptMessage(ctx, m); err != nil {
			return got, err
		}
	}
	return got, nil
}

func roundTrip(ctx context.Context, s *amqp.Session, queue string) error {
	if err := send(ctx, s, queue, &amqp.Message{Value: "ping"}); err != nil {
		return err
	}
	got, err := receive(ctx, s, queue, 1)
	if err != nil {
		return err
	}
	if got[0] != "ping" {
		return fmt.Errorf("got %q", got[0])
	}
	return nil
}

func main() {
	check("PLAIN login + round trip", func(ctx context.Context) error {
		c, s, err := dial(ctx, plain, login())
		if err != nil {
			return err
		}
		defer c.Close()
		return roundTrip(ctx, s, "/queues/goamqp-q")
	})
	check("wrong password refused", func(ctx context.Context) error {
		c, _, err := dial(ctx, plain, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain(user, "nope")})
		if err == nil {
			c.Close()
			return fmt.Errorf("connected with a wrong password")
		}
		return nil
	})
	check("exchanges: key in address + key from subject", func(ctx context.Context) error {
		c, s, err := dial(ctx, plain, login())
		if err != nil {
			return err
		}
		defer c.Close()
		if err := send(ctx, s, "/exchanges/events/goamqp.order.created", &amqp.Message{Value: "keyed"}); err != nil {
			return err
		}
		subject := "goamqp.order.paid"
		msg := &amqp.Message{Value: "by-subject", Properties: &amqp.MessageProperties{Subject: &subject}}
		if err := send(ctx, s, "/exchanges/events", msg); err != nil {
			return err
		}
		got, err := receive(ctx, s, "/queues/goamqp-orders", 2)
		if err != nil {
			return err
		}
		sort.Strings(got)
		if fmt.Sprint(got) != "[by-subject keyed]" {
			return fmt.Errorf("routed %v", got)
		}
		return nil
	})
	check("unknown exchange refused", func(ctx context.Context) error {
		c, s, err := dial(ctx, plain, login())
		if err != nil {
			return err
		}
		defer c.Close()
		if err := send(ctx, s, "/exchanges/missing/k", &amqp.Message{Value: "x"}); err == nil {
			return fmt.Errorf("sent to an unknown exchange")
		}
		return nil
	})
	check("amqps (private CA, hostname verified)", func(ctx context.Context) error {
		pem, err := os.ReadFile(os.Getenv("MATRIX_CA"))
		if err != nil {
			return err
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(pem)
		opts := login()
		opts.TLSConfig = &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}
		c, s, err := dial(ctx, "amqps://localhost:"+os.Getenv("MATRIX_TLS_PORT"), opts)
		if err != nil {
			return err
		}
		defer c.Close()
		return roundTrip(ctx, s, "/queues/goamqp-tls")
	})
	if failures == 0 {
		fmt.Println("ALL_OK")
		return
	}
	fmt.Printf("%d FAILED\n", failures)
	os.Exit(1)
}
