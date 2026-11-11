// RabbitMQ's own AMQP 1.0 Go client (rabbitmq-amqp-go-client) against
// ramqp-broker: what a team leaving RabbitMQ 4.x keeps using unchanged.
// Publishing (exchange + queue addresses), consuming and amqps work;
// topology management over RabbitMQ's `/management` node is answered with a
// clear 501 (declare in the ramqp-brokerd config file or admin API instead).
// Reads MATRIX_* env (../matrix/run.sh); prints PASS/FAIL per scenario.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

var failures = 0

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

func url(scheme, host, port string) string {
	return fmt.Sprintf("%s://%s:%s@%s:%s/", scheme, os.Getenv("MATRIX_USER"), os.Getenv("MATRIX_PASS"), host, port)
}

func plain() string { return url("amqp", os.Getenv("MATRIX_HOST"), os.Getenv("MATRIX_PLAIN_PORT")) }

func publishConsume(ctx context.Context, conn *rmq.AmqpConnection, target rmq.ITargetAddress, queue, body string) error {
	pub, err := conn.NewPublisher(ctx, target, nil)
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close(ctx)
	res, err := pub.Publish(ctx, rmq.NewMessage([]byte(body)))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	if _, ok := res.Outcome.(*rmq.StateAccepted); !ok {
		return fmt.Errorf("outcome %T", res.Outcome)
	}
	cons, err := conn.NewConsumer(ctx, queue, nil)
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	defer cons.Close(ctx)
	d, err := cons.Receive(ctx)
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}
	if got := string(d.Message().GetData()); got != body {
		return fmt.Errorf("got %q", got)
	}
	return d.Accept(ctx)
}

func main() {
	check("connect (RabbitMQ client opens its /management links)", func(ctx context.Context) error {
		conn, err := rmq.Dial(ctx, plain(), nil)
		if err != nil {
			return err
		}
		return conn.Close(ctx)
	})
	check("publish to an ExchangeAddress, consume the bound queue", func(ctx context.Context) error {
		conn, err := rmq.Dial(ctx, plain(), nil)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		return publishConsume(ctx, conn,
			&rmq.ExchangeAddress{Exchange: "events", Key: "rabbitmqgo.order.created"},
			"rabbitmqgo-orders", "via-exchange")
	})
	check("publish to a QueueAddress, consume it", func(ctx context.Context) error {
		conn, err := rmq.Dial(ctx, plain(), nil)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		return publishConsume(ctx, conn, &rmq.QueueAddress{Queue: "rabbitmqgo-q"}, "rabbitmqgo-q", "via-queue")
	})
	check("management (DeclareQueue) fails fast with 501", func(ctx context.Context) error {
		conn, err := rmq.Dial(ctx, plain(), nil)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: "nope"})
		if err == nil || !strings.Contains(err.Error(), "501") {
			return fmt.Errorf("expected a 501 error, got %v", err)
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
		conn, err := rmq.Dial(ctx, url("amqps", "localhost", os.Getenv("MATRIX_TLS_PORT")),
			&rmq.AmqpConnOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"}})
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		return publishConsume(ctx, conn, &rmq.QueueAddress{Queue: "rabbitmqgo-tls"}, "rabbitmqgo-tls", "over-tls")
	})
	if failures == 0 {
		fmt.Println("ALL_OK")
		return
	}
	fmt.Printf("%d FAILED\n", failures)
	os.Exit(1)
}
