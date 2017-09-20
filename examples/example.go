package main

import (
	"context"
	"fmt"
	"time"

	"github.com/DmitriyMV/amqpkeeper/connection"
	"github.com/DmitriyMV/amqpkeeper/consumer"
	"github.com/DmitriyMV/amqpkeeper/sender"
	"github.com/streadway/amqp"
)

func main() {

}

func main2() {
	settings := connection.Settings{
		Host:            "rabbit1.localhost",
		Port:            5672,
		Username:        "root",
		Password:        "root",
		VirtualHost:     "root",
		MaxRetryTimeout: 17 * time.Second,
	}

	// Basic context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Creation of connector
	connector, err := connection.ConnectTo(ctx,
		settings,
		func(cn *amqp.Connection, s *connection.Settings, e error) error {
			// connection configuration. Settings can be updated from here too.
			// return no nil error
			return nil
		},
		func() time.Duration { return 3 * time.Second },
	)
	if err != nil {
		// Handle connection error
		panic(err)
	}

	// Creation of consumer
	cm := consumer.NewConsumer(connector, func(ch *amqp.Channel) error {
		// Here, we are declaring any kind of stuff we'll require during our consume cycle.
		// Errors returned from here will
		err := ch.ExchangeDeclare("test_exchange", "fanout", false, false, false, false, nil)
		if err != nil {
			return err
		}
		queue, err := ch.QueueDeclare("test_queue", false, false, false, false, nil)
		if err != nil {
			return err
		}
		return ch.QueueBind(queue.Name, "#", "test_exchange", false, nil)
	})

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		err := cm.Consume(ctx, "test_queue", "", func(delivery amqp.Delivery) consumer.Result {
			// Handle deliveries
			fmt.Println(string(delivery.Body))
			return consumer.ResultOK
		})
		if err != nil {
			// Handle errors
		}
	}()

	cm.UpdateOnChannel(context.Background(), func(ch *amqp.Channel) error { // Use different declarations
		err := ch.ExchangeDeclare("other_exchange", "fanout", false, false, false, false, nil)
		if err != nil {
			return err
		}
		queue, err := ch.QueueDeclare("test_queue", false, false, false, false, nil)
		if err != nil {
			return err
		}
		return ch.QueueBind(queue.Name, "#", "other_exchange", false, nil)
	})

	sender := sender.NewRetrySender(connector)
	for i := 0; i < 10; i++ {
		err = sender.SendRaw(ctx, "other_exchange", "random_key", amqp.Publishing{
			Body: []byte("This is a message body"),
		})

		if err != nil {
			// Should be nil if message was successfully sent
			panic(err)
		}
	}

	time.Sleep(3 * time.Second)
	cancel()
	fmt.Println(connector.Wait())
}
