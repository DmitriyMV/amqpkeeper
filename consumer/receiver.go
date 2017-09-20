package consumer

import (
	"context"

	"github.com/DmitriyMV/amqpkeeper/connection"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

var errOnAck = errors.New("Error on (n)ack")
var errOnCh = errors.New("Error from result channel")

type Result int

const (
	ResultOK Result = iota
	ResultBad
	ResultBadRequeue
)

// OnDataHandler is a callback function for delivery handling. Depending on the Result value - it will send Ack, Nack or Nack with requeue
type OnDataHandler func(amqp.Delivery) Result

// OnChannel is a callback function for channel manipulation's. It is called each time we get a new channel or UpdateOnChannel \ ForceCallOnChannel is called.
// Any non-nil error will close existing channel and attempt to get the new one.
type OnChannel func(*amqp.Channel) error

func isFinalError(ctx context.Context, err error) bool {
	if err != nil {
		select {
		case <-ctx.Done():
			return true
		default:
		}
	}
	return false
}

func handleAckError(ctx context.Context, err error) error {
	if err != nil {
		if isFinalError(ctx, err) {
			err = errors.WithMessage(err, "this is final ack error")
			return err
		}
		return errors.WithStack(errOnAck)
	}
	return nil
}

type Consumer struct {
	connector *connection.Connector
	onChan    OnChannel

	funcCh  chan OnChannel
	channel *amqp.Channel
}

// NewConsumer creates new consumer with provided Connector object and OnChannel handler.
func NewConsumer(connector *connection.Connector, onChan OnChannel) *Consumer {
	funcCh := make(chan OnChannel)

	return &Consumer{
		connector: connector,
		onChan:    onChan,
		funcCh:    funcCh,
	}
}

func (c *Consumer) receiveData(ctx context.Context, dCh <-chan amqp.Delivery, errCh chan *amqp.Error, handler OnDataHandler) error {
	// priority select for updating our callback function
	select {
	case newFunc := <-c.funcCh:
		c.onChan = newFunc
		if err := c.onChan(c.channel); err != nil {
			return errOnCh
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		return nil
	default:
	}
	select {
	case newFunc := <-c.funcCh:
		c.onChan = newFunc
		if err := c.onChan(c.channel); err != nil {
			return errOnCh
		}
	case data := <-dCh:
		switch handler(data) {
		case ResultOK:
			return handleAckError(ctx, data.Ack(false))
		case ResultBad:
			return handleAckError(ctx, data.Nack(false, false))
		case ResultBadRequeue:
			return handleAckError(ctx, data.Nack(false, true))
		}
	case amqpErr, ok := <-errCh:
		var err error
		if !ok {
			err = errors.New("errCh was closed but no delivery happened")
		} else {
			err = amqpErr
		}
		if isFinalError(ctx, err) {
			err := errors.WithMessage(err, "this is final error inside receiveData")
			return err
		}
		return errOnCh
	case <-ctx.Done():
		return errors.WithMessage(ctx.Err(), "this is context cancellation inside receiveData")
	}
	return nil
}

// Consume handle incoming deliveries on provided queue with data handler. It will return in following situations:
//
// 1. Cancellation of context - this could happen during error handling, in which case the actual error will be returned,
// or during "waiting" phase in which case the context error will be returned.
// 2. Failed to get the connection from connector - in this case amqp.ErrClosed will be returned.
//
// Also - every returned error is annotated using github.com/pkg/errors - so you will need Cause func from that package to get internal error
func (c *Consumer) Consume(ctx context.Context, queueName, tag string, handler OnDataHandler) error {
	for {
		conn := c.connector.GetConnection()
		if conn == nil {
			return errors.WithMessage(amqp.ErrClosed, "connector returned nil connection")
		}

		channel, err := conn.Channel()
		if err != nil {
			if isFinalError(ctx, err) {
				err = errors.WithMessage(err, "this is final error on get channel")
				return err
			}
			continue
		}
		c.channel = channel

		closeCh := c.channel.NotifyClose(make(chan *amqp.Error, 1))
		err = c.onChan(c.channel)
		if err != nil {
			if isFinalError(ctx, err) {
				err = errors.WithMessage(err, "this is final error on new channel handler")
				return err
			}
			continue
		}

		deliveryCh, err := c.channel.Consume(queueName, tag, false, false, true, false, nil)
		if err != nil {
			c.channel.Close()
			if isFinalError(ctx, err) {
				err = errors.WithMessage(err, "this is final error on get consumer")
				return err
			}
			continue
		}

		for {
			err = c.receiveData(ctx, deliveryCh, closeCh, handler)
			if err != nil {
				break
			}
		}
		c.channel.Close()

		if errors.Cause(err) == errOnAck || errors.Cause(err) == errOnCh {
			continue
		}

		return errors.WithMessage(err, "this is final error from consumer")
	}
}

// UpdateOnChannel accepts one and more functions that will be executed with the consumer channel.
// This method is blocking - it will block until all passed functions had completed their execution or until one of them had returned an error.
// The last executed function will be the new callback for the current Consumer instance.
// If one of the passed callbacks has resulted in error this error will be returned, and remaining functions will not be executed.
// This method will also wait until OnDataHandler callback is complete, and we have a working amqp connection.
//
// Note: This method will block, until there is running Consume method on the current instance.
// If you unsure about this method ever being called - pass the context and cancel it,
// as soon as you done with it or want to cancel it.
// Note 2: DO NOT use it directly from the OnChannel callback, because it will deadlock. In that case, call that function from another goroutine.
func (c *Consumer) UpdateOnChannel(ctx context.Context, fns ...OnChannel) error {
	for _, fn := range fns {
		errCh := make(chan error)
		closure := func(ch *amqp.Channel) error {
			var err error

			defer func() {
				c.onChan = fn
				errCh <- err
			}()

			err = fn(ch)
			return err
		}

		select {
		case c.funcCh <- closure:
		case <-ctx.Done():
		}

		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}
