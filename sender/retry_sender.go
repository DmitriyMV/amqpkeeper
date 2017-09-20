package sender

import (
	"context"
	"sync"

	"github.com/DmitriyMV/amqpkeeper/connection"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type RetrySender struct {
	connector *connection.Connector

	mx       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	npublish chan amqp.Confirmation
	nclose   chan *amqp.Error
}

type ToPublisher interface {
	ToPublishing() amqp.Publishing
}

// NewRetrySender returns a new sender.
func NewRetrySender(connector *connection.Connector) *RetrySender {
	return &RetrySender{
		connector: connector,
	}
}

func (rs *RetrySender) initConnection() error {
	if rs.conn == nil {
		rs.conn = rs.connector.GetConnection()
		if rs.conn == nil {
			return amqp.ErrClosed
		}
	}
	return nil
}

func (rs *RetrySender) initChannel() error {
	if rs.channel == nil {
		ch, err := rs.conn.Channel()
		if err != nil {
			return err
		}

		err = ch.Confirm(false)
		if err != nil {
			return err
		}

		rs.channel = ch
		rs.npublish = rs.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		rs.nclose = rs.channel.NotifyClose(make(chan *amqp.Error, 1))
	}
	return nil
}

// Send is wrapper around SendRaw and custom message types.
func (rs *RetrySender) Send(ctx context.Context, exchange, key string, msg ToPublisher) error {
	return rs.SendRaw(ctx, exchange, key, msg.ToPublishing())
}

// SendRaw is used to send raw message. It will block until is either message is received, context has cancelled or connector instance was destroyed.
func (rs *RetrySender) SendRaw(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	rs.mx.Lock()
	defer rs.mx.Unlock()
	var lastError error

tryAgain:
	for {
		err := rs.initConnection()
		if err != nil {
			return err
		}

		err = rs.initChannel()
		if err != nil {
			rs.conn = nil
			rs.channel = nil
			if isFinalError(ctx, err) {
				return err
			}
			continue
		}

		err = rs.channel.Publish(exchange, key, false, false, msg)
		if err != nil {
			rs.conn = nil
			rs.channel = nil
			if isFinalError(ctx, err) {
				return err
			}
			continue
		}

		select {
		case <-ctx.Done():
			if lastError != nil {
				return lastError
			}
			return ctx.Err()
		case <-rs.npublish:
			return nil
		case d := <-rs.nclose:
			rs.conn = nil
			rs.channel = nil
			lastError = errors.WithMessage(d, "failed to deliver a message, because connection was closed")
			continue tryAgain
		}
	}
}

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
