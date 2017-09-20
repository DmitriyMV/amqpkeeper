package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
)

type Settings struct {
	Host             string
	Port             int
	Username         string
	Password         string
	VirtualHost      string
	MaxRetryTimeout  time.Duration
	MaxRetryAttempts int
}

func (s *Settings) URI() string {
	return fmt.Sprintf(
		"amqp://%v:%v@%v:%v/%v",
		s.Username,
		s.Password,
		s.Host,
		s.Port,
		s.VirtualHost,
	)
}

type Connector struct {
	connCh chan *amqp.Connection
	group  *errgroup.Group
}

// OnReconnect is function that receive amqp.Connection and Settings pointers and is used to set initial params on
// amqp connection during connect/reconnect. Settings is pointer, so it can be updated from inside. It could also receive an error which has occurred on attempting to reconnect.
// If error is not nil, then *amqp.Connection is nil and vice-versa.
type OnReconnect func(*amqp.Connection, *Settings, error) error

// Backoffer is used to determine how much time will be spent between reconnecting.
type Backoffer func() time.Duration

func createConnect(ctx context.Context, settings *Settings, orf OnReconnect, backoffer Backoffer, maxAttempts int, timeout time.Duration) (*amqp.Connection, chan *amqp.Error, error) {
	var reserror error
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for i := 0; i < maxAttempts; i++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			conn, err := amqp.Dial(settings.URI())
			if orf != nil {
				if funcErr := orf(conn, settings, err); funcErr != nil {
					return nil, nil, funcErr
				}
			}

			if err != nil {
				//opErr, ok := err.(*net.OpError)
				reserror = err
				time.Sleep(backoffer())
				continue
			}

			closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
			return conn, closeCh, nil
		}
	}

	return nil, nil, reserror
}

func isErrClosedConn(err error) bool {
	return err == amqp.ErrClosed
}

// ConnectTo is used to create connector which is used by Sender and Consumer. It will return connector instance or error if there were
// one during initial connection creation. All errors that come after that will be handled using OnReconnect handler.
//
// Use context cancellation to stop or return no nil error from OnReconnect handler.
// Settings MaxRetryTimeout is used to determinate max amount of time given to establish a new connection (including attempts that resulted in error).
// If it's not set in settings, then default value of 24 hours will be used instead.
// Settings MaxRetryAttempts is used set the amount of attempts connector can use to reestablish a connection, after connection drop,
// until it will stop and return an error.
// If it's not set in settings, then default value of 100000 will be used instead.
//
// The final error is always accessible from Wait method.
func ConnectTo(ctx context.Context, settings Settings, orf OnReconnect, backoffer Backoffer) (*Connector, error) {
	if settings.MaxRetryTimeout == 0 {
		settings.MaxRetryTimeout = 24 * time.Hour
	}

	if settings.MaxRetryAttempts == 0 {
		settings.MaxRetryAttempts = 100000
	}

	if backoffer == nil {
		backoffer = func() time.Duration { return 3 * time.Second }
	}
	conn, closeCh, err := createConnect(ctx, &settings, orf, backoffer, 1, settings.MaxRetryTimeout)

	if err != nil {
		return nil, &Error{err}
	} else if conn == nil {
		return nil, &Error{errors.WithMessage(err, "connectTo got nil connection. This is internal error, please report it to developer.")}
	}

	group, ctx := errgroup.WithContext(ctx)

	result := &Connector{
		group:  group,
		connCh: make(chan *amqp.Connection),
	}

	group.Go(func() error {
		defer close(result.connCh)

		for {
			select {
			case <-ctx.Done():
				conErr := conn.Close()
				if conErr != nil {
					if isErrClosedConn(conErr) {
						return nil
					}

					return errors.WithMessage(conErr, "conn.Close error")
				}
				return nil
			case <-closeCh:
				conn, closeCh, err = createConnect(ctx, &settings, orf, backoffer, settings.MaxRetryAttempts, settings.MaxRetryTimeout)
				if err != nil {
					return errors.WithMessage(err, "createConnect error, max attempts reached")
				} else if conn == nil {
					return errors.WithMessage(err, "connectTo recreation got nil connection. This is internal error, please report it to developer.")
				}
			case result.connCh <- conn:
			}
		}
	})

	return result, nil
}

// GetConnection returns amqp.Connection if connector instance is alive or nil if connector was terminated.
func (c *Connector) GetConnection() *amqp.Connection {
	return <-c.connCh
}

// Wait is used to wait until connector is destroyed.
// It will return error if there were any. Context error will only be returned if context was cancel during connection attempt.
func (c *Connector) Wait() error {
	err := c.group.Wait()
	return err
}

type Error struct {
	err error
}

func (e Error) Error() string { return fmt.Sprintf("Connection error: %s", e.err.Error()) }
