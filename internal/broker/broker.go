// Package broker is the Executor's narrow view of NATS: core request/reply,
// JetStream publish with acknowledgement, raw pull fetch from a durable
// consumer, and connection events. The NATS implementation lives in
// nats.go; tests use the in-process fake under internal/mocks/broker.
package broker

import (
	"context"
	"errors"
	"time"
)

// Header is a set of NATS headers (one value each).
type Header map[string]string

// Errors of a request or publish.
var (
	ErrNoResponders = errors.New("broker: no responders")
	ErrTimeout      = errors.New("broker: timeout")
	ErrNotConnected = errors.New("broker: not connected")
	ErrRevoked      = errors.New("broker: authorization revoked")
	ErrNoAck        = errors.New("broker: no JetStream acknowledgement")
)

// Reply is the answer to a core NATS request.
type Reply struct {
	Status int
	Header Header
	Data   []byte
}

// Msg is one message fetched from a durable pull consumer.
type Msg struct {
	Subject string
	Header  Header
	Data    []byte
	// Acker settles the message; nil in a message that needs no settle.
	Acker Acker
}

// Acker settles a fetched message.
type Acker interface {
	Ack() error
	Term() error
	Nak() error
}

// Ack acknowledges the message.
func (m *Msg) Ack() error {
	if m.Acker == nil {
		return nil
	}
	return m.Acker.Ack()
}

// Term terminates the message: it is never redelivered.
func (m *Msg) Term() error {
	if m.Acker == nil {
		return nil
	}
	return m.Acker.Term()
}

// Nak asks for redelivery.
func (m *Msg) Nak() error {
	if m.Acker == nil {
		return nil
	}
	return m.Acker.Nak()
}

// EventKind is a connection event.
type EventKind int

const (
	// Disconnected: the connection dropped; the client keeps reconnecting.
	Disconnected EventKind = iota + 1
	// Reconnected: the connection is back.
	Reconnected
	// Revoked: the server closed the connection because the JWT was
	// revoked; the client does not reconnect.
	Revoked
)

// Event is one connection event.
type Event struct{ Kind EventKind }

// Conn is a broker connection.
type Conn interface {
	Request(ctx context.Context, subject string, hdr Header, data []byte, timeout time.Duration) (*Reply, error)
	Publish(ctx context.Context, subject string, hdr Header, data []byte) error
	Fetch(ctx context.Context, stream, durable string, batch int, expires time.Duration) ([]*Msg, error)
	Connected() bool
	Events() <-chan Event
	Close()
}

// DialOptions configures a connection.
type DialOptions struct {
	URL         string
	Token       string
	CAFile      string
	InboxPrefix string
	Name        string
}

// Dialer opens a connection.
type Dialer func(ctx context.Context, opts DialOptions) (Conn, error)
