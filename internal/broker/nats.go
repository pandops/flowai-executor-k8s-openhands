package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// natsConn is a Conn over nats.go. It uses only the subjects the
// Executor's JWT allows: its own request and publish subjects, its command
// consumer create, raw pulls ($JS.API.CONSUMER.MSG.NEXT) and acknowledgements.
type natsConn struct {
	nc      *nats.Conn
	events  chan Event
	mu      sync.Mutex
	revoked bool
}

// DialNATS connects to NATS over TLS with the Executor's bearer JWT.
func DialNATS(ctx context.Context, opts DialOptions) (Conn, error) {
	c := &natsConn{events: make(chan Event, 64)}
	token := opts.Token
	nopts := []nats.Option{
		nats.Name(opts.Name),
		// Bearer JWT: the server does not check the nonce signature.
		nats.UserJWT(func() (string, error) { return token, nil }, func([]byte) ([]byte, error) { return []byte{}, nil }),
		nats.CustomInboxPrefix(opts.InboxPrefix),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(*nats.Conn, error) { c.emit(Disconnected) }),
		nats.ReconnectHandler(func(*nats.Conn) { c.emit(Reconnected) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if isAuthRevoked(err) {
				c.revoke()
			}
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if isAuthRevoked(nc.LastError()) {
				c.revoke()
			}
		}),
	}
	if opts.CAFile != "" {
		nopts = append(nopts, nats.RootCAs(opts.CAFile))
	}
	nc, err := nats.Connect(opts.URL, nopts...)
	if err != nil {
		if isAuthRevoked(err) {
			return nil, ErrRevoked
		}
		// The error never contains the JWT.
		return nil, fmt.Errorf("broker: connect: %w", err)
	}
	c.nc = nc
	return c, nil
}

func isAuthRevoked(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrAuthRevoked) || errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "revoked") || strings.Contains(s, "authorization violation")
}

func (c *natsConn) revoke() {
	c.mu.Lock()
	already := c.revoked
	c.revoked = true
	c.mu.Unlock()
	if already {
		return
	}
	c.emit(Revoked)
	if c.nc != nil {
		go c.nc.Close()
	}
}

func (c *natsConn) emit(k EventKind) {
	select {
	case c.events <- Event{Kind: k}:
	default:
	}
}

func (c *natsConn) isRevoked() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.revoked }

func toNATSHeader(h Header) nats.Header {
	out := nats.Header{}
	for k, v := range h {
		out[k] = []string{v}
	}
	return out
}

func fromNATSHeader(h nats.Header) Header {
	out := Header{}
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func (c *natsConn) mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case c.isRevoked():
		return ErrRevoked
	case errors.Is(err, nats.ErrNoResponders):
		return ErrNoResponders
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return ErrTimeout
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrDisconnected):
		return ErrNotConnected
	}
	return err
}

// Request implements Conn.
func (c *natsConn) Request(ctx context.Context, subject string, hdr Header, data []byte, timeout time.Duration) (*Reply, error) {
	if !c.Connected() {
		return nil, c.mapErr(nats.ErrDisconnected)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg := &nats.Msg{Subject: subject, Header: toNATSHeader(hdr), Data: data}
	resp, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return nil, c.mapErr(err)
	}
	h := fromNATSHeader(resp.Header)
	status := 200
	if s, ok := h["status"]; ok {
		if n, err := strconv.Atoi(s); err == nil {
			status = n
		}
	}
	if s, ok := h["Status"]; ok && len(resp.Data) == 0 {
		if s == "503" {
			return nil, ErrNoResponders
		}
	}
	return &Reply{Status: status, Header: h, Data: resp.Data}, nil
}

// Publish implements Conn: a JetStream publish answered by a PubAck.
func (c *natsConn) Publish(ctx context.Context, subject string, hdr Header, data []byte) error {
	if !c.Connected() {
		return c.mapErr(nats.ErrDisconnected)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Header: toNATSHeader(hdr), Data: data})
	if err != nil {
		if e := c.mapErr(err); errors.Is(e, ErrRevoked) || errors.Is(e, ErrNotConnected) {
			return e
		}
		return ErrNoAck
	}
	var ack struct {
		Stream string `json:"stream"`
		Error  *struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if json.Unmarshal(resp.Data, &ack) != nil || ack.Error != nil || ack.Stream == "" {
		return ErrNoAck
	}
	return nil
}

type natsAcker struct {
	nc    *nats.Conn
	reply string
}

func (a natsAcker) send(body string) error {
	if a.reply == "" {
		return nil
	}
	return a.nc.Publish(a.reply, []byte(body))
}
func (a natsAcker) Ack() error  { return a.send("+ACK") }
func (a natsAcker) Term() error { return a.send("+TERM") }
func (a natsAcker) Nak() error  { return a.send("-NAK") }

// Fetch implements Conn with a raw pull request on
// $JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>.
func (c *natsConn) Fetch(ctx context.Context, stream, durable string, batch int, expires time.Duration) ([]*Msg, error) {
	if !c.Connected() {
		return nil, c.mapErr(nats.ErrDisconnected)
	}
	if batch <= 0 {
		return nil, nil
	}
	inbox := c.nc.NewRespInbox()
	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, c.mapErr(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	req, _ := json.Marshal(map[string]any{"batch": batch, "expires": expires.Nanoseconds()})
	if err := c.nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT."+stream+"."+durable, inbox, req); err != nil {
		return nil, c.mapErr(err)
	}
	deadline := time.Now().Add(expires + time.Second)
	var out []*Msg
	for len(out) < batch {
		wait := time.Until(deadline)
		if wait <= 0 {
			break
		}
		m, err := sub.NextMsgWithContext(contextWithTimeout(ctx, wait))
		if err != nil {
			if len(out) > 0 || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
				break
			}
			return nil, c.mapErr(err)
		}
		if status := m.Header.Get("Status"); status != "" && len(m.Data) == 0 {
			// 404 no messages, 408 request expired, 409 consumer gone: end of pull.
			if status == "409" && len(out) == 0 {
				return nil, fmt.Errorf("broker: pull %s/%s: %s", stream, durable, m.Header.Get("Description"))
			}
			break
		}
		out = append(out, &Msg{Subject: m.Subject, Header: fromNATSHeader(m.Header), Data: m.Data, Acker: natsAcker{nc: c.nc, reply: m.Reply}})
	}
	return out, nil
}

func contextWithTimeout(ctx context.Context, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(ctx, d)
	go func() { <-c.Done(); cancel() }()
	return c
}

// Connected implements Conn.
func (c *natsConn) Connected() bool { return c.nc != nil && c.nc.IsConnected() && !c.isRevoked() }

// Events implements Conn.
func (c *natsConn) Events() <-chan Event { return c.events }

// Close implements Conn.
func (c *natsConn) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}
