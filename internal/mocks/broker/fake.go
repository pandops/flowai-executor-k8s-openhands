// Package broker is the in-process fake of the Executor's broker.Conn. It
// records every request, publish and fetch with its simulated time, answers
// requests through per-subject responders, keeps durable-consumer queues
// with ack/term/nak state, answers the JetStream consumer-create API, and
// lets tests drop, restore or revoke the connection. It is a test fixture,
// not a NATS server: it models only what the Executor's code relies on.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-executor-k8s-openhands/internal/broker"
	"github.com/pandops/flowai-executor-k8s-openhands/internal/clock"
)

// Record is one recorded request or publish.
type Record struct {
	Subject string
	Header  broker.Header
	Data    []byte
	At      time.Time
	// Err is the error the Executor got back (publishes and requests).
	Err error
	// Order is the global position of this call among all broker calls
	// (dials, requests, publishes, fetches, acknowledgements), taken when
	// the call starts.
	Order int64
}

// JSON decodes the record body into a map.
func (r Record) JSON() map[string]any {
	var out map[string]any
	_ = json.Unmarshal(r.Data, &out)
	return out
}

// Responder answers one request.
type Responder func(req Record) (*broker.Reply, error)

// Queued is one message in a durable consumer queue.
type Queued struct {
	Msg      broker.Msg
	mu       sync.Mutex
	inFlight bool
	acked    bool
	termed   bool
	naks     int
	order    int
}

// Acked reports an acknowledgement.
func (q *Queued) Acked() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.acked }

// Termed reports a termination.
func (q *Queued) Termed() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.termed }

// Settled reports ack or term.
func (q *Queued) Settled() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.acked || q.termed }

type acker struct {
	f *Fake
	q *Queued
}

func (a acker) Ack() error  { return a.settle("ack") }
func (a acker) Term() error { return a.settle("term") }
func (a acker) Nak() error  { return a.settle("nak") }

func (a acker) settle(kind string) error {
	a.f.mu.Lock()
	ok := a.f.connected
	if ok {
		a.f.settles = append(a.f.settles, Settle{Subject: a.q.Msg.Subject, Kind: kind, Order: a.f.next()})
	}
	a.f.mu.Unlock()
	if !ok {
		return broker.ErrNotConnected
	}
	a.q.mu.Lock()
	defer a.q.mu.Unlock()
	a.q.inFlight = false
	switch kind {
	case "ack":
		a.q.acked = true
	case "term":
		a.q.termed = true
	case "nak":
		a.q.naks++
	}
	return nil
}

// ConsumerCreate is one recorded consumer-create request.
type ConsumerCreate struct {
	Subject string
	Body    map[string]any
	At      time.Time
	Order   int64
}

// Fetch is one recorded pull.
type Fetch struct {
	Stream  string
	Durable string
	Batch   int
	Got     int
	At      time.Time
	Order   int64
}

// Fake is the fake broker.
type Fake struct {
	mu    sync.Mutex
	clock clock.Clock

	DialErr   error
	Dials     []broker.DialOptions
	connected bool
	revoked   bool
	events    chan broker.Event

	requests  []Record
	published []Record
	attempts  []Record
	stored    map[string]bool

	failPublishes int
	loseAcks      int
	loseAckOn     map[string]int
	responders    []responderEntry

	queues    map[string][]*Queued
	seq       int
	consumers map[string]map[string]any
	creates   []ConsumerCreate
	// CreateFailures makes the next n consumer-create requests fail.
	createFailures int
	createTimeouts bool
	fetches        []Fetch
	order          int64
	settles        []Settle
	dialOrders     []int64

	// OnPublish is called for every stored publish.
	OnPublish func(Record)
	// OnRequest is called for every request.
	OnRequest func(Record)
}

// Settle is one recorded acknowledgement of a pulled message.
type Settle struct {
	Subject string
	Kind    string
	Order   int64
}

// next returns the next global order number; f.mu must be held.
func (f *Fake) next() int64 { f.order++; return f.order }

// NextOrder takes the next global order number, so other fakes can place
// their calls among the broker calls.
func (f *Fake) NextOrder() int64 { f.mu.Lock(); defer f.mu.Unlock(); return f.next() }

// Settles returns the recorded acknowledgements.
func (f *Fake) Settles() []Settle {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Settle(nil), f.settles...)
}

// DialOrders returns the global order numbers of the dials.
func (f *Fake) DialOrders() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.dialOrders...)
}

type responderEntry struct {
	pattern string
	fn      Responder
}

// New returns a disconnected fake broker using clk for record times.
func New(clk clock.Clock) *Fake {
	return &Fake{
		clock:     clk,
		events:    make(chan broker.Event, 64),
		stored:    map[string]bool{},
		queues:    map[string][]*Queued{},
		consumers: map[string]map[string]any{},
	}
}

// Dial is a broker.Dialer.
func (f *Fake) Dial(ctx context.Context, opts broker.DialOptions) (broker.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Dials = append(f.Dials, opts)
	if f.DialErr != nil {
		return nil, f.DialErr
	}
	if f.revoked {
		return nil, broker.ErrRevoked
	}
	f.connected = true
	f.dialOrders = append(f.dialOrders, f.next())
	f.events = make(chan broker.Event, 64)
	return f, nil
}

// DialCount is the number of dial attempts.
func (f *Fake) DialCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.Dials) }

// LastDial returns the last dial options.
func (f *Fake) LastDial() broker.DialOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Dials) == 0 {
		return broker.DialOptions{}
	}
	return f.Dials[len(f.Dials)-1]
}

// Disconnect drops the connection; the client would keep reconnecting.
func (f *Fake) Disconnect() {
	f.mu.Lock()
	f.connected = false
	ch := f.events
	f.mu.Unlock()
	ch <- broker.Event{Kind: broker.Disconnected}
}

// Reconnect restores the connection.
func (f *Fake) Reconnect() {
	f.mu.Lock()
	f.connected = true
	ch := f.events
	f.mu.Unlock()
	ch <- broker.Event{Kind: broker.Reconnected}
}

// Revoke closes the connection as the server does for a revoked JWT.
func (f *Fake) Revoke() {
	f.mu.Lock()
	f.connected = false
	f.revoked = true
	ch := f.events
	f.mu.Unlock()
	ch <- broker.Event{Kind: broker.Revoked}
}

// ResetRevocation lets a new JWT dial again (a reissue).
func (f *Fake) ResetRevocation() { f.mu.Lock(); f.revoked = false; f.mu.Unlock() }

// Connected implements broker.Conn.
func (f *Fake) Connected() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.connected }

// Events implements broker.Conn.
func (f *Fake) Events() <-chan broker.Event { f.mu.Lock(); defer f.mu.Unlock(); return f.events }

// Close implements broker.Conn.
func (f *Fake) Close() { f.mu.Lock(); f.connected = false; f.mu.Unlock() }

// Respond installs a responder for subjects matching pattern ("*" matches
// one token, ">" the rest). Later responders win.
func (f *Fake) Respond(pattern string, fn Responder) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responders = append(f.responders, responderEntry{pattern: pattern, fn: fn})
}

// FailPublishes makes the next n publishes fail without storing them.
func (f *Fake) FailPublishes(n int) { f.mu.Lock(); f.failPublishes = n; f.mu.Unlock() }

// LoseAcks makes the next n publishes store the message but return no ack.
func (f *Fake) LoseAcks(n int) { f.mu.Lock(); f.loseAcks = n; f.mu.Unlock() }

// LoseAcksOn stores the next n publishes whose subject matches pattern but
// returns no acknowledgement for them.
func (f *Fake) LoseAcksOn(pattern string, n int) {
	f.mu.Lock()
	if f.loseAckOn == nil {
		f.loseAckOn = map[string]int{}
	}
	f.loseAckOn[pattern] = n
	f.mu.Unlock()
}

// loseAckMatch consumes one lost acknowledgement for subject; f.mu held.
func (f *Fake) loseAckMatch(subject string) bool {
	for p, n := range f.loseAckOn {
		if n > 0 && Match(p, subject) {
			f.loseAckOn[p] = n - 1
			return true
		}
	}
	return false
}

// FailConsumerCreates makes the next n consumer creates answer an error.
func (f *Fake) FailConsumerCreates(n int) { f.mu.Lock(); f.createFailures = n; f.mu.Unlock() }

// TimeoutConsumerCreates makes consumer creates time out until cleared.
func (f *Fake) TimeoutConsumerCreates(on bool) { f.mu.Lock(); f.createTimeouts = on; f.mu.Unlock() }

// Request implements broker.Conn.
func (f *Fake) Request(ctx context.Context, subject string, hdr broker.Header, data []byte, timeout time.Duration) (*broker.Reply, error) {
	rec := Record{Subject: subject, Header: cloneHeader(hdr), Data: append([]byte(nil), data...), At: f.clock.Now()}
	f.mu.Lock()
	rec.Order = f.next()
	connected, revoked := f.connected, f.revoked
	var fn Responder
	for i := len(f.responders) - 1; i >= 0; i-- {
		if Match(f.responders[i].pattern, subject) {
			fn = f.responders[i].fn
			break
		}
	}
	f.mu.Unlock()
	var reply *broker.Reply
	var err error
	switch {
	case revoked:
		err = broker.ErrRevoked
	case !connected:
		err = broker.ErrNotConnected
	case strings.HasPrefix(subject, "$JS.API.CONSUMER.CREATE."):
		reply, err = f.consumerCreate(rec)
	case fn == nil:
		err = broker.ErrNoResponders
	default:
		reply, err = fn(rec)
	}
	rec.Err = err
	f.mu.Lock()
	f.requests = append(f.requests, rec)
	hook := f.OnRequest
	f.mu.Unlock()
	if hook != nil {
		hook(rec)
	}
	return reply, err
}

func (f *Fake) consumerCreate(rec Record) (*broker.Reply, error) {
	var body map[string]any
	_ = json.Unmarshal(rec.Data, &body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, ConsumerCreate{Subject: rec.Subject, Body: body, At: rec.At, Order: rec.Order})
	if f.createTimeouts {
		return nil, broker.ErrTimeout
	}
	if f.createFailures > 0 {
		f.createFailures--
		return &broker.Reply{Status: 200, Data: []byte(`{"type":"io.nats.jetstream.api.v1.consumer_create_response","error":{"code":503,"err_code":10039,"description":"jetstream not available"}}`)}, nil
	}
	cfg, _ := body["config"].(map[string]any)
	name, _ := cfg["durable_name"].(string)
	stream, _ := body["stream_name"].(string)
	f.consumers[stream+"/"+name] = cfg
	out, _ := json.Marshal(map[string]any{"type": "io.nats.jetstream.api.v1.consumer_create_response", "stream_name": stream, "name": name, "config": cfg})
	return &broker.Reply{Status: 200, Data: out}, nil
}

// Publish implements broker.Conn (JetStream publish with ack).
func (f *Fake) Publish(ctx context.Context, subject string, hdr broker.Header, data []byte) error {
	rec := Record{Subject: subject, Header: cloneHeader(hdr), Data: append([]byte(nil), data...), At: f.clock.Now()}
	f.mu.Lock()
	rec.Order = f.next()
	var err error
	store := false
	switch {
	case f.revoked:
		err = broker.ErrRevoked
	case !f.connected:
		err = broker.ErrNotConnected
	case f.failPublishes > 0:
		f.failPublishes--
		err = broker.ErrNoAck
	case f.loseAcks > 0:
		f.loseAcks--
		store, err = true, broker.ErrNoAck
	case f.loseAckMatch(subject):
		store, err = true, broker.ErrNoAck
	default:
		store = true
	}
	rec.Err = err
	f.attempts = append(f.attempts, rec)
	var hook func(Record)
	if store {
		id := hdr["Nats-Msg-Id"]
		if id == "" || !f.stored[id] {
			if id != "" {
				f.stored[id] = true
			}
			stored := rec
			stored.Err = nil
			f.published = append(f.published, stored)
			hook = f.OnPublish
		}
	}
	f.mu.Unlock()
	if hook != nil {
		hook(rec)
	}
	return err
}

// Enqueue adds a message to a durable consumer's queue.
func (f *Fake) Enqueue(stream, durable, subject string, hdr broker.Header, data []byte) *Queued {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	q := &Queued{Msg: broker.Msg{Subject: subject, Header: cloneHeader(hdr), Data: append([]byte(nil), data...)}, order: f.seq}
	key := stream + "/" + durable
	f.queues[key] = append(f.queues[key], q)
	return q
}

// Fetch implements broker.Conn: it returns up to batch available messages
// in queue order. A command consumer must have been created first.
func (f *Fake) Fetch(ctx context.Context, stream, durable string, batch int, expires time.Duration) ([]*broker.Msg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.revoked {
		return nil, broker.ErrRevoked
	}
	if !f.connected {
		return nil, broker.ErrNotConnected
	}
	key := stream + "/" + durable
	if stream == "TASK_MANAGER_COMMAND" {
		if _, ok := f.consumers[key]; !ok {
			return nil, fmt.Errorf("consumer %s not found", key)
		}
	}
	var out []*broker.Msg
	for _, q := range f.queues[key] {
		if len(out) >= batch {
			break
		}
		q.mu.Lock()
		available := !q.inFlight && !q.acked && !q.termed
		if available {
			q.inFlight = true
		}
		q.mu.Unlock()
		if available {
			m := q.Msg
			m.Acker = acker{f: f, q: q}
			out = append(out, &m)
		}
	}
	f.fetches = append(f.fetches, Fetch{Stream: stream, Durable: durable, Batch: batch, Got: len(out), At: f.clock.Now(), Order: f.next()})
	return out, nil
}

// Requests returns recorded requests whose subject matches pattern.
func (f *Fake) Requests(pattern string) []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Record
	for _, r := range f.requests {
		if Match(pattern, r.Subject) {
			out = append(out, r)
		}
	}
	return out
}

// Published returns stored publishes whose subject matches pattern.
func (f *Fake) Published(pattern string) []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Record
	for _, r := range f.published {
		if Match(pattern, r.Subject) {
			out = append(out, r)
		}
	}
	return out
}

// Attempts returns every publish attempt whose subject matches pattern.
func (f *Fake) Attempts(pattern string) []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Record
	for _, r := range f.attempts {
		if Match(pattern, r.Subject) {
			out = append(out, r)
		}
	}
	return out
}

// Creates returns the recorded consumer-create requests.
func (f *Fake) Creates() []ConsumerCreate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ConsumerCreate(nil), f.creates...)
}

// Fetches returns the recorded pulls of one durable.
func (f *Fake) Fetches(durable string) []Fetch {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Fetch
	for _, x := range f.fetches {
		if durable == "" || x.Durable == durable {
			out = append(out, x)
		}
	}
	return out
}

// Consumer returns a created consumer's configuration.
func (f *Fake) Consumer(stream, durable string) (map[string]any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.consumers[stream+"/"+durable]
	return c, ok
}

// Match matches a NATS subject against a pattern with * and >.
func Match(pattern, subject string) bool {
	if pattern == "" || pattern == ">" {
		return true
	}
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

func cloneHeader(h broker.Header) broker.Header {
	out := broker.Header{}
	for k, v := range h {
		out[k] = v
	}
	return out
}

// JSONReply builds a reply with status and a JSON body.
func JSONReply(status int, body any) *broker.Reply {
	data, _ := json.Marshal(body)
	return &broker.Reply{Status: status, Header: broker.Header{"status": fmt.Sprint(status)}, Data: data}
}

// ErrorReply builds an error reply {code, message, request_id}.
func ErrorReply(status int, code string) *broker.Reply {
	return JSONReply(status, map[string]any{"code": code, "message": code, "request_id": 1})
}

var _ broker.Conn = (*Fake)(nil)
