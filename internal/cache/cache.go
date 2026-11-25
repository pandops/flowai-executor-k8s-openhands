// Package cache is the K8s Executor's durable recovery cache: one bbolt
// database at <cache_dir>/executor.db with the versioned buckets metadata,
// assignments and event_outbox, guarded by a non-blocking exclusive OS file
// lock on <cache_dir>/executor.lock. It holds no claim token, environment
// value or secret.
package cache

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

// SchemaVersion is the bbolt schema this Executor reads.
const SchemaVersion uint32 = 2

// ErrLocked is returned when another process holds the cache lock.
var ErrLocked = errors.New("cache: executor.lock is held by another process")

// Claim states.
const (
	StateClaiming = "claiming"
	StatePending  = "pending"
	StateRunning  = "running"
	StateTerminal = "terminal"
)

// Claim is one claim's recovery record, keyed by (task_id, attempt, claim_seq).
type Claim struct {
	TaskID         int64     `json:"task_id"`
	Attempt        int64     `json:"attempt"`
	ClaimSeq       int64     `json:"claim_seq"`
	TeamID         int64     `json:"team_id"`
	ExecutorID     int64     `json:"executor_id"`
	State          string    `json:"state"`
	CreatedSeq     int64     `json:"created_seq,omitempty"`
	CreatedSent    bool      `json:"created_sent,omitempty"`
	AcknowledgedAt time.Time `json:"acknowledged_at,omitempty"`
	PendingSince   time.Time `json:"pending_since,omitempty"`
	PendingReason  string    `json:"pending_reason,omitempty"`
	PodNamespace   string    `json:"pod_namespace,omitempty"`
	PodName        string    `json:"pod_name,omitempty"`
	PodUID         string    `json:"pod_uid,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	LastToolState  string    `json:"last_tool_state,omitempty"`
	ResolvedImage  string    `json:"resolved_image,omitempty"`
	ImageSource    string    `json:"image_source,omitempty"`
	TerminalKind   string    `json:"terminal_kind,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Outbox delivery states.
const (
	OutboxPending = "pending"
	OutboxSent    = "sent"
)

// OutboxEvent is one event persisted before its publish, keyed by publisher_seq.
type OutboxEvent struct {
	PublisherSeq int64  `json:"publisher_seq"`
	Subject      string `json:"subject"`
	MsgID        string `json:"msg_id"`
	Team         string `json:"team"`
	Kind         string `json:"kind"`
	Body         []byte `json:"body"`
	// ClaimKey is empty for self events.
	ClaimKey string    `json:"claim_key,omitempty"`
	State    string    `json:"state"`
	At       time.Time `json:"at"`
}

var (
	bucketMetadata    = []byte("metadata")
	bucketAssignments = []byte("assignments")
	bucketOutbox      = []byte("event_outbox")
	keySchema         = []byte("schema_version")
	keyExecutorID     = []byte("executor_id")
	keyLastHeartbeat  = []byte("last_heartbeat")
)

// ClaimKey is the assignments key of a claim: task_id.attempt.claim_seq in decimal.
func ClaimKey(taskID, attempt, claimSeq int64) string {
	return fmt.Sprintf("%d.%d.%d", taskID, attempt, claimSeq)
}

func seqKey(seq int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(seq))
	return b
}

// Store is the opened cache.
type Store struct {
	dir string
	db  *bolt.DB
}

// Open opens or creates the cache in dir (owner-only). An unknown schema
// version, open failure or corrupt database fails closed.
func Open(dir string) (*Store, error) {
	if st, err := os.Lstat(dir); err == nil {
		if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
			return nil, fmt.Errorf("cache: %s is not a plain directory", dir)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("cache: create %s: %w", dir, err)
		}
	} else {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "executor.db")
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("cache: open %s: %w", path, err)
	}
	_ = os.Chmod(path, 0o600)
	err = db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMetadata)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketAssignments); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketOutbox); err != nil {
			return err
		}
		v := meta.Get(keySchema)
		if v == nil {
			return meta.Put(keySchema, []byte(strconv.FormatUint(uint64(SchemaVersion), 10)))
		}
		n, err := strconv.ParseUint(string(v), 10, 32)
		if err != nil || uint32(n) != SchemaVersion {
			return fmt.Errorf("cache: unsupported schema version %q", v)
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{dir: dir, db: db}, nil
}

// Dir is the cache directory.
func (s *Store) Dir() string { return s.dir }

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) getMeta(key []byte) (string, error) {
	var out string
	err := s.db.View(func(tx *bolt.Tx) error {
		out = string(tx.Bucket(bucketMetadata).Get(key))
		return nil
	})
	return out, err
}

func (s *Store) putMeta(key []byte, value string) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketMetadata).Put(key, []byte(value)) })
}

// ExecutorID returns the stored executor_id (0 when none).
func (s *Store) ExecutorID() (int64, error) {
	v, err := s.getMeta(keyExecutorID)
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// PutExecutorID stores the JWT's executor_id.
func (s *Store) PutExecutorID(id int64) error {
	return s.putMeta(keyExecutorID, strconv.FormatInt(id, 10))
}

// LastHeartbeat returns the time of the last successful heartbeat (zero when none).
func (s *Store) LastHeartbeat() (time.Time, error) {
	v, err := s.getMeta(keyLastHeartbeat)
	if err != nil || v == "" {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, v)
}

// PutLastHeartbeat stores the time of the last successful heartbeat.
func (s *Store) PutLastHeartbeat(t time.Time) error {
	return s.putMeta(keyLastHeartbeat, t.UTC().Format(time.RFC3339Nano))
}

// PutClaim writes a claim record.
func (s *Store) PutClaim(c Claim) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put([]byte(ClaimKey(c.TaskID, c.Attempt, c.ClaimSeq)), data)
	})
}

// GetClaim reads one claim.
func (s *Store) GetClaim(taskID, attempt, claimSeq int64) (Claim, bool, error) {
	var c Claim
	var ok bool
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketAssignments).Get([]byte(ClaimKey(taskID, attempt, claimSeq)))
		if v == nil {
			return nil
		}
		ok = true
		return json.Unmarshal(v, &c)
	})
	return c, ok, err
}

// DeleteClaim removes one claim.
func (s *Store) DeleteClaim(taskID, attempt, claimSeq int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Delete([]byte(ClaimKey(taskID, attempt, claimSeq)))
	})
}

// ListClaims returns every claim in key order.
func (s *Store) ListClaims() ([]Claim, error) {
	var out []Claim
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).ForEach(func(_, v []byte) error {
			var c Claim
			if err := json.Unmarshal(v, &c); err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	})
	return out, err
}

// PutOutbox persists an event before its publish.
func (s *Store) PutOutbox(ev OutboxEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketOutbox).Put(seqKey(ev.PublisherSeq), data) })
}

// MarkSent marks an event acknowledged by JetStream.
func (s *Store) MarkSent(publisherSeq int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOutbox)
		v := b.Get(seqKey(publisherSeq))
		if v == nil {
			return nil
		}
		var ev OutboxEvent
		if err := json.Unmarshal(v, &ev); err != nil {
			return err
		}
		ev.State = OutboxSent
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return b.Put(seqKey(publisherSeq), data)
	})
}

// ListOutbox returns every event in publisher_seq order.
func (s *Store) ListOutbox() ([]OutboxEvent, error) {
	var out []OutboxEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOutbox).ForEach(func(_, v []byte) error {
			var ev OutboxEvent
			if err := json.Unmarshal(v, &ev); err != nil {
				return err
			}
			out = append(out, ev)
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].PublisherSeq < out[j].PublisherSeq })
	return out, err
}

// PendingOutbox returns the unsent events in publisher_seq order.
func (s *Store) PendingOutbox() ([]OutboxEvent, error) {
	all, err := s.ListOutbox()
	var out []OutboxEvent
	for _, ev := range all {
		if ev.State != OutboxSent {
			out = append(out, ev)
		}
	}
	return out, err
}

// DropUnsentForClaim deletes a claim's unsent events.
func (s *Store) DropUnsentForClaim(claimKey string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOutbox)
		var drop [][]byte
		err := b.ForEach(func(k, v []byte) error {
			var ev OutboxEvent
			if err := json.Unmarshal(v, &ev); err != nil {
				return err
			}
			if ev.ClaimKey == claimKey && ev.State != OutboxSent {
				drop = append(drop, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, k := range drop {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteSent removes events already acknowledged, keeping the outbox small.
func (s *Store) DeleteSent() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOutbox)
		var drop [][]byte
		_ = b.ForEach(func(k, v []byte) error {
			var ev OutboxEvent
			if json.Unmarshal(v, &ev) == nil && ev.State == OutboxSent {
				drop = append(drop, append([]byte(nil), k...))
			}
			return nil
		})
		for _, k := range drop {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// LockFile is the held OS lock.
type LockFile struct {
	path string
	file *os.File
}

// Lock takes the non-blocking exclusive flock on <dir>/executor.lock. The
// Executor holds it for its whole lifetime, before connecting to the broker.
func Lock(dir string) (*LockFile, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := LockPath(dir)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %w", ErrLocked, err)
	}
	return &LockFile{path: path, file: f}, nil
}

// LockPath is <dir>/executor.lock.
func LockPath(dir string) string { return filepath.Join(dir, "executor.lock") }

// Release drops the lock.
func (l *LockFile) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	return err
}
