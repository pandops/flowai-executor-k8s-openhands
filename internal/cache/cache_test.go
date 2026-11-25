package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestOpenCreatesOwnerOnlyDatabaseWithBuckets(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = store.Close()
	for _, p := range []string{dir, filepath.Join(dir, "executor.db")} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s mode %v is not owner-only", p, st.Mode().Perm())
		}
	}
	db, err := bolt.Open(filepath.Join(dir, "executor.db"), 0o600, &bolt.Options{Timeout: time.Second, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_ = db.View(func(tx *bolt.Tx) error {
		for _, b := range []string{"metadata", "assignments", "event_outbox"} {
			if tx.Bucket([]byte(b)) == nil {
				t.Fatalf("bucket %s missing", b)
			}
		}
		if v := tx.Bucket([]byte("metadata")).Get([]byte("schema_version")); string(v) != fmt.Sprint(SchemaVersion) {
			t.Fatalf("schema_version %q", v)
		}
		return nil
	})
}

func TestStoresExecutorIDAndLastHeartbeat(t *testing.T) {
	store := openTestStore(t)
	if id, err := store.ExecutorID(); err != nil || id != 0 {
		t.Fatalf("initial executor_id %d %v", id, err)
	}
	if err := store.PutExecutorID(7); err != nil {
		t.Fatal(err)
	}
	if id, _ := store.ExecutorID(); id != 7 {
		t.Fatalf("executor_id %d", id)
	}
	at := time.Date(2026, 11, 7, 10, 0, 0, 123e6, time.UTC)
	if err := store.PutLastHeartbeat(at); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.LastHeartbeat(); !got.Equal(at) {
		t.Fatalf("last heartbeat %v", got)
	}
}

func TestOpenRejectsUnsupportedNewerSchema(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = store.Close()
	if err := forceStoredSchemaVersion(filepath.Join(dir, "executor.db"), SchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatalf("newer schema accepted")
	}
}

func TestOpenRejectsCorruptDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "executor.db"), []byte("garbage that is not bbolt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatalf("corrupt database accepted")
	}
}

func TestClaimLifecycle(t *testing.T) {
	store := openTestStore(t)
	c := Claim{TaskID: 10, Attempt: 2, ClaimSeq: 3, TeamID: 3, ExecutorID: 7, State: StateClaiming, UpdatedAt: time.Now().UTC()}
	if err := store.PutClaim(c); err != nil {
		t.Fatal(err)
	}
	other := Claim{TaskID: 10, Attempt: 2, ClaimSeq: 4, TeamID: 3, ExecutorID: 7, State: StateClaiming}
	if err := store.PutClaim(other); err != nil {
		t.Fatal(err)
	}
	c.State, c.PodName, c.PodUID = StateRunning, "task-10", "uid-1"
	if err := store.PutClaim(c); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.GetClaim(10, 2, 3)
	if err != nil || !ok || got.State != StateRunning || got.PodUID != "uid-1" {
		t.Fatalf("claim %+v %v %v", got, ok, err)
	}
	if list, _ := store.ListClaims(); len(list) != 2 {
		t.Fatalf("claims %v: (task, attempt, claim_seq) is the key", list)
	}
	if err := store.DeleteClaim(10, 2, 3); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.GetClaim(10, 2, 3); ok {
		t.Fatalf("claim still present")
	}
	if ClaimKey(10, 2, 3) == ClaimKey(10, 2, 4) || ClaimKey(1, 23, 4) == ClaimKey(12, 3, 4) {
		t.Fatalf("claim keys collide")
	}
}

// TestAssignmentLifecycle keeps the pre-v0048 oracle on the v0048 claim
// record (assignments became claims; the record still lives in the
// assignments bucket): put, read back by key, update the state, read the new
// state back by key, delete, and the record is gone.
func TestAssignmentLifecycle(t *testing.T) {
	store := openTestStore(t)

	rec := Claim{
		TaskID:     101,
		Attempt:    2,
		ClaimSeq:   5,
		TeamID:     3,
		ExecutorID: 7,
		State:      StateClaiming,
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.PutClaim(rec); err != nil {
		t.Fatalf("put assignment: %v", err)
	}
	got, ok, err := store.GetClaim(rec.TaskID, rec.Attempt, rec.ClaimSeq)
	if err != nil || !ok {
		t.Fatalf("assignment: %v, found %v", err, ok)
	}
	if got.TaskID != rec.TaskID || got.Attempt != rec.Attempt || got.ClaimSeq != rec.ClaimSeq {
		t.Fatalf("assignment record mismatch: %+v", got)
	}
	// Distinct key parts: a permuted key must not find the claim.
	if _, ok, _ := store.GetClaim(rec.Attempt, rec.ClaimSeq, rec.TaskID); ok {
		t.Fatalf("a permuted claim key found the claim")
	}

	rec.State = StateRunning
	if err := store.PutClaim(rec); err != nil {
		t.Fatalf("update assignment: %v", err)
	}
	got, _, _ = store.GetClaim(rec.TaskID, rec.Attempt, rec.ClaimSeq)
	if got.State != StateRunning {
		t.Fatalf("assignment state = %q, want running", got.State)
	}

	if err := store.DeleteClaim(rec.TaskID, rec.Attempt, rec.ClaimSeq); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, ok, _ := store.GetClaim(rec.TaskID, rec.Attempt, rec.ClaimSeq); ok || got.TaskID != 0 {
		t.Fatalf("expected task to be gone, got %+v", got)
	}
}

func TestOutboxLifecycle(t *testing.T) {
	store := openTestStore(t)
	key := ClaimKey(10, 1, 1)
	for seq := int64(1); seq <= 3; seq++ {
		ev := OutboxEvent{PublisherSeq: seq, Subject: "executor.event.7.log.pub", MsgID: fmt.Sprintf("executor-3-7-%d", seq), Kind: "log", Body: []byte(`{}`), ClaimKey: key, State: OutboxPending}
		if seq == 3 {
			ev.ClaimKey = ""
		}
		if err := store.PutOutbox(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkSent(1); err != nil {
		t.Fatal(err)
	}
	pending, _ := store.PendingOutbox()
	if len(pending) != 2 || pending[0].PublisherSeq != 2 || pending[1].PublisherSeq != 3 {
		t.Fatalf("pending %+v, want 2 and 3 in publisher_seq order", pending)
	}
	if err := store.DropUnsentForClaim(key); err != nil {
		t.Fatal(err)
	}
	pending, _ = store.PendingOutbox()
	if len(pending) != 1 || pending[0].PublisherSeq != 3 {
		t.Fatalf("pending after drop %+v", pending)
	}
	all, _ := store.ListOutbox()
	for _, e := range all {
		if e.PublisherSeq == 1 && e.State != OutboxSent {
			t.Fatalf("sent event lost its state: %+v", e)
		}
	}
}

func TestLockSecondHolderFails(t *testing.T) {
	dir := t.TempDir()
	first, err := Lock(dir)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer first.Release()
	if _, err := Lock(dir); err == nil {
		t.Fatalf("second lock succeeded")
	}
	if LockPath(dir) != filepath.Join(dir, "executor.lock") {
		t.Fatalf("lock path %q", LockPath(dir))
	}
}

func TestLockReleaseAllowsRelock(t *testing.T) {
	dir := t.TempDir()
	first, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Lock(dir)
	if err != nil {
		t.Fatalf("relock: %v", err)
	}
	_ = second.Release()
}

func TestOpenRejectsSymlinkDirectory(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(link); err == nil {
		t.Fatalf("symlinked cache dir accepted")
	}
}

func forceStoredSchemaVersion(dbPath string, version uint32) error {
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("metadata"))
		if b == nil {
			return fmt.Errorf("metadata bucket missing")
		}
		return b.Put([]byte("schema_version"), []byte(fmt.Sprint(version)))
	})
}
