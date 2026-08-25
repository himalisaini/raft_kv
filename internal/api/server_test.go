package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/himalisaini/raftkv/internal/engine"
	"github.com/himalisaini/raftkv/internal/raft"
)

// newTestServer spins up a real HTTP server backed by a throwaway data dir.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	// A cluster of one: majority is 1, so the leader's own copy commits every
	// entry immediately. Single-node mode needs no special casing.
	rlog, err := raft.OpenLog(filepath.Join(t.TempDir(), "raft.log"))
	if err != nil {
		t.Fatalf("open raft log: %v", err)
	}

	e := engine.New()
	node, err := raft.NewNode(raft.Options{
		Config: raft.Config{ID: "1", Addr: "unused"},
		Log:    rlog,
		Apply:  e.Apply,
	}, raft.PersistentState{})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	node.BecomeLeader()

	srv := httptest.NewServer(NewServer(node, e))

	t.Cleanup(func() {
		srv.Close()
		rlog.Close()
	})
	return srv
}

// do sends one request and returns the status and body.
func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	defer resp.Body.Close()

	body2, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body2)
}

func TestPutGetDelete(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/kv/city"

	// A key that was never set.
	if code, _ := do(t, "GET", url, ""); code != http.StatusNotFound {
		t.Fatalf("GET missing key = %d, want 404", code)
	}

	// Write it.
	if code, _ := do(t, "PUT", url, "Delhi"); code != http.StatusNoContent {
		t.Fatalf("PUT = %d, want 204", code)
	}

	// Read it back.
	code, body := do(t, "GET", url, "")
	if code != http.StatusOK || body != "Delhi" {
		t.Fatalf("GET = %d %q, want 200 \"Delhi\"", code, body)
	}

	// Overwrite.
	do(t, "PUT", url, "Mumbai")
	if _, body := do(t, "GET", url, ""); body != "Mumbai" {
		t.Fatalf("GET after overwrite = %q, want \"Mumbai\"", body)
	}

	// Delete, then confirm it is gone.
	if code, _ := do(t, "DELETE", url, ""); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", code)
	}
	if code, _ := do(t, "GET", url, ""); code != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %d, want 404", code)
	}
}

// TestDeleteIsIdempotent: deleting twice is not an error.
func TestDeleteIsIdempotent(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/kv/ghost"

	for i := 0; i < 2; i++ {
		if code, _ := do(t, "DELETE", url, ""); code != http.StatusNoContent {
			t.Fatalf("DELETE #%d = %d, want 204", i+1, code)
		}
	}
}

// TestEmptyValueIsNotMissing is the comma-ok distinction, over HTTP:
// a key holding "" must be 200 with an empty body, NOT 404.
func TestEmptyValueIsNotMissing(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/kv/blank"

	do(t, "PUT", url, "")

	code, body := do(t, "GET", url, "")
	if code != http.StatusOK || body != "" {
		t.Fatalf("GET = %d %q, want 200 and an empty body", code, body)
	}
}

// TestWrongMethodIs405: ServeMux gives us this for free from the patterns.
func TestWrongMethodIs405(t *testing.T) {
	srv := newTestServer(t)

	if code, _ := do(t, "POST", srv.URL+"/kv/city", "x"); code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d, want 405", code)
	}
}

// TestOversizedValueIsRejected checks the MaxBytesReader cap.
func TestOversizedValueIsRejected(t *testing.T) {
	srv := newTestServer(t)

	huge := strings.Repeat("x", maxValueBytes+1)
	code, _ := do(t, "PUT", srv.URL+"/kv/big", huge)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized PUT = %d, want 413", code)
	}

	// And it must not have been stored.
	if code, _ := do(t, "GET", srv.URL+"/kv/big", ""); code != http.StatusNotFound {
		t.Fatalf("GET after rejected PUT = %d, want 404", code)
	}
}
