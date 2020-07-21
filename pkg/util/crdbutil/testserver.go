package crdbutil

import (
	"database/sql"
	"flag"
	"path"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach-go/testserver"
)

func setCockroachBinaryFlag(t *testing.T) {
	f := flag.Lookup("cockroach-binary")
	if f == nil {
		t.Fatal("flag cockroach-binary not found")
	}
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("could not get runtime.Caller")
	}
	cockroach := path.Join(path.Dir(filename), "../../../../bin/cockroach")
	if err := f.Value.Set(cockroach); err != nil {
		t.Fatal("could not set value of cockroach-binary flag", err)
	}
}

// NewTestServer creates a new TestServer, but does not start it.
// This function assumes that a cockroach binary is present in
// src/go/bin.
func NewTestServer(t *testing.T) (*testserver.TestServer, error) {
	setCockroachBinaryFlag(t)
	return testserver.NewTestServer()
}

// NewDBForTest creates a testing SQL db connection using cockroachdb
// testserver. This function assumes that a cockroach binary is present in
// src/go/bin. Returns a sql *DB instance, and a shutdown function. The
// caller is responsible for executing the returned shutdown function on exit.
func NewDBForTest(t *testing.T) (*sql.DB, func()) {
	setCockroachBinaryFlag(t)
	return testserver.NewDBForTest(t)
}
