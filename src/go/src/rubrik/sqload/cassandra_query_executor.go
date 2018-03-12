package sqload

import (
	"github.com/kaavee315/gocql"

	"rubrik/util/cqlutil"
)

// CassandraQueryExecutor used to execute CQL
type CassandraQueryExecutor struct {
	s *gocql.Session
}

// Close cassandra session
func (c *CassandraQueryExecutor) Close() {
	c.s.Close()
}

// setupResultHolders prepares resultData in txnContext for a Scan()
func (c *CassandraQueryExecutor) setupResultHolders(t *txnContext, idx int, it *gocql.Iter) error {
	rowData, err := it.RowData()
	if err != nil {
		return err
	}
	t.queryCtx[idx].resultData = rowData.Values
	return nil
}

// iterator returns an iterator for the given parameterized query and params
func (c *CassandraQueryExecutor) iterator(
	parameterizedQuery string,
	params []interface{},
) (*gocql.Iter, error) {
	resolvedQuery, err := resolvePlaceholders(parameterizedQuery, params)
	if err != nil {
		return nil, err
	}

	return c.s.Query(resolvedQuery).Iter(), nil
}

// Query stores the result of a query in txnContext
func (c *CassandraQueryExecutor) Query(t *txnContext, qIdx int) (*QueryResponse, error) {
	it, err := c.iterator(t.queryCtx[qIdx].parameterizedQuery, t.paramArray(qIdx))
	if err != nil {
		return nil, err
	}
	defer it.Close()

	if !t.hasResultHoldersSetup(qIdx) {
		if err := c.setupResultHolders(t, qIdx, it); err != nil {
			return nil, err
		}
	}

	rowCount := 0
	for it.Scan(t.queryCtx[qIdx].resultData...) {
		rowCount++
	}

	// Close() is idempotent
	if err := it.Close(); err != nil {
		return nil, err
	}

	return &QueryResponse{RowCount: int64(rowCount)}, nil
}

// Exec executes a query and returns the rows affected
func (c *CassandraQueryExecutor) Exec(t *txnContext, qIdx int) (*QueryResponse, error) {
	it, err := c.iterator(t.queryCtx[qIdx].parameterizedQuery, t.paramArray(qIdx))
	if err != nil {
		return nil, err
	}

	if err := it.Close(); err != nil {
		return nil, err
	}
	return &QueryResponse{RowCount: int64(it.NumRows())}, nil
}

// NewCassandraQueryExecutor creates a new CassandraQueryExecutor with given
// options
func NewCassandraQueryExecutor(
	opts cqlutil.CassandraOptions,
) (QueryExecutor, error) {
	s, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	return &CassandraQueryExecutor{s: s}, nil
}
