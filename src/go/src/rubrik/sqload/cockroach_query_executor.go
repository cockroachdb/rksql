package sqload

import (
	"database/sql"
	"strings"

	//load postgres driver
	_ "github.com/Kount/pq-timeouts"
)

type txExecutor interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// CockroachQueryExecutor Used to execute SQL on CockroachDB
type CockroachQueryExecutor struct {
	DB txExecutor
}

// Close CockroachDB connection
func (c *CockroachQueryExecutor) Close() {
	db := c.DB.(*sql.DB)
	db.Close()
}

// setupResultHolders prepares resultData in txnContext for a Scan()
func (c *CockroachQueryExecutor) setupResultHolders(t *txnContext, idx int, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colCount := len(cols)
	resultValues := make([]interface{}, colCount)
	scanDestArr := make([]interface{}, colCount)
	for i := 0; i < colCount; i++ {
		scanDestArr[i] = &resultValues[i]
	}
	t.queryCtx[idx].resultData = resultValues
	t.queryCtx[idx].resultScanDest = scanDestArr
	return nil
}

// Query stores the result of a query in txnContext
func (c *CockroachQueryExecutor) Query(t *txnContext, qIdx int) (*QueryResponse, error) {
	qCtx := t.queryCtx[qIdx]
	sql := qCtx.parameterizedQuery
	if qCtx.isAsOfQuery() {
		// AS_OF should be replaced before executing query.
		// It is not supported as a parameter
		offsetTime, err := getOffsetFormattedTime(qCtx.asOfRelative)
		if err != nil {
			return nil, err
		}

		sql = strings.Replace(sql, "$AS_OF", offsetTime, 1)
	}

	params := t.paramArray(qIdx)
	rows, err := c.DB.Query(sql, params...)
	if err != nil {
		return nil, err
	}
	// Defer close in case the function returns before checking the error in
	// rows.Close(). This function is idempotent
	defer rows.Close()

	if !t.hasResultHoldersSetup(qIdx) {
		if err := c.setupResultHolders(t, qIdx, rows); err != nil {
			return nil, err
		}
	}

	scanDest := t.resultScanDest(qIdx)
	rowCount := 0
	for rows.Next() {
		if err != nil {
			return nil, err
		}
		if err := rows.Scan(scanDest...); err != nil {
			return nil, err
		}

		rowCount++
	}

	if err = rows.Close(); err != nil {
		return nil, err
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &QueryResponse{RowCount: int64(rowCount)}, nil
}

// Exec executes a query and returns the rows affected
func (c *CockroachQueryExecutor) Exec(t *txnContext, qIdx int) (*QueryResponse, error) {
	sql := t.queryCtx[qIdx].parameterizedQuery
	params := t.paramArray(qIdx)

	res, err := c.DB.Exec(sql, params...)
	if err != nil {
		return nil, err
	}

	rowCount, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return &QueryResponse{RowCount: rowCount}, nil
}

// NewCockroachQueryExecutor creates a new CockroachQueryExecutor
// from the given URL
func NewCockroachQueryExecutor(url string) (QueryExecutor, error) {
	db, err := sql.Open("pq-timeouts", url)
	if err != nil {
		return nil, err
	}
	return &CockroachQueryExecutor{DB: db}, nil
}

// NewCockroachTransactionExecutor creates a new CockroachQueryExecutor
// from sql.Tx
func NewCockroachTransactionExecutor(tx *sql.Tx) QueryExecutor {
	return &CockroachQueryExecutor{DB: tx}
}
