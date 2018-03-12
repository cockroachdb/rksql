package sqload

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"

	"rubrik/util/log"
)

const (
	// graphiteStatsRegisterWaitTime Time to wait for stats to get registered
	// in graphite
	graphiteStatsRegisterWaitTime = 10 * time.Second
	rowCountMetric                = "row_count"
	rowDiffMetric                 = "row_diff"
)

type idxTyp struct {
	idx int
	typ colType
}

type queryContext struct {
	//params supplied by param-src or query-results
	queryParamIds []string
	//sql query string with numeric param refs counting 1 onwards
	parameterizedQuery string
	//result-columns to be captured (queries that follow need these)
	//  this doesn't include meta-fields like row_count
	queryResultIds map[string]idxTyp

	//actual results for all result columns (not just the referred ones)
	resultData []interface{}
	//Used by Cockroach Query Executor since it needs pointers for Scan()
	//It is a member of query context so that the splice does not have to be re-created
	//  for every execution
	resultScanDest []interface{}

	//asOfRelative is the as of time the query should use
	//if this is nil, then query is executed as of current time
	asOfRelative *time.Duration
}

func (q *queryContext) isAsOfQuery() bool {
	return q.asOfRelative != nil
}

type txnContext struct {
	//query level contextual information
	queryCtx []queryContext

	//map of param-ref to its value
	paramValues map[string]interface{}

	isReadOnly bool
}

type paramRunOutError struct{}

func (e paramRunOutError) Error() string {
	return "Param-src ran out of data"
}

func (t *txnContext) loadExternalParamRow(
	ctx context.Context,
	rdr paramReader,
) error {
	row, err := rdr.NextRow(ctx)
	if err != nil {
		return err
	}
	if row == nil {
		return paramRunOutError{}
	}
	for k := range t.paramValues {
		delete(t.paramValues, k)
	}
	for idx, v := range row {
		t.paramValues[paramNameForIdx(idx)] = v
	}
	return nil
}

func (t *txnContext) paramArray(idx int) []interface{} {
	paramNames := t.queryCtx[idx].queryParamIds
	paramArray := make([]interface{}, len(paramNames))
	for pIdx, pName := range paramNames {
		v := t.paramValues[pName]
		paramArray[pIdx] = v
	}
	return paramArray
}

func (t *txnContext) hasResultHoldersSetup(idx int) bool {
	return t.queryCtx[idx].resultData != nil
}

func (t *txnContext) resultScanDest(idx int) []interface{} {
	return t.queryCtx[idx].resultScanDest
}

func (t *txnContext) loadResultBasedParams(idx int) error {
	resIdsToExtract := t.queryCtx[idx].queryResultIds
	resultData := t.queryCtx[idx].resultData
	for k, v := range resIdsToExtract {
		paramVal, err := convert(resultData[v.idx], v.typ)
		if err != nil {
			return err
		}
		t.paramValues[k] = paramVal
	}
	return nil
}

func (t *txnContext) setResultBasedMetaParams(
	resultName string,
	rowCount int,
) {
	rowCountParamName := metaParamNameFor(resultName, metaReturnParamRowCount)
	t.paramValues[rowCountParamName] = rowCount
}

// QueryResponse for executing a query
type QueryResponse struct {
	RowCount int64
}

// QueryExecutor interface for a Query Executor
// SQLoad runs load on any struct implementing this interface
type QueryExecutor interface {
	// Exec executes a query and returns the rows affected
	// qIdx is the index of the query in txnContext to execute
	Exec(t *txnContext, qIdx int) (*QueryResponse, error)
	// Query stores the result of a query in txnContext
	// qIdx is the index of the query in txnContext to execute
	Query(t *txnContext, qIdx int) (*QueryResponse, error)
	// Close query executor
	Close()
}

func newTxnContext(ctx context.Context, w *WorkLoad) (*txnContext, error) {
	queryCount := len(w.Transaction)
	txnCtx := &txnContext{
		queryCtx:    make([]queryContext, queryCount),
		paramValues: make(map[string]interface{}),
	}
	txnCtx.isReadOnly = true
	resultNameQueryIdxMap := make(map[string]int)

	for qIdx, q := range w.Transaction {
		txnCtx.isReadOnly = txnCtx.isReadOnly && q.IsReadQuery
		paramRefs := getParamsReferred(q.Query)
		if log.V(8) {
			log.Infof(ctx, "Query: [%s] referrs to params: '%v'", q.Query, paramRefs)
		}

		fragStart := 0
		query := ""

		if len(q.ResultName) != 0 {
			resultNameQueryIdxMap[q.ResultName] = qIdx
			txnCtx.queryCtx[qIdx].queryResultIds = make(map[string]idxTyp)
		}

		paramIds := make([]string, len(paramRefs))
		for refIdx, ref := range paramRefs {
			if (ref.start - fragStart) > 0 {
				query += q.Query[fragStart:ref.start]
				query += paramNameForIdx(refIdx)
				fragStart = ref.end
			}
			refStr := q.Query[ref.start:ref.end]
			paramIds[refIdx] = refStr
			if ref.resultParam {
				resName, refName, refType, err :=
					getResultParamExpectedType(w.Name, refStr)
				if err != nil {
					return nil, err
				}
				resIdsToExtract :=
					txnCtx.queryCtx[resultNameQueryIdxMap[resName]].queryResultIds
				paramIdx, isMetadataField, err := resultParamIdx(refName)
				if err != nil {
					return nil, err
				}
				if !isMetadataField {
					resIdsToExtract[refStr] = idxTyp{paramIdx, refType}
				}
			}
		}
		if len(q.Query) > fragStart {
			query += q.Query[fragStart:len(q.Query)]
		}
		if log.V(8) {
			log.Infof(ctx, "Sql-Query after param-preparation: [%s]", query)
		}
		txnCtx.queryCtx[qIdx].parameterizedQuery = query
		txnCtx.queryCtx[qIdx].queryParamIds = paramIds
		if len(w.AsOf) > 0 {
			d, err := time.ParseDuration(w.AsOf)
			if err != nil {
				return nil, err
			}
			txnCtx.queryCtx[qIdx].asOfRelative = &d
		}
	}

	return txnCtx, nil
}

//generateLoad has blocking semantics, caller must perform WaitGroup#add(1)
// before dispatching generateLoad (as a goroutine)
func generateLoad(
	ctx context.Context,
	qe QueryExecutor,
	w *WorkLoad,
	statsRec StatsRecorder,
	stopCh <-chan struct{},
	wg *sync.WaitGroup,
	errCh chan error,
) {
	defer wg.Done()
	ctx = log.WithLogTag(ctx, w.Name, nil)

	rdr, err := w.Params.Reader(w.rs)
	if err != nil {
		errCh <- err
		return
	}
	defer rdr.Close()
	txnCtx, err := newTxnContext(ctx, w)
	if err != nil {
		errCh <- err
		return
	}

	if log.V(8) {
		log.Infof(ctx, "TxParams : %#v", txnCtx)
	}

	//marking txn read-only generates this error:
	//    Tx-execution failed: read-only transactions not supported
	txOpts := sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: txnCtx.isReadOnly && false}

	zeroDuration := time.Duration(0)
	idleItvl := w.Interval.Duration
loop:
	for {
		timer := time.NewTimer(idleItvl)
		select {
		case <-stopCh:
			timer.Stop()
			if log.V(3) {
				log.Infof(
					ctx,
					"Received stop signal, breaking out of workload: %s",
					w.Name,
				)
			}
			break loop
		case <-timer.C:
			startTm := time.Now()
			if err = executeTx(ctx, txnCtx, rdr, qe, w, &txOpts, statsRec); err != nil {
				_, runOut := err.(paramRunOutError)
				if runOut {
					statsCtx := statsRec.Start("error." + w.Name)
					if err == nil {
						defer statsCtx.Stop()
						statsCtx.ReportStr("param_run_out", w.Name)
					}
					log.Warningf(ctx, "Param-src ran out for workload %s", w.Name)
					break loop
				}

				errCh <- err
			}
			opLatency := time.Since(startTm)
			idleItvl := w.Interval.Duration - opLatency
			if idleItvl < zeroDuration {
				idleItvl = zeroDuration
			}
		}
	}
}

func executeTx(
	ctx context.Context,
	txnCtx *txnContext,
	rdr paramReader,
	qe QueryExecutor,
	w *WorkLoad,
	txOpts *sql.TxOptions,
	statsRec StatsRecorder,
) error {
	if err := txnCtx.loadExternalParamRow(ctx, rdr); err != nil {
		return err
	}

	return func() error {
		statsCtx := statsRec.Start(w.Name)
		defer statsCtx.Stop()

		shouldAbort := false
		var err error

		roachQE, ok := qe.(*CockroachQueryExecutor)
		if ok {
			if w.isAsOfQuery() || w.RunWithoutTx {
				// As Of Queries cannot be executed in a transaction
				err = executeQuery(ctx, w, qe, txnCtx, &shouldAbort, statsCtx)
			} else {
				// Execute CockroachDB Statements in a transaction
				// Queries which time out are retried if run outside a transaction
				// which can become a problem for some queries like INSERT
				// Most of our production workloads will be executed as transactions
				// So all queries except AS_OF queries should be execute inside a transaction
				err = crdb.ExecuteTx(context.TODO(), roachQE.DB.(*sql.DB), txOpts, func(tx *sql.Tx) error {
					crTxExecutor := NewCockroachTransactionExecutor(tx)
					return executeQuery(ctx, w, crTxExecutor, txnCtx, &shouldAbort, statsCtx)
				})
			}
		} else {
			// Not a CockroachDB Executor
			if len(w.Transaction) == 1 {
				err = executeQuery(ctx, w, qe, txnCtx, &shouldAbort, statsCtx)
			} else {
				err = errors.New("multi query transactions is only supported by CockroachDB")
				shouldAbort = true
			}
		}
		if err != nil {
			statsCtx.ReportInt("tx_err", 1)
			log.Warningf(ctx, "Tx-execution failed: %s", err.Error())
		}
		if shouldAbort {
			return err
		}
		return nil
	}()
}

func reportError(ctx context.Context, statsCtx StatsCtx, namespace, query string, err error) {
	statsCtx.ReportStr(namespace, err.Error())
	log.Warningf(ctx, "query '%s' encountered error %s", query, err.Error())
}

func executeQuery(
	ctx context.Context,
	w *WorkLoad,
	qe QueryExecutor,
	txnCtx *txnContext,
	abortRun *bool,
	statsCtx StatsCtx,
) error {
	lastIdx := len(w.Transaction) - 1
	var rowCount int
	for idx, q := range w.Transaction {
		sqlQuery := txnCtx.queryCtx[idx].parameterizedQuery
		if log.V(8) {
			log.Infof(ctx, "Preparing to execute query: [%s]", sqlQuery)
		}

		if q.IsReadQuery {
			resp, err := qe.Query(txnCtx, idx)
			if err != nil {
				reportError(ctx, statsCtx, "query_err", q.Query, err)
				return err
			}
			rowCount = int(resp.RowCount)
			if rowCount > 1 && idx != lastIdx {
				statsCtx.ReportStr("too_many_rows", q.Query)
			}

			if err := txnCtx.loadResultBasedParams(idx); err != nil {
				*abortRun = true
				return err
			}
		} else {
			execResp, err := qe.Exec(txnCtx, idx)
			if err != nil {
				reportError(ctx, statsCtx, "query_err", q.Query, err)
				return err
			}
			rowCount = int(execResp.RowCount)
		}
		txnCtx.setResultBasedMetaParams(q.ResultName, rowCount)
		if log.V(7) {
			log.Infof(ctx, "ROWS: actual = %d, expected = %d", rowCount, q.NumRows)
		}
		rowCountDiff := rowCount - q.NumRows
		// row diff count is incremented for every statement of the transaction
		statsCtx.ReportInt(rowDiffMetric, rowCountDiff)
	}

	// row count is incremented for the last statement of the transaction
	statsCtx.ReportInt(rowCountMetric, rowCount)
	return nil
}

//LoadUp generates load as specified in given workload
func LoadUp(
	ctx context.Context,
	qe QueryExecutor,
	work []*WorkLoad,
	statsRec StatsRecorder,
	d time.Duration,
) (err error) {
	ctx = log.WithLogTag(ctx, "load-up", nil)
	var wg sync.WaitGroup

	totalConcurrency := 0

	for _, w := range work {
		totalConcurrency += w.Concurrency
	}

	errCh := make(chan error, totalConcurrency)
	stopCh := make(chan struct{})

	//Initialize stats
	log.Info(ctx, "Initializing stats recorders")
	for _, w := range work {
		statsRec.Initialize(w.Name)
	}
	statsRec.Flush()

	if _, ok := statsRec.(*graphiteStatsRecorder); ok {
		// Graphite records data points only at certain intervals
		// Wait till the data points from stats initialization are registered
		time.Sleep(graphiteStatsRegisterWaitTime)
	}

	log.Info(ctx, "Starting workloads")
	for idx, w := range work {
		for i := 0; i < w.Concurrency; i++ {
			wg.Add(1)
			go generateLoad(ctx, qe, work[idx], statsRec, stopCh, &wg, errCh)
		}
	}

	// Closing stopCh is async. Load generation stops when all the
	// load generating go routines return by themselves, or stop timer hits, or
	// there is a fatal error
	go func() {
		select {
		case err = <-errCh:
			log.Errorf(ctx, "Load-run failed, error: %s", err)
		case <-time.After(d):
		}
		close(stopCh)
		log.Info(ctx, "Stop timer hit")
	}()
	wg.Wait()
	log.Info(ctx, "Load generator threads completed")

	return err
}
