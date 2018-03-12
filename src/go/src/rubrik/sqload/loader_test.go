// +build testserver

package sqload

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/testserver"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/suite"

	"rubrik/sqload/randsrc"
	"rubrik/util/crdbutil"
)

const (
	timeout = 25 * time.Second
)

type LoadGenTestSuite struct {
	suite.Suite
	server   *testserver.TestServer
	dbURL    *crdbutil.DBURLParams
	tmpFiles tmpFileTracker
}

func (l *LoadGenTestSuite) queryExecutor() (*CockroachQueryExecutor, error) {
	u, err := l.dbURL.URL()
	if err != nil {
		return nil, err
	}
	db, err := NewCockroachQueryExecutor(u.String())
	if err != nil {
		return nil, err
	}
	return db.(*CockroachQueryExecutor), nil
}

func (l *LoadGenTestSuite) SetupTest() {
	var err error
	l.server, err = testserver.NewTestServer()
	l.Nil(err)
	err = l.server.Start()
	l.Nil(err)
	pgURL := l.server.PGURL()
	port, err := strconv.Atoi(pgURL.Port())
	l.Nil(err)
	l.dbURL = &crdbutil.DBURLParams{
		SocketAddr:         crdbutil.SocketAddress{pgURL.Hostname(), port},
		DbName:             "sqload",
		Insecure:           true,
		SocketReadTimeout:  timeout,
		SocketWriteTimeout: timeout,
		User:               "root",
	}

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	_, err = qe.DB.Exec("CREATE DATABASE sqload")
	l.Nil(err)

	_, err = qe.DB.Exec("CREATE TABLE sqload.foo " +
		"(a INT PRIMARY KEY, b INT, c STRING)")
	l.Nil(err)

	_, err = qe.DB.Exec("CREATE TABLE sqload.bar " +
		"(a INT PRIMARY KEY, b int)")
	l.Nil(err)
}

type recordFoo struct {
	a int
	b int
	c string
}

type recordBar struct {
	a int
	b int
}

func (l *LoadGenTestSuite) TearDownTest() {
	ctx := context.Background()
	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	_, err = qe.DB.Exec("DROP DATABASE IF EXISTS sqload CASCADE")
	l.Nil(err)

	l.server.Stop()

	err = l.tmpFiles.purge(ctx)
	l.Nil(err)
}

func (l *LoadGenTestSuite) runWorkload(
	ctx context.Context,
	w *WorkLoad,
	testLen time.Duration,
	dataPuller func(*countingStatsRecorder)) {
	statsRec := NewCountingStatsRecorder()

	errCh := make(chan error, 5)

	stopCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)

	start := time.Now()
	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()
	go generateLoad(ctx, qe, w, statsRec, stopCh, &wg, errCh)

	select {
	case err := <-errCh:
		l.FailNow("Unexpected error in load execution: %s [%v]", err.Error(), err)
	case <-time.After(testLen):
	}
	close(stopCh)
	wg.Wait()
	workLen := time.Since(start)

	countStatsRec := statsRec.(*countingStatsRecorder)
	countStatsRec.do(func() {
		dataPuller(countStatsRec)
	})

	l.InDelta(testLen.Nanoseconds(), workLen.Nanoseconds(),
		float64(time.Duration(200*time.Millisecond).Nanoseconds()))
}

func (l *LoadGenTestSuite) TestLoadGenerationWithAQueryThatSelectsNothing() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "read_a_b",
		Transaction: []query{
			{
				Query:       "SELECT b FROM foo WHERE a = $1 AND c = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{10, 10, 100},
						},
						RandomSalt: 0,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(ctx,
		&w,
		1*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["read_a_b"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.InDelta(10, queryCount, 3)
}

func (l *LoadGenTestSuite) TestLoadGenerationWithAQueryThatInsertsRows() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "insert_abc",
		Transaction: []query{
			{
				Query:       "INSERT INTO foo VALUES ($1, $2, $3)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2", "g3"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  8,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{10, 10, 100},
						},
						RandomSalt: 0,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(randsrc.RolloverThresh(10)),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(
		ctx,
		&w,
		15*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["insert_abc"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.Equal(10, queryCount)

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	rows, err := qe.DB.Query("SELECT * from foo ORDER BY a DESC")
	l.Nil(err)
	defer rows.Close()

	expectedRows := []recordFoo{
		{91, 48, "CdDgpDHpwS"},
		{81, 29, "3uPgjDKpsA"},
		{75, 4, "SfMQ3fN8GO"},
		{71, 13, "dq9dY38sTO"},
		{70, 79, "QkeBU9amWH"},
		{60, 77, "x0akw1BjP6"},
		{54, 45, "v6z1N5Vy5O"},
		{50, 25, "G0OxJz7JLd"},
		{46, 39, "EOFCedkzAa"},
		{10, 23, "ZgfCQ4XbAA"},
	}
	rowCount := 0
	for rows.Next() {
		var foo recordFoo
		rows.Scan(&foo.a, &foo.b, &foo.c)
		l.Equal(expectedRows[rowCount], foo)
		rowCount++
	}

	l.Equal(queryCount, rowCount)
}

func (l *LoadGenTestSuite) TestLoadGenerationWithATxnThatUpdatesInsertedRows() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "insert_abc",
		Transaction: []query{
			{
				Query:       "INSERT INTO foo VALUES ($1, $2, $3)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
			{
				Query:       "UPDATE foo SET c = $4 WHERE a = $1",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2", "g3", "g4"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  8,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{10, 10, 100},
						},
						RandomSalt: 0,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{2, 2, 100},
						},
						RandomSalt: 1,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(randsrc.RolloverThresh(2)),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(
		ctx,
		&w,
		15*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["insert_abc"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.Equal(2, queryCount)

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	rows, err := qe.DB.Query("SELECT * from foo ORDER BY a desc")
	l.Nil(err)
	defer rows.Close()

	expectedRows := []recordFoo{
		{91, 48, "du"},
		{46, 39, "gG"},
	}

	rowCount := 0
	for rows.Next() {
		var foo recordFoo
		rows.Scan(&foo.a, &foo.b, &foo.c)
		l.Equal(expectedRows[rowCount], foo)
		rowCount++
	}

	l.Equal(queryCount, rowCount)
}

func (l *LoadGenTestSuite) TestLoadGenerationWithMetaResultParam() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "foo_bar",
		Transaction: []query{
			{
				Query:       "INSERT INTO foo VALUES ($1, $2, $3)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
			{
				Query:       "UPDATE foo SET c = $3",
				NumRows:     2,
				ResultName:  "all",
				IsReadQuery: false},
			{
				Query:       "INSERT INTO bar VALUES ($1, $all:row_count)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2", "g3"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  8,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{5, 5, 100},
						},
						RandomSalt: 0,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(randsrc.RolloverThresh(10)),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(
		ctx,
		&w,
		15*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["foo_bar"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.Equal(10, queryCount)

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	rows, err := qe.DB.Query("SELECT * from foo ORDER BY a")
	l.Nil(err)
	defer rows.Close()
	expectedRows := []recordFoo{
		{10, 23, "1BjP6"},
		{46, 39, "1BjP6"},
		{50, 25, "1BjP6"},
		{54, 45, "1BjP6"},
		{60, 77, "1BjP6"},
		{70, 79, "1BjP6"},
		{71, 13, "1BjP6"},
		{75, 4, "1BjP6"},
		{81, 29, "1BjP6"},
		{91, 48, "1BjP6"},
	}
	rowCount := 0
	for rows.Next() {
		var foo recordFoo
		rows.Scan(&foo.a, &foo.b, &foo.c)
		l.Equal(expectedRows[rowCount], foo)
		rowCount++
	}
	l.Equal(queryCount, rowCount)

	barRows, err := qe.DB.Query("SELECT * from bar ORDER BY a DESC")
	l.Nil(err)
	defer barRows.Close()

	expectedBarRows := []recordBar{
		{91, 2},
		{81, 3},
		{75, 5},
		{71, 4},
		{70, 7},
		{60, 10},
		{54, 9},
		{50, 6},
		{46, 1},
		{10, 8},
	}
	rowCount = 0
	for barRows.Next() {
		var bar recordBar
		barRows.Scan(&bar.a, &bar.b)
		l.Equal(expectedBarRows[rowCount], bar)
		rowCount++
	}

	l.InDelta(10, rowCount, 4)
}

func (l *LoadGenTestSuite) TestLoadGenerationWithResultParams() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "foo_bar",
		Transaction: []query{
			{
				Query:       "INSERT INTO foo VALUES ($1, $2, $3)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
			{
				Query: "SELECT SUM_INT(a) AS sa, MAX(a), MIN(a) FROM foo" +
					" WHERE b = $2 GROUP BY b ORDER by sa",
				NumRows:     1,
				ResultName:  "aggr",
				IsReadQuery: true,
			},
			{
				Query:       "INSERT INTO bar VALUES ($1, $aggr:1:int)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2", "g3"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         0,
						Max:         2,
						RandomSalt:  9,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{5, 5, 100},
						},
						RandomSalt: 0,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(randsrc.RolloverThresh(10)),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(
		ctx,
		&w,
		15*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["foo_bar"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.Equal(10, queryCount)

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	rows, err := qe.DB.Query("SELECT * from foo ORDER BY c DESC")
	l.Nil(err)
	defer rows.Close()

	expectedRows := []recordFoo{
		{50, 0, "z7JLd"},
		{75, 0, "fN8GO"},
		{46, 1, "dkzAa"},
		{81, 1, "DKpsA"},
		{91, 0, "DHpwS"},
		{70, 0, "9amWH"},
		{54, 1, "5Vy5O"},
		{10, 0, "4XbAA"},
		{71, 1, "38sTO"},
		{60, 0, "1BjP6"},
	}
	rowCount := 0
	for rows.Next() {
		var foo recordFoo
		rows.Scan(&foo.a, &foo.b, &foo.c)
		l.Equal(expectedRows[rowCount], foo)
		rowCount++
	}

	l.Equal(queryCount, rowCount)

	barRows, err := qe.DB.Query("SELECT * from bar ORDER BY a DESC")
	l.Nil(err)
	defer barRows.Close()

	expectedBarRows := []recordBar{
		{91, 91},
		{81, 127},
		{75, 166},
		{71, 198},
		{70, 286},
		{60, 356},
		{54, 252},
		{50, 216},
		{46, 46},
		{10, 296},
	}
	rowCount = 0
	for barRows.Next() {
		var bar recordBar
		barRows.Scan(&bar.a, &bar.b)
		l.Equal(expectedBarRows[rowCount], bar)
		rowCount++
	}

	l.InDelta(9, rowCount, 4)
}

func (l *LoadGenTestSuite) TestLoadGenerationWithAQueryWithoutResultName() {
	ctx := context.Background()
	w := WorkLoad{
		Name: "read_a_b",
		Transaction: []query{
			{
				Query:       "SELECT b FROM foo WHERE a = $1 AND c = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
			{
				Query:       "SELECT b FROM foo WHERE a = $1 AND c = $2",
				NumRows:     1,
				ResultName:  "bar",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         100,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         0,
						Max:         2,
						RandomSalt:  9,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs: randsrc.NewRandSrc(
			randsrc.RolloverThresh(3),
			randsrc.Repeat(true),
		),
	}

	var queryCount int
	var rowDeltaMean float64
	var rowDeltaSd float64

	l.runWorkload(
		ctx,
		&w,
		1*time.Second,
		func(countStatsRec *countingStatsRecorder) {
			stats := countStatsRec.idCtrs["read_a_b"]
			queryCount = stats.latency.count
			l.T().Log(spew.Sprintf("stats = %+v\n", stats))
			rd := stats.eventCounter["row_diff"]
			rowDeltaMean = float64(rd.sum) / float64(rd.count)
			rowDeltaSd = math.Sqrt(float64(rd.sumOfSq) / float64(rd.count))
		},
	)

	l.InDelta(10, queryCount, 3)
}

func (l *LoadGenTestSuite) TestLoadUp() {
	ctx := context.Background()
	w1 := WorkLoad{
		Name: "c_foo_to_bar",
		Transaction: []query{
			{
				Query:       "SELECT count(*) FROM foo",
				NumRows:     1,
				ResultName:  "count_foo",
				IsReadQuery: true,
			},
			{
				Query:       "INSERT INTO bar VALUES ($count_foo:1:int)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  7,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(randsrc.RolloverThresh(32)),
	}
	w2 := WorkLoad{
		Name: "s_bar_to_foo",
		Transaction: []query{
			{
				Query:       "SELECT COUNT(*) FROM bar",
				NumRows:     1,
				ResultName:  "count_bar",
				IsReadQuery: true,
			},
			{
				Query:       "INSERT INTO foo VALUES ($count_bar:1:int)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  7,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs: randsrc.NewRandSrc(
			randsrc.RolloverThresh(32),
			randsrc.Repeat(true),
		),
	}

	statsRec := NewNoOpStatsRecorder()

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	startTm := time.Now()
	err = LoadUp(
		ctx,
		qe,
		[]*WorkLoad{&w1, &w2},
		statsRec,
		time.Duration(10*time.Second),
	)
	actualDuration := float64(time.Since(startTm).Nanoseconds())
	l.Nil(err)

	var count int
	rows, err := qe.DB.Query("SELECT COUNT(*) FROM foo")
	l.Nil(err)
	numRows := 0
	for rows.Next() {
		err = rows.Scan(&count)
		l.Nil(err)
		numRows++
	}
	l.Equal(numRows, 1)
	l.InDelta(32, count, 10)

	rows, err = qe.DB.Query("SELECT COUNT(*) FROM bar")
	l.Nil(err)
	numRows = 0
	for rows.Next() {
		err = rows.Scan(&count)
		l.Nil(err)
		numRows++
	}
	l.Equal(numRows, 1)
	l.InDelta(32, count, 10)

	expectedDuration := float64(
		time.Duration(10 * time.Second).Nanoseconds(),
	)
	mesurementErr := float64(time.Duration(1 * time.Second).Nanoseconds())
	l.InDelta(expectedDuration, actualDuration, mesurementErr)
}

func (l *LoadGenTestSuite) TestLoadAgainstGeneratedCorpus() {
	ctx := context.Background()
	//first, insert generated data
	workInsert := WorkLoad{
		Name: "insert",
		Transaction: []query{
			{
				Query:       "INSERT INTO bar VALUES ($1, $2)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 100000,
						Mean:        10000,
						StdDev:      1500,
						RandomSalt:  10,
					},
				},
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        300,
						StdDev:      200,
						RandomSalt:  5,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(),
	}

	statsRec := NewNoOpStatsRecorder()

	qe, err := l.queryExecutor()
	l.Nil(err)
	defer qe.Close()

	err = LoadUp(
		ctx,
		qe,
		[]*WorkLoad{&workInsert},
		statsRec,
		time.Duration(4*time.Second),
	)
	l.Nil(err)

	var totalRows int
	rows, err := qe.DB.Query("SELECT COUNT(*) FROM bar")
	l.Nil(err)
	numRows := 0
	for rows.Next() {
		err = rows.Scan(&totalRows)
		l.Nil(err)
		numRows++
	}
	l.InDelta(200, totalRows, 30)

	workUpdate := WorkLoad{
		Name: "update",
		Transaction: []query{
			{
				Query:       "UPDATE bar SET b = 1 WHERE a = $1",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 100000,
						Mean:        10000,
						StdDev:      1500,
						RandomSalt:  10,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(100 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
		rs:            randsrc.NewRandSrc(),
	}

	//insert workload managed to insert `totalRows` rows
	// in 2 seconds, we expect update to update all of them
	// since we give it more than 2 seconds
	err = LoadUp(
		ctx,
		qe,
		[]*WorkLoad{&workUpdate},
		statsRec,
		time.Duration(6*time.Second),
	)

	l.Nil(err)

	var bSum int
	rows, err = qe.DB.Query("SELECT SUM_INT(b) FROM bar")
	l.Nil(err)
	numRows = 0
	for rows.Next() {
		err = rows.Scan(&bSum)
		l.Nil(err)
		numRows++
	}
	l.Nil(err)
	l.InDelta(totalRows, bSum, 10)
}

func TestLoader(t *testing.T) {
	suite.Run(t, new(LoadGenTestSuite))
}
