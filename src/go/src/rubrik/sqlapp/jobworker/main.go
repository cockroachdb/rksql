package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	_ "github.com/jinzhu/gorm/dialects/postgres"

	"rubrik/sqlapp"
	"rubrik/sqlapp/job"
	"rubrik/sqlapp/sqlutil"
	"rubrik/util/crdbutil"
	"rubrik/util/log"
)

// Main runs one job test worker. It assumes that the socket addresses for
// the cockroach nodes that are provided correspond to the same cluster and
// that there is a database with the appropriate name and user with all
// privileges in this cluster. Optionally, the schema can be created from this
// worker (this flag should not be used if workers are being run as part of
// the rktest framework, since such a test would require a preliminary step that
// installs the schema before any worker begins.
// Note that jobworker is expected to be run inside of sqlapp/coordinator while
// jobcoordinator exists for the purpose of allowing lightweight local tests to
// run with multiple workers running in separate goroutines.
func main() {
	ctx := context.Background()
	go func() {
		hostPort := fmt.Sprintf("localhost:%d", job.PprofPort)
		log.Info(ctx, http.ListenAndServe(hostPort, nil))
	}()

	var err error

	cockroachSocketAddrsCSV :=
		flag.String(
			job.CockroachSocketAddrsCSV,
			"",
			"Comma-separated list of socket addresses of cockroach nodes that workers can use.",
		)
	durationSecs :=
		flag.Int(
			sqlapp.DurationSecs,
			0,
			"Duration of the test after which workers begin their final check pass.",
		)
	workerIndex :=
		flag.Int(
			sqlapp.WorkerIndex,
			0,
			"Index of the worker.",
		)
	numWorkers :=
		flag.Int(
			sqlapp.NumWorkers,
			0,
			"Total number of workers that are part of this test.",
		)
	numJobsPerWorker :=
		flag.Int(
			job.NumJobsPerWorker,
			0,
			"Number of periodic jobs to schedule on each worker.",
		)
	jobPeriodScaleMillis :=
		flag.Int64(
			job.JobPeriodScaleMillis,
			0,
			"The scale for the period of the jobs, which will range from "+
				"5 * jobPeriodScaleMillis to 9 * jobPeriodScaleMillis.",
		)
	useLocalCockroach :=
		flag.Bool(
			job.UseLocalCockroach,
			false,
			"Whether to use local cockroach, and default port.",
		)
	installSchema :=
		flag.Bool(
			sqlapp.InstallSchema,
			false,
			"Whether to create the schema (used for test mode).",
		)
	certsDir := flag.String(
		sqlapp.CertsDir,
		"",
		"Directory containing TLS certificates.")
	insecure := flag.Bool(sqlapp.Insecure, true, "Connect to CockroachDB in insecure mode")

	flag.Parse()
	defer log.Flush()
	if len(*cockroachSocketAddrsCSV) <= 0 {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Must provide at least one socket address of a cockroach service. "+
				"cockroachSocketAddrsCSV: %v",
			*cockroachSocketAddrsCSV,
		)
	}
	if *durationSecs <= 0 {
		flag.PrintDefaults()
		log.Fatalf(ctx, "Duration of test must be greater than 0: %v", *durationSecs)
	}
	if *workerIndex < 0 {
		flag.PrintDefaults()
		log.Fatalf(ctx, "Worker index must be greater than or equal to 0: %v", *workerIndex)
	}
	if *workerIndex >= *numWorkers {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Worker index must be less than numWorkers. workerIndex: %v numWorkers: %v",
			*workerIndex,
			*numWorkers,
		)
	}
	if *numJobsPerWorker <= 0 {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Num jobs per worker must be greater than 0: %v",
			*numJobsPerWorker,
		)
	}
	if *jobPeriodScaleMillis <= 0 {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Scale of period for jobs must be greater than 0: %v",
			*jobPeriodScaleMillis,
		)
	}

	cockroachSocketAddrs := strings.Split(*cockroachSocketAddrsCSV, ",")
	var socketAddr crdbutil.SocketAddress
	if *useLocalCockroach {
		socketAddr = crdbutil.SocketAddress{"localhost", crdbutil.DefaultPort}
	} else {
		// TODO: we should also support connecting to multiple socket addresses in a single worker.
		cockroachSocketAddr := cockroachSocketAddrs[*workerIndex%len(cockroachSocketAddrs)]
		socketAddr, err = crdbutil.ParseSocketAddress(cockroachSocketAddr)
	}

	if err != nil {
		log.Fatal(ctx, err)
	}
	dbs, err :=
		sqlutil.GormDBs(
			ctx,
			[]crdbutil.SocketAddress{socketAddr},
			job.DefaultDatabaseName,
			*certsDir,
			*insecure,
			!*installSchema,
		)
	if err != nil {
		log.Fatal(ctx, err)
	}
	workerPoolSize := 128
	for _, db := range dbs {
		db.SingularTable(true)
		db.LogMode(true)
		db.DB().SetMaxOpenConns(2 * workerPoolSize)
	}
	db := dbs[0]
	if *installSchema {
		job.CreateSchema(db)
	} else {
		testWorkersRunning := sync.WaitGroup{}
		testWorkersRunning.Add(1)
		currTime := time.Now().UnixNano()
		testDuration := int64(*durationSecs) * int64(time.Second)
		go job.RunTestWorker(
			ctx,
			*workerIndex,
			*numWorkers,
			len(cockroachSocketAddrs),
			*numJobsPerWorker,
			*jobPeriodScaleMillis,
			workerPoolSize,
			currTime,
			testDuration,
			db,
			&testWorkersRunning,
		)
		testWorkersRunning.Wait()
	}
}
