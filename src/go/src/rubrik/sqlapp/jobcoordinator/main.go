package main

import _ "net/http/pprof"
import _ "net/http"
import (
	"context"
	"flag"
	"net/http"
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

// Main runs several job test workers in different go routines, providing a
// lightweight way to run the distributed jobs test on a dev machine given
// access to socket addresses for cockroach nodes where a database with the
// appropriate name and user with all privileges.
// Note that jobworker is expected to be run inside of sqlapp/coordinator while
// jobcoordinator exists for the purpose of allowing lightweight local tests to
// run with multiple workers running in separate goroutines.
func main() {
	ctx := context.Background()
	go func() {
		log.Info(ctx, http.ListenAndServe("localhost:6060", nil))
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
	numWorkers :=
		flag.Int(
			sqlapp.NumWorkers,
			0,
			"Number of workers to run jfl and validation.",
		)
	certsDir := flag.String(
		sqlapp.CertsDir,
		"",
		"Directory containing TLS certificates.")
	insecure := flag.Bool(sqlapp.Insecure, true, "Connect to CockroachDB in insecure mode")

	flag.Parse()
	defer log.Flush()
	if err != nil {
		flag.PrintDefaults()
		log.Fatalf(ctx, "Received error while parsing flags: %v", err)
	}
	if len(*cockroachSocketAddrsCSV) <= 0 {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Must supply at least one cockroach socket address: %v",
			*cockroachSocketAddrsCSV,
		)
	}
	if *durationSecs <= 0 {
		flag.PrintDefaults()
		log.Fatalf(
			ctx,
			"Duration of test must be greater than 0: %v",
			*durationSecs,
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
	if *numWorkers <= 0 {
		flag.PrintDefaults()
		log.Fatalf(ctx, "Num workers must be greater than 0: %v", *numWorkers)
	}

	cockroachSocketAddrStrs := strings.Split(*cockroachSocketAddrsCSV, ",")
	cockroachSocketAddrs := make([]crdbutil.SocketAddress, len(cockroachSocketAddrStrs))
	for i, s := range cockroachSocketAddrStrs {
		cockroachSocketAddrs[i], err = crdbutil.ParseSocketAddress(s)
		if err != nil {
			log.Fatalf(ctx, "Error parsing socket address. s: %v err: %v", s, err)
		}
	}
	dbs, err :=
		sqlutil.GormDBs(
			ctx,
			cockroachSocketAddrs,
			job.DefaultDatabaseName,
			*certsDir,
			*insecure,
			false,
		)
	if err != nil {
		log.Fatalf(ctx, "Error creating dbs. err: %v", err)
	}
	workerPoolSize := 32
	for _, db := range dbs {
		db.SingularTable(true)
		db.LogMode(true)
		db.DB().SetMaxOpenConns(2 * workerPoolSize)
	}
	job.CreateSchema(dbs[0])

	testWorkersRunning := sync.WaitGroup{}
	testWorkersRunning.Add(*numWorkers)
	currTime := time.Now().UnixNano()
	testDuration := int64(*durationSecs) * int64(time.Second)

	for i := 0; i < *numWorkers; i++ {
		go job.RunTestWorker(
			ctx,
			i,
			*numWorkers,
			len(dbs),
			*numJobsPerWorker,
			*jobPeriodScaleMillis,
			workerPoolSize,
			currTime,
			testDuration,
			dbs[i%len(cockroachSocketAddrs)],
			&testWorkersRunning,
		)
	}
	log.Infof(ctx, "Waiting for test workers to complete.")
	testWorkersRunning.Wait()
	log.Infof(ctx, "Test workers have completed.")
}
