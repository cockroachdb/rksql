package main

import (
	"context"
	"flag"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"rubrik/sqlapp"
	"rubrik/sqlapp/sqlutil"
	"rubrik/util/crdbutil"
	"rubrik/util/log"
)

const numResources = 4
const (
	opAcquire int = iota
	opRelease
	opAddCpacity
	opReduceCapacity
	numOpTypes
)
const databaseName = "ds"
const ambiguousCommitErrorRetryBound = 10

// testerID of this tester.
var testerID = flag.Int(sqlapp.WorkerIndex, 0, "Index of the worker")

func resourceStart() int {
	return *testerID * numResources
}

func main() {
	ctx := context.Background()
	cockroachIPAddressesCSV :=
		flag.String(
			sqlapp.CockroachIPAddressesCSV,
			"localhost",
			"Comma-separated list of CockroachDb nodes' IP addresses."+
				" The IP addresses can optionally have ports specified in the "+
				"format <ip1>:<port1>,<ip2>:<port2>",
		)
	numWorkers := flag.Int("num_workers", 5, "Concurrent workers")
	durationSecs := flag.Int(sqlapp.DurationSecs, 10, "Number of seconds to run each worker")
	installSchema := flag.Bool(
		sqlapp.InstallSchema,
		false,
		"Install schema for this test and then exit.")
	certsDir := flag.String(
		sqlapp.CertsDir,
		"",
		"Directory containing TLS certificates.")
	insecure := flag.Bool(sqlapp.Insecure, true, "Connect to CockroachDB in insecure mode")
	flag.Parse()
	defer log.Flush()
	if len(*cockroachIPAddressesCSV) == 0 {
		log.Fatalf(ctx, "cockroachIPAddressesCSV cannot be empty: %s", *cockroachIPAddressesCSV)
	}
	cockroachIPAddrStrs := strings.Split(*cockroachIPAddressesCSV, ",")
	cockroachSocketAddrs := make([]crdbutil.SocketAddress, len(cockroachIPAddrStrs))
	for i, s := range cockroachIPAddrStrs {
		a, err := crdbutil.ParseSocketAddress(s)
		if err != nil {
			log.Fatal(ctx, err)
		}
		cockroachSocketAddrs[i] = a
	}
	dbs, err :=
		sqlutil.GormDBs(
			ctx,
			cockroachSocketAddrs,
			databaseName,
			*certsDir,
			*insecure,
			!*installSchema,
		)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if *installSchema {
		if err := setupDB(ctx, dbs[rand.Intn(len(dbs))]); err != nil {
			log.Fatal(ctx, err)
		}
		return
	}
	rp := NewCockroachPool(dbs)
	defer rp.PrintStats()
	rand.Seed(time.Now().UTC().UnixNano())
	log.Info(ctx, "Running Consistency Test\n")
	if !consistencyTest(ctx, rp, *numWorkers, *durationSecs/2) {
		log.Fatal(ctx, "Consistency Test Failed")
	}
	log.Info(ctx, "Consistency Test Succeeded")
	rp.PrintStats()

	log.Info(ctx, "Running Progress Test\n")
	if !progressTest(ctx, rp, *numWorkers, *durationSecs/2) {
		log.Fatal(ctx, "Progress Test Failed\n")
	}
	log.Info(ctx, "Progress Test Succeeded\n")
}

func setupDB(ctx context.Context, db *gorm.DB) error {
	log.Info(ctx, "Migrating tables")
	db.DropTableIfExists("resources")
	err := db.CreateTable(&resource{}).Error
	if err != nil {
		return err
	}

	db.DropTableIfExists("allocations")
	err = db.CreateTable(&allocation{}).Error
	if err != nil {
		return err
	}
	db.Model(&resource{}).AddUniqueIndex("idx_pk", "resource_id")
	db.Model(&allocation{}).AddUniqueIndex("idx_pk", "resource_id", "job_id")
	log.Info(ctx, "Migration complete")
	return nil
}

func consistencyTest(
	ctx context.Context,
	rp ResourcePool,
	numWorkers int,
	duration int,
) bool {
	var done = make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go consistencyWorker(ctx, i, rp, done, &wg)
	}
	passed := validator(ctx, duration, rp.ValidityCheck)
	close(done)
	wg.Wait()
	return passed
}

func ignoreAmbiguousCommitError(err error) error {
	if _, ok := err.(*crdb.AmbiguousCommitError); ok {
		return nil
	}
	return err
}

func consistencyWorker(
	ctx context.Context,
	id int,
	rp ResourcePool,
	done <-chan struct{},
	wg *sync.WaitGroup,
) {
	for {
		select {
		case <-done:
			wg.Done()
			return
		default:
			units := rand.Intn(10)
			res := resourceStart() + rand.Intn(numResources)
			switch rand.Intn(numOpTypes) {
			case opAcquire:
				err := ignoreAmbiguousCommitError(rp.TryAcquire(res, units, id))
				switch err {
				case ErrNoSuchResource, ErrResourceExhausted:
					if err := ignoreAmbiguousCommitError(rp.AddCapacity(res, units)); err != nil {
						log.Fatal(ctx, err)
					}
				case nil:
				default:
					log.Fatal(ctx, err)
				}
			case opRelease:
				if err := ignoreAmbiguousCommitError(rp.ReleaseAll(res, id)); err != nil {
					log.Fatal(ctx, err)
				}
			case opAddCpacity:
				if err := ignoreAmbiguousCommitError(rp.AddCapacity(res, units)); err != nil {
					log.Fatal(ctx, err)
				}
			case opReduceCapacity:
				err := ignoreAmbiguousCommitError(rp.ReduceCapacity(res, units))
				switch err {
				case ErrNoSuchResource, ErrResourceExhausted, nil:
				default:
					log.Fatal(ctx, err)
				}
			}
		}
	}
}

func validator(
	ctx context.Context,
	duration int,
	check func() (bool, error),
) bool {
	// The test would often fail because its progress check did not complete
	// before the coordinator timed out and killed it. The logs showed that the
	// progress check ran many times. It failed in the unhappy circumstance
	// where `check` started executing close to the deadline and wasn't able to
	// complete before the process was killed. By shortening the duration, we'll
	// avoid such case.
	const leeway = 0.96
	ticker := time.NewTicker(time.Second * 2)
	deadline := time.Now().Add(time.Second * time.Duration(float64(duration)*leeway))
	for range ticker.C {
		if passed, err := check(); !passed || err != nil {
			log.Infof(ctx, "Validation check unsuccessful, passed: %v, err: %v", passed, err)
			return false
		}
		if time.Now().After(deadline) {
			break
		}
	}

	return true
}

func progressTest(
	ctx context.Context,
	rp ResourcePool,
	numWorkers int,
	duration int,
) bool {
	var done = make(chan struct{})
	rwmut := &sync.RWMutex{}
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go progressWorker(ctx, i, rp, done, &wg, rwmut.RLocker())
	}
	passed :=
		validator(
			ctx,
			duration,
			func() (bool, error) { return rp.ProgressCheck(rwmut) },
		)
	close(done)
	wg.Wait()
	return passed
}

func progressWorker(
	ctx context.Context,
	id int,
	rp ResourcePool,
	done chan struct{},
	wg *sync.WaitGroup,
	locker sync.Locker,
) {
	for units := 1; ; units += 2 {
		select {
		case <-done:
			wg.Done()
			return
		default:
			res := resourceStart() + rand.Intn(numResources)
			locker.Lock()
			log.Infof(
				ctx,
				"Acquiring %d units of resource_id %d for job_id %d\n",
				units,
				res,
				id,
			)
			errorCount := 0
			for err := rp.TryAcquire(res, units, id); err != nil; err = rp.TryAcquire(res, units, id) {
				if (err == ErrResourceExhausted) || (err == ErrNoSuchResource) {
					if e := ignoreAmbiguousCommitError(rp.AddCapacity(res, units*4)); e != nil {
						log.Fatal(ctx, e)
					}
					continue
				}
				if errorCount > ambiguousCommitErrorRetryBound {
					log.Fatal(
						ctx,
						"number of ambiguousCommitError exceeded ambiguousCommitErrorRetryBound of ",
						ambiguousCommitErrorRetryBound,
					)
				}
				if ignoreAmbiguousCommitError(err) == nil {
					errorCount++
				} else {
					log.Fatal(ctx, err)
				}
			}
			log.Infof(
				ctx,
				"Releasing %d units of resource_id  %d from job_id %d\n",
				units/2,
				res,
				id,
			)
			if err := ignoreAmbiguousCommitError(rp.Release(res, units/2, id)); err != nil {
				log.Fatal(ctx, err)
			}
			locker.Unlock()
		}
	}
}
