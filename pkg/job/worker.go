package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/jinzhu/gorm"
)

type jobSpec struct {
	jobID        string
	periodMillis int64
	jobConfig    string
}

// jobSpecs creates and returns a slice of jobSpec objects according to the
// parameters specified. The period for the jobs will range from
// 5 * scaleMillis to 9 * scaleMillis.
func jobSpecs(workerIndex int, numJobsPerWorker int, scaleMillis int64) []jobSpec {
	ret := make([]jobSpec, numJobsPerWorker)
	for i := 0; i < numJobsPerWorker; i++ {
		jobID := fmt.Sprintf("job_node%v_%v", workerIndex, i)
		periodMillis := (int64(i)%5)*scaleMillis + 5*scaleMillis
		jobConfig := fmt.Sprintf("jobConfig%v", i)
		ret[i] = jobSpec{jobID, periodMillis, jobConfig}
	}
	return ret
}

// RunTestWorker starts a worker to act as a mock node while r many jobs.
// Also schedules the specified number of jobs to
// distribute the work of job scheduling.
func RunTestWorker(
	ctx context.Context,
	workerIndex int,
	numWorkers int,
	numRoaches int,
	numJobsPerWorker int,
	jobPeriodScaleMillis int64,
	workerPoolSize int,
	currentTime int64,
	testDuration int64,
	db *gorm.DB,
	testWorkersRunning *sync.WaitGroup,
) {
	// signal that we are done when the main thread completes r function
	defer testWorkersRunning.Done()

	// split test time into run phase and validation phase so that validation will
	// have enough time to complete
	validationTimeout :=
		postCheckTimeout(
			jobPeriodScaleMillis,
			testDuration,
			int64(numRoaches),
		)
	// no more than half of the test should be spent in validation
	if validationTimeout.Nanoseconds() > testDuration/2 {
		log.Fatalf(
			ctx,
			"Bad params. We want at least half of the total time spent "+
				"running the test. testDuration: %v validationTimeout: %v",
			testDuration,
			validationTimeout,
		)
	}
	validationStopTime := currentTime + testDuration
	finalValidationStartTime :=
		validationStopTime - validationTimeout.Nanoseconds()
	log.Infof(
		ctx,
		"Running test for the following duration before validation begins: %v",
		finalValidationStartTime-currentTime,
	)

	scheduler := NewScheduler(db, numWorkers)
	jobSpecs := jobSpecs(workerIndex, numJobsPerWorker, jobPeriodScaleMillis)
	numChunks := 4
	scheduleWg := sync.WaitGroup{}
	scheduleWg.Add(numChunks)
	for ci := 0; ci < numChunks; ci++ {
		go func(ci int) {
			lowerIncl := ci * len(jobSpecs) / numChunks
			upperExcl := (ci + 1) * len(jobSpecs) / numChunks
			for ji := lowerIncl; ji < upperExcl; ji++ {
				jobSpec := jobSpecs[ji]
				for i := 0; ; i++ {
					succ := scheduler.ScheduleRecurringJob(
						jobSpec.jobID,
						jobSpec.periodMillis,
						jobSpec.jobConfig,
					)
					if !succ {
						log.Errorf(
							ctx,
							"Error scheduling job. attempt number: %v jobID: %v",
							i+1,
							jobSpec.jobID,
						)
					} else {
						break
					}
				}
			}
			scheduleWg.Done()
		}(ci)
	}
	scheduleWg.Wait()

	nodePublisherPeriod := 5 * time.Second
	nodeID := fmt.Sprintf("node%v", workerIndex)
	nodePublisher := newNodePublisher(db, nodeID, nodePublisherPeriod)
	nodePublisher.start()
	fetcher := NewFetcher(db, nodeID, workerIndex, numWorkers)
	dispatcher :=
		NewDispatcher(
			db,
			nodeID,
			10000*nodePublisherPeriod, // make stealing extremely unlikely for now
			workerPoolSize,
			jobPeriodScaleMillis,
			numWorkers,
		)

	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
	Loop:
		for {
			select {
			case <-done:
				break Loop
			default:
				assertJobsHaveArtifacts(ctx, db, workerIndex, numWorkers)
				time.Sleep(1 * time.Second)
			}
		}
		wg.Done()
	}()

	go func() {
	Loop:
		for {
			select {
			case <-done:
				break Loop
			default:
				assertArtifactsHaveJobs(ctx, db, workerIndex, numWorkers)
				time.Sleep(1 * time.Second)
			}
		}
		wg.Done()
	}()

	go func() {
	Loop:
		for {
			select {
			case <-done:
				break Loop
			default:
				instances, err := fetcher.Fetch()
				if err == nil {
					for _, instance := range instances {
						dispatcher.Dispatch(instance)
					}
				}
				time.Sleep(1 * time.Second)
			}
		}
		wg.Done()
	}()

	time.Sleep(time.Until(time.Unix(0, finalValidationStartTime)))
	log.Infof(ctx, "Beginning progress check for additional successes.")
	stop := make(chan struct{})
	time.AfterFunc(validationTimeout, func() { close(stop) })
	awaitOneAdditionalSuccess(
		ctx,
		db,
		jobSpecs,
		stop,
	)
	log.Infof(
		ctx,
		"Finished progress check for additional successes. "+
			"Waiting for timeout to stop fetcher.",
	)
	select {
	case <-stop:
	}
	close(done)
	log.Infof(ctx, "Waiting for fetcher and concurrent checks to complete.")
	wg.Wait()
	log.Infof(ctx, "Stopping worker's thread pool: %v", workerIndex)
	dispatcher.stopAndAwait()
	log.Infof(ctx, "Stopping node publisher: %v", workerIndex)
	nodePublisher.stopAndAwait()

	log.Infof(ctx, "Asserting jobs have artifacts. workerIndex: %v", workerIndex)
	assertJobsHaveArtifacts(ctx, db, workerIndex, numWorkers)
	log.Infof(ctx, "Asserting artifacts have jobs. workerIndex: %v", workerIndex)
	assertArtifactsHaveJobs(ctx, db, workerIndex, numWorkers)
	log.Infof(ctx, "Worker completed: %v", workerIndex)
}
