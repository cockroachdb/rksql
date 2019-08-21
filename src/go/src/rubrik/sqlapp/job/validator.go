package job

import (
	"context"
	"sync"
	"time"

	"github.com/jinzhu/gorm"

	"rubrik/util/log"
)

// We compute time duration in which the post-checks should be able to complete
// as a function of the following:
// 1. jobPeriodScaleMillis, which determines how long it may
//    take for a job to run one more time to pass the progress check
// 2. testDuration and numWorkers, which determines how long a node may be
//    unable to make progress due to a domino failure
func postCheckTimeout(
	jobPeriodScaleMillis int64,
	testDuration int64,
	numRoaches int64,
) time.Duration {
	return 30*time.Duration(jobPeriodScaleMillis)*time.Millisecond +
		time.Duration(testDuration/int64(numRoaches))
}

// mostRecentTerminalInstanceID computes the largest instanceID starting from lastTerminalInstanceID
// for which the job instance is in a terminal state. lastTerminalInstanceID is assumed to be in
// terminal state and will be returned if no additional terminal job instance is found, so by convention
// 0 can be passed to begin the search.
// return the most recent terminal instance ID and the most recent successful instanceID.
func mostRecentTerminalInstanceID(
	ctx context.Context,
	db *gorm.DB,
	jobID string,
	scanAfter int64, // InstanceID after which to start scanning
	stop chan struct{},
) (int64, int64) {
	const limit = 10
	retTerm := scanAfter
	retSucc := scanAfter
Outer:
	for done := false; !done; {
		select {
		case <-stop:
			break Outer
		default:
			instances, err :=
				scanInstancesWithJobID(
					db,
					jobID,
					scanAfter,
					limit,
				)
			if err != nil {
				// retry
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				for _, ji := range instances {
					if ji.Status == succeededStr || ji.Status == failedStr {
						retTerm = ji.InstanceID
					} else {
						log.Fatalf(
							ctx,
							"Unexpected non-terminal job instance found. instance: %v",
							ji,
						)
					}
					if ji.Status == succeededStr {
						retSucc = ji.InstanceID
					}
					if ji.InstanceID > scanAfter {
						scanAfter = ji.InstanceID
					}
				}
			}
			if len(instances) < limit {
				done = true
			}
		}
	}
	return retTerm, retSucc
}

// awaitOneAdditionalSuccess waits for one more successful job instance for each job
// that was scheduled by this node (i.e., whose spec is contained in jobSpecs).
// More specifically, it checks the current InstanceID for each job and requires a
// success to occur for an instance whose InstanceID is strictly greater than this
// initial current InstanceID.
func awaitOneAdditionalSuccess(
	ctx context.Context,
	db *gorm.DB,
	jobSpecs []jobSpec,
	stop chan struct{},
) {
	scanAfter := make([]int64, len(jobSpecs))
	startTime := time.Now()
	wp := newWorkerPool(16)
	wg := sync.WaitGroup{}
	wg.Add(len(jobSpecs))
	for i, js := range jobSpecs {
		i := i
		js := js
		wp.submit(
			func() {
				defer wg.Done()
				var res txResult
			Loop:
				for done := false; !done; done = res == committed {
					select {
					case <-stop:
						break Loop
					default:
						res = withTransaction(
							db,
							func(tx *gorm.DB) bool {
								var err error
								var job Job
								err = tx.First(
									&job,
									"id = ?",
									js.jobID,
								).Error
								log.Infof(
									ctx,
									"Attempted query of job during verification. "+
										"jobSpec: %v err: %v",
									js,
									err,
								)
								if err != nil {
									return false
								}
								scanAfter[i] = job.CurrentInstanceID
								return true
							},
						)
					}
				}
			},
		)
	}
	wg.Wait()
	wp.stopAndAwait()
	scanAfterTime := time.Now()
	log.Infof(
		ctx,
		"Finished computing scanAfter (or timed out). "+
			"time elapsed (sec): %v scanAfter: %v",
		scanAfterTime.Sub(startTime).Seconds(),
		scanAfter,
	)
Outer2:
	for i, js := range jobSpecs {
		// look for a success of a job instance with an InstanceID that is strictly greater than
		// what was read as the current InstanceID.
	NextJob:
		for {
			select {
			case <-stop:
				break Outer2
			default:
				term, succ :=
					mostRecentTerminalInstanceID(ctx, db, js.jobID, scanAfter[i], stop)
				if succ > scanAfter[i] {
					log.Infof(
						ctx,
						"Detected new successful job instance. jobID: %v instanceID: %v",
						js.jobID,
						succ,
					)
					break NextJob
				}
				scanAfter[i] = term
				log.Infof(
					ctx,
					"No new successes detected. jobID: %v terminal instanceID: %v",
					js.jobID,
					term,
				)
				time.Sleep(2 * time.Second)
			}
		}
	}
	// non-blocking check to see whether we timed out
	select {
	case <-stop:
		log.Fatalf(
			ctx,
			"Hit timeout while waiting for jobs to succeed. jobSpecs: %v",
			jobSpecs,
		)
	default:
	}
}

// assertJobsHaveArtifacts checks that each job instance that was run
// successfully has an artifact.
func assertJobsHaveArtifacts(
	ctx context.Context,
	db *gorm.DB,
	workerIndex int,
	numWorkers int,
) {
	nextLowerBound := &ArchivedInstance{
		"",
		1,
		0,
		"",
		0,
		0,
		nil,
		0,
	}
	i := 0
	for nextLowerBound != nil {
		nextLowerBound =
			assertNextPageOfJobsHaveArtifacts(
				ctx,
				db,
				workerIndex,
				numWorkers,
				nextLowerBound.StartTime,
				nextLowerBound.JobID,
				nextLowerBound.InstanceID,
				100,
			)
		i++
	}
}

// assertArtifactsHaveJobs checks that each artifact present has a valid
// job instance associated with it.
func assertArtifactsHaveJobs(
	ctx context.Context,
	db *gorm.DB,
	workerIndex int,
	numWorkers int,
) {
	// smallest possible string lexicographically
	nextLowerBound := &Artifact{"", 0, ""}
	i := 0
	for nextLowerBound != nil {
		nextLowerBound =
			assertNextPageOfArtifactsHaveJobInstances(
				ctx,
				db,
				workerIndex,
				numWorkers,
				nextLowerBound.ID,
				100,
			)
		i++
	}
}

// return the next lower bound to use for the next page (can be nil)
func assertNextPageOfJobsHaveArtifacts(
	ctx context.Context,
	db *gorm.DB,
	workerIndex int,
	numWorkers int,
	startTime int64,
	jobID string,
	instanceID int64,
	limit int,
) *ArchivedInstance {
	var err error
	var instances []*ArchivedInstance
	var txResult txResult
	var successes []*ArchivedInstance
	var hasValidArtifactArr []bool
	for done := false; !done; done = txResult == committed {
		time.Sleep(100 * time.Millisecond)
		txResult =
			withTransaction(
				db,
				func(tx *gorm.DB) bool {
					instances, err =
						scanAllInstances(
							tx,
							workerIndex,
							numWorkers,
							startTime,
							jobID,
							instanceID,
							limit,
						)
					if err != nil {
						return false
					}
					successes = []*ArchivedInstance{}
					for _, instance := range instances {
						if instance.Status == succeededStr {
							successes = append(successes, instance)
						}
					}
					hasValidArtifactArr = make([]bool, len(successes))
					for i, success := range successes {
						hasValidArtifactArr[i] = hasValidArtifact(tx, success)
					}
					return true
				},
			)
		if txResult != committed {
			log.Errorf(
				ctx,
				"Possible error while committing. jobID: %v instanceID: %v",
				jobID,
				instanceID,
			)
		}
	}
	for i := range successes {
		if !hasValidArtifactArr[i] {
			log.Fatalf(ctx, "no valid artifact found for successful job instance: %v", successes[i])
		}
	}
	if len(instances) < limit {
		return nil
	}
	return instances[len(instances)-1]
}

// assertNextPageOfArtifactsHaveJobInstances checks whether a page of Artifacts
// have associated JobInstances. Returns the next lower bound to use for the
// next page (can be nil).
func assertNextPageOfArtifactsHaveJobInstances(
	ctx context.Context,
	db *gorm.DB,
	workerIndex int,
	numWorkers int,
	artifactID string,
	limit int,
) *Artifact {
	var err error
	var artifacts []*Artifact
	var txResult txResult
	var hasValidJobInstanceArr []hasValidJobInstanceResult
	for done := false; !done; done = txResult == committed {
		time.Sleep(100 * time.Millisecond)
		txResult =
			withTransaction(
				db,
				func(tx *gorm.DB) bool {
					artifacts, err =
						scanArtifacts(
							tx,
							workerIndex,
							numWorkers,
							artifactID,
							limit,
						)
					if err != nil {
						return false
					}
					hasValidJobInstanceArr =
						make([]hasValidJobInstanceResult, len(artifacts))
					for i, artifact := range artifacts {
						hasValidJobInstanceArr[i], err =
							hasValidJobInstance(ctx, tx, artifact)
						if err != nil {
							return false
						}
					}
					return true
				},
			)
		if txResult != committed {
			log.Errorf(
				ctx,
				"Possible error while committing. artifactID: %v",
				artifactID,
			)
		}
	}
	for i := range artifacts {
		if !hasValidJobInstanceArr[i].result {
			log.Fatalf(
				ctx,
				"no valid job instance found. "+
					"artifact: %v archived_instance: %v instance: %v",
				artifacts[i],
				hasValidJobInstanceArr[i].archivedInstance,
				hasValidJobInstanceArr[i].instance,
			)
		}
		log.Infof(ctx, "Validated job instance for artifact: %v", artifacts[i])
	}
	if len(artifacts) < limit {
		return nil
	}
	return artifacts[len(artifacts)-1]
}

// scanAllInstances returns the next page of job instances across all jobs,
// ordering first by JobID and then by InstanceID, starting with the first
// InstanceID that is strictly greater than the supplied instanceID.
// If there is an error returned, the slice of instances returned is nil.
func scanAllInstances(
	tx *gorm.DB,
	workerIndex int,
	numWorkers int,
	startTime int64,
	jobID string,
	instanceID int64,
	limit int,
) ([]*ArchivedInstance, error) {
	var err error
	var instances []*ArchivedInstance
	err =
		tx.Order(
			"start_time",
		).Order(
			"job_id",
		).Order(
			"instance_id",
		).Limit(
			limit,
		).Find(
			&instances,
			"shard = ? "+
				"AND start_time > ? "+
				"AND (job_id = ? AND instance_id > ? OR job_id > ?)",
			(workerIndex+3)%numWorkers, // not the executor if numWorkers > 3
			startTime,
			jobID,
			instanceID,
			jobID,
		).Error
	if err != nil {
		return nil, err
	}
	return instances, nil
}

// scanInstancesWithJobID returns the next page of job instances across all
// jobs, ordering first by JobID and then by InstanceID, starting with the
// first InstanceID that is strictly greater than the supplied instanceID.
// If there is an error returned, the slice of instances returned is nil.
func scanInstancesWithJobID(
	tx *gorm.DB,
	jobID string,
	instanceID int64,
	limit int,
) ([]*ArchivedInstance, error) {
	var err error
	var instances []*ArchivedInstance
	err =
		tx.Order(
			"instance_id",
		).Limit(
			limit,
		).Find(
			&instances,
			"job_id = ? AND instance_id > ?",
			jobID,
			instanceID,
		).Error
	if err != nil {
		return nil, err
	}
	return instances, nil
}

func scanArtifacts(
	tx *gorm.DB,
	workersIndex int,
	numWorkers int,
	artifactID string,
	limit int,
) ([]*Artifact, error) {
	var err error
	var artifacts []*Artifact
	err =
		tx.Order(
			"index_id",
		).Limit(
			limit,
		).Find(
			&artifacts,
			"shard = ? AND index_id > ?",
			(workersIndex+3)%numWorkers, // not the executor if numWorkers > 3
			artifactID,
		).Error
	return artifacts, err
}

func hasValidArtifact(tx *gorm.DB, archivedInstance *ArchivedInstance) bool {
	var err error
	var artifact Artifact

	if archivedInstance.Status != succeededStr {
		panic("should only call q with successful instances: %v\n")
	}

	artifactID := artifactID(archivedInstance)
	err = tx.First(&artifact, "id = ?", artifactID).Error
	return err == nil && artifact.ID == artifactID
}

type hasValidJobInstanceResult struct {
	result           bool
	archivedInstance *ArchivedInstance
	instance         *Instance
}

func hasValidJobInstance(
	ctx context.Context,
	tx *gorm.DB,
	artifact *Artifact,
) (hasValidJobInstanceResult, error) {
	var err error
	var archivedInstances []ArchivedInstance
	var instances []Instance

	artifactData := parsedArtifactID(ctx, artifact.ID)
	err =
		tx.Find(
			&archivedInstances,
			"job_id = ? AND instance_id = ?",
			artifactData.jobID,
			artifactData.instanceID,
		).Error
	if err != nil {
		return hasValidJobInstanceResult{false, nil, nil}, err
	}
	if len(archivedInstances) > 0 {
		if len(archivedInstances) > 1 {
			log.Fatalf(
				ctx,
				"returned slice should not have more than one element: %v",
				archivedInstances,
			)
		}
		instance := archivedInstances[0]
		ret := instance.JobID == artifactData.jobID &&
			instance.InstanceID == artifactData.instanceID &&
			instance.Status != failedStr
		return hasValidJobInstanceResult{ret, &instance, nil}, nil
	}
	err =
		tx.Find(
			&instances,
			"job_id = ? AND instance_id = ?",
			artifactData.jobID,
			artifactData.instanceID,
		).Error
	if err != nil {
		return hasValidJobInstanceResult{false, nil, nil}, err
	} else if len(instances) == 0 {
		return hasValidJobInstanceResult{false, nil, nil}, nil
	} else if len(instances) != 1 {
		log.Fatalf(
			ctx,
			"returned slice should not have more than one element: %v",
			instances,
		)
	}
	instance := instances[0]
	ret := instance.JobID == artifactData.jobID &&
		instance.InstanceID == artifactData.instanceID &&
		instance.Status != failedStr
	return hasValidJobInstanceResult{ret, nil, &instance}, nil
}
