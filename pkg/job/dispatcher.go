package job

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/jinzhu/gorm"
)

type dispatcher struct {
	db            *gorm.DB
	activeJobIDs  map[string]bool
	currentNodeID string
	mutex         *sync.Mutex
	runner        Runner
	wp            *workerPool
}

// A Dispatcher provides the ability to run a Instance asynchronously.
type Dispatcher interface {
	Dispatch(instance *Instance)
	stopAndAwait()
}

var _ Dispatcher = (*dispatcher)(nil)

// NewDispatcher creates a Dispatcher that uses db to track job state transitions,
// and that runs/claims jobs as currentNodeID.
func NewDispatcher(
	db *gorm.DB,
	currentNodeID string,
	nodeStalenessThreshold time.Duration,
	workerPoolSize int,
	failureDelayMillis int64,
	numWorkers int,
) Dispatcher {
	return &dispatcher{
		db,
		make(map[string]bool),
		currentNodeID,
		&sync.Mutex{},
		NewRunner(
			db,
			currentNodeID,
			nodeStalenessThreshold,
			failureDelayMillis,
			numWorkers,
		),
		newWorkerPool(workerPoolSize),
	}
}

func (d *dispatcher) setInactive(jobID string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.activeJobIDs, jobID)
}

func (d *dispatcher) Dispatch(instance *Instance) {
	var abort = false
	func() {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.activeJobIDs[instance.JobID] {
			abort = true
			return
		}
		d.activeJobIDs[instance.JobID] = true
	}()
	if abort {
		log.Infof(
			context.TODO(),
			"Instance is already active, aborting: %v",
			instance,
		)
		return
	}
	log.Infof(context.TODO(), "Enqueuing job instance: %v", instance)
	d.wp.submit(
		func() {
			defer d.setInactive(instance.JobID)
			log.Infof(context.TODO(), "Starting run of job instance: %v", instance)
			d.runner.Run(instance)
			log.Infof(context.TODO(), "Finished run of job instance: %v", instance)
		},
	)
}

func (d *dispatcher) stopAndAwait() {
	d.wp.stopAndAwait()
}
