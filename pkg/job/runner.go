package job

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/jinzhu/gorm"
)

type runner struct {
	db                     *gorm.DB
	currentNodeID          string
	nodeStalenessThreshold time.Duration
	failureDelayMillis     int64
	numWorkers             int
}

// A Runner runs job instances, transitioning each job instance through its states
// until there is no next state to iterate.
type Runner interface {
	Run(instance *Instance)
}

var _ Runner = (*runner)(nil)

// NewRunner creates a new Runner that uses db for persistence, has currentNodeID as
// its identity.
func NewRunner(
	db *gorm.DB,
	currentNodeID string,
	nodeStalenessThreshold time.Duration,
	failureDelayMillis int64,
	numWorkers int,
) Runner {
	return &runner{
		db,
		currentNodeID,
		nodeStalenessThreshold,
		failureDelayMillis,
		numWorkers,
	}
}

// should really be a private method inside Dispatcher because for correctness we
// need Run to be called at most once concurrently on a node
func (r *runner) Run(instance *Instance) {
	// fetch job instance, checking for errors, and iterate state until nil
	state := r.createState(instance)
	for state != nil {
		state = r.iterateJobState(state)
	}
}

func assertStrEq(a string, b string) {
	if a != b {
		panic(fmt.Sprintf("strings do not equal. a: %v b %v", a, b))
	}
}

func assertContainsStr(isValid map[string]bool, str string) {
	if !isValid[str] {
		panic(fmt.Sprintf("string is not valid. isValid: %v str: %v", isValid, str))
	}
}

func assertNodeNil(instance *Instance) {
	if instance.NodeID != nil {
		panic(fmt.Sprintf("node should be nil: %v\n", *instance.NodeID))
	}
}

func assertNodeNotNil(instance *Instance) {
	if instance.NodeID == nil {
		panic(fmt.Sprintf("node should not be nil: %v\n", instance.NodeID))
	}
}

// all states loaded here will be immediately claimed or unclaimed
func (r *runner) createState(instance *Instance) state {
	switch instance.Status {
	case queuedStr:
		return &queued{instance}
	case runningStr:
		return &enteringToUndo{instance}
	case toUndoStr:
		return &toUndo{instance}
	case undoingStr:
		return &enteringToUndo{instance}
	case succeededStr:
		return nil
	case failedStr:
		return nil
	default:
		panic(fmt.Sprintf("unexpected status: %v\n", instance.Status))
	}
}

// getGID gets the current goroutine id. Used for debugging.
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func (r *runner) iterateJobState(currState state) state {
	assertContainsStr(currState.expectedStatuses(), currState.instance().Status)
	oldSeqNumber := currState.instance().SequenceNumber
	oldStatus := currState.instance().Status

	// execute runHook
	if runHookErr := currState.runHook(r.db, r.numWorkers); runHookErr != nil {
		// abort
		return nil
	}

	var nextState state
	txResult :=
		withTransaction(
			r.db,
			func(tx *gorm.DB) bool {
				var err error

				var node Node
				// rollback if the job instance is unclaimable
				currNodeID := currState.instance().NodeID
				if currNodeID != nil && *currNodeID != r.currentNodeID {
					err = tx.First(&node, "id = ?", *currState.instance().NodeID).Error
					if err != nil {
						return false
					}
					now := time.Now()
					if now.Sub(time.Unix(0, node.LastUpdateTime)) <=
						r.nodeStalenessThreshold {
						return false
					}
					log.Infof(
						context.TODO(),
						"Proceeding to steal job instance. stale node: %v instance: %v",
						node,
						currState.instance(),
					)
				}

				endTime := time.Now().UnixNano()
				re :=
					tx.Model(
						currState.instance(),
					).Where(
						"sequence_number = ?",
						oldSeqNumber,
					).Updates(
						map[string]interface{}{
							"status":          currState.nextStatus(),
							"node_id":         currState.nextNodeID(r.currentNodeID),
							"sequence_number": oldSeqNumber + 1,
							"end_time":        endTime,
						},
					).RowsAffected
				if re != 1 {
					return false
				}

				if currState.nextIsTerminal() {
					// create next job instance and current instance pointer in
					// job if we are entering a terminal currState
					nextInstanceID := currState.instance().InstanceID + 1
					var j Job
					err = tx.First(&j, "id = ?", currState.instance().JobID).Error
					if err != nil {
						return false
					}

					var nextStartTime int64
					switch nextStatus := currState.nextStatus(); nextStatus {
					case succeededStr:
						nextStartTime = endTime + j.PeriodMillis*int64(time.Millisecond)
					case failedStr:
						nextStartTime = endTime + r.failureDelayMillis
					default:
						log.Fatalf(
							context.TODO(),
							"Unexpected status: %v", currState.nextStatus(),
						)
					}

					// create the next instance
					var nextInstance = Instance{
						currState.instance().JobID,
						nextInstanceID,
						rand.Intn(r.numWorkers),
						queuedStr,
						nextStartTime,
						0,
						nil,
						0,
					}
					err = tx.Create(&nextInstance).Error
					if err != nil {
						return false
					}

					// create the archived instance
					ci := currState.instance()
					var archivedInstance = ArchivedInstance{
						ci.JobID,
						ci.InstanceID,
						ci.Shard,
						ci.Status,
						ci.StartTime,
						ci.EndTime,
						ci.NodeID,
						ci.SequenceNumber,
					}
					err = tx.Create(&archivedInstance).Error
					if err != nil {
						return false
					}
					// delete current instance
					err = tx.Delete(&ci).Error
					if err != nil {
						return false
					}

					// increment current instance id in job
					re :=
						tx.Model(j).Updates(
							map[string]interface{}{
								"current_instance_id": nextInstanceID,
							},
						).RowsAffected
					if re != 1 {
						return false
					}
					return true
				}
				// reload and create the next currState
				var reloadedInstance Instance
				err =
					tx.First(
						&reloadedInstance,
						"job_id = ? AND instance_id = ?",
						currState.instance().JobID,
						currState.instance().InstanceID,
					).Error
				nextState = currState.createNextState(&reloadedInstance)
				return true
			},
		)
	log.Infof(
		context.TODO(),
		"Transition attempted. currNode: %v GID: %v "+
			"job_id: %v instance_id: %v status: %v nextStatus: %v txResult: %v",
		r.currentNodeID,
		getGID(),
		currState.instance().JobID,
		currState.instance().InstanceID,
		oldStatus,
		currState.nextStatus(),
		txResult,
	)
	if txResult == committed {
		return nextState
	}
	return nil
}
