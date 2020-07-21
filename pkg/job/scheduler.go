package job

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/jinzhu/gorm"
)

type scheduler struct {
	db         *gorm.DB
	numWorkers int
}

// A Scheduler schedules jobs to run periodically.
type Scheduler interface {
	// ScheduleRecurringJob schedules a job with the specified id, period, and
	// config, and returns true if a job with the specified id already exists
	// or if it did not exist but was created by this method. If the internal
	// db operations failed or left the db in an ambiguous state, we return false.
	ScheduleRecurringJob(id string, periodMillis int64, jobConfig string) bool
}

var _ Scheduler = (*scheduler)(nil)

// NewScheduler creates a Scheduler that is backed by db.
func NewScheduler(db *gorm.DB, numWorkers int) Scheduler {
	return &scheduler{db, numWorkers}
}

func (s *scheduler) ScheduleRecurringJob(id string, periodMillis int64, jobConfig string) bool {
	txResult :=
		withTransaction(
			s.db,
			func(tx *gorm.DB) bool {
				var err error

				var jobs []*Job
				err = tx.Find(&jobs, "id = ?", id).Error
				if err != nil {
					return false
				} else if len(jobs) == 1 {
					return true
				} else if len(jobs) != 0 {
					panic(fmt.Sprintf("Unexpected slice of jobs: %v", jobs))
				}

				err = tx.Create(
					&Job{
						ID:                id,
						CurrentInstanceID: 1,
						PeriodMillis:      periodMillis,
						JobConfig:         jobConfig,
					},
				).Error
				if err != nil {
					log.Errorf(context.TODO(), "Error while scheduling job: %v", err)
					return false
				}

				startTime := time.Now().UnixNano()
				err = tx.Create(
					&Instance{
						id,
						1,
						rand.Intn(s.numWorkers),
						queuedStr,
						startTime,
						0,
						nil,
						0,
					},
				).Error
				if err != nil {
					return false
				}
				return true
			},
		)
	return txResult == committed
}
