package job

import (
	"time"

	"github.com/jinzhu/gorm"
)

type fetcher struct {
	db         *gorm.DB
	nodeID     string
	nodeIndex  int
	numWorkers int
}

// A Fetcher fetches JobInstances that need work to be performed on them (e.g.,
// start time has passed and they are unclaimed or belong to a stale node.
type Fetcher interface {
	Fetch() ([]*Instance, error)
}

var _ Fetcher = (*fetcher)(nil)

// NewFetcher creates a Fetcher that uses db for persistence operations and has
// an identity of nodeID.
func NewFetcher(
	db *gorm.DB,
	nodeID string,
	nodeIndex int,
	numWorkers int,
) Fetcher {
	return &fetcher{db, nodeID, nodeIndex, numWorkers}
}

func (f *fetcher) Fetch() ([]*Instance, error) {
	var instances []*Instance
	now := time.Now().UnixNano()
	var err error
	err =
		f.db.Order(
			"start_time",
		).Find(
			&instances,
			"shard in (?, ?, ?) AND start_time <= ? ",
			f.nodeIndex,
			(f.nodeIndex+1)%f.numWorkers,
			(f.nodeIndex+2)%f.numWorkers,
			now,
		).Error
	if err != nil {
		return nil, err
	}
	return instances, nil
}
