package job

import (
	"context"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"

	"rubrik/util/log"
)

type nodePublisher struct {
	db     *gorm.DB
	nodeID string
	period time.Duration
	stopCh chan struct{}
}

func updateWithCurrentTime(ctx context.Context, db *gorm.DB, nodeID string) {
	node := Node{}
	timestamp := time.Now().UnixNano()
	ur :=
		db.Model(&node).Where("id = ?", nodeID).Updates(
			map[string]interface{}{
				"last_update_time": timestamp,
			},
		)
	var err error
	err = ur.Error
	if err != nil {
		log.Errorf(
			ctx,
			"Received error while updating node table. nodeID: %v err: %v",
			nodeID,
			err,
		)
		return
	}
	re := ur.RowsAffected
	if re == 1 {
		log.Infof(ctx, "Updated node %v with timestamp %v", nodeID, timestamp)
	} else {
		panic(
			fmt.Sprintf(
				"Unexpectedly did not update any rows. nodeID: %v node: %v\n",
				nodeID,
				node,
			),
		)
	}
}

func newNodePublisher(
	db *gorm.DB,
	nodeID string,
	period time.Duration,
) *nodePublisher {
	return &nodePublisher{
		db,
		nodeID,
		period,
		make(chan struct{}),
	}
}

func (np *nodePublisher) performInitialNodeInsert() txResult {
	return withTransaction(
		np.db,
		func(tx *gorm.DB) bool {
			var err error
			var nodes []*Node
			err = tx.Find(&nodes, "id = ?", np.nodeID).Error
			if err != nil {
				log.Errorf(
					context.TODO(),
					"Failed to find nodes during initial insert. err: %v",
					err,
				)
				return false
			}
			if len(nodes) == 0 {
				node := Node{np.nodeID, time.Now().UnixNano()}
				err = tx.Create(&node).Error
				if err != nil {
					log.Errorf(
						context.TODO(),
						"Failed to create node during initial insert. err: %v",
						err,
					)
					return false
				}
				return true
			} else if len(nodes) == 1 {
				return true // we already inserted it
			} else {
				panic(fmt.Sprintf("Unexpected slice of nodes: %v", nodes))
			}
		},
	)
}

func (np *nodePublisher) start() {
	// insert node with current time or update existing node with current time
	for done := false; !done; {
		res := np.performInitialNodeInsert()
		done = res == committed
		log.Infof(
			context.TODO(),
			"Attempted to perform initial node insert. result: %v",
			res,
		)
	}

	// start a daemon that will update the node with the current time periodically
	go func() {
		ticker := time.NewTicker(np.period)
		for {
			select {
			case <-ticker.C:
				updateWithCurrentTime(context.TODO(), np.db, np.nodeID)
			case <-np.stopCh:
				return
			}
		}
	}()
}

func (np *nodePublisher) stopAndAwait() {
	// stopCh has size 0 so nodePublisher blocks until the publisher go routine receives the message
	np.stopCh <- struct{}{}
}
