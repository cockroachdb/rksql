package job

import (
	"context"
	"time"

	"strings"

	"github.com/jinzhu/gorm"

	"rubrik/util/log"
)

func shouldRollback(err error) bool {
	return err != nil &&
		!strings.Contains(
			err.Error(),
			"sql: Transaction has already been committed or rolled back",
		)
}

type txResult int

const (
	committed txResult = iota
	rolledBack
	ambiguous
)

func withTransaction(
	db *gorm.DB,
	f func(tx *gorm.DB) bool,
) txResult {
	tx := db.Begin()
	var err error
	if err := tx.Error; err != nil {
		return rolledBack // assume that the tx was not created
	}

	commit := f(tx)
	if commit {
		err = tx.Commit().Error
	} else {
		err = tx.Rollback().Error
	}
	if err == nil {
		if commit {
			return committed
		}
		return rolledBack
	}
	for shouldRollback(err) {
		log.Errorf(context.TODO(), "Caught error. will sleep: %v", err)
		time.Sleep(1000 * time.Millisecond)
		err = tx.Rollback().Error
	}
	return ambiguous
}
