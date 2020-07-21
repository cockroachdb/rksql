package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/rksql/pkg/sqlapp"
	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type resource struct {
	gorm.Model
	ResourceID int
	Free       int
	Total      int
}

type allocation struct {
	gorm.Model
	ResourceID int
	JobID      int
	Units      int
}

type cockroachPool struct {
	prevAllocMap map[string]int
	db           *sqlapp.RobustDB
}

// NewCockroachPool returns ResourcePool interface type implemented
// on CockroachDB
func NewCockroachPool(dbs []*gorm.DB) ResourcePool {
	return &cockroachPool{
		db: sqlapp.NewRobustDB(dbs, sqlapp.RetryOnConnectionError),
	}
}

func findAllocation(tx *gorm.DB, r int, j int) (*allocation, error) {
	var ac []allocation
	err := tx.Where("resource_id = ? AND job_id = ?", r, j).Find(&ac).Error
	if len(ac) == 0 {
		return nil, err
	}
	return &ac[0], err
}

func findResource(tx *gorm.DB, r int) (*resource, error) {
	var cr []resource
	err := tx.Where("resource_id = ?", r).Find(&cr).Error
	if len(cr) == 0 {
		return nil, err
	}
	return &cr[0], err
}

func (cp *cockroachPool) TryAcquire(r int, u int, j int) error {
	err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		cr, err := findResource(tx, r)
		if err != nil {
			return err
		}
		if cr == nil {
			return ErrNoSuchResource
		}
		if cr.Free < u {
			return ErrResourceExhausted
		}
		ac, err := findAllocation(tx, r, j)
		if err != nil {
			return err
		}
		cr.Free -= u
		if err := tx.Save(cr).Error; err != nil {
			return err
		}

		if ac == nil {
			alloc := allocation{ResourceID: r, JobID: j, Units: u}
			if err := tx.Create(&alloc).Error; err != nil {
				return err
			}
		} else {
			ac.Units += u
			if err := tx.Save(ac).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (cp *cockroachPool) Release(r int, u int, j int) error {
	err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		ac, err := findAllocation(tx, r, j)
		if err != nil {
			return err
		}
		if ac == nil {
			return nil
		}
		cr, err := findResource(tx, ac.ResourceID)
		if err != nil {
			return err
		}
		if ac.Units == u {
			u = ac.Units
		}
		ac.Units -= u
		if err := tx.Save(ac).Error; err != nil {
			return err
		}
		cr.Free += u
		return tx.Save(cr).Error
	})
	return err
}

func (cp *cockroachPool) ReleaseAll(r int, j int) error {
	err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		ac, err := findAllocation(tx, r, j)
		if err != nil {
			return err
		}
		if ac == nil {
			return nil
		}
		cr, err := findResource(tx, ac.ResourceID)
		if err != nil {
			return err
		}
		cr.Free += ac.Units
		if err := tx.Unscoped().Delete(ac).Error; err != nil {
			return err
		}
		if err := tx.Save(cr).Error; err != nil {
			return err
		}
		return nil
	})
	return err
}

func (cp *cockroachPool) AddCapacity(r int, u int) error {
	err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		cr, err := findResource(tx, r)
		if err != nil {
			return err
		}
		if cr == nil {
			cres := resource{ResourceID: r, Free: u, Total: u}
			if err := tx.Create(&cres).Error; err != nil {
				fmt.Println(err)
				return err
			}
		} else {
			cr.Total += u
			cr.Free += u
			if err := tx.Save(cr).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (cp *cockroachPool) ReduceCapacity(r int, u int) error {
	err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		cr, err := findResource(tx, r)
		if err != nil {
			return err
		}
		if cr == nil {
			return ErrNoSuchResource
		}
		if cr.Free < u {
			return ErrResourceExhausted
		}
		cr.Total -= u
		cr.Free -= u
		return tx.Save(cr).Error
	})
	return err
}

func (cp *cockroachPool) ValidityCheck() (bool, error) {
	var cr []resource
	var ac []allocation
	tm := time.Now().UTC().Add(time.Millisecond * (-1000)).
		Format("2006-01-02 15:04:05.000000")
	// GORM Transaction does not allow AS OF.
	query := fmt.Sprintf(
		"SELECT * FROM allocations AS OF SYSTEM TIME '%s' WHERE resource_id >= %d AND resource_id < %d;",
		tm,
		resourceStart(),
		resourceStart()+numResources,
	)
	if err := cp.db.ExecuteRawQuery(
		func(db *gorm.DB) error {
			return db.Raw(query).Scan(&ac).Error
		},
	); err != nil {
		return false, err
	}

	query = fmt.Sprintf(
		"SELECT * FROM resources AS OF SYSTEM TIME '%s' WHERE resource_id >= %d AND resource_id < %d;",
		tm, resourceStart(),
		resourceStart()+numResources,
	)
	if err := cp.db.ExecuteRawQuery(
		func(db *gorm.DB) error {
			return db.Raw(query).Scan(&cr).Error
		},
	); err != nil {
		return false, err
	}

	total := make(map[int]int)
	computed := make(map[int]int)
	for _, res := range cr {
		total[res.ResourceID] = res.Total
		computed[res.ResourceID] = res.Free
	}
	for _, alloc := range ac {
		computed[alloc.ResourceID] += alloc.Units
	}
	for k, v := range total {
		if v != computed[k] {
			return false, nil
		}
	}
	return true, nil
}

func (cp *cockroachPool) ProgressCheck(locker sync.Locker) (bool, error) {
	var currAllocation []allocation
	var currAllocMap = make(map[string]int)

	locker.Lock()
	if err := cp.db.ExecuteTx(func(tx *gorm.DB) error {
		err := tx.Where("resource_id >= ? AND resource_id < ?", resourceStart(), resourceStart()+numResources).Find(&currAllocation).Error
		return err
	}); err != nil {
		locker.Unlock()
		return false, err
	}
	locker.Unlock()
	for _, alloc := range currAllocation {
		currAllocMap[fmt.Sprintf("%d:%d", alloc.ResourceID, alloc.JobID)] = alloc.Units
	}
	if cp.prevAllocMap == nil {
		cp.prevAllocMap = currAllocMap
		return true, nil
	}
	log.Infof(context.TODO(), "Validation Started")
	numAllocChanged := 0
	for k, v := range currAllocMap {
		if cp.prevAllocMap[k] > v {
			log.Fatal(context.TODO(), "Error: Regressed instead of Progress")
		}
		if cp.prevAllocMap[k] != v {
			numAllocChanged++
		}
	}
	cp.prevAllocMap = currAllocMap
	if numAllocChanged > 0 {
		log.Infof(
			context.TODO(),
			"%d allocations changed from previous validation ",
			numAllocChanged,
		)
		return true, nil
	}
	return false, nil
}

func (cp *cockroachPool) PrintStats() {
	cp.db.PrintStats()
}
