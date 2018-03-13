package main

// Execute these commands before using
// CREATE KEYSPACE glosem WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
// USE glosem;
// CREATE TABLE resources (resource_id int, total_units int, free_units int, allocation map<int, int>, PRIMARY KEY (resource_id ));

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/kaavee315/gocql"
)

type cassandraPool struct {
	session *gocql.Session
}

// NewCassandraPool returns Cassandra implementation of ResourcePool
// interface.
func NewCassandraPool() ResourcePool {
	ip := "127.0.0.1"
	keyspace := "glosem"
	consistency := gocql.Quorum

	cluster := gocql.NewCluster(ip)
	cluster.Keyspace = keyspace
	cluster.Consistency = consistency
	Session, _ := cluster.CreateSession()
	return &cassandraPool{session: Session}
}

// TryAcquire implements ResourcePool.TryAcquire
func (cp *cassandraPool) TryAcquire(r int, u int, j int) error {
	applied := false
	resourceMap := make(map[string]interface{})
	iter := cp.session.Query(`SELECT free_units, allocation FROM resources
                                 WHERE resource_id = ?`, r).Iter()
	if iter.NumRows() == 0 {
		return ErrNoSuchResource
	}
	iter.MapScan(resourceMap)
	freeUnits := resourceMap["free_units"].(int)
	allocationMap := resourceMap["allocation"].(map[int]int)
	for applied == false {
		if freeUnits < u {
			return ErrResourceExhausted
		}
		applied, _ = cp.session.Query(`UPDATE resources SET
                                              free_units = ?,
                                              allocation[?] = ?
                                              WHERE resource_id = ? IF
                                              free_units = ?`,
			freeUnits-u, j, allocationMap[j]+u,
			r, freeUnits).
			ScanCAS(&freeUnits)
	}
	return nil
}

// Release impements ResourcePool.Release
func (cp *cassandraPool) ReleaseAll(r int, j int) error {

	resourceMap := make(map[string]interface{})
	iter := cp.session.Query(`SELECT free_units, allocation FROM resources
                                 WHERE resource_id = ?`, r).Iter()
	if iter.NumRows() == 0 {
		return ErrNoSuchResource
	}
	iter.MapScan(resourceMap)
	freeUnits := resourceMap["free_units"].(int)
	allocationMap := resourceMap["allocation"].(map[int]int)
	applied := false
	for applied == false {
		if _, ok := allocationMap[j]; !ok {
			return nil
		}
		applied, _ = cp.session.Query(`UPDATE resources SET
	                                          free_units = ?,
	                                          allocation = allocation - {`+fmt.Sprintf("%d", j)+`}
	                                          WHERE resource_id = ? IF
	                                          free_units = ?`,
			freeUnits+allocationMap[j], r, freeUnits).
			ScanCAS(&freeUnits)
	}
	return nil
}

func (cp *cassandraPool) Release(r int, u int, j int) error {
	//TODO : This
	return errors.New("Not Implemented")
}

// AddCapacity implements ResourcePool.AddCapacity
func (cp *cassandraPool) AddCapacity(r int, u int) error {
	applied := false
	totalUnits := 0
	freeUnits := 0
	for applied == false {
		iter := cp.session.Query(`SELECT total_units, free_units FROM resources
                                 WHERE resource_id = ?`, r).Iter()
		switch {
		case iter.NumRows() > 0:
			iter.Scan(&totalUnits, &freeUnits)
			for applied == false {
				applied, _ = cp.session.Query(`UPDATE resources SET
                                              free_units = ?, total_units = ?
                                              WHERE resource_id = ? IF
                                              total_units = ? AND
                                              free_units = ?`,
					freeUnits+u, totalUnits+u, r, totalUnits, freeUnits).
					ScanCAS(&totalUnits, &freeUnits)
			}
		case iter.NumRows() == 0:
			applied, _ = cp.session.Query(`INSERT INTO resources (resource_id,
                                             free_units, total_units) VALUES
                                             (?, ?, ?) IF NOT EXISTS`,
				r, freeUnits+u, totalUnits+u).
				ScanCAS()
		default:
			return errors.New("Unknown Error")
		}
	}
	return nil
}

// ReduceCapacity implements ResourcePool.ReduceCapacity
func (cp *cassandraPool) ReduceCapacity(r int, u int) error {
	applied := false
	totalUnits := 0
	freeUnits := 0
	for applied == false {
		err := cp.session.Query(`SELECT total_units, free_units FROM resources
                                 WHERE resource_id = ?`, r).
			Scan(&totalUnits, &freeUnits)
		switch {
		case err == nil:
			for applied == false {
				if freeUnits < u {
					return ErrResourceExhausted
				}
				applied, err = cp.session.Query(`UPDATE resources SET
                                              free_units = ?, total_units = ?
                                              WHERE resource_id = ? IF
                                              total_units = ? AND
                                              free_units = ?`,
					freeUnits-u, totalUnits-u, r, totalUnits, freeUnits).
					ScanCAS(&totalUnits, &freeUnits)
				if err != nil {
					log.Fatal(err)
				}
			}
		case err == gocql.ErrNotFound:
			return ErrNoSuchResource
		default:
			log.Fatal(err)
		}
	}
	return nil
}

// ValidityCheck implements ResourcePool.ValidityCheck
func (cp *cassandraPool) ValidityCheck() (bool, error) {
	valid := true
	iter := cp.session.Query(`SELECT * FROM resources`).Iter()
	for valid == true {
		resourceMap := make(map[string]interface{})
		if !iter.MapScan(resourceMap) {
			break
		}
		allocated := 0
		for _, v := range resourceMap["allocation"].(map[int]int) {
			allocated += v
		}
		valid = (resourceMap["total_units"] ==
			resourceMap["free_units"].(int)+allocated) &&
			(resourceMap["free_units"].(int) >= 0)
	}
	return valid, nil
}

func (cp *cassandraPool) ProgressCheck(locker sync.Locker) (bool, error) {
	return true, nil
}

func (cp *cassandraPool) PrintStats() {
	log.Print("Not implemented for cassandraPool")
}
