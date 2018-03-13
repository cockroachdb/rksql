package main

import (
	"fmt"
	"sync"
)

// ResourcePool is the main interface that is implemented by all backends.
type ResourcePool interface {
	// Tries to acquire u units of resource r for job j. gives error if
	// resource does not exist or not enough free resource is available
	TryAcquire(r int, u int, j int) error

	// Releases all units of resource r occupied by job j.
	// Returns error if resource does not exist.
	ReleaseAll(r int, j int) error

	// Releases u units of resource r occupied by job j.
	// Returns error if resource does not exist.
	Release(r int, u int, j int) error

	// Increases capacity of resource r by u units. Creates resource if it
	// does not exist.
	AddCapacity(r int, u int) error

	// Decreases capacity of resource r by u units. Returns error if enough units of
	// resource_id were not available.
	ReduceCapacity(r int, u int) error

	// Checks the consistency of database.
	// eg. free + allocated == total, free > 0
	ValidityCheck() (bool, error)

	// ProgressCheck checks progress by verifying that allocation to atleast 1
	// (job_id, resource_id) has increased from previous iteration
	// It acquires locker.Lock() before fetching data from database, and releases after.
	ProgressCheck(locker sync.Locker) (bool, error)

	// PrintStats may print some statistics about its usage.
	PrintStats()
}

//Error type of this package
type Error int

const (
	// ErrNoSuchResource denotes resource did not exist
	// while doing TryAcquire or ReduceCapacity
	ErrNoSuchResource Error = iota
	// ErrResourceExhausted denotes less than mentioned
	// Units of resource during TryAcquire or ReduceCapacity
	ErrResourceExhausted
)

//go:generate stringer -type Error

func (e Error) Error() string {
	return fmt.Sprintf("Resouce error : %s", e.String())
}
