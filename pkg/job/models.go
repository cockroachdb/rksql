package job

import (
	"github.com/jinzhu/gorm"
)

// Constants used by testing for interacting with cockroach.
const (
	DefaultDatabaseName string = "job"
)

// CreateSchema creates the schema for the job test in the specified db, but first drop the tables if they exist.
func CreateSchema(db *gorm.DB) {
	db.DropTableIfExists(&Artifact{})
	db.DropTableIfExists(&Job{})
	db.DropTableIfExists(&ArchivedInstance{})
	db.DropTableIfExists(&Instance{})
	db.DropTableIfExists(&Node{})
	db.AutoMigrate(&Artifact{})
	db.AutoMigrate(&Job{})
	db.AutoMigrate(&ArchivedInstance{})
	db.AutoMigrate(&Instance{})
	db.AutoMigrate(&Node{})
}

// An Artifact mocks result of the simulated jobs.
type Artifact struct {
	ID      string `gorm:"primary_key"`
	Shard   int    `gorm:"index:idx_shard_index_id"`
	IndexID string `gorm:"index:idx_shard_index_id"` // HACK to get 2-d index
}

// A Job provides a specification of a recurring job to run.
type Job struct {
	ID                string `gorm:"primary_key"`
	CurrentInstanceID int64
	PeriodMillis      int64
	JobConfig         string
}

// An InstanceSpecifier provides a common interface for Instance
// and ArchivedInstance for code reuse in client code that only
// depends on the ability to get a job id and an instance id.
type InstanceSpecifier interface {
	// JobAndInstance returns the job id as a string and the instance id as an
	// int64.
	JobAndInstance() (string, int64)
}

// An Instance represents one particular instance of a recurring Job.
type Instance struct {
	JobID          string `gorm:"primary_key"`
	InstanceID     int64  `gorm:"primary_key"`
	Shard          int    `gorm:"index:idx_shard_start"`
	Status         string `sql:"not null"`
	StartTime      int64  `gorm:"index:idx_shard_start"`
	EndTime        int64
	NodeID         *string
	SequenceNumber int64
}

var _ InstanceSpecifier = (*Instance)(nil)

// JobAndInstance returns the job id as a string and the instance id as an
// int64.
func (ji *Instance) JobAndInstance() (string, int64) {
	return ji.JobID, ji.InstanceID
}

// TableName provides a custom table name to gorm.
func (ji *Instance) TableName() string {
	return "job_instance"
}

// An ArchivedInstance represents one particular instance of a recurring Job
// that has terminated.
type ArchivedInstance struct {
	JobID          string `gorm:"primary_key"`
	InstanceID     int64  `gorm:"primary_key"`
	Shard          int    `gorm:"index:idx_shard_start"`
	Status         string `sql:"not null"`
	StartTime      int64  `gorm:"index:idx_shard_start"`
	EndTime        int64
	NodeID         *string
	SequenceNumber int64
}

var _ InstanceSpecifier = (*ArchivedInstance)(nil)

// JobAndInstance returns the job id as a string and the instance id as an
// int64.
func (ai *ArchivedInstance) JobAndInstance() (string, int64) {
	return ai.JobID, ai.InstanceID
}

// TableName provides a custom table name to gorm.
func (ai *ArchivedInstance) TableName() string {
	return "archived_job_instance"
}

// A Node represents a node that runs a job.
type Node struct {
	ID             string `gorm:"primary_key"`
	LastUpdateTime int64
}
