package cass

import (
	"time"

	"github.com/kaavee315/gocql"
	"github.com/pkg/errors"
)

// Driver implements filesys.Driver interface using cassandra.
type Driver struct {
	db *gocql.Session
}

// New returns a new driver. It also installs necessary databases and tables.
// Existing databases/tables are dropped if drop is set.
func New(drop bool) (*Driver, error) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Consistency = gocql.One
	cluster.Timeout = 300 * time.Second
	cluster.ConnectTimeout = 300 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	if drop {
		if err := db.Query("DROP KEYSPACE IF EXISTS bench_fs").Exec(); err != nil {
			return nil, err
		}
	}
	if err := db.Query(`
	CREATE KEYSPACE IF NOT EXISTS bench_fs
	WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`).
		Exec(); err != nil {
		return nil, err
	}
	if err := db.Query(`
	CREATE TABLE IF NOT EXISTS bench_fs.file (
		uuid TEXT,
		stripe_id INT,
		stripe_metadata TEXT,
		size INT STATIC,
		linearizer INT STATIC,
		PRIMARY KEY (uuid, stripe_id)
	)`).Exec(); err != nil {
		return nil, err
	}
	return &Driver{db}, nil
}

// Lookup looks up a stripe with key (uuid, stripeID).
func (d *Driver) Lookup(uuid string, stripeID int) error {
	var x, y string
	var z, w int
	// returns ErrNotFound when the row does not exist
	return d.db.Query(`
	SELECT uuid, stripe_id, stripe_metadata, size
	FROM bench_fs.file WHERE uuid = ? AND stripe_id = ?`, uuid, stripeID).
		Scan(&x, &z, &y, &w)
}

// Persist persists metadata for stripe (uuid, stripeID).
func (d *Driver) Persist(uuid string, stripeID int, metadata string) error {
	dest := make(map[string]interface{})
	// linearizer = null condition exists only so that cassandra is forced to
	// invoke a lightweight transaction.
	applied, err := d.db.Query(`
	UPDATE bench_fs.file SET stripe_metadata = ?, size = ?
	WHERE uuid = ? AND stripe_id = ? IF linearizer = null`,
		metadata, stripeID+1, uuid, stripeID).MapScanCAS(dest)
	if err != nil {
		return err
	}
	if !applied {
		return errors.Errorf("update not applied %s, %d. conflict: %v", uuid, stripeID, dest)
	}
	return nil
}

// Delete deletes all stripes for uuid.
func (d *Driver) Delete(uuid string) error {
	return d.db.Query(`DELETE FROM bench_fs.file WHERE uuid = ?`, uuid).Exec()
}

// PrintStats prints statistics collected by the driver.
func (d *Driver) PrintStats(elapsed time.Duration) {
}
