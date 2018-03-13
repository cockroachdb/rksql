package roach

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	// Import postgres driver
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	metrics "github.com/rcrowley/go-metrics"

	"rubrik/util/crdbutil"
)

var dbURLParams = crdbutil.DeclareCockroachDbURLFlags(flag.CommandLine)

// Driver implements filesys.Driver interface using cockroach.
type Driver struct {
	db                *sql.DB
	mainUpsertTimer   metrics.Timer
	staticUpsertTimer metrics.Timer
}

// New returns a new driver. It also installs necessary databases and tables.
// Existing databases/tables are dropped if drop is set.
func New(drop bool) (*Driver, error) {
	dbName := "bench_fs"
	if drop {
		crdbutil.DropAndCreateDatabase(
			dbURLParams.SocketAddr,
			dbName,
			dbURLParams.CertsDir,
			dbURLParams.Insecure,
		)
	} else {
		crdbutil.CreateDatabaseIfNotExists(
			dbURLParams.SocketAddr,
			dbName,
			dbURLParams.CertsDir,
			dbURLParams.Insecure,
		)
	}

	url, err := dbURLParams.URL()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", url.String())
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS bench_fs.file (
		uuid STRING,
		stripe_id INT,
		stripe_metadata STRING,
		PRIMARY KEY (uuid, stripe_id)
	)`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS bench_fs.file_static (
		uuid STRING,
		size INT,
		PRIMARY KEY (uuid)
	)`); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	return &Driver{
		db:                db,
		mainUpsertTimer:   metrics.NewTimer(),
		staticUpsertTimer: metrics.NewTimer(),
	}, nil
}

// Lookup looks up a stripe with key (uuid, stripeID).
func (d *Driver) Lookup(uuid string, stripeID int) error {
	rows, err := d.db.Query(`
	SELECT uuid, stripe_id, stripe_metadata, size FROM (
		SELECT uuid, stripe_id, stripe_metadata 
		FROM bench_fs.file 
		WHERE uuid=$1 AND stripe_id=$2
  ) AS a JOIN (
		SELECT uuid, size
		FROM bench_fs.file_static
		WHERE uuid=$1
	) AS b USING (uuid)`, uuid, stripeID)
	if err != nil {
		return err
	}
	defer rows.Close()
	row := make([]interface{}, 4)
	scanArgs := make([]interface{}, 4)
	for i := range scanArgs {
		scanArgs[i] = &row[i]
	}
	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		rowCount++
	}
	if rowCount != 1 {
		return errors.Errorf("No such stripe %s, %d", uuid, stripeID)
	}
	return nil
}

// Persist persists metadata for stripe (uuid, stripeID).
func (d *Driver) Persist(uuid string, stripeID int, metadata string) error {
	fn := func(tx *sql.Tx) error {
		start := time.Now()
		if _, err := tx.Exec(`
		UPSERT INTO bench_fs.file(uuid, stripe_id, stripe_metadata)
		VALUES ($1, $2, $3) RETURNING NOTHING`, uuid, stripeID, metadata); err != nil {
			return err
		}
		d.mainUpsertTimer.Update(time.Since(start))
		start = time.Now()
		if _, err := tx.Exec(`
		UPSERT INTO bench_fs.file_static(uuid, size)
		VALUES ($1, $2) RETURNING NOTHING`, uuid, stripeID+1); err != nil {
			return err
		}
		d.staticUpsertTimer.Update(time.Since(start))
		return nil
	}
	return crdb.ExecuteTx(context.TODO(), d.db, nil, fn)
}

// Delete deletes all stripes for uuid.
func (d *Driver) Delete(uuid string) error {
	fn := func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
		DELETE FROM bench_fs.file WHERE uuid=$1`, uuid); err != nil {
			return err
		}
		if _, err := tx.Exec(`
		DELETE FROM bench_fs.file_static WHERE uuid=$1`, uuid); err != nil {
			return err
		}
		return nil
	}
	return crdb.ExecuteTx(context.TODO(), d.db, nil, fn)
}

// PrintStats prints statistics collected by the driver.
func (d *Driver) PrintStats(elapsed time.Duration) {
	mainUpsertTimerSnapshot := d.mainUpsertTimer.Snapshot()
	staticUpsertTimerSnapshot := d.staticUpsertTimer.Snapshot()
	fmt.Printf("%7.1fs %s %8d %14.1f %8.1f %8.1f %8.1f %8.1f\n",
		elapsed.Seconds(),
		"main upsert",
		mainUpsertTimerSnapshot.Count(),
		mainUpsertTimerSnapshot.RateMean(),
		mainUpsertTimerSnapshot.Mean()/1e6,
		mainUpsertTimerSnapshot.Percentile(0.5)/1e6,
		mainUpsertTimerSnapshot.Percentile(0.95)/1e6,
		mainUpsertTimerSnapshot.Percentile(0.99)/1e6,
	)
	fmt.Printf("%7.1fs %s %6d %14.1f %8.1f %8.1f %8.1f %8.1f\n",
		elapsed.Seconds(),
		"static upsert",
		staticUpsertTimerSnapshot.Count(),
		staticUpsertTimerSnapshot.RateMean(),
		staticUpsertTimerSnapshot.Mean()/1e6,
		staticUpsertTimerSnapshot.Percentile(0.5)/1e6,
		staticUpsertTimerSnapshot.Percentile(0.95)/1e6,
		staticUpsertTimerSnapshot.Percentile(0.99)/1e6,
	)
}
