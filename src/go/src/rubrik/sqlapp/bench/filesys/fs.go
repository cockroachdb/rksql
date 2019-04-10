package filesys

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	metrics "github.com/rcrowley/go-metrics"

	"rubrik/sqlapp/bench/filesys/cass"
	"rubrik/sqlapp/bench/filesys/roach"
	"rubrik/util/randutil"
)

var perStripeDelayMillis = flag.Int(
	"per_stripe_delay_ms",
	0,
	"Per stripe read/write delay.",
)

// Driver is an interface that a filesystem metadata engine can implement.
type Driver interface {
	Lookup(uuid string, stripeID int) error
	Persist(uuid string, stripeID int, metadata string) error
	Delete(uuid string) error
	PrintStats(elapsed time.Duration)
}

// FS is a simulated filesystem that supports basic read/write methods to
// generate metadata workload.
type FS struct {
	createdAt    time.Time
	driver       Driver
	persistTimer metrics.Timer
	lookupTimer  metrics.Timer
}

type chunk struct {
	State   int
	Index   int
	Checked bool
	UUID    string
	Node    string
	Disk    string
	Path    string
}

type stripe struct {
	Chunks []chunk
	Crcs   []int64
	State  int
}

func jsonStripe(uuid string, stripeID int) string {
	s := stripe{State: 2}
	for i := 0; i < 6; i++ {
		c := chunk{
			State:   1,
			Index:   i,
			Checked: true,
			UUID:    uuid,
			Node:    "local",
			Disk:    "sdx",
			Path:    uuid + "_" + fmt.Sprint(stripeID) + "_" + string(randutil.RandBytes(16)),
		}
		s.Chunks = append(s.Chunks, c)
		s.Crcs = append(s.Crcs, rand.Int63())
	}
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// New returns a new filesystem with a driver initialized based on driverName.
// drop is passed on to the driver on initialization, drivers typically will use
// to determine whether existing tables should be dropped upon initialization.
func New(driverName string, drop bool) (*FS, error) {
	var d Driver
	var err error
	switch driverName {
	case "roach":
		d, err = roach.New(drop)
	case "cass":
		d, err = cass.New(drop)
	default:
		return nil, errors.Errorf("Unknown driver %s", driverName)
	}
	if err != nil {
		return nil, err
	}
	return &FS{
		createdAt:    time.Now(),
		driver:       d,
		persistTimer: metrics.NewTimer(),
		lookupTimer:  metrics.NewTimer(),
	}, nil
}

func (fs *FS) persist(uuid string, stripeID int, metadata string) error {
	start := time.Now()
	defer func() {
		fs.persistTimer.Update(time.Since(start))
	}()
	return fs.driver.Persist(uuid, stripeID, metadata)
}

// Write file filename with size number of stripes.
func (fs *FS) Write(filename string, size int, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := fs.persist(filename, -1, ""); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < size; i++ {
		if err := fs.persist(filename, i, jsonStripe(filename, i)); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Duration(*perStripeDelayMillis) * time.Millisecond)
	}
}

func (fs *FS) lookup(uuid string, stripeID int) error {
	start := time.Now()
	defer func() {
		fs.lookupTimer.Update(time.Since(start))
	}()
	return fs.driver.Lookup(uuid, stripeID)
}

// Read file filename sequentially up to size number of stripes.
func (fs *FS) Read(filename string, size int, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := fs.lookup(filename, -1); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < size; i++ {
		if err := fs.lookup(filename, i); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Duration(*perStripeDelayMillis) * time.Millisecond)
	}
}

// Delete file filename along with all its stripes.
func (fs *FS) Delete(filename string) {
	if err := fs.driver.Delete(filename); err != nil {
		log.Fatal(err)
	}
}

// PrintStats prints statistics collected during filesystem workload execution.
func (fs *FS) PrintStats() {
	persistTimerSnapshot := fs.persistTimer.Snapshot()
	lookupTimerSnapshot := fs.lookupTimer.Snapshot()
	elapsed := time.Since(fs.createdAt)
	fmt.Println("\n_elapsed_op_______________ops_______ops/sec(cum)__pMean(ms)__p50(ms)__p95(ms)__p99(ms)")
	fmt.Printf("%7.1fs %s %13d %14.1f %8.1f %8.1f %8.1f %8.1f\n",
		elapsed.Seconds(),
		"lookup",
		lookupTimerSnapshot.Count(),
		lookupTimerSnapshot.RateMean(),
		lookupTimerSnapshot.Mean()/1e6,
		lookupTimerSnapshot.Percentile(0.5)/1e6,
		lookupTimerSnapshot.Percentile(0.95)/1e6,
		lookupTimerSnapshot.Percentile(0.99)/1e6,
	)
	fmt.Printf("%7.1fs %s %12d %14.1f %8.1f %8.1f %8.1f %8.1f\n",
		elapsed.Seconds(),
		"persist",
		persistTimerSnapshot.Count(),
		persistTimerSnapshot.RateMean(),
		persistTimerSnapshot.Mean()/1e6,
		persistTimerSnapshot.Percentile(0.5)/1e6,
		persistTimerSnapshot.Percentile(0.95)/1e6,
		persistTimerSnapshot.Percentile(0.99)/1e6,
	)
	fs.driver.PrintStats(elapsed)
}
