package sqload

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	graphite "github.com/cyberdelia/go-metrics-graphite"
	stats "github.com/rcrowley/go-metrics"

	"rubrik/util/log"
)

//StatsCtx abstracts ability to post performance mesurements by context
type StatsCtx interface {
	//ReportInt reports int as counter/guages
	ReportInt(name string, val int)
	//ReportStr reports errors
	ReportStr(name string, val string)
	//Stop stops stats-ctx
	Stop()
}

//StatsRecorder provides ability to report performance stats
//  in a context aware way
type StatsRecorder interface {
	//Initialize for an ID. It is not necessary to initialize an ID.
	//Initialize can be used to create new counters and flush them
	//to the appropriate endpoint. With this 0 counts can be flushed
	Initialize(id string)
	//Start a new stats-ctx
	Start(id string) StatsCtx
	//Flush stats so that they are persisted
	Flush()
	//Summary returns a summary of the metrics
	Summary() string
}

type intCtr struct {
	sum     int64
	sumOfSq int64
	count   int
}

type idCtr struct {
	//key: event-name (eg. row_count_diff, difference between expected and actual)
	//value: raw data to help calculate mean and variance / sd
	eventCounter map[string]*intCtr
	//L1 key: event-name (eg. txn_failure)
	//L2 key: error-message, err.Error()
	//value: count
	eventMsgCounter map[string]map[string]int
	//latency mean, variance / sd of reporting operation
	latency intCtr
}

//countingStatsRecorder maintains stats locally and flushes it on demand
// this is a toy, it won't be used in production, we'll use something that
// will not only aggregate this data, but also post to some
// cluster-level-aggregator and also with much better quality aggregations
type countingStatsRecorder struct {
	//key: work name
	//value: errors, events distribution + latency
	idCtrs map[string]*idCtr
	mut    sync.Mutex
}

//countingStatsCtx is context for counting-stats-recorder
type countingStatsCtx struct {
	r       *countingStatsRecorder
	startTm time.Time
	id      string
}

func (c *intCtr) add(v int64) {
	c.sum += v
	c.sumOfSq += v * v
	c.count++
}

type operation func()

//NewCountingStatsRecorder creates counting-stats-recorder
func NewCountingStatsRecorder() StatsRecorder {
	var csr countingStatsRecorder
	csr.idCtrs = make(map[string]*idCtr)
	return &csr
}

func (r *countingStatsRecorder) do(op operation) {
	r.mut.Lock()
	defer r.mut.Unlock()
	op()
}

//Summary returns a summary of the metrics
func (r *countingStatsRecorder) Summary() string {
	return ""
}

func (r *countingStatsRecorder) Flush() {
}

//Start the counting stats context
func (r *countingStatsRecorder) Start(id string) StatsCtx {
	r.do(func() {
		if _, ok := r.idCtrs[id]; !ok {
			ctr := new(idCtr)
			r.idCtrs[id] = ctr
			ctr.eventCounter = make(map[string]*intCtr)
			ctr.eventMsgCounter = make(map[string]map[string]int)
		}
	})
	return &countingStatsCtx{r, time.Now(), id}
}

//Initialize the counting stats context
func (r *countingStatsRecorder) Initialize(id string) {
}

//Stop counting-stats ctx
func (c *countingStatsCtx) Stop() {
	c.r.do(func() {
		deltaNs := time.Since(c.startTm).Nanoseconds()
		ctr := c.r.idCtrs[c.id]
		ctr.latency.add(deltaNs)
	})
}

//ReportInt report integer in counting stats ctx
func (c *countingStatsCtx) ReportInt(name string, val int) {
	c.r.do(func() {
		ctr := c.r.idCtrs[c.id]
		subCtr, ok := ctr.eventCounter[name]
		if !ok {
			subCtr = new(intCtr)
			ctr.eventCounter[name] = subCtr
		}
		subCtr.add(int64(val))
	})
}

//ReportStr report string in counting stats ctx
func (c *countingStatsCtx) ReportStr(name string, val string) {
	c.r.do(func() {
		ctr := c.r.idCtrs[c.id]
		if _, ok := ctr.eventMsgCounter[name]; !ok {
			ctr.eventMsgCounter[name] = make(map[string]int)
		}
		ctr.eventMsgCounter[name][val]++
	})
}

type graphiteStatsRecorder struct {
	reg    stats.Registry
	cfg    graphite.Config
	tickCh <-chan time.Time
}

//graphiteStatsCtx is stats context for graphite-recorder
type graphiteStatsCtx struct {
	r       *graphiteStatsRecorder
	startTm time.Time
	id      string
}

func relayToGraphite(r *graphiteStatsRecorder) {
	for _ = range r.tickCh {
		r.Push()
	}
}

//NewGraphiteStatsRecorder creates a recorder that pushes data to graphite
func NewGraphiteStatsRecorder(
	hostPort string,
	flushInterval time.Duration,
	senderID string,
) (
	StatsRecorder, error,
) {
	return newGraphiteStatsRecorderWithExternalTicker(
		hostPort,
		flushInterval,
		senderID,
		time.Tick(flushInterval))
}

func newGraphiteStatsRecorderWithExternalTicker(
	hostPort string,
	flushInterval time.Duration,
	senderID string,
	tickCh <-chan time.Time,
) (
	StatsRecorder, error,
) {
	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}
	var r graphiteStatsRecorder
	r.reg = stats.NewRegistry()
	r.cfg.Prefix = fmt.Sprintf("sqload.%s", senderID)
	r.cfg.Addr = addr
	r.cfg.Registry = r.reg
	r.cfg.DurationUnit = time.Nanosecond
	r.cfg.FlushInterval = flushInterval
	r.cfg.Percentiles = []float64{0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.999}
	r.tickCh = tickCh
	go relayToGraphite(&r)
	return &r, nil
}

func (r *graphiteStatsRecorder) Push() {
	if err := graphite.Once(r.cfg); err != nil {
		log.Errorf(
			context.Background(),
			"Couldn't push stats to graphite, error: %s",
			err,
		)
	}
}

func (r *graphiteStatsRecorder) Flush() {
	r.Push()
}

//Start the graphite stats context
func (r *graphiteStatsRecorder) Start(id string) StatsCtx {
	return &graphiteStatsCtx{r, time.Now(), id}
}

//Summary returns a summary of the metrics
func (r *graphiteStatsRecorder) Summary() string {
	var b bytes.Buffer
	b.WriteString("Stats summary:\n")

	r.reg.Each(func(name string, s interface{}) {
		switch s := s.(type) {
		case stats.Timer:
			b.WriteString(
				fmt.Sprintf(
					"%s 90 Pctile Latency: %f\n",
					name,
					s.Percentile(90)/float64(time.Millisecond),
				),
			)
		}
	})

	return b.String()
}

//Initialize stats recorder for an ID
func (r *graphiteStatsRecorder) Initialize(id string) {
	stats.NewRegisteredTimer(id, r.reg)
	ctx := r.Start(id)
	ctx.ReportInt(rowCountMetric, 0)
	ctx.ReportInt(rowDiffMetric, 0)
}

//Stop graphite-stats ctx
func (c *graphiteStatsCtx) Stop() {
	timeTaken := time.Since(c.startTm)
	t := stats.GetOrRegisterTimer(c.id, c.r.reg)
	t.Update(timeTaken)
}

//ReportInt report integer in graphite stats ctx
func (c *graphiteStatsCtx) ReportInt(name string, val int) {
	t := stats.GetOrRegisterCounter(
		strings.Join([]string{c.id, name}, "."),
		c.r.reg,
	)
	t.Inc(int64(val))
}

//ReportStr report string in graphite stats ctx
func (c *graphiteStatsCtx) ReportStr(name string, val string) {
	val = strings.Replace(val, "_", "__", -1)
	val = strings.Replace(val, " ", "_", -1)
	t := stats.GetOrRegisterCounter(
		strings.Join([]string{c.id, name, val}, "."),
		c.r.reg,
	)
	t.Inc(1)
}

type noOpStatsContext struct{}
type noOpStatsRecorder struct{}

//NewNoOpStatsRecorder creates a recorder that discards data (used for warmup)
func NewNoOpStatsRecorder() StatsRecorder {
	return &noOpStatsRecorder{}
}

//Start a context that discards all data
func (s *noOpStatsRecorder) Start(id string) StatsCtx {
	return &noOpStatsContext{}
}

//Summary returns a summary of the metrics
func (s *noOpStatsRecorder) Summary() string {
	return ""
}

//Initialize a context that discards all data
func (s *noOpStatsRecorder) Initialize(id string) {
}

func (s *noOpStatsRecorder) Flush() {
}

//ReportInt drops int val
func (c *noOpStatsContext) ReportInt(name string, val int) {}

//ReportStr drops string val
func (c *noOpStatsContext) ReportStr(name string, val string) {}

//Stop does nothing
func (c *noOpStatsContext) Stop() {}
