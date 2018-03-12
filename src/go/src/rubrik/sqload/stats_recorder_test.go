package sqload

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	graphite "github.com/cyberdelia/go-metrics-graphite"
	stats "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
)

func TestCountingStatsNamedCounterReporting(t *testing.T) {
	assert := assert.New(t)
	rec := NewCountingStatsRecorder()
	var err error
	for i := 0; i < 3; i++ {
		func() {
			statsCtx := rec.Start("foo_bar")
			statsCtx.ReportInt("err_baz", 1)
			statsCtx.ReportInt("err_quux", 3)
			defer statsCtx.Stop()
		}()
	}

	assert.Nil(err)
	stats := rec.(*countingStatsRecorder).idCtrs["foo_bar"]
	assert.Equal(3, stats.eventCounter["err_baz"].count)
	assert.Equal(int64(3), stats.eventCounter["err_baz"].sum)
	assert.Equal(3, stats.eventCounter["err_quux"].count)
	assert.Equal(int64(9), stats.eventCounter["err_quux"].sum)
}

func TestCountingStatsNamedStringHistogramReporting(t *testing.T) {
	assert := assert.New(t)
	rec := NewCountingStatsRecorder()
	var err error
	for i := 0; i < 3; i++ {
		func() {
			statsCtx := rec.Start("foo_bar")
			statsCtx.ReportStr("err_baz", "corge happened")
			statsCtx.ReportStr("err_baz", "grault happened")
			statsCtx.ReportStr("err_baz", "corge happened")
			statsCtx.ReportStr("err_quux", "grault happened")
			defer statsCtx.Stop()
		}()
	}

	assert.Nil(err)
	stats := rec.(*countingStatsRecorder).idCtrs["foo_bar"]

	baz := stats.eventMsgCounter["err_baz"]
	assert.Equal(2, len(baz))
	assert.Equal(6, baz["corge happened"])
	assert.Equal(3, baz["grault happened"])
	quux := stats.eventMsgCounter["err_quux"]
	assert.Equal(1, len(quux))
	assert.Equal(3, quux["grault happened"])
}

//NewTestGraphiteServer is a copy of `func NewTestServer...`
// that has the same signature. This had to be done because
// graphite support library doesn't export test-server utility.
// TODO: Eventually we should patch it and use the exported function
// once the patch is merged.
func NewTestGraphiteServer(
	t *testing.T,
	prefix string,
) (
	map[string]float64,
	net.Listener,
	stats.Registry,
	graphite.Config,
	*sync.WaitGroup,
) {
	res := make(map[string]float64)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue // the test failure will catch this
			}
			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')
			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseFloat(parts[1], 0)
				if testing.Verbose() {
					t.Log("recv", parts[0], i)
				}
				res[parts[0]] = res[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			wg.Done()
			conn.Close()
		}
	}()

	r := stats.NewRegistry()

	c := graphite.Config{
		Addr:          ln.Addr().(*net.TCPAddr),
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        prefix,
	}

	return res, ln, r, c, &wg
}

func newGraphiteStatsRecorderForTests(
	cfg graphite.Config,
	senderID string,
) (
	StatsRecorder,
	chan<- time.Time,
	error,
) {
	tickCh := make(chan time.Time)
	r, err := newGraphiteStatsRecorderWithExternalTicker(
		cfg.Addr.String(),
		cfg.FlushInterval,
		senderID,
		tickCh,
	)
	if err != nil {
		return nil, nil, err
	}
	return r, tickCh, nil
}

func TestGraphiteStatsNamedCounterReporting(t *testing.T) {
	assert := assert.New(t)
	res, l, _, c, wg := NewTestGraphiteServer(t, "sqload_test")
	defer l.Close()

	rec, tickCh, err := newGraphiteStatsRecorderForTests(c, "host1")
	defer close(tickCh)
	assert.Nil(err)

	for i := 0; i < 3; i++ {
		func() {
			statsCtx := rec.Start("foo_bar")
			defer statsCtx.Stop()
			statsCtx.ReportInt("err_baz", 1)
			statsCtx.ReportInt("err_quux", 3)
		}()
	}
	wg.Add(1)
	tickCh <- time.Now()
	wg.Wait()

	assert.Equal(float64(3), res["sqload.host1.foo_bar.err_baz.count"])
	assert.Equal(float64(9), res["sqload.host1.foo_bar.err_quux.count"])
}

func TestGraphiteStatsNamedStringHistogramReporting(t *testing.T) {
	assert := assert.New(t)
	res, l, _, c, wg := NewTestGraphiteServer(t, "sqload_test")
	defer l.Close()

	rec, tickCh, err := newGraphiteStatsRecorderForTests(c, "node2")
	defer close(tickCh)
	assert.Nil(err)

	for i := 0; i < 3; i++ {
		func() {
			statsCtx := rec.Start("foo_bar")
			defer statsCtx.Stop()
			statsCtx.ReportStr("err_baz", "corge happened")
			statsCtx.ReportStr("err_baz", "grault happened")
			statsCtx.ReportStr("err_baz", "corge happened")
			statsCtx.ReportStr("err_quux", "grault_happened")
		}()
	}
	wg.Add(1)
	tickCh <- time.Now()
	wg.Wait()

	assert.Equal(float64(6),
		res["sqload.node2.foo_bar.err_baz.corge_happened.count"])
	assert.Equal(float64(3),
		res["sqload.node2.foo_bar.err_baz.grault_happened.count"])
	assert.Equal(float64(3),
		res["sqload.node2.foo_bar.err_quux.grault__happened.count"])
}
