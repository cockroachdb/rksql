package sqload

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"rubrik/util/cqlutil"
	"rubrik/util/crdbutil"
	"rubrik/util/log"
)

func printUsage() {
	fmt.Print(`
Load generator for SQL / CQL.

Usage:
  sqload [flags] command

Available Commands:
  cockroach      Performs load test on CockroachDB
  cassandra      Performs load test on Cassandra
  cqlproxy       Performs load test on CQLProxy

Flags:
`)
	flag.PrintDefaults()
}

type sqloadOptions struct {
	wlFile         string
	genFile        string
	collectStats   bool
	testDur        time.Duration
	rolloverThresh uint64
	graphiteAddr   string
	statsFlushItvl time.Duration
	loaderID       int
	repeat         bool
}

func parseSQLoadOptions(f *flag.FlagSet) *sqloadOptions {
	o := new(sqloadOptions)
	f.StringVar(
		&o.wlFile,
		"work",
		"./workload.json",
		"Workload definition (in json format)",
	)

	f.StringVar(
		&o.genFile,
		"generators",
		"./generators.json",
		"generator definitions (in json format)",
	)

	f.BoolVar(
		&o.collectStats,
		"collect_stats",
		true,
		"Whether to publish stats to graphite",
	)

	f.DurationVar(
		&o.testDur,
		"test_tm",
		30*time.Second,
		"Test duration",
	)

	f.Uint64Var(
		&o.rolloverThresh,
		"rollover_thresh",
		math.MaxUint64,
		"Used by generator based workloads to roll over row generation",
	)

	f.BoolVar(
		&o.repeat,
		"repeat",
		false,
		"Whether to repeat the workload once it executes rollover_thresh "+
			"number of times",
	)

	f.StringVar(
		&o.graphiteAddr,
		"graphite_addr",
		"127.0.0.1:2003",
		"Graphite endpoint (in host:port format)",
	)

	f.DurationVar(
		&o.statsFlushItvl,
		"stats_flush_interval",
		10*time.Second,
		"Stats publishing interval",
	)

	f.IntVar(
		&o.loaderID,
		"loader_id",
		0,
		"Loader ID is used to seed random number generators. Caller must ensure that each"+
			" loader in a load test run has different loader id",
	)

	return o
}

func statsRec(
	graphite string,
	loaderID int,
	statsFlushItvl time.Duration,
	hostname string,
) (StatsRecorder, error) {
	return NewGraphiteStatsRecorder(
		graphite,
		statsFlushItvl,
		fmt.Sprintf(
			"%s.%d",
			strings.Replace(hostname, ".", "_", -1),
			loaderID,
		),
	)
}

// Main function for running load generator
func Main() {
	defer log.Flush()
	ctx := log.WithLogTag(context.Background(), "main", nil)
	cockroachCommand := flag.NewFlagSet("cockroach", flag.ExitOnError)
	cassandraCommand := flag.NewFlagSet("cassandra", flag.ExitOnError)

	flag.Usage = printUsage
	flag.Parse()
	if len(flag.Args()) == 0 {
		printUsage()
		os.Exit(1)
	}

	var loadOpts *sqloadOptions
	var qe QueryExecutor
	var err error
	dbType := flag.Arg(0)
	switch dbType {
	case "cockroach":
		roachDbOpts := crdbutil.DeclareCockroachDbURLFlags(cockroachCommand)
		loadOpts = parseSQLoadOptions(cockroachCommand)
		cockroachCommand.Parse(flag.Args()[1:])
		url, err := roachDbOpts.URL()
		if err != nil {
			log.Fatal(ctx, err)
		}
		qe, err = NewCockroachQueryExecutor(url.String())
		Check(err)
	case "cassandra", "cqlproxy":
		cassOpts := cqlutil.DeclareCassandraConnFlags(cassandraCommand)
		loadOpts = parseSQLoadOptions(cassandraCommand)
		cassandraCommand.Parse(flag.Args()[1:])
		qe, err = NewCassandraQueryExecutor(*cassOpts)
		Check(err)
	default:
		log.Fatalf(ctx, "unknown database type %s", dbType)
	}

	if _, err := os.Stat(loadOpts.wlFile); os.IsNotExist(err) {
		log.Fatalf(ctx, "workload file '%s' doesn't exist", loadOpts.wlFile)
	}

	wlConfData, err := ioutil.ReadFile(loadOpts.wlFile)
	Check(err)

	var generators generators

	if _, err := os.Stat(loadOpts.genFile); os.IsNotExist(err) {
		log.Warningf(ctx, "generators file '%s' doesn't exist", loadOpts.genFile)
	} else {
		genConfData, err := ioutil.ReadFile(loadOpts.genFile)
		Check(err)
		generators, err = parseGenerators(genConfData)
		Check(err)
	}

	work, err := ParseWorkLoadWithLoadOpts(
		ctx,
		wlConfData,
		generators,
		loadOpts,
	)
	Check(err)

	var s StatsRecorder
	if loadOpts.collectStats {
		hostname, err := os.Hostname()
		Check(err)
		s, err = statsRec(
			loadOpts.graphiteAddr,
			loadOpts.loaderID,
			loadOpts.statsFlushItvl,
			hostname)
		Check(err)
	} else {
		s = NewNoOpStatsRecorder()
	}
	defer s.Flush()

	err = LoadUp(ctx, qe, work, s, loadOpts.testDur)
	Check(err)

	log.Info(ctx, "Load-run completed successfully!")
	log.Info(ctx, s.Summary())
}
