package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"rubrik/sqlapp"

	_ "github.com/lib/pq"

	"net/http"
	_ "net/http/pprof"

	"rubrik/sqlapp/job"
	"rubrik/util/crdbutil"
	"rubrik/util/log"
)

const (
	defaultID = -1

	filesystemSimulator  = "fs"
	jflSimulator         = "job"
	distributedSemaphore = "ds"
)

/*
  The test coordinator invokes the test executable based on the "tests" arg specified.
  Tests are responsible for running transaction workload & relevant consistency checks.
  Tests are registered in testMap.
  - exits with success if no inconsistencies were found after test has run for specified duration
  - exits with an error code if an inconsistency is detected or other error occurred
*/

type testResult struct {
	testName string
	err      error
}

type basicArgs struct {
	workerIndex             int
	numWorkers              int
	cockroachIPAddressesCSV string
	durationSecs            int
	installSchema           bool
	certsDir                string
	insecure                bool
}

// A CmdFactory represents a way of transforming a set of basic program
// arguments // to a command to execute a specific binary that may have a
// slightly different set of arguments.
type CmdFactory func(context.Context, string, basicArgs) *exec.Cmd

func getDefaultArgs(args basicArgs) []string {
	ret :=
		[]string{
			fmt.Sprintf("--%s=%d", sqlapp.DurationSecs, args.durationSecs),
			fmt.Sprintf("--%s=%v", sqlapp.InstallSchema, args.installSchema),
			fmt.Sprintf("--%s=%d", sqlapp.WorkerIndex, args.workerIndex),
			fmt.Sprintf("--%s=%s", sqlapp.CertsDir, args.certsDir),
			fmt.Sprintf("--%s=%v", sqlapp.Insecure, args.insecure),
		}
	return ret
}

func appendCockroachIPAddrFlag(
	workerArgs []string,
	ipAddrsCSV string,
) []string {
	return append(
		workerArgs,
		fmt.Sprintf("--%s=%s", sqlapp.CockroachIPAddressesCSV, ipAddrsCSV),
	)
}

func filesystemCmd(ctx context.Context, currDir string, args basicArgs) *exec.Cmd {
	testExe := path.Join(currDir, "filesystem_simulator")
	a := getDefaultArgs(args)
	a = appendCockroachIPAddrFlag(a, args.cockroachIPAddressesCSV)
	return exec.CommandContext(
		ctx,
		testExe,
		a...,
	)
}

func distributedSemaphoreCmd(ctx context.Context, currDir string, args basicArgs) *exec.Cmd {
	testExe := path.Join(currDir, "distributed_semaphore")
	a := getDefaultArgs(args)
	a = appendCockroachIPAddrFlag(a, args.cockroachIPAddressesCSV)
	return exec.CommandContext(
		ctx,
		testExe,
		a...,
	)
}

func jobworkerCmd(ctx context.Context, currDir string, args basicArgs) *exec.Cmd {
	testExe := path.Join(currDir, "jobworker")

	// add port to compute socket addresses
	socketAddrs := strings.Split(args.cockroachIPAddressesCSV, ",")
	for i, ip := range socketAddrs {
		socketAddrs[i] = fmt.Sprintf("%v:%v", ip, crdbutil.DefaultPort)
	}
	csv := strings.Join(socketAddrs, ",")

	a := getDefaultArgs(args)
	a = append(a, fmt.Sprintf("--%s=%v", job.UseLocalCockroach, false))
	a = append(a, fmt.Sprintf("--%s=%s", job.CockroachSocketAddrsCSV, csv))
	a = append(a, fmt.Sprintf("--%s=%d", sqlapp.NumWorkers, args.numWorkers))
	a = append(a, fmt.Sprintf("--%s=10", job.NumJobsPerWorker))
	a = append(a, fmt.Sprintf("--%s=1000", job.JobPeriodScaleMillis))
	return exec.CommandContext(
		ctx,
		testExe,
		a...,
	)
}

//stores mapping from <testShortName> to <testExe>
var testMap = map[string]CmdFactory{
	filesystemSimulator:  filesystemCmd,
	jflSimulator:         jobworkerCmd,
	distributedSemaphore: distributedSemaphoreCmd,
}

func validateTests(ctx context.Context, tests []string) bool {
	for _, testName := range tests {
		if _, ok := testMap[testName]; !ok {
			log.Infof(ctx, "test %s is not valid", testName)
			return false
		}
	}
	return true
}

func runTest(
	testName string,
	testCmdFactory CmdFactory,
	args basicArgs,
	wg *sync.WaitGroup,
	results chan testResult,
) {
	defer wg.Done()

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			time.Duration(args.durationSecs)*time.Second+(5*time.Minute),
		)

	defer cancel()

	currExe, err := os.Executable()
	if err != nil {
		log.Fatalf(ctx, "Error finding exe path: %v", err)
	}

	currDir, err := filepath.Abs(filepath.Dir(currExe))
	if err != nil {
		log.Fatalf(ctx, "Error creating absolute dir path for exe: %v", err)
	}
	cmd := testCmdFactory(ctx, currDir, args)

	log.Info(ctx, "Running command: ", cmd.Args)
	outBuff, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf(ctx, testName, " failed: ", err)
		log.Errorf(ctx, string(err.(*exec.ExitError).Stderr))
	}
	log.Info(ctx, "****** Begin Test Output ******")
	log.Info(ctx, string(outBuff))
	log.Info(ctx, "****** End Test Output ******")

	if ctx.Err() == context.DeadlineExceeded {
		log.Fatalf(ctx, "Deadline exceeded: %s, %v", testName, err)
	}

	results <- testResult{testName, err}
}

func analyzeTestResults(ctx context.Context, results chan testResult) int {
	numFailures := 0
	log.Info(
		ctx,
		"Analysing test results. Please bear in mind that "+
			"log.Fatal() causes process to exit with status 255",
	)
	for result := range results {
		if result.err != nil {
			numFailures++
			log.Errorf(ctx, result.err.Error())
		}
	}

	log.Info(ctx, "Number of tests that failed: ", numFailures)
	return numFailures
}

func main() {
	ctx := context.Background()
	go func() {
		hostPort := fmt.Sprintf("localhost:%d", sqlapp.PprofPort)
		log.Info(ctx, http.ListenAndServe(hostPort, nil))
	}()

	tests := flag.String("tests", "", "Comma-separated list of tests (eg: file)")
	certsDir := flag.String(sqlapp.CertsDir, "", "Directory for certificates if secure mode")
	insecure := flag.Bool(sqlapp.Insecure, true, "Connect to CockroachDB in insecure mode")
	cockroachIPAddressesCSV :=
		flag.String(
			sqlapp.CockroachIPAddressesCSV,
			"localhost",
			"Comma-separated list of CockroachDb nodes' IP addresses",
		)
	id := flag.Int(sqlapp.WorkerIndex, defaultID, "Identifier for this process")
	numTesters := flag.Int(sqlapp.NumTesters, 1, "Number of test executables across the cluster")
	durationSecs := flag.Int(sqlapp.DurationSecs, 10, "How long to run, in seconds")
	installSchema := flag.Bool(
		sqlapp.InstallSchema,
		false,
		"Install schemas for tests instead of running them.")
	flag.Parse()
	defer log.Flush()

	if len(*cockroachIPAddressesCSV) == 0 {
		log.Fatalf(ctx, "cockroach_ip_addresses_csv is empty: %s", *cockroachIPAddressesCSV)
	} else if *id == defaultID {
		log.Fatalf(ctx, "invalid %s specified: %d", sqlapp.WorkerIndex, *id)
	} else if *numTesters < 1 {
		log.Fatalf(ctx, "num_testers should be positive: %d", *numTesters)
	} else if *durationSecs < 1 {
		log.Fatalf(ctx, "duration should be positive: %d", *durationSecs)
	} else if len(*tests) == 0 {
		log.Fatalf(ctx, "tests is empty: %s", *tests)
	}

	testList := strings.Split(*tests, ",")
	if ok := validateTests(ctx, testList); !ok {
		log.Fatal(ctx, "Invalid test list: ", *tests)
	}

	numTests := len(testList)
	var wg sync.WaitGroup
	results := make(chan testResult, numTests)

	for _, testName := range testList {
		cmdFactory, _ := testMap[testName]
		wg.Add(1)
		args :=
			basicArgs{
				*id,
				*numTesters,
				*cockroachIPAddressesCSV,
				*durationSecs,
				*installSchema,
				*certsDir,
				*insecure,
			}
		go runTest(
			testName,
			cmdFactory,
			args,
			&wg,
			results,
		)
	}
	log.Info(ctx, "Started tests")

	wg.Wait()
	close(results)

	if numFailures := analyzeTestResults(ctx, results); numFailures != 0 {
		log.Fatalf(ctx, "%d of %d tests failed", numFailures, numTests)
	}
	log.Infof(ctx, "All %d tests passed", numTests)
}
