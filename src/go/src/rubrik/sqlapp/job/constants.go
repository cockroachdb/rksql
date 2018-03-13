package job

const (
	// PprofPort is the listener port for pprof server for jobworker
	PprofPort = 9998
	// CockroachSocketAddrsCSV lists cockroach node socket addresses to use.
	// For example, "<ip1>:<port1>,<ip2>:<port2>,<ip3>:<port3>".
	CockroachSocketAddrsCSV = "cockroach_socket_addresses_csv"
	// UseLocalCockroach sets each tester to use localhost for cockroach
	// at the default port.
	UseLocalCockroach = "use_local_cockroach"
	// NumJobsPerWorker is the flag for number of jobs each worker performs
	NumJobsPerWorker = "num_jobs_per_worker"
	// JobPeriodScaleMillis is the flag for the scale for the period of the jobs
	JobPeriodScaleMillis = "job_period_scale_millis"
)
