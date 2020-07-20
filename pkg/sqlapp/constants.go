package sqlapp

const (
	// CockroachIPAddressesCSV is the flag for cockroach node ips
	CockroachIPAddressesCSV = "cockroach_ip_addresses_csv"
	// DurationSecs is the flag for the duration a test should run
	DurationSecs = "duration_secs"
	// InstallSchema is the flag for log for whether to create db / tables
	InstallSchema = "install_schema"
	// WorkerIndex is the flag for worker id
	WorkerIndex = "worker_index"
	// CertsDir is the flag for location of cockroach certificates
	CertsDir = "certs_dir"
	// Insecure is the flag for whether cockroachDB is running in insecure mode
	Insecure = "insecure"
	// NumWorkers is the flag for number of workers
	NumWorkers = "num_workers"
)
