# Consistency Tests

## Description 

These are a suite of applications (filesystem-simulator, job-worker and
distributed-semaphore) that are designed to be used to check the consistency of
a database. Each spawns several goroutines, or workers, which perform
operations. Another goroutine will continually be checking invariants to
capture any inconsistency, which could be caused by failure of isolation
between transactions. Multiple instances of each can run on the same cluster,
as each takes an index which it uses to partition the keyspace.

The coordinator starts each test program and communicates results back. The
test programs can also be run directly.

We've found these useful to run with a chaos testing framework. Examples
include: network partitions, node and service restarts, ENOSPACE, clock 
issues etc.

## Usage

You first need to install the schema. You can either do this directly for a
program: `filesystem-simulator --install_schema`, or via the
coordinator: `coordinator --install_schema --tests=fs`. 

Valid options for `--tests` are `fs`, `job` and `ds`.

If invoking the coordinator, it may appear to hang. This is because the logs
from the test program it spawns are only streamed when that process terminates.

To run the test: `coordinator --tests=fs` or directly using
`filesystem_simulator`.

For running on a secure cluster, provide the argument `--certs_dir` and `insecure=false`

### Supported flags to the coordinator

```
--certs_dir: location of certificates, if running in secure mode
--cockroach_ip_addresses_csv: comma-separated list of IP addresses of nodes
--duration_secs: how long to run the test for
--insecure: whether CockrochDB is being run in secure mode
--num_testers: number of coordinators you're using
--worker_index: int ID of this coordinator, starting from 0 and consecutive.
```

An example using all the parameters:

```
coordinator --cockroach_ip_addresses_csv=127.0.0.1,127.0.0.2 \
  --insecure=false \
  --certs_dir=/path/to/certs \
  --worker_index=0
  --tests=job
```
