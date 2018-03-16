# Sqload : sql load generator

## Purpose
Sqload was written to generate application-like
 load on cockraochdb cluster. The idea is to put cockroachdb through actual
 app-like access patterns and see if it causes any performance problems. It also
 supports running workloads on Cassandra.

Sqload has a declarative way of specifying desired load. Eg. one can configure
 it to run transactions that select a row, then use one of the columns as value
 of another column in an update query in the same transaction etc. All of this
 may be happening while other transactions, accessing completely unrelated
 tables or rows in the same table are running at desired concurrency and
 intervals.

It can be used to benchmark DB for a particular kind of query. Eg. one can
 configure a workload that executes only 1 transaction which does a CAS
 operation on a small table to check for lock-contention, but this is not a
 usecase sqload is designed for. Its incidental that its declarative workload
 spec allows it to do this.

## Generators: building corpus and perf-testing with it
Generators allow a workload to generate data for query params. But for this
  to be effective, both in-database corpus (existing data in the database) and
  queries that workload executes must use similar data (so all, or most
  transactions end up selecting and/or modifying data that actually exists,
  unless workload aims to test no-such-key scenario).

This is achieved through 'random_salt'. random_salt allows user to control
  the random-sequence at generator x loader level (where a loader is an
  instance of sqload with a specific loader_id). If all other params (eg.
  Cardinality, Mean etc) are the same, setting the same random_salt at
  generator level ensures exactly the same sequence of values will be generated
  for a particular generator x loader. Each workload will have a single source
  of randomness seeded with the loaderID (an instance of sqload). For every
  iteration of the workload, one random number is pulled out of the source, and
  that in combination with the random_salt for each generator determines the
  generated value. So for the same random source and same random salts, the
  sequence of generated param tuples will be the same. Random salts make sure
  that the sequence of values generated from each generator are not correlated.
  Without the random salt, two param generators of the same type would end up
  with the exact same sequence of values governed by the workload level source.
  random_salt is an optional field with a default value that aims to reach the
  highest level of randomness across all generators (a unique random seed is
  used for each generator)

When using generators in workloads, an offset can be added to the 'random_salt'
  of the generator by using it as <gen_id>:offset. This makes it easy to
  re-use the same generator with different salts.

## Supported generators
### Int Gaussian Generator
```json
{
    "name": "int_gaussian",
    "params": {
        "cardinality": 1000,
        "mean": 200,
        "sd": 3000,
        "random_salt": 5
    }
}
```

Generates Integers with in a Gaussian Distribution with a given Mean and
Standard Deviation

### Uniform Int Generator
```json
{
    "name": "int_uniform",
    "params": {
      "min": 1,
      "cardinality": 100,
      "max": 100,
      "random_salt": 5
    }
}
```
Generates integers with uniform distribution between min and max
(max not inclusive)

### Random String Generator
```json
{
    "name": "str_histo_prefix_len",
    "params": {
        "cardinality": 50000,
        "len": [{
            "min": 50,
            "max": 100,
            "pct": 33.3
        }, {
            "min": 101,
            "max": 200,
            "pct": 66.7
        }],
        "prefix": [{
            "str": "ftp://",
            "pct": 50.0
        }, {
            "str": "rsync://",
            "pct": 50.0
        }],
        "random_salt": 5
    }
}
```
Generates random strings of given length with certain prefix distribution
(prefixes are optional)

### Timestamp generator
```json
{
    "name": "timestamp_offset",
    "params": {
        "offset": "-5m",
        "max_deviation": "10s",
        "random_salt": 10
    }
}
```
Generate timestamps at a particular offset to current time. Generates timestamps
uniformly between (Now - Offset +- MaxDeviation)


## Usage help, examples
Example workloads and generators are given in the workload directory.
schema.sql specifies the SQL to create tables used in the workloads.

###Running sqload:
Sqload can be used to pre-populate data in a database and then run test
  workloads to measure performance. Stats can be emitted by sqload to a graphite
  endpoint.

Sqload can be run on each node of a distributed database concurrently by
  specifying a different loader_id for each instance. The actual rows generated
  by sqload is a combination of `random_salt` and `loader_id`.

Sqload can be used to populate rows in an empty table and then run workloads
  to measure performance of different statements.


Supported flags
```
--log-dir: path to output logs
--loader_id: Integer. Should be different for each instance of sqload
--collect_stats: True/False. Whether to emit stats to the graphite endpoint
--graphite_addr: host:port of the graphite endpoint
--test_tm: Duration. The duration to run the sqload. Sqload stops once it hits this timer or the number of executions reach rollover_thresh
--generators: path of generator json file
--work: path to workload json
--rollover_thresh: The number of executions after which to reset the rand generators. Once this is hit generated values start repeating
```
CockroachDB specific flags
```
--roach_db_name: database name of cockroachdb
--roach_certs_dir: certificats directory of cockroachdb
--roach_socket_read_timeout: socket read timeout
--roach_socket_write_timeout: socket write timeout
--roach_user: cockroachdb user to use
--insecure: whether to connect to CockroachDB in insecure mode
```
Cassandra specific flags
```
--cassandra_host: cassandra host
--cassandra_port: cassandra port
--cassandra_query_timeout: cassandra query timeout
--connection_timeout: connection timeout
```
The first argument of `sqload` should be specified as `cockroach` or `cassandra`.

Example, if you want to run an insert workload to populate 1000 rows and then
  run a select workload for 15 min and measure statistics:
1. Build the binary using BUILD.py
2. Create table using schema.sql `cockroach sql --insecure < src/go/src/rubrik/sqload/workloads/schema.sql`
3. Populate 1000 rows using
```
src/go/bin/sqload \
  cockroach \
  --collect_stats=False \
  --loader_id=0 \
  --repeat=false \
  --test_tm=1h \
  --generators=src/go/src/rubrik/sqload/workloads/generators.json \
  --rollover_thresh=1000 \
  --work=src/go/src/rubrik/sqload/workloads/micro_benchmark/populate.json \
  --roach_db_name=sqload \
  --insecure=true \
  --roach_user=root
```
4. Run a test workload for 15 minutes using. Since `repeat` is true, the
  selected rows will repeat after reaching `rollover_thresh` of 1000. So the
  rows being selected are guaranteed to exist because of the populate phase in
  step 3.
```
src/go/bin/sqload \
  cockroach \
  --collect_stats=True \
  --graphite_addr=localhost:2001 \
  --loader_id=0 \
  --repeat=true \
  --test_tm=15m \
  --generators=src/go/src/rubrik/sqload/workloads/generators.json \
  --rollover_thresh=1000 \
  --work=src/go/src/rubrik/sqload/workloads/micro_benchmark/test/select_all.json \
  --roach_db_name=sqload \
  --insecure=true \
  --roach_user=root
```

### Example insert workload
```
[{
    "name": "insert_workload",
    "transaction": [{
        "query": "INSERT INTO sqload.micro_benchmark
        (job_id, job_config, node_id) VALUES ($1, $2, $3)",
        "num_rows": 1
    }],
    "params": {
      "generators": [
        "int_job_id_0",
        "str_large_0",
        "int_small_0"
      ]
    },
    "concurrency": 10,
    "interval": "0s",
    "latency_thresh": "1s",
    "force_ignore_rollover": true
}]
```
If force_ignore_rollover is true, then the generated rows are not repeated after
  hitting rollover_thresh. This is useful in insert workloads so that rows with
  same primary keys are not inserted again.

###Example multi statement transaction workload

```
[{
  "name": "multi_query_txn_update_node_id",
  "transaction": [
    {
      "query": "SELECT node_id FROM sqload.multi_query_txn where job_id = $1",
      "result_name": "read_node_id",
      "num_rows": 1
    },
    {
      "query": "UPDATE sqload.multi_query_txn set node_id=$read_node_id:1:int + 10 where job_id = $1",
      "num_rows": 1,
      "result_name": "update_node_id"
    }],
  "params": {
    "generators": [
      "int_job_id_0",
    ]
  },
  "concurrency": 4,
  "interval": "1.2s"
}
```

### Generators
Generators specify how to fill in placeholders like $1, $2 ... used in the
  queries. The same generator always generates random values in the same order.
  This sequence changes for each node. All concurrent threads of a workload pick
  random values from the same source.

Update workload with PK generator g updates rows inserted by Insert workload
  with PK generator g. The sequence of values generated depends on random salt
  and loader id.

Example generator specification

String generator:
```
{
  "str_gen_0": {
    "name": "str_histo_prefix_len",
    "params": {
      "cardinality": 10000000000,
      "len": [{
          "min": 101,
          "max": 200,
          "pct": 40
        }, {
          "min": 201,
          "max": 300,
          "pct": 60
      }],
      "random_salt": 0,
    }
}
```

Integer Generator:
```
{
  "int_gen_1": {
    "name": "int_uniform",
    "params": {
      "cardinality": 500,
      "min": 1,
      "max": 1000,
      "random_salt": 1
    }
  }
}
```

If fields like cardinality are not specified, cardinality of the generated
  values is not constrained. Some generators default fields if not specified.
  For example int_uniform defaults min to math.MinInt64 and max to math.MaxInt64

The workloads specify which generators to use in the params field of the json.
For example
```
"params": {
        "generators": [
          "int_job_id_0:10",
          "str_large_0:20",
          "int_small_0:42"
        ]
      },
```

If a generator is used in the form <gen>:<offset> then offset is added to the
  random salt of the generator. This makes it easy to re-use the same generator
  with different salts.
  

# To run unit tests
```
export GOPATH=<>/rksql/src/go
go test ./src/rubrik/sqload/... --tags=testserver
```
