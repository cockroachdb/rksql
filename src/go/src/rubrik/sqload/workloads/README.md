# workloads

## Purpose
Workloads specified here are used to load test cockroachdb

## Adding a new workload to load_test
* Add the table schema to schema.json
* Create a directory in this folder having
  * populate.json: This has the workload used to insert data into an empty
    table (defined in schema.json).
    This can be executed on a single cockroachdb node or on all nodes
    based on load_spec
  * workload.json: This has the actual workload to run for load testing.
    This is run on all the cockroachdb nodes
* If CSV based params are being used, they can be uploaded to
  files-master.rubrik-lab.com under /srv/ftp/tests/cockroachdb/
* Add an entry in spec.py
* schema.json is used for CQL workloads
* schema.sql is used for SQL workloads
