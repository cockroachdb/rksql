# rksql (fork of scaledata/rksql)

Correctness and performance testing tools for distributed SQL databases.

### Edge Binaries

CockroachDB uses binaries generated through this repo when running the
scaledata roachtests. It picks them up off of edge-binaries.cockroachdb.com/.
Use `scripts/s3-push.sh` to update them manually (while we fix things up to run
CI here to do so automatically).
