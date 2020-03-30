# rksql (fork of scaledata/rksql)

Correctness and performance testing tools for distributed SQL databases.
These tools can be found in [sqlapp](https://github.com/cockroachdb/rksql/tree/master/src/go/src/rubrik/sqlapp)
and [sqload](https://github.com/cockroachdb/rksql/tree/master/src/go/src/rubrik/sqload)
respectively, where you will find further details.

### Build

To build you first need to install `go` tools. We recommend using go-1.8. You
can follow https://golang.org/doc/install#install 

You would also need `dep` and `goimports` installed and available in your PATH. 
You can follow the instructions on https://github.com/golang/dep and
https://godoc.org/golang.org/x/tools/cmd/goimports

Once you have the dependencies you can run this python script: `src/go/BUILD.py`

To run unit tests: `src/go/BUILD.py --test`

If you need to clean your workspace: `src/go/BUILD.py --clean`

### Edge Binaries

CockroachDB uses binaries generated through this repo when running the
scaledata roachtests. It picks them up off of
https://edge-binaries.cockroachdb.com/. Use `s3-push.sh` to update them
manually (while we fix things up to run CI here to do so automatically).
