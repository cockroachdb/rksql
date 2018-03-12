package cqlutil

import (
	"flag"
	"strings"
	"time"

	"github.com/kaavee315/gocql"
)

// CassandraOptions options for creating a cassandra query executor
type CassandraOptions struct {
	Host                     string
	Port                     int
	Timeout                  time.Duration
	ConnectTimeout           time.Duration
	NumConnections           int
	DisableInitialHostLookup bool
	IgnorePeerAddr           bool
	SkipPreparedStmt         bool
	Consistency              gocql.Consistency
	PageSize                 int
}

// GocqlConsistency converts consistency as string to gocql.Consistency
func GocqlConsistency(c string) gocql.Consistency {
	switch strings.ToUpper(c) {
	case gocql.All.String():
		return gocql.All
	case gocql.LocalQuorum.String():
		return gocql.LocalQuorum
	case gocql.One.String():
		return gocql.One
	default:
		return gocql.LocalQuorum
	}
}

// DeclareCassandraConnFlags adds flags required by CassandraOptions
// to the given FlagSet and returns CassandraOptions
func DeclareCassandraConnFlags(f *flag.FlagSet) *CassandraOptions {
	o := new(CassandraOptions)
	f.StringVar(&o.Host,
		"cassandra_host",
		"127.0.0.1",
		"Cassandra Host")
	f.IntVar(&o.Port,
		"cassandra_port",
		9042,
		"Cassandra Port")
	f.DurationVar(&o.Timeout,
		"cassandra_query_timeout",
		90*time.Second,
		"Timeout for cassandra")
	f.DurationVar(&o.ConnectTimeout,
		"connection_timeout",
		5*time.Second,
		"Connection timeout for cassandra")
	f.IntVar(&o.NumConnections,
		"cassandra_num_connections",
		10,
		"Number of connections to make to cassandra")
	f.BoolVar(&o.DisableInitialHostLookup,
		"cassandra_disable_init_host_lookup",
		false,
		"Whether to disable initial host lookup")
	f.BoolVar(&o.IgnorePeerAddr,
		"cassandra_ignore_peer_addr",
		false,
		"Whether to ignore peer addresses")
	f.BoolVar(&o.SkipPreparedStmt,
		"cassandra_skip_prepared_statement",
		true,
		"Whether to skip prepared statement")
	var consistency string
	f.StringVar(&consistency,
		"cassandra_consistency",
		gocql.LocalQuorum.String(),
		"Consistency to be used with cassandra")
	o.Consistency = GocqlConsistency(consistency)
	f.IntVar(&o.PageSize,
		"cassandra_page_size",
		5000,
		"Page size to be used for cassandra queriess")
	return o
}

// Connect creates a cassandra session
func (c *CassandraOptions) Connect() (*gocql.Session, error) {
	cluster := gocql.NewCluster(c.Host)
	cluster.Consistency = c.Consistency
	cluster.Port = c.Port
	cluster.IgnorePeerAddr = c.IgnorePeerAddr
	cluster.DisableInitialHostLookup = c.DisableInitialHostLookup
	cluster.Timeout = c.Timeout
	cluster.ConnectTimeout = c.ConnectTimeout
	cluster.NumConns = c.NumConnections
	cluster.PageSize = c.PageSize
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	s.SetSkipPrepStmt(c.SkipPreparedStmt)
	return s, nil
}
