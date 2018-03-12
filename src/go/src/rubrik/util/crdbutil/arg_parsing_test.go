package crdbutil

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func p(s string) *string {
	return &s
}

var (
	testCases = []struct {
		addr         *string
		port         *string
		db           *string
		user         *string
		insecure     *string
		certsDir     *string
		readTimeout  *string
		writeTimeout *string

		want string
		err  *string
	}{
		{
			insecure: p("true"),
			want:     "postgresql://rk_user@localhost:26257/rubrik?connect_timeout=10&sslmode=disable",
		},
		{
			err: p("crdbutil: must supply either insecure or certsDir"),
		},
		{
			addr:     p("localhost"),
			port:     p("26257"),
			db:       p("dbName"),
			user:     p("foo"),
			insecure: p("true"),
			want:     "postgresql://foo@localhost:26257/dbName?connect_timeout=10&sslmode=disable",
		},
		{
			addr:     p("10.0.20.34"),
			port:     p("8081"),
			db:       p("dbName"),
			user:     p("bar"),
			insecure: p("true"),
			want:     "postgresql://bar@10.0.20.34:8081/dbName?connect_timeout=10&sslmode=disable",
		},
		{
			addr:     p("  "),
			port:     p("8081"),
			db:       p("dbName"),
			user:     p("quux"),
			insecure: p("true"),
			err:      p("crdbutil: empty host in socket address"),
		},
		{
			addr:     p("vnode1"),
			port:     p("-1"),
			db:       p("dbName"),
			user:     p("quux"),
			insecure: p("true"),
			err:      p("crdbutil: negative port -1 in socket address"),
		},
		{
			addr:     p("localhost"),
			port:     p("26257"),
			user:     p("foo"),
			insecure: p("true"),
			want:     "postgresql://foo@localhost:26257/rubrik?connect_timeout=10&sslmode=disable",
		},
		{
			certsDir: p("/var/lib/certs"),
			want: "postgresql://rk_user@localhost:26257/rubrik?" +
				"connect_timeout=10&" +
				"sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.crt&" +
				"sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.key&" +
				"sslmode=verify-full&" +
				"sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
		},
		{
			certsDir: p("/var/lib/certs"),
			insecure: p("false"),
			want: "postgresql://rk_user@localhost:26257/rubrik?" +
				"connect_timeout=10&" +
				"sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.crt&" +
				"sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.key&" +
				"sslmode=verify-full&" +
				"sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
		},
		{
			addr:     p("mydb"),
			port:     p("8080"),
			db:       p("public"),
			user:     p("anon"),
			certsDir: p("/var/lib/certs"),
			want: "postgresql://anon@mydb:8080/public?" +
				"connect_timeout=10&" +
				"sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.anon.crt&" +
				"sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.anon.key&" +
				"sslmode=verify-full&" +
				"sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
		},
		{
			addr:         p("localhost"),
			port:         p("26257"),
			user:         p("foo"),
			insecure:     p("true"),
			readTimeout:  p("1m"),
			writeTimeout: p("2m"),
			want: "postgresql://foo@localhost:26257/rubrik?" +
				"connect_timeout=10&" +
				"read_timeout=60000&" +
				"sslmode=disable&" +
				"write_timeout=120000",
		},
		{
			certsDir:    p("/var/lib/certs"),
			readTimeout: p("10s"),
			want: "postgresql://rk_user@localhost:26257/rubrik?" +
				"connect_timeout=10&" +
				"read_timeout=10000&" +
				"sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.crt&" +
				"sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.key&" +
				"sslmode=verify-full&" +
				"sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
		},
		{
			insecure:     p("true"),
			writeTimeout: p("2ms"),
			want: "postgresql://rk_user@localhost:26257/rubrik?" +
				"connect_timeout=10&" +
				"sslmode=disable&" +
				"write_timeout=2",
		},
	}
)

func setFlag(name string, val *string) {
	if val != nil {
		flag.Set(name, *val)
	}
}

func TestFlagBasedDbUrlConstruction(t *testing.T) {
	assert := assert.New(t)

	for _, c := range testCases {
		flag.CommandLine = flag.NewFlagSet("test set", flag.PanicOnError)
		flag.CommandLine.Usage = flag.Usage

		dbOpts := DeclareCockroachDbURLFlags(flag.CommandLine)

		setFlag("roach", c.addr)
		setFlag("roach_port", c.port)
		setFlag("roach_db_name", c.db)
		setFlag("roach_user", c.user)
		setFlag("insecure", c.insecure)
		setFlag("roach_certs_dir", c.certsDir)
		setFlag("roach_socket_read_timeout", c.readTimeout)
		setFlag("roach_socket_write_timeout", c.writeTimeout)

		url, err := dbOpts.URL()
		if c.err == nil {
			assert.Nil(err)
			assert.Equal(url.String(), c.want)
		} else {
			assert.Nil(url)
			assert.Equal(*c.err, err.Error())
		}
	}
}
