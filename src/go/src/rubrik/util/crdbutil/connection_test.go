// Copyright 2017 Rubrik, Inc.

package crdbutil

import (
	"fmt"
	"testing"
	"time"
)

func TestParseSocketAddress(t *testing.T) {
	cases := []struct {
		socketAddr string
		wantAddr   string
		wantPort   int
		err        bool
	}{
		{
			"1.2.3.4:567",
			"1.2.3.4",
			567,
			false,
		},
		{
			" 10.0.20.34 :123",
			" 10.0.20.34 ",
			123,
			false,
		},
		{
			"foo:123",
			"foo",
			123,
			false,
		},
		{
			"1.2.3.4:-1",
			"1.2.3.4",
			-1,
			false,
		},
		{
			":123",
			"",
			123,
			false,
		},
		{
			"1.2.3.4:",
			"",
			0,
			true,
		},
		{
			"1.2.3.4:123:",
			"",
			0,
			true,
		},
		{
			"1.2.3.4:123:456",
			"",
			0,
			true,
		},
		{
			":123:456",
			"",
			0,
			true,
		},
		{
			"1.2.3.4::456",
			"",
			0,
			true,
		},
	}
	for _, c := range cases {
		got, err := ParseSocketAddress(c.socketAddr)
		msg := fmt.Sprintf("ParseSocketAddress(%s)", c.socketAddr)
		if c.err && err == nil {
			t.Errorf("%s == %v, want err", msg, got)
		} else if !c.err && err != nil {
			t.Errorf(
				"%s caused %v, wantAddr: %v wantPort: %v",
				msg,
				err,
				c.wantAddr,
				c.wantPort,
			)
		} else if !c.err && (got.Host != c.wantAddr || got.Port != c.wantPort) {
			t.Errorf(
				"%s == %v, wantAddr: %v wantPort: %v",
				msg,
				got,
				c.wantAddr,
				c.wantPort,
			)
		}
	}
}

func TestDatabaseURL(t *testing.T) {
	cases := []struct {
		addr         string
		port         int
		dbName       string
		user         string
		insecure     bool
		certsDir     string
		readTimeout  time.Duration
		writeTimeout time.Duration
		want         string
		err          bool
	}{
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "rubrik",
			user:     "rk_user",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "postgresql://rk_user@localhost:26257/rubrik?connect_timeout=10&sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.crt&sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.rk_user.key&sslmode=verify-full&sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
			err:      false,
		},
		{
			addr:     " vnode1",
			port:     8081,
			dbName:   "correctness",
			user:     "maxroach",
			insecure: false,
			certsDir: " /var/lib/certs ",
			want:     "postgresql://maxroach@%20vnode1:8081/correctness?connect_timeout=10&sslcert=+%2Fvar%2Flib%2Fcerts+%2Fclient.maxroach.crt&sslkey=+%2Fvar%2Flib%2Fcerts+%2Fclient.maxroach.key&sslmode=verify-full&sslrootcert=+%2Fvar%2Flib%2Fcerts+%2Fca.crt",
			err:      false,
		},
		{
			addr:     "    ",
			port:     26257,
			dbName:   "rubrik",
			user:     "rk_user",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     -1,
			dbName:   "rubrik",
			user:     "rk_user",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "rubrik",
			user:     "root",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "postgresql://root@localhost:26257/rubrik?connect_timeout=10&sslcert=%2Fvar%2Flib%2Fcerts%2Fclient.root.crt&sslkey=%2Fvar%2Flib%2Fcerts%2Fclient.root.key&sslmode=verify-full&sslrootcert=%2Fvar%2Flib%2Fcerts%2Fca.crt",
			err:      false,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "",
			user:     "user",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "rubrik",
			user:     "",
			insecure: false,
			certsDir: "/var/lib/certs",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "rubrik",
			user:     "root",
			insecure: false,
			certsDir: "   ",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "dbName",
			user:     "foo",
			insecure: true,
			certsDir: "",
			want:     "postgresql://foo@localhost:26257/dbName?connect_timeout=10&sslmode=disable",
			err:      false,
		},
		{
			addr:     " 10.0.20.34 ",
			port:     8081,
			dbName:   "dbName",
			user:     "bar",
			insecure: true,
			certsDir: "",
			want:     "postgresql://bar@%2010.0.20.34%20:8081/dbName?connect_timeout=10&sslmode=disable",
			err:      false,
		},
		{
			addr:     "  ",
			port:     8081,
			dbName:   "dbName",
			user:     "quux",
			insecure: true,
			certsDir: "",
			want:     "",
			err:      true,
		},
		{
			addr:     "vnode1",
			port:     -1,
			dbName:   "dbName",
			user:     "quux",
			insecure: true,
			certsDir: "",
			want:     "",
			err:      true,
		},
		{
			addr:     "localhost",
			port:     26257,
			dbName:   "",
			user:     "foo",
			insecure: true,
			certsDir: "",
			want:     "",
			err:      true,
		},
		{
			addr:         " 10.0.20.34 ",
			port:         8081,
			dbName:       "dbName",
			user:         "bar",
			insecure:     true,
			certsDir:     "",
			readTimeout:  time.Duration(150 * time.Millisecond),
			writeTimeout: time.Duration(12 * time.Second),
			want:         "postgresql://bar@%2010.0.20.34%20:8081/dbName?connect_timeout=10&read_timeout=150&sslmode=disable&write_timeout=12000",
			err:          false,
		},
		{
			addr:         " 10.0.20.34 ",
			port:         8081,
			dbName:       "dbName",
			user:         "bar",
			insecure:     true,
			certsDir:     "",
			writeTimeout: time.Duration(1 * time.Minute),
			want:         "postgresql://bar@%2010.0.20.34%20:8081/dbName?connect_timeout=10&sslmode=disable&write_timeout=60000",
			err:          false,
		},
		{
			addr:        "localhost",
			port:        8081,
			dbName:      "dbName",
			user:        "bar",
			insecure:    true,
			certsDir:    "",
			readTimeout: time.Duration(30 * time.Second),
			want:        "postgresql://bar@localhost:8081/dbName?connect_timeout=10&read_timeout=30000&sslmode=disable",
			err:         false,
		},
	}
	for _, c := range cases {
		got, err :=
			DBURLParams{
				SocketAddr:         SocketAddress{c.addr, c.port},
				DbName:             c.dbName,
				User:               c.user,
				Insecure:           c.insecure,
				CertsDir:           c.certsDir,
				SocketReadTimeout:  c.readTimeout,
				SocketWriteTimeout: c.writeTimeout,
			}.URL()

		msg := fmt.Sprintf(
			"URL(%v, %v, %v, %v, %v)",
			c.addr,
			c.port,
			c.dbName,
			c.insecure,
			c.certsDir,
		)
		if c.err && err == nil {
			t.Errorf("%s == %v, want err", msg, got)
		} else if !c.err && err != nil {
			t.Errorf("%s caused %v, want %s", msg, err, c.want)
		} else if !c.err && got.String() != c.want {
			t.Errorf("%s == %v, want %s", msg, got, c.want)
		}
	}
}
