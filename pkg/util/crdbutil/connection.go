// Copyright 2017 Rubrik, Inc.

package crdbutil

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/rksql/pkg/util/log"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	scheme = "postgresql"
	// DefaultPort is the default port on which crdb runs.
	DefaultPort = 26257
	// RootUser is the default user for crdb.
	RootUser = "root"
	// RkOwner is the database user that has DDL access to all DBs
	RkOwner = "rk_owner"
	// RkUser is the database user that has DML access to all DBs
	RkUser = "rk_user"
	// ConnectTimeoutSeconds to establish a connection
	ConnectTimeoutSeconds = "10"
)

func isAllWhitespace(s string) bool {
	return strings.TrimSpace(s) == ""
}

// A SocketAddress represents the combination of an ip address and a port.
type SocketAddress struct {
	Host string
	Port int
}

func (s SocketAddress) String() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

func (s SocketAddress) valid() error {
	if isAllWhitespace(s.Host) {
		return errors.New("crdbutil: empty host in socket address")
	}
	if s.Port <= 0 {
		return errors.Errorf("crdbutil: negative port %d in socket address", s.Port)
	}
	return nil
}

// ParseSocketAddress returns the SocketAddress represented by s, which should be
// formatted as as "<ip>:<port>". If port mapping is not given, default port is assumed
func ParseSocketAddress(s string) (SocketAddress, error) {
	parts := strings.Split(s, ":")
	var port int
	var err error
	if len(parts) == 1 {
		port = DefaultPort
	} else if len(parts) == 2 {
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			return SocketAddress{}, err
		}
	} else {
		return SocketAddress{}, fmt.Errorf("too many parts. s: %v", s)
	}
	return SocketAddress{parts[0], port}, nil
}

// DBURLParams captures values for various params required to connect to
// CockroachDB
type DBURLParams struct {
	SocketAddr         SocketAddress
	DbName             string
	User               string
	Insecure           bool
	CertsDir           string
	SocketReadTimeout  time.Duration
	SocketWriteTimeout time.Duration
}

// DeclareCockroachDbURLFlags declares flags necessary for passing
// all information relevant to connecting to CockroachDB
func DeclareCockroachDbURLFlags(f *flag.FlagSet) *DBURLParams {
	const usagePrefix = "CockroachDB connection-param: "
	var dbParams DBURLParams
	f.StringVar(
		&dbParams.SocketAddr.Host, "roach", "localhost", usagePrefix+"host",
	)
	f.IntVar(
		&dbParams.SocketAddr.Port, "roach_port", DefaultPort, usagePrefix+"port",
	)
	f.StringVar(
		&dbParams.DbName, "roach_db_name", "rubrik", usagePrefix+"database name",
	)
	f.StringVar(
		&dbParams.User, "roach_user", RkUser, usagePrefix+"username",
	)
	f.StringVar(
		&dbParams.CertsDir, "roach_certs_dir", "", usagePrefix+"directory with keycerts",
	)
	f.BoolVar(
		&dbParams.Insecure, "insecure", false, usagePrefix+"insecure mode",
	)
	f.DurationVar(
		&dbParams.SocketReadTimeout,
		"roach_socket_read_timeout",
		0,
		usagePrefix+"socket read timeout (0s implies no timeout)",
	)
	f.DurationVar(
		&dbParams.SocketWriteTimeout,
		"roach_socket_write_timeout",
		0,
		usagePrefix+"socket write timeout (0s implies no timeout)",
	)
	return &dbParams
}

func (p DBURLParams) valid() error {
	if err := p.SocketAddr.valid(); err != nil {
		return err
	}
	if isAllWhitespace(p.User) {
		return errors.New("crdbutil: empty user in db params")
	}
	if p.User == RootUser {
		// NOTE: We need to allow root user here because sqlapp/sqlutil/gorm.go
		// needs to access the db as root user. We can potentially try to fix that
		// and disallow root completely.
		log.Info(context.TODO(), "WARNING: crdbutil: connecting to crdb using root user")
	}
	if isAllWhitespace(p.DbName) {
		return errors.New("crdbutil: empty database name in db params")
	}
	return nil
}

// URL returns the url built from p. It performs basic validation on all
// parameters, and returns an error in case some validation fails.
func (p DBURLParams) URL() (*url.URL, error) {
	if err := p.valid(); err != nil {
		return nil, err
	}
	return p.url()
}

// url is the unexported form of URL which does not perform most validation
// checks. It is used internally by this package, for example, to access the
// database as root user, which would normally fail validation checks.
func (p DBURLParams) url() (*url.URL, error) {
	q := url.Values{}
	if p.Insecure {
		q.Add("sslmode", "disable")
	} else if !isAllWhitespace(p.CertsDir) {
		q.Add("sslmode", "verify-full")
		q.Add("sslcert", fmt.Sprintf("%s/client.%s.crt", p.CertsDir, p.User))
		q.Add("sslkey", fmt.Sprintf("%s/client.%s.key", p.CertsDir, p.User))
		q.Add("sslrootcert", fmt.Sprintf("%s/ca.crt", p.CertsDir))
	} else {
		return nil, errors.New("crdbutil: must supply either insecure or certsDir")
	}
	q.Add("connect_timeout", ConnectTimeoutSeconds)
	if p.SocketReadTimeout != 0 {
		// read_timeout is specified in milliseconds
		q.Add("read_timeout", fmt.Sprint(int64(p.SocketReadTimeout/time.Millisecond)))
	}
	if p.SocketWriteTimeout != 0 {
		// write_timeout is specified in milliseconds
		q.Add("write_timeout", fmt.Sprint(int64(p.SocketWriteTimeout/time.Millisecond)))
	}
	return &url.URL{
		Scheme:   scheme,
		User:     url.User(p.User),
		Host:     p.SocketAddr.String(),
		RawQuery: q.Encode(),
		Path:     p.DbName,
	}, nil
}

func openDBAsRoot(socketAddr SocketAddress, insecure bool, certsDir string) (*sql.DB, error) {
	url, err :=
		DBURLParams{
			SocketAddr: socketAddr,
			User:       RootUser,
			Insecure:   insecure,
			CertsDir:   certsDir,
		}.url()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", url.String())
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getUsers(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT username FROM system.users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []string
	var user string
	for rows.Next() {
		err := rows.Scan(&user)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

func createDatabaseAndGrantPermissions(db *sql.DB, dbName string) error {
	createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	if _, err := db.Exec(createSQL); err != nil {
		return err
	}

	users, err := getUsers(db)
	if err != nil {
		return err
	}

	userExists := func(user string) bool {
		for _, u := range users {
			if u == user {
				return true
			}
		}
		return false
	}

	if !userExists(RkOwner) {
		createRkOwnerSQL := fmt.Sprintf("CREATE USER %s", RkOwner)
		if _, err := db.Exec(createRkOwnerSQL); err != nil {
			return err
		}
	}
	permissionRkOwnerSQL := fmt.Sprintf(
		"GRANT CREATE, DROP, SELECT, UPDATE, DELETE, INSERT ON DATABASE %s to %s;",
		dbName,
		RkOwner,
	)
	if _, err := db.Exec(permissionRkOwnerSQL); err != nil {
		return err
	}

	if !userExists(RkUser) {
		createRkUserSQL := fmt.Sprintf("CREATE USER %s", RkUser)
		if _, err := db.Exec(createRkUserSQL); err != nil {
			return err
		}
	}
	permissionRkUserSQL := fmt.Sprintf(
		"GRANT SELECT, UPDATE, DELETE, INSERT ON DATABASE %s to %s;",
		dbName,
		RkUser,
	)
	if _, err := db.Exec(permissionRkUserSQL); err != nil {
		return err
	}
	return nil
}

// DropAndCreateDatabase creates a fresh database dbName in the DB at
// socketAddr, dropping any existing database of the same name.  It also grants
// DDL privileges to rk_owner, and DML privileges to rk_user. It connects to
// the DB as root user.
func DropAndCreateDatabase(
	socketAddr SocketAddress, dbName string, certsDir string, insecure bool,
) error {
	db, err := openDBAsRoot(socketAddr, insecure, certsDir)
	if err != nil {
		return err
	}
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName)
	if _, err := db.Exec(dropSQL); err != nil {
		return err
	}
	if err := createDatabaseAndGrantPermissions(db, dbName); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}

// CreateDatabaseIfNotExists creates a fresh database dbName in the DB at
// socketAddr, if it doesn't already exist.  It also grants DDL privileges to
// rk_owner, and DML privileges to rk_user. It connects to the DB as root user.
func CreateDatabaseIfNotExists(
	socketAddr SocketAddress, dbName string, certsDir string, insecure bool,
) error {
	db, err := openDBAsRoot(socketAddr, insecure, certsDir)
	if err != nil {
		return err
	}
	if err := createDatabaseAndGrantPermissions(db, dbName); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}
