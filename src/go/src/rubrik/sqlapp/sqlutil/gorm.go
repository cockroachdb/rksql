package sqlutil

import (
	"context"
	"database/sql"
	"net/url"

	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq" //load postgres driver

	"rubrik/util/crdbutil"
	"rubrik/util/log"
)

type logger struct{}

// Print logs the supplied argument. This is hack used to implement gorm's
// private logging interface by duck typing. This is required
// because gorm uses a private interface to inject the logger.
func (l *logger) Print(v ...interface{}) {
	log.Info(context.TODO(), v...)
}

func urls(
	ctx context.Context,
	addrs []crdbutil.SocketAddress,
	dbName string,
	certsDir string,
	insecure bool,
	dbExists bool,
) ([]*url.URL, error) {
	if insecure && len(certsDir) != 0 {
		log.Info(ctx, "certs directory is passed in insecure cockroach connection")
	}

	if !insecure && len(certsDir) == 0 {
		log.Info(ctx, "certs directory is not passed in for secure cockroach connection")
	}

	if !dbExists {
		err := crdbutil.DropAndCreateDatabase(addrs[0], dbName, certsDir, insecure)
		if err != nil {
			return nil, err
		}
	}

	var u []*url.URL
	for _, sAddr := range addrs {
		if insecure {
			log.Info(ctx, "connecting to CockroachDB in insecure mode")
		}
		var url *url.URL
		var err error
		url, err =
			crdbutil.DBURLParams{
				SocketAddr: sAddr,
				DbName:     dbName,
				User:       crdbutil.RootUser,
				Insecure:   insecure,
				CertsDir:   certsDir,
			}.URL()
		if err != nil {
			return nil, err
		}
		u = append(u, url)
	}
	return u, nil
}

// GormDBs creates connections to each host using GORM ORM. If the database
// does not already exist, it is first created. The caller is responsible for
// closing these connections.
// THIS IS DEPRECATED IN FAVOUR OF DBs()
func GormDBs(
	ctx context.Context,
	addrs []crdbutil.SocketAddress,
	dbName string,
	certsDir string,
	insecure bool,
	dbExists bool,
) ([]*gorm.DB, error) {
	var u []*url.URL
	var e error
	u, e = urls(ctx, addrs, dbName, certsDir, insecure, dbExists)
	if e != nil {
		return nil, e
	}
	l := &logger{}
	var dbs []*gorm.DB
	for _, url := range u {
		db, err := gorm.Open("postgres", url.String())
		if err != nil {
			return nil, err
		}
		db.SetLogger(l)
		dbs = append(dbs, db)
	}
	return dbs, nil
}

// DBs creates connections to each host. If the database does not already exist,
// it is first created. The caller is responsible for closing these connections.
func DBs(
	ctx context.Context,
	addrs []crdbutil.SocketAddress,
	dbName string,
	certsDir string,
	insecure bool,
	dbExists bool,
) ([]*sql.DB, error) {
	var u []*url.URL
	var e error
	u, e = urls(ctx, addrs, dbName, certsDir, insecure, dbExists)
	if e != nil {
		return nil, e
	}
	var dbs []*sql.DB
	for _, url := range u {
		db, err := sql.Open("postgres", url.String())
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}
