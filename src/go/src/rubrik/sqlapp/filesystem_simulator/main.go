package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	pathlib "path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"rubrik/sqlapp"
	"rubrik/sqlapp/sqlutil"
	"rubrik/util/crdbutil"
	"rubrik/util/log"
	"rubrik/util/randutil"
)

type fileType int

const (
	fileTypeUnknown fileType = iota
	fileTypeFile
	fileTypeDirectory
)

type opType int

const (
	// Directories.
	opTypeMkdir opType = iota
	opTypeRmdir
	// Files.
	opTypeMknod
	opTypeWrite
	opTypeUnlink
)

const (
	numBuckets                  = 256
	root                        = "/"
	databaseName                = "fs"
	allowableAmbiguity          = 0.25
	ambiguousCommitRetryCount   = 10
	thresholdOpsPerWorkerMinute = 5
)

var (
	shard = flag.String(
		sqlapp.WorkerIndex,
		"default",
		"Index of the worker",
	)
)

type file struct {
	uuid string
	typ  fileType
	size int
	// subShard is used to partition files within a shard, to implement
	// picking a file at random from the table.
	subShard uint32
	// shard is used to isolate rows across multiple simultaneous instances
	// of the test. Each instance is identified uniquely by instanceID flag.
	shard string
}

const createFilesTable = `
CREATE TABLE files(
	uuid TEXT PRIMARY KEY,
	type INT,
	size INT,
	sub_shard INT,
	shard TEXT,
	INDEX idx_sub_shard (sub_shard),
	INDEX idx_shard (shard)
)`

type childRelation struct {
	parentUUID string
	name       string
	childUUID  string
	shard      string
}

const createChildRelationsTable = `
CREATE TABLE child_relations (
	parent_uuid text,
	name text,
	child_uuid text,
	shard text,
	PRIMARY KEY (parent_uuid, name),
	INDEX idx_child_uuid (child_uuid),
	INDEX idx_shard (shard)
)`

type stripe struct {
	uuid  string
	idx   int
	shard string
}

const createStripesTable = `
CREATE TABLE stripes (
	uuid TEXT,
	idx INT,
	shard TEXT,
	PRIMARY KEY (uuid, idx),
	INDEX idx_shard (shard)
)`

// stripeSorter - type to sort
type stripeSorter []stripe

func (s stripeSorter) Len() int {
	return len(s)
}

func (s stripeSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// to sort the stripe on the basis of Idx
func (s stripeSorter) Less(i, j int) bool {
	return s[i].idx < s[j].idx
}

func getRoot() string {
	return root + *shard
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func initializeTables(ctx context.Context, db *sql.DB) {
	ts := []struct {
		name string
		sql  string
	}{
		{"files", createFilesTable},
		{"child_relations", createChildRelationsTable},
		{"stripes", createStripesTable},
	}
	for _, t := range ts {
		s := fmt.Sprintf("DROP TABLE IF EXISTS %s", t.name)
		if _, err := db.ExecContext(ctx, s); err != nil {
			log.Fatal(ctx, err)
		}
		if _, err := db.ExecContext(ctx, t.sql); err != nil {
			log.Fatal(ctx, err)
		}
	}
	log.Info(ctx, "Created tables")
}

func createFile(
	ctx context.Context,
	tx *sql.Tx,
	parentUUID string,
	name string,
	fileType fileType,
) (string, error) {
	uuid := uuid.New().String()
	if err := createFileWithUUID(ctx, tx, uuid, parentUUID, name, fileType); err != nil {
		return "", err
	}
	return uuid, nil
}

func execOneRow(
	ctx context.Context,
	tx *sql.Tx,
	s string,
	msgPrefix string,
	args ...interface{},
) (err error) {
	var res sql.Result
	if res, err = tx.ExecContext(ctx, s, args...); err != nil {
		return err
	}
	var n int64
	if n, err = res.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("%s affected %d rows, expected 1", msgPrefix, n)
	}
	return nil
}

func createFileWithUUID(
	ctx context.Context,
	tx *sql.Tx,
	uuid string,
	parentUUID string,
	name string,
	fileType fileType,
) error {
	var size = 0
	if fileType == fileTypeDirectory {
		size = 4096
	}

	if err := execOneRow(
		ctx,
		tx,
		"INSERT INTO files (uuid, type, size, sub_shard, shard) VALUES ($1, $2, $3, $4, $5)",
		"INSERT INTO files",
		uuid,
		fileType,
		size,
		hash(uuid)%numBuckets,
		*shard,
	); err != nil {
		return err
	}
	if uuid != getRoot() {
		// The txn that created parentUUID in the files table should have happened
		// before this txn in the test but if it used a different cockroach node
		// than the one this txn is happening on, it is possible that this txn gets
		// an earlier timestamp (due to clock skew.)
		// Prevent this from happening by introducing a write to the parentUUID row
		// which will order this txn after the txn that creates the parent.
		if err := execOneRow(
			ctx,
			tx,
			// Resetting the shard to the same value is a no-op.
			"UPDATE fs.files SET shard = $1 WHERE uuid = $2",
			fmt.Sprintf("Updating parent uuid %s", parentUUID),
			*shard,
			parentUUID,
		); err != nil {
			return err
		}
		if err := execOneRow(
			ctx,
			tx,
			"INSERT INTO child_relations (parent_uuid,name,child_uuid,shard) VALUES ($1, $2, $3, $4)",
			"INSERT INTO child_relations",
			parentUUID,
			name,
			uuid,
			*shard,
		); err != nil {
			return err
		}
	} else if parentUUID != "" {
		log.Fatal(ctx, getRoot(), " should not have parent")
	}
	log.Infof(ctx, "Created file %s with uuid %s and parent %s", name, uuid, parentUUID)
	return nil
}

func getChildMap(
	ctx context.Context,
	tx *sql.Tx,
	uuid string,
) (map[string]string, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT child_uuid, name FROM child_relations WHERE parent_uuid = $1 ORDER BY name ASC",
		uuid,
	)
	if err != nil {
		return nil, err
	}
	childMap := make(map[string]string)
	for rows.Next() {
		var childUUID, childName string
		if err := rows.Scan(&childUUID, &childName); err != nil {
			return nil, err
		}
		childMap[childName] = childUUID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return childMap, nil
}

func randomFile(ctx context.Context, tx *sql.Tx) (*file, error) {
	const numRetries = 20
	// Trying for <numRetries> to get the file and then falls back to do a
	// full table scan due to possibility of empty buckets
	for i := 0; i < numRetries; i++ {
		randBucket := uint32(rand.Intn(numBuckets))
		files, err := selectFilesWhere(
			ctx,
			tx,
			"type = $1 AND sub_shard = $2 AND shard = $3",
			fileTypeFile,
			randBucket,
			*shard,
		)
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			continue
		}
		return &files[rand.Intn(len(files))], nil
	}
	// Fallback if not found in numTries buckets
	files, err := selectFilesWhere(
		ctx,
		tx,
		"type = $1 AND shard = $2",
		fileTypeFile,
		*shard,
	)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return &files[rand.Intn(len(files))], nil
}

func removeFile(ctx context.Context, tx *sql.Tx, file *file) error {
	log.Info(ctx, "Removing ", file)
	switch file.typ {
	case fileTypeFile:
		if _, err := tx.ExecContext(
			ctx,
			"DELETE FROM stripes WHERE uuid = $1",
			file.uuid,
		); err != nil {
			return err
		}
		log.Infof(ctx, "Deleted stripes for uuid %s", file.uuid)
	case fileTypeDirectory:
		log.Warningf(ctx, "TODO: check no children")
	}

	if err := execOneRow(
		ctx,
		tx,
		"DELETE FROM child_relations WHERE child_uuid = $1",
		fmt.Sprintf("Deleting entry in child_map for %s", file.uuid),
		file.uuid,
	); err != nil {
		return err
	}
	log.Infof(ctx, "Deleted child_relations for uuid %s", file.uuid)

	return execOneRow(
		ctx,
		tx,
		"DELETE FROM files WHERE uuid = $1",
		fmt.Sprintf("Deleting file %s", file.uuid),
		file.uuid,
	)
}

func removeRandomFile(
	ctx context.Context,
	tx *sql.Tx,
) (*file, error) {
	file, err := randomFile(ctx, tx)
	if err != nil {
		return nil, err
	}
	if nil == file {
		log.Info(ctx, "No files")
		return nil, nil
	}

	if err = removeFile(ctx, tx, file); err == nil {
		log.Info(ctx, "Deleted ", file)
	}
	return file, err
}

func writeRandomFile(ctx context.Context, tx *sql.Tx) (*file, *stripe, error) {
	file, err := randomFile(ctx, tx)
	if nil != err {
		return nil, nil, err
	}
	if nil == file {
		return nil, nil, nil
	}
	nextStripe := file.size
	log.Infof(ctx, "Writing new stripe %d", nextStripe)
	s := &stripe{uuid: file.uuid, idx: nextStripe, shard: *shard}
	log.Info(ctx, s)
	if err := execOneRow(
		ctx,
		tx,
		"INSERT INTO stripes (uuid, idx, shard) VALUES ($1, $2, $3)",
		fmt.Sprintf("Writing stripe %d to %s", nextStripe, file.uuid),
		file.uuid,
		nextStripe,
		*shard,
	); err != nil {
		return file, s, err
	}
	file.size++
	return file, s, execOneRow(
		ctx,
		tx,
		"UPDATE files SET size = $1 WHERE uuid = $2",
		fmt.Sprintf("Updating %s's size to %d", file.uuid, nextStripe),
		file.size,
		file.uuid,
	)
}

func findFile(ctx context.Context, tx *sql.Tx, uuid string) (*file, error) {
	files, err := selectFilesWhere(
		ctx,
		tx,
		"uuid = $1",
		uuid,
	)
	if err != nil {
		return nil, err
	}
	switch len(files) {
	case 0:
		return nil, nil
	case 1:
		return &files[0], err
	default:
		return nil, fmt.Errorf("found multiple files for %s: %v", uuid, files)
	}
}

func printDirectoriesRecursive(
	ctx context.Context,
	tx *sql.Tx,
	uuid string,
	path string,
) error {
	childMap, err := getChildMap(ctx, tx, uuid)
	if err != nil {
		return err
	}
	if len(childMap) == 0 {
		return nil
	}

	var childNames []string
	for childName := range childMap {
		childNames = append(childNames, childName)
	}
	sort.Strings(childNames)
	fmt.Println(path)
	fmt.Print(childNames, "\n\n")

	for _, childName := range childNames {
		childUUID := childMap[childName]
		file, err := findFile(ctx, tx, childUUID)
		if err != nil {
			return err
		}
		if fileTypeDirectory == (*file).typ {
			return printDirectoriesRecursive(
				ctx,
				tx,
				childUUID,
				pathlib.Join(path, childName),
			)
		}
	}
	return nil
}

func printDirectories(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	uuid string,
	path string,
) {
	executeTxOrDie(
		ctx,
		rd,
		func(tx *sql.Tx) error {
			return printDirectoriesRecursive(ctx, tx, uuid, path)
		},
	)
}

func selectFilesWhere(
	ctx context.Context,
	tx *sql.Tx,
	where string,
	args ...interface{},
) ([]file, error) {
	rows, err := tx.QueryContext(
		ctx,
		fmt.Sprintf(
			"SELECT uuid, type, size, sub_shard, shard FROM files WHERE %s",
			where,
		),
		args...,
	)
	if err != nil {
		return nil, err
	}
	var files []file
	for rows.Next() {
		var uuid, shard string
		var typ, size int
		var subShard uint32
		if err := rows.Scan(&uuid, &typ, &size, &subShard, &shard); err != nil {
			return nil, err
		}
		files = append(files, file{uuid, fileType(typ), size, subShard, shard})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

func selectFiles(ctx context.Context, tx *sql.Tx, files *[]file) (err error) {
	*files, err = selectFilesWhere(ctx, tx, "shard = $1", *shard)
	return err
}

func selectChildRelations(
	ctx context.Context,
	tx *sql.Tx,
	chrs *[]childRelation,
) error {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT parent_uuid, name, child_uuid FROM child_relations WHERE shard = $1",
		*shard,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var pUUID, name, chUUID string
		if err := rows.Scan(&pUUID, &name, &chUUID); err != nil {
			return err
		}
		*chrs = append(*chrs, childRelation{pUUID, name, chUUID, *shard})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func selectStripes(
	ctx context.Context,
	tx *sql.Tx,
	ss *[]stripe,
) error {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT uuid, idx FROM stripes WHERE shard = $1",
		*shard,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var uuid string
		var idx int
		if err := rows.Scan(&uuid, &idx); err != nil {
			return err
		}
		*ss = append(*ss, stripe{uuid, idx, *shard})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func checkProgress(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	opLog *opLog,
	durationSecs int,
	numWorkers int,
) {
	concurrency := numWorkers
	if numWorkers > 16 {
		concurrency = 16
	}
	thresholdOperations :=
		thresholdOpsPerWorkerMinute *
			concurrency *
			(durationSecs / 60)
	if len(opLog.operations) < thresholdOperations {
		log.Fatalf(
			ctx,
			"log size %d below threshold %d",
			len(opLog.operations),
			thresholdOperations,
		)
	}
	if opLog.ambiguousOps > int(float64(len(opLog.operations))*allowableAmbiguity) {
		log.Fatalf(
			ctx,
			"number of ambiguous Ops - %d, more than allowable Ops - %f",
			opLog.ambiguousOps,
			int(float64(len(opLog.operations))*allowableAmbiguity),
		)
	}
	r := playLogInMem(ctx, opLog)
	var files []file
	var childRelations []childRelation
	executeTxOrDie(
		ctx,
		rd,
		func(tx *sql.Tx) error {
			files = make([]file, 0)
			if err := selectFiles(ctx, tx, &files); err != nil {
				return err
			}
			childRelations = make([]childRelation, 0)
			if err := selectChildRelations(ctx, tx, &childRelations); err != nil {
				return err
			}
			return nil
		},
	)
	uuidToFile := make(map[string]*file)
	childToParent := make(map[string]*childRelation)

	for _, currFile := range files {
		uuidToFile[currFile.uuid] = &file{
			uuid: currFile.uuid,
			typ:  currFile.typ,
			size: currFile.size,
		}
	}

	for _, childRel := range childRelations {
		childToParent[childRel.childUUID] = &childRelation{
			parentUUID: childRel.parentUUID,
			name:       childRel.name,
			childUUID:  childRel.childUUID,
		}
	}

	for uuid := range opLog.ambiguousFiles {
		delete(uuidToFile, uuid)
		delete(childToParent, uuid)
	}

	if !reflect.DeepEqual(uuidToFile, r.uuidToFile) {
		log.Fatal(
			ctx,
			errors.New("uuidToFile doesn't match"),
			"From DataBase - \n",
			uuidToFile,
			"From Memory - \n",
			r.uuidToFile,
		)
	}
	if !reflect.DeepEqual(childToParent, r.childToParent) {
		log.Fatal(
			ctx,
			errors.New("childToParent doesn't match"),
			"From DataBase - \n",
			childToParent,
			"From Memory - \n",
			r.childToParent,
		)
	}
}

func getUUIDToStripes(stripes []stripe) map[string]([]stripe) {
	uuidToStripes := make(map[string][]stripe)
	for _, s := range stripes {
		if uuidToStripes[s.uuid] == nil {
			uuidToStripes[s.uuid] = make([]stripe, 0)
		}
		uuidToStripes[s.uuid] = append(uuidToStripes[s.uuid], s)
	}
	for _, stripeList := range uuidToStripes {
		sort.Sort(stripeSorter(stripeList))
	}
	return uuidToStripes
}

func checkConsistency(ctx context.Context, db *sqlapp.RobustDB) (bool, error) {
	var stripes []stripe
	var files []file
	var childRelations []childRelation
	executeTxOrDie(
		ctx,
		db,
		func(tx *sql.Tx) error {
			files = make([]file, 0)
			if err := selectFiles(ctx, tx, &files); err != nil {
				return err
			}
			stripes = make([]stripe, 0)
			if err := selectStripes(ctx, tx, &stripes); err != nil {
				return err
			}
			childRelations = make([]childRelation, 0)
			if err := selectChildRelations(ctx, tx, &childRelations); err != nil {
				return err
			}
			return nil
		},
	)

	log.Infof(
		ctx,
		"Consistency Test sizes :- files - %d, childRelations - %d, stripes - %d\n",
		len(files),
		len(childRelations),
		len(stripes),
	)

	uuidToStripes := getUUIDToStripes(stripes)

	uuidToFile := make(map[string]file)
	for _, file := range files {
		if val, exists := uuidToFile[file.uuid]; exists {
			log.Fatalf(ctx, "%s appears multiple (%d) times", file.uuid, val)
		}

		if file.typ == fileTypeFile {
			// Verify stripes exist.

			if len(uuidToStripes[file.uuid]) != file.size {
				log.Fatalf(
					ctx,
					"File %v: %s num stripes %d != file size %d",
					file,
					file.uuid,
					len(uuidToStripes[file.uuid]),
					file.size,
				)
			}

			for i, stripe := range uuidToStripes[file.uuid] {
				if i != stripe.idx {
					log.Fatalf(
						ctx,
						"File %v: %s stripe index %d != %d, stripes - %v",
						file,
						file.uuid,
						stripe.idx,
						i,
						uuidToStripes[file.uuid],
					)
				}
			}
		}
		uuidToFile[file.uuid] = file
	}

	childToParent := make(map[string]string)
	for _, childRelation := range childRelations {
		parentUUID := childRelation.parentUUID
		childUUID := childRelation.childUUID
		if parent, exists := uuidToFile[parentUUID]; exists {
			if parent.typ != fileTypeDirectory {
				log.Fatalf(
					ctx,
					"ChildRelation %v: %s not a directory",
					childRelation,
					parentUUID,
				)
			}
		} else {
			log.Fatalf(
				ctx,
				"ChildRelation %v: %s parent does not exist in files",
				childRelation,
				parentUUID,
			)
		}
		if _, exists := uuidToFile[childUUID]; !exists {
			log.Fatalf(
				ctx,
				"ChildRelation %v: %s child does not exist in files",
				childRelation,
				childUUID,
			)
		}

		if _, exists := childToParent[childUUID]; exists {
			log.Fatalf(
				ctx,
				"ChildRelation %v: %s appears in multiple parents",
				childRelation,
				childUUID,
			)
		}

		childToParent[childUUID] = parentUUID
	}

	// Verify every file has a parent.
	for uuid := range uuidToFile {
		if parentUUID, exists := childToParent[uuid]; !exists {
			if uuid != getRoot() {
				log.Fatalf(ctx, "%v is parentless", uuid)
			}
		} else if uuid == getRoot() {
			log.Fatal(ctx, getRoot(), " should not have a parent ", parentUUID)
		}
	}

	for _, stripe := range stripes {
		_, ok := uuidToFile[stripe.uuid]
		if !ok {
			log.Fatalf(
				ctx,
				"File uuid %s corresponding to stripe %d not found",
				stripe.uuid,
				stripe.idx,
			)
		}
	}
	return true, nil
}

func createFileAndRecordOp(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	id string,
	fileType fileType,
	opLog *opLog,
) {
	var modifiedUUID string
	if err := rd.ExecuteSQLTx(
		ctx,
		func(tx *sql.Tx) error {
			var err error
			modifiedUUID, err = createFile(ctx, tx, getRoot(), id, fileType)
			return err
		},
	); err != nil {
		if _, ok := err.(*crdb.AmbiguousCommitError); ok {
			opLog.markAmbiguous(modifiedUUID)
			return
		}
		log.Fatalf(ctx, "Database - %v %v %v", id, opTypeMkdir, err)
	}
	if modifiedUUID != "" {
		opLog.append(
			opDesc{
				operation:    opTypeMkdir,
				modifiedUUID: modifiedUUID,
				parentUUID:   getRoot(),
				id:           id,
				fileType:     fileType,
			},
		)
	}
}

func removeFileAndRecordOp(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	id string,
	fileType fileType,
	opLog *opLog,
) {
	var modifiedUUID string
	if err := rd.ExecuteSQLTx(
		ctx,
		func(tx *sql.Tx) error {
			removed, err := removeRandomFile(ctx, tx)
			if removed != nil {
				modifiedUUID = removed.uuid
			} else {
				modifiedUUID = ""
			}
			return err
		},
	); err != nil {
		if _, ok := err.(*crdb.AmbiguousCommitError); ok {
			opLog.markAmbiguous(modifiedUUID)
			return
		}
		log.Fatalf(ctx, "Database - %v %v %v", id, opTypeRmdir, err)
	}
	if modifiedUUID != "" {
		opLog.append(
			opDesc{
				operation:    opTypeRmdir,
				modifiedUUID: modifiedUUID,
				fileType:     fileType,
			},
		)
	}
}

func writeFileAndRecordOp(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	id string,
	opLog *opLog,
) {
	var modifiedUUID string
	var modifiedStripe *stripe
	if err := rd.ExecuteSQLTx(
		ctx,
		func(tx *sql.Tx) error {
			var err error
			var written *file
			written, modifiedStripe, err = writeRandomFile(ctx, tx)
			if written != nil {
				modifiedUUID = written.uuid
			} else {
				modifiedUUID = ""
			}
			return err
		},
	); err != nil {
		if _, ok := err.(*crdb.AmbiguousCommitError); ok {
			opLog.markAmbiguous(modifiedUUID)
			return
		}
		log.Fatalf(ctx, "Database - %v %v %v", id, opTypeWrite, err)
	}
	if modifiedUUID != "" && modifiedStripe != nil {
		opLog.append(
			opDesc{
				operation:      opTypeWrite,
				modifiedUUID:   modifiedUUID,
				modifiedStripe: modifiedStripe,
			},
		)
	}
}

func runRandomOp(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	id string,
	log *opLog,
) {

	// Increasing the chances to create, write than to remove
	weights := []float64{30.0, 30.0, 20.0, 10.0, 10.0}
	opTypes := []opType{opTypeMkdir, opTypeMknod, opTypeWrite, opTypeRmdir, opTypeUnlink}

	chosenOpType := opTypes[randutil.WeightedChoice(weights)]

	switch chosenOpType {
	case opTypeMkdir:
		createFileAndRecordOp(ctx, rd, id, fileTypeDirectory, log)
	case opTypeRmdir:
		removeFileAndRecordOp(ctx, rd, id, fileTypeDirectory, log)
	case opTypeMknod:
		createFileAndRecordOp(ctx, rd, id, fileTypeFile, log)
	case opTypeWrite:
		writeFileAndRecordOp(ctx, rd, id, log)
	case opTypeUnlink:
		removeFileAndRecordOp(ctx, rd, id, fileTypeFile, log)
	}
}

func runWorkerLoop(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	workerI int,
	done chan struct{},
	wg *sync.WaitGroup,
	opLog *opLog,
) {
	for i := 0; ; i++ {
		select {
		case <-done:
			wg.Done()
			log.Flush()
			return
		default:
			runRandomOp(ctx, rd, fmt.Sprintf("%d_%d", workerI, i), opLog)
		}
	}
}

// executeTxOrDie expects an idempotent function/read-only function to execute a given number of times
func executeTxOrDie(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	fn func(tx *sql.Tx) error,
) {
	for i := 0; i < ambiguousCommitRetryCount; i++ {
		if err := rd.ExecuteSQLTx(ctx, fn); err != nil {
			if _, ok := err.(*crdb.AmbiguousCommitError); ok {
				continue
			}
			log.Fatal(ctx, err)
		}
		break
	}

}

func runTest(
	ctx context.Context,
	rd *sqlapp.RobustDB,
	numWorkers int,
	durationSecs int,
) {
	opLog := newOpLog()
	// Perform random ops and consistency checks in parallel.
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go runWorkerLoop(ctx, rd, i, done, &wg, opLog)
	}

	duration := time.Duration(durationSecs) * time.Second
	for startTime := time.Now(); time.Since(startTime) < duration; {
		if passed, err := checkConsistency(ctx, rd); !passed || err != nil {
			log.Fatalf(ctx, "Consistency failed with error :- %v", err)
		}
	}
	close(done)
	wg.Wait()
	log.Infof(ctx, "log size: %d", len(opLog.operations))
	log.Infof(ctx, "number of ambiguous operations: %d", opLog.ambiguousOps)
	rd.PrintStats()
	if passed, err := checkConsistency(ctx, rd); !passed || err != nil {
		log.Fatalf(ctx, "Consistency failed with error :- %v", err)
	}
	checkProgress(ctx, rd, opLog, durationSecs, numWorkers)
}

func doesFileExists(ctx context.Context, tx *sql.Tx, uuid string) (bool, error) {
	f, err := findFile(ctx, tx, uuid)
	return f != nil, err
}

func main() {
	ctx := context.Background()
	cockroachIPAddressesCSV :=
		flag.String(
			sqlapp.CockroachIPAddressesCSV,
			"localhost",
			"Comma-separated list of CockroachDb nodes' IP addresses."+
				" The IP addresses can optionally have ports specified in the "+
				"format <ip1>:<port1>,<ip2>:<port2>",
		)
	durationSecs := flag.Int(
		sqlapp.DurationSecs,
		10,
		"Duration (in seconds) to run.")
	numWorkers := flag.Int(sqlapp.NumWorkers, 10, "Concurrent workers")
	installSchema := flag.Bool(
		sqlapp.InstallSchema,
		false,
		"Install schema for this test and then exit.")
	certsDir := flag.String(
		sqlapp.CertsDir,
		"",
		"Directory containing TLS certificates.")
	insecure := flag.Bool(sqlapp.Insecure, true, "Connect to CockroachDB in insecure mode")
	flag.Parse()
	defer log.Flush()
	log.Infof(ctx, "numWorkers: %v", *numWorkers)
	if len(*cockroachIPAddressesCSV) == 0 {
		log.Fatalf(ctx, "hostnames cannot be empty: %s", *cockroachIPAddressesCSV)
	}
	cockroachIPAddrStrs := strings.Split(*cockroachIPAddressesCSV, ",")
	cockroachSocketAddrs := make([]crdbutil.SocketAddress, len(cockroachIPAddrStrs))
	for i, s := range cockroachIPAddrStrs {
		a, err := crdbutil.ParseSocketAddress(s)
		if err != nil {
			log.Fatal(ctx, err)
		}
		cockroachSocketAddrs[i] = a
	}
	dbs, err :=
		sqlutil.DBs(
			ctx,
			cockroachSocketAddrs,
			databaseName,
			*certsDir,
			*insecure,
			!*installSchema,
		)
	if err != nil {
		log.Fatal(ctx, err)
	}
	// dbs will be closed when process exits
	rd := sqlapp.NewRobustSQLDB(dbs, sqlapp.RetryOnConnectionError)
	defer rd.PrintStats()

	if *installSchema {
		initializeTables(ctx, rd.RandomSQLDB(ctx))
	} else {
		// Create the root with retry in idempotent manner
		executeTxOrDie(
			ctx,
			rd,
			func(tx *sql.Tx) error {
				rootExists, err := doesFileExists(ctx, tx, getRoot())
				if err != nil {
					return err
				}
				if rootExists {
					return nil
				}
				return execOneRow(
					ctx,
					tx,
					"INSERT INTO files (uuid, type, size, sub_shard, shard) VALUES ($1, $2, $3, $4, $5)",
					"Creating root /",
					getRoot(),
					fileTypeDirectory,
					4096,
					numBuckets,
					*shard,
				)
			},
		)
		runTest(ctx, rd, *numWorkers, *durationSecs)
	}
}
