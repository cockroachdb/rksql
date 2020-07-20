package main

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/rksql/pkg/util/log"
	"github.com/pkg/errors"
)

type opDesc struct {
	operation      opType
	modifiedUUID   string
	modifiedStripe *stripe
	parentUUID     string
	id             string
	fileType       fileType
}

type fileSet map[string]bool

type opLog struct {
	operations     []opDesc
	lock           *sync.Mutex
	ambiguousFiles fileSet
	ambiguousOps   int
}

type replayedLog struct {
	uuidToFile    map[string]*file
	childToParent map[string]*childRelation
	parentToChild map[string][]*childRelation
	uuidToStripes map[string][]*stripe
}

func newOpLog() *opLog {
	return &opLog{
		operations:     []opDesc(nil),
		lock:           new(sync.Mutex),
		ambiguousFiles: make(map[string]bool),
	}
}

func (log *opLog) append(le opDesc) {
	log.lock.Lock()
	defer log.lock.Unlock()
	log.operations = append(log.operations, le)
}

func (log *opLog) markAmbiguous(uuid string) {
	log.ambiguousOps++
	log.ambiguousFiles[uuid] = true
}

// opLogScorter - type to sort
type opLogSorter []opDesc

func (s opLogSorter) Len() int {
	return len(s)
}

func (s opLogSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// to sort the opDesc on the basis of UUIDs, so that same UUIDs come together,
// then getting the create first, then all the writes and then delete
func (s opLogSorter) Less(i, j int) bool {
	isCreate := func(op opDesc) bool {
		return op.operation == opTypeMkdir || op.operation == opTypeMknod
	}
	isDelete := func(op opDesc) bool {
		return op.operation == opTypeRmdir || op.operation == opTypeUnlink
	}
	if s[i].modifiedUUID < s[j].modifiedUUID {
		return true
	}
	if s[j].modifiedUUID < s[i].modifiedUUID {
		return false
	}
	if isCreate(s[i]) || isDelete(s[j]) {
		return true
	}
	if isDelete(s[i]) || isCreate(s[j]) {
		return false
	}
	return s[i].modifiedStripe.idx < s[j].modifiedStripe.idx
}

func newEmptyReplayedLogWithRoot() *replayedLog {
	r := replayedLog{
		uuidToFile:    make(map[string]*file),
		childToParent: make(map[string]*childRelation),
		parentToChild: make(map[string][]*childRelation),
		uuidToStripes: make(map[string][]*stripe)}

	r.uuidToFile[getRoot()] = &file{uuid: getRoot(), typ: fileTypeDirectory, size: 4096}
	return &r
}

func playLogInMem(ctx context.Context, opLog *opLog) *replayedLog {
	r := newEmptyReplayedLogWithRoot()
	sort.Sort(opLogSorter(opLog.operations))
	for _, le := range opLog.operations {
		if opLog.ambiguousFiles[le.modifiedUUID] {
			// Skip log entries for ambiguous files
			continue
		}
		var err error
		switch le.operation {
		case opTypeMknod:
			err = r.createFile(
				le.modifiedUUID,
				le.parentUUID,
				le.id,
				le.fileType,
			)
		case opTypeMkdir:
			err = r.createFile(
				le.modifiedUUID,
				le.parentUUID,
				le.id,
				le.fileType,
			)
		case opTypeWrite:
			err = r.writeFile(le.modifiedUUID, le.modifiedStripe)
		case opTypeUnlink:
			err = r.removeFile(le.modifiedUUID, le.fileType)
		case opTypeRmdir:
			err = r.removeFile(le.modifiedUUID, le.fileType)
		}
		if err != nil {
			log.Fatalf(ctx, "InMem - %v %v %v", le.operation, le.id, err)
		}
	}
	return r
}

func (r *replayedLog) createFile(
	UUID string, parentUUID string, id string, fileType fileType,
) error {
	if fileType == fileTypeFile {
		r.uuidToFile[UUID] = &file{uuid: UUID, typ: fileType, size: 0}
	} else {
		r.uuidToFile[UUID] = &file{uuid: UUID, typ: fileType, size: 4096}
	}
	if UUID != getRoot() {
		relation := &childRelation{
			parentUUID: parentUUID,
			name:       id,
			childUUID:  UUID,
		}
		//TODO may be have to check for ok and loop
		r.childToParent[UUID] = relation
		r.parentToChild[parentUUID] = append(r.parentToChild[parentUUID], relation)
	}
	return nil
}

func (r *replayedLog) removeFile(UUID string, fileType fileType) error {
	if _, ok := r.uuidToFile[UUID]; !ok {
		return errors.New("UUID " + UUID + " not present")
	}
	if fileType == fileTypeFile {
		delete(r.uuidToStripes, UUID)
	} else {
		//TODO Handle the case for children files
	}
	parent := r.childToParent[UUID].parentUUID
	for i, siblings := range r.parentToChild[parent] {
		if siblings.childUUID == UUID {
			r.parentToChild[parent] = append(
				r.parentToChild[parent][:i], r.parentToChild[parent][i+1:]...)
			break
		}
	}
	delete(r.childToParent, UUID)
	delete(r.uuidToFile, UUID)
	return nil
}

func (r *replayedLog) writeFile(UUID string, stripeInp *stripe) error {
	if _, ok := r.uuidToFile[UUID]; !ok {
		return errors.New("UUID " + UUID + " not present")
	}
	if _, ok := r.uuidToStripes[UUID]; ok {
		r.uuidToStripes[UUID] = append(r.uuidToStripes[UUID],
			&stripe{
				uuid: stripeInp.uuid,
				idx:  stripeInp.idx,
			})
	} else {
		r.uuidToStripes[UUID] = []*stripe{&stripe{
			uuid: stripeInp.uuid,
			idx:  stripeInp.idx,
		}}
	}
	r.uuidToFile[UUID].size++
	return nil
}

func (r *replayedLog) printRecursive(uuid string) {
	if len(r.parentToChild[uuid]) == 0 {
		return
	}
	var childNames []string
	var childUUIDs []string
	for _, child := range r.parentToChild[uuid] {
		childNames = append(childNames, child.name)
		childUUIDs = append(childUUIDs, child.childUUID)
	}
	sort.Strings(childNames)
	// TODO change in case of tree structure directory will need path
	fmt.Println(uuid)
	fmt.Print(childNames, "\n\n")
	for _, childUUID := range childUUIDs {
		if fileTypeDirectory == (r.uuidToFile[childUUID]).typ {
			r.printRecursive(childUUID)
		}
	}
}
