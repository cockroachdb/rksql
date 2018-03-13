package job

import (
	"math/rand"

	"github.com/jinzhu/gorm"
)

const (
	queuedStr    string = "QUEUED"
	runningStr   string = "RUNNING"
	toUndoStr    string = "TO_UNDO"
	undoingStr   string = "UNDOING"
	succeededStr string = "SUCCEEDED"
	failedStr    string = "FAILED"
)

type state interface {
	instance() *Instance
	expectedStatuses() map[string]bool
	nextStatus() string
	nextNodeID(currentNode string) *string
	nextIsTerminal() bool
	createNextState(*Instance) state
	runHook(db *gorm.DB, numWorkers int) error
}

type queued struct {
	instanceObj *Instance
}

var _ state = (*queued)(nil)

func (q *queued) instance() *Instance                     { return q.instanceObj }
func (q *queued) nextStatus() string                      { return runningStr }
func (q *queued) nextNodeID(currentNodeID string) *string { return &currentNodeID }
func (q *queued) nextIsTerminal() bool                    { return false }
func (q *queued) expectedStatuses() map[string]bool       { return map[string]bool{queuedStr: true} }
func (q *queued) createNextState(instance *Instance) state {
	return &running{instance}
}
func (q *queued) runHook(db *gorm.DB, _ int) error { return nil }

type running struct {
	instanceObj *Instance
}

var _ state = (*running)(nil)

func (r *running) instance() *Instance                     { return r.instanceObj }
func (r *running) nextStatus() string                      { return succeededStr }
func (r *running) nextNodeID(currentNodeID string) *string { return &currentNodeID }
func (r *running) nextIsTerminal() bool                    { return true }
func (r *running) expectedStatuses() map[string]bool       { return map[string]bool{runningStr: true} }
func (r *running) createNextState(instance *Instance) state {
	return nil
}
func (r *running) runHook(db *gorm.DB, numWorkers int) error {
	aid := artifactID(r.instance())
	var artifact = Artifact{aid, rand.Intn(numWorkers), aid}
	err := db.Create(&artifact).Error
	return err
}

type enteringToUndo struct {
	instanceObj *Instance
}

var _ state = (*enteringToUndo)(nil)

func (e *enteringToUndo) instance() *Instance                     { return e.instanceObj }
func (e *enteringToUndo) nextStatus() string                      { return toUndoStr }
func (e *enteringToUndo) nextNodeID(currentNodeID string) *string { return nil }
func (e *enteringToUndo) nextIsTerminal() bool                    { return false }
func (e *enteringToUndo) expectedStatuses() map[string]bool {
	return map[string]bool{runningStr: true, undoingStr: true}
}
func (e *enteringToUndo) createNextState(instance *Instance) state {
	return &toUndo{instance}
}

func (e *enteringToUndo) runHook(db *gorm.DB, _ int) error { return nil }

type toUndo struct {
	instanceObj *Instance
}

var _ state = (*toUndo)(nil)

func (t *toUndo) instance() *Instance                     { return t.instanceObj }
func (t *toUndo) nextStatus() string                      { return undoingStr }
func (t *toUndo) nextNodeID(currentNodeID string) *string { return &currentNodeID }
func (t *toUndo) nextIsTerminal() bool                    { return false }
func (t *toUndo) expectedStatuses() map[string]bool       { return map[string]bool{toUndoStr: true} }
func (t *toUndo) createNextState(instance *Instance) state {
	return &undoing{instance}
}
func (t *toUndo) runHook(db *gorm.DB, _ int) error { return nil }

type undoing struct {
	instanceObj *Instance
}

var _ state = (*undoing)(nil)

func (u *undoing) instance() *Instance                     { return u.instanceObj }
func (u *undoing) nextStatus() string                      { return failedStr }
func (u *undoing) nextNodeID(currentNodeID string) *string { return &currentNodeID }
func (u *undoing) nextIsTerminal() bool                    { return true }
func (u *undoing) expectedStatuses() map[string]bool       { return map[string]bool{undoingStr: true} }
func (u *undoing) createNextState(instance *Instance) state {
	return nil
}
func (u *undoing) runHook(db *gorm.DB, numWorkers int) error {
	aid := artifactID(u.instance())
	var artifact = Artifact{aid, rand.Intn(numWorkers), aid}
	err := db.Delete(&artifact).Error
	return err
}
