package fetch

import (
	"fmt"
	"os"

	"github.com/elliotchance/orderedmap/v3"
	"golang.org/x/xerrors"
)

type TaskManager struct {
	maxUnorganizedBlockCount int
	taskId                   int
	pendingFetch             *orderedmap.OrderedMap[uint64, struct{}]
	inFetch                  map[uint64]struct{}
	inStage                  map[uint64]struct{}
}

func NewTaskManager(maxUnorganizedBlockCount int) *TaskManager {
	return &TaskManager{
		maxUnorganizedBlockCount: maxUnorganizedBlockCount,
		taskId:                   0,
		pendingFetch:             orderedmap.NewOrderedMap[uint64, struct{}](),
		inFetch:                  make(map[uint64]struct{}),
		inStage:                  make(map[uint64]struct{}),
	}
}

func (tm *TaskManager) getTask() (uint64, error) {

	if tm.pendingFetch.Len() != 0 {
		e := tm.pendingFetch.Front()
		height := e.Key
		return height, nil
	}

	return 0, xerrors.New("no valid task")
}

func (tm *TaskManager) popTask() (int, uint64, error) {
	if tm.pendingFetch.Len() != 0 {
		e := tm.pendingFetch.Front()
		height := e.Key
		tm.pendingFetch.Delete(height)
		tm.inFetch[height] = struct{}{}
		tm.taskId++
		return tm.taskId, height, nil
	}

	return 0, 0, xerrors.New("no valid task")
}

func (tm *TaskManager) extendTask(startHeight uint64, endHeight uint64) error {
	currentTaskCount := tm.pendingFetch.Len() + len(tm.inFetch) + len(tm.inStage)
	if currentTaskCount >= tm.maxUnorganizedBlockCount {
		return xerrors.New("out of maxTaskCount")
	}

	leftTaskCapacity := uint64(tm.maxUnorganizedBlockCount) - uint64(currentTaskCount)
	gap := endHeight - startHeight

	count := gap
	if gap > leftTaskCapacity {
		count = leftTaskCapacity
	} else {
		count = gap
	}

	for i := uint64(1); i <= count; i++ {
		h := startHeight + i
		if tm.pendingFetch.Len() > 0 {
			existMaxHeight := tm.pendingFetch.Back().Key
			if h <= existMaxHeight {
				continue
			}
		}

		if _, exist := tm.inFetch[h]; exist {
			continue
		}

		if _, exist := tm.inStage[h]; exist {
			continue
		}

		tm.pendingFetch.Set(h, struct{}{})
	}

	return nil
}

func (tm *TaskManager) fetchSuccess(height uint64) {
	_, exist := tm.inFetch[height]
	if !exist {
		fmt.Println("fetch success height not found", height)
		os.Exit(0)
	}

	// move to inStage
	delete(tm.inFetch, height)
	tm.inStage[height] = struct{}{}
}

func (tm *TaskManager) fetchFailed(height uint64) {
	_, exist := tm.inFetch[height]
	if !exist {
		fmt.Println("fetch success height not found", height)
		os.Exit(0)
	}

	// move to pendingFetch
	delete(tm.inFetch, height)
	tm.pendingFetch.Set(height, struct{}{})
}

func (tm *TaskManager) processSuccess(height uint64) {
	_, exist := tm.inStage[height]
	if !exist {
		fmt.Println("process success height not found", height)
		os.Exit(0)
	}

	delete(tm.inStage, height)
}

func (tm *TaskManager) clear() {
	for e := tm.pendingFetch.Front(); e != nil; e = e.Next() {
		tm.pendingFetch.Delete(e.Key)
	}

	tm.inFetch = nil
	tm.inFetch = make(map[uint64]struct{})
	tm.inStage = nil
	tm.inStage = make(map[uint64]struct{})
}
