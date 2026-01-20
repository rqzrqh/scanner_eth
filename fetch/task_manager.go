package fetch

import (
	"container/list"
	"fmt"
	"os"

	"golang.org/x/xerrors"
)

type TaskManager struct {
	maxTaskCount int
	taskId       int
	pendingFetch *list.List // soft in height
	inFetch      map[uint64]struct{}
	inStage      map[uint64]struct{}
}

func NewTaskManager(maxTaskCount int) *TaskManager {
	return &TaskManager{
		maxTaskCount: maxTaskCount,
		taskId:       0,
		pendingFetch: list.New(),
		inFetch:      make(map[uint64]struct{}),
		inStage:      make(map[uint64]struct{}),
	}
}

func (tm *TaskManager) getTask() (uint64, error) {
	if tm.pendingFetch.Len() != 0 {
		e := tm.pendingFetch.Front()
		height := e.Value.(uint64)
		return height, nil
	}

	return 0, xerrors.New("no valid task")
}

func (tm *TaskManager) popTask() (int, uint64, error) {
	if tm.pendingFetch.Len() != 0 {
		e := tm.pendingFetch.Front()
		height := e.Value.(uint64)
		tm.pendingFetch.Remove(e)
		tm.inFetch[height] = struct{}{}
		tm.taskId++
		return tm.taskId, height, nil
	}

	return 0, 0, xerrors.New("no valid task")
}

func (tm *TaskManager) clear() {
	for e := tm.pendingFetch.Front(); e != nil; e = e.Next() {
		tm.pendingFetch.Remove(e)
	}

	tm.inFetch = nil
	tm.inFetch = make(map[uint64]struct{})
	tm.inStage = nil
	tm.inStage = make(map[uint64]struct{})
}

func (tm *TaskManager) extendTask(startHeight uint64, endHeight uint64) error {
	currentTaskCount := tm.pendingFetch.Len() + len(tm.inFetch) + len(tm.inStage)
	if currentTaskCount >= tm.maxTaskCount {
		return xerrors.New("out of maxTaskCount")
	}

	leftTaskCapacity := uint64(tm.maxTaskCount) - uint64(currentTaskCount)
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
			existMaxHeight := tm.pendingFetch.Back().Value.(uint64)
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

		tm.pendingFetch.PushBack(h)
	}

	return nil
}

func (tm *TaskManager) fetchSuccess(height uint64) {
	_, exist := tm.inFetch[height]
	if !exist {
		fmt.Println("fetch success height not found", height)
		os.Exit(0)
	}

	delete(tm.inFetch, height)
	tm.inStage[height] = struct{}{}
}

func (tm *TaskManager) processSuccess(height uint64) {
	_, exist := tm.inStage[height]
	if !exist {
		fmt.Println("process success height not found", height)
		os.Exit(0)
	}

	delete(tm.inStage, height)
}
