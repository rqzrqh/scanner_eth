package task

func DispatchSyncTask(
	task *SyncTask,
	stopCh <-chan struct{},
	handleBody func(hash string, stopCh <-chan struct{}) bool,
	handleHeaderHash func(hash string) bool,
	handleHeaderHeight func(height uint64) bool,
) bool {
	if task == nil {
		return true
	}
	if task.Kind == SyncTaskKindBody || (task.Kind == 0 && task.Hash != "") {
		if handleBody == nil {
			return false
		}
		return handleBody(task.Hash, stopCh)
	}
	if task.Kind == SyncTaskKindHeaderHash {
		if handleHeaderHash == nil {
			return false
		}
		return handleHeaderHash(task.Hash)
	}
	if handleHeaderHeight == nil {
		return false
	}
	return handleHeaderHeight(task.Height)
}
