package fetch

import (
	"scanner_eth/data"
	"scanner_eth/protocol"
	"scanner_eth/store"
	"sync"
)

// 事件扫描高度项状态
const (
	EventHeightStateSyncing = iota + 1 // 同步中
	EventHeightStateNoData             // 无数据（同步完成但无区块）
	EventHeightStateHasData            // 有数据（待衔接写库）
	EventHeightStateWriting            // 写入中
)

// EventBlockData 合并三种区块表示：原始 FullBlock、存储块、协议块
type EventBlockData struct {
	FullBlock         *data.FullBlock
	StorageFullBlock  *store.StorageFullBlock
	ProtocolFullBlock *protocol.FullBlock
}

type EventHeightItem struct {
	State      int
	SyncTaskId int // 该高度同步任务的唯一 ID
	BlockData  *EventBlockData // 有数据时包含 FullBlock / 存储块 / 协议块，无数据为 nil
}

// EventHeightManager 管理事件扫描区间内每个高度的状态与数据
type EventHeightManager struct {
	mu             sync.RWMutex
	items          map[uint64]*EventHeightItem
	nextSyncTaskId int
}

func NewEventHeightManager() *EventHeightManager {
	return &EventHeightManager{
		items: make(map[uint64]*EventHeightItem),
	}
}

// Get 获取某高度的项，exists 表示是否已存在
func (ehm *EventHeightManager) Get(height uint64) (item *EventHeightItem, exists bool) {
	ehm.mu.RLock()
	defer ehm.mu.RUnlock()
	item, exists = ehm.items[height]
	return item, exists
}

// GetOrCreate 不存在则新建并返回 created=true，状态为 Syncing，并分配同步任务 ID
func (ehm *EventHeightManager) GetOrCreate(height uint64) (item *EventHeightItem, created bool) {
	ehm.mu.Lock()
	defer ehm.mu.Unlock()
	if item, ok := ehm.items[height]; ok {
		return item, false
	}
	ehm.nextSyncTaskId++
	item = &EventHeightItem{
		State:      EventHeightStateSyncing,
		SyncTaskId: ehm.nextSyncTaskId,
	}
	ehm.items[height] = item
	return item, true
}

// SetResult 同步协程完成后调用：有数据设为 HasData 并写入合并的 BlockData，无数据设为 NoData
func (ehm *EventHeightManager) SetResult(height uint64, blockData *EventBlockData) {
	ehm.mu.Lock()
	defer ehm.mu.Unlock()
	item, ok := ehm.items[height]
	if !ok {
		return
	}
	if blockData != nil {
		item.State = EventHeightStateHasData
		item.BlockData = blockData
	} else {
		item.State = EventHeightStateNoData
		item.BlockData = nil
	}
}

// SetState 设置状态（如写入中）
func (ehm *EventHeightManager) SetState(height uint64, state int) {
	ehm.mu.Lock()
	defer ehm.mu.Unlock()
	if item, ok := ehm.items[height]; ok {
		item.State = state
	}
}
