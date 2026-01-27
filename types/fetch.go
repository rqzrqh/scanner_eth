package types

import "time"

type FetchResult struct {
	NodeId      int
	TaskId      int
	ForkVersion uint64
	Height      uint64
	FullBlock   *FullBlock
	Events      []EventItem
	CostTime    time.Duration
}

type EventItem struct {
	eventType string
	data      map[string]interface{}
}
