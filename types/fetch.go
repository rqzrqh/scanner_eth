package types

import "time"

type FetchResult struct {
	NodeId      int
	TaskId      int
	ForkVersion uint64
	Height      uint64
	FullBlock   *FullBlock
	CostTime    time.Duration
}
