package logreport

import (
	"bufio"
	"fmt"
	"html/template"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	Title string
	Since time.Time
	Until time.Time
	Now   time.Time
}

type Report struct {
	Title         string
	GeneratedAt   time.Time
	Sources       []string
	WindowStart   time.Time
	WindowEnd     time.Time
	LinesTotal    uint64
	LinesParsed   uint64
	Warnings      []string
	Summary       Summary
	Nodes         []NodeStats
	Methods       []MethodStats
	TaskPool      TaskPoolStats
	BodySync      BodySyncStats
	NodeSelection NodeSelectionStats
	ScanStages    []ScanStageStats
	Store         StoreStats
	RuntimeLatest RuntimeSnapshot
	RuntimeSeries []RuntimeSnapshot
	Anomalies     []Anomaly
}

type Summary struct {
	NodeCount         int
	TotalNodeRequests uint64
	TotalNodeFailures uint64
	NodeFailureRate   string
	TaskSucceeded     uint64
	TaskFailed        uint64
	TaskSuccessRate   string
	StoreSucceeded    uint64
	StoreFailed       uint64
	StoreSuccessRate  string
	RemoteLatest      uint64
	StoredCount       uint64
	Lag               string
}

type BodySyncStats struct {
	Started     uint64
	Succeeded   uint64
	Failed      uint64
	SuccessRate string
	AvgCostUS   uint64
	MaxCostUS   uint64
	AvgTxs      uint64
	LastHeight  uint64
	LastHash    string
	costTotalUS uint64
	costSamples uint64
	txTotal     uint64
	txSamples   uint64
}

type NodeSelectionStats struct {
	Events        uint64
	ValidNodes    uint64
	Disabled      uint64
	NotReady      uint64
	Cooldown      uint64
	HeightTooLow  uint64
	RemoteUnknown uint64
	NilNodes      uint64
	NoValidNodes  uint64
}

type NodeStats struct {
	ID              int
	Requests        uint64
	Successes       uint64
	Failures        uint64
	FailureRate     string
	AvgCostUS       uint64
	MaxCostUS       uint64
	Ready           string
	RemoteHeight    uint64
	TopFailure      string
	Methods         []MethodStats
	methods         map[string]*MethodStats
	methodSnapshots map[string]*counterWindow
	costTotalUS     uint64
	costSamples     uint64
	failuresByOp    map[string]uint64
}

type MethodStats struct {
	Name        string
	NodeID      int
	Requests    uint64
	Successes   uint64
	Failures    uint64
	Retries     uint64
	FailureRate string
	AvgCostUS   uint64
	MaxCostUS   uint64
	costTotalUS uint64
	costSamples uint64
}

type TaskKindStats struct {
	Name        string
	Enqueued    uint64
	Dequeued    uint64
	Succeeded   uint64
	Failed      uint64
	Retried     uint64
	Dropped     uint64
	Tracked     uint64
	SuccessRate string
}

type TaskPoolStats struct {
	Totals              TaskKindStats
	Kinds               []TaskKindStats
	PendingHigh         uint64
	PendingNormal       uint64
	Tracked             uint64
	WorkerCount         uint64
	MaxRetry            uint64
	HighQueueCapacity   uint64
	NormalQueueCapacity uint64
}

type ScanStageStats struct {
	Stage           string
	Runs            uint64
	Failures        uint64
	SuccessRate     string
	LastTarget      string
	LastTargetCount uint64
	LastDuration    string
	LastError       string
}

type StoreStats struct {
	Submitted             uint64
	Skipped               uint64
	Succeeded             uint64
	Failed                uint64
	Canceled              uint64
	QueuePending          uint64
	Processing            uint64
	SkippedMissingBody    uint64
	SkippedParentNotReady uint64
	FailedDB              uint64
	SuccessRate           string
}

type RuntimeSnapshot struct {
	Time              time.Time
	RemoteLatest      uint64
	NodeReady         uint64
	NodeTotal         uint64
	BlocktreeStart    uint64
	BlocktreeEnd      uint64
	Linked            uint64
	Leaves            uint64
	Branches          uint64
	Orphans           uint64
	OrphanParents     uint64
	StagingBlocks     uint64
	PendingHeaders    uint64
	PendingBodies     uint64
	CompleteBlocks    uint64
	StoredCount       uint64
	TaskPendingHigh   uint64
	TaskPendingNormal uint64
	TaskTracked       uint64
	TaskSucceeded     uint64
	TaskFailed        uint64
	TaskRetried       uint64
	StoreSubmitted    uint64
	StoreSucceeded    uint64
	StoreFailed       uint64
	StoreSkipped      uint64
	StoreQueuePending uint64
}

type Anomaly struct {
	Level      string
	Category   string
	Message    string
	Count      uint64
	Suggestion string
}

type analyzer struct {
	report       *Report
	nodes        map[int]*NodeStats
	methodTotals map[string]*MethodStats
	taskWindow   *taskPoolWindow
	storeWindow  *storeWindow
	scanStages   map[string]*ScanStageStats
	anomalyIndex map[string]*Anomaly
	now          time.Time
}

type counterWindow struct {
	seen    bool
	samples uint64
	first   uint64
	last    uint64
}

type taskPoolSnapshot struct {
	total TaskKindStats
	body  TaskKindStats
	hh    TaskKindStats
	hs    TaskKindStats
}

type taskPoolWindow struct {
	seen    bool
	samples uint64
	first   taskPoolSnapshot
	last    taskPoolSnapshot
}

type storeSnapshot struct {
	Submitted             uint64
	Skipped               uint64
	Succeeded             uint64
	Failed                uint64
	Canceled              uint64
	QueuePending          uint64
	Processing            uint64
	SkippedMissingBody    uint64
	SkippedParentNotReady uint64
	FailedDB              uint64
}

type storeWindow struct {
	seen    bool
	samples uint64
	first   storeSnapshot
	last    storeSnapshot
}

var (
	ansiRE             = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	timePrefixRE       = regexp.MustCompile(`^([A-Z][a-z]{2}\s+\d{1,2}\s+\d\d:\d\d:\d\d\.\d{3})`)
	intPairRE          = regexp.MustCompile(`([A-Za-z_]+):([0-9]+)`)
	taskPoolRE         = regexp.MustCompile(`task pool stats enqueued:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) dequeued:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) succeeded:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) failed:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) retried:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) dropped:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) pending_high:(\d+) pending_normal:(\d+) tracked:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) workers:(\d+) max_retry:(\d+)`)
	scanStageRE        = regexp.MustCompile(`scan stage event stage:([^ ]+) target:([^ ]*) ?(?:target_count:(\d+) )?success:(true|false) duration:([^ ]+)(?: err:(.*?))?(?: @|$)`)
	nodeRPCStatsRE     = regexp.MustCompile(`\[nodeOperator RPC\] nodeId=(\d+) (.*?)(?: @|$)`)
	fullRPCSuccessRE   = regexp.MustCompile(`fetch full block rpc success\. op:([^ ]+) nodeId:(\d+) taskId:[^ ]+ height:[^ ]+ attempts:(\d+) retries:[^ ]+ selected_node_ids:[^ ]* cost_us:(\d+)`)
	fullRPCFailedRE    = regexp.MustCompile(`fetch full block rpc failed\. op:([^ ]+) nodeId:(\d+) taskId:[^ ]+ height:[^ ]+ attempt:(\d+) retries:[^ ]+ err:(.*?)(?: @|$)`)
	fullRPCExhaustedRE = regexp.MustCompile(`fetch full block rpc exhausted retries\. op:([^ ]+) taskId:[^ ]+ height:[^ ]+ attempts:(\d+) retries:[^ ]+ selected_node_ids:([^ ]*) cost_us:(\d+) err:(.*?)(?: @|$)`)
	headerFailRE       = regexp.MustCompile(`fetch header(?: by hash)? failed\. nodeId:(\d+) .*? error:(.*?)(?: height:| hash:| @|$)`)
	headerSuccessRE    = regexp.MustCompile(`fetch header(?: by hash)? success\. nodeId:(\d+) .*? cost:([^ ]+)`)
	bodySyncStartRE    = regexp.MustCompile(`body sync start\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+)`)
	bodySyncSuccessRE  = regexp.MustCompile(`body sync success\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+) node_ids:[^ ]* txs:(\d+) cost_us:(\d+)`)
	bodySyncFailedRE   = regexp.MustCompile(`body sync failed\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+) node_ids:[^ ]* cost_us:(\d+)`)
	bodyNoValidNodesRE = regexp.MustCompile(`body sync no valid nodes\. height:(\d+) hash:([^ ]+)`)
	storeStatsRE       = regexp.MustCompile(`store block worker stats submitted:(\d+) skipped:(\d+) succeeded:(\d+) failed:(\d+) canceled:(\d+) queue_pending:(\d+) processing:(\d+) skipped_missing_body:(\d+) skipped_parent_not_ready:(\d+) failed_db:(\d+)`)
	runtimeHealthRE    = regexp.MustCompile(`runtime health stats remote_latest:(\d+) node_ready:(\d+)/(\d+) blocktree_range:\[(\d+),(\d+)\] linked:(\d+) leaves:(\d+) branches:(\d+) orphans:(\d+) orphan_parents:(\d+) staging_blocks:(\d+) pending_headers:(\d+) pending_bodies:(\d+) complete_blocks:(\d+) stored_count:(\d+) task_pending_high:(\d+) task_pending_normal:(\d+) task_tracked:(\d+) task_succeeded:(\d+) task_failed:(\d+) task_retried:(\d+) store_submitted:(\d+) store_succeeded:(\d+) store_failed:(\d+) store_skipped:(\d+) store_queue_pending:(\d+)`)
	validNodesRE       = regexp.MustCompile(`valid node operators selected\.`)
	duplicateRE        = regexp.MustCompile(`(?i)(duplicate|Error 1062|Duplicate entry)`)
	deadlockRE         = regexp.MustCompile(`(?i)(deadlock|lock wait timeout)`)
)

func AnalyzeFiles(paths []string, opts Options) (*Report, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no input log files")
	}
	a := newAnalyzer(paths, opts)
	for _, path := range paths {
		if err := a.readFile(path, opts); err != nil {
			return nil, err
		}
	}
	a.finish()
	return a.report, nil
}

func Analyze(r io.Reader, source string, opts Options) (*Report, error) {
	a := newAnalyzer([]string{source}, opts)
	if err := a.scan(r, opts); err != nil {
		return nil, err
	}
	a.finish()
	return a.report, nil
}

func newAnalyzer(sources []string, opts Options) *analyzer {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	title := strings.TrimSpace(opts.Title)
	if title == "" {
		title = "scanner_eth Log Analysis Report"
	}
	return &analyzer{
		report: &Report{
			Title:       title,
			GeneratedAt: now,
			Sources:     sources,
		},
		nodes:        make(map[int]*NodeStats),
		methodTotals: make(map[string]*MethodStats),
		taskWindow:   &taskPoolWindow{},
		storeWindow:  &storeWindow{},
		scanStages:   make(map[string]*ScanStageStats),
		anomalyIndex: make(map[string]*Anomaly),
		now:          now,
	}
}

func (a *analyzer) readFile(path string, opts Options) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return a.scan(f, opts)
}

func (a *analyzer) scan(r io.Reader, opts Options) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	for scanner.Scan() {
		raw := scanner.Text()
		a.report.LinesTotal++
		line := normalizeLine(raw)
		ts, hasTS := parseLogTime(line, a.now)
		if hasTS {
			if !opts.Since.IsZero() && ts.Before(opts.Since) {
				continue
			}
			if !opts.Until.IsZero() && ts.After(opts.Until) {
				continue
			}
			a.extendWindow(ts)
		}
		if a.parseLine(line, ts) {
			a.report.LinesParsed++
		}
	}
	return scanner.Err()
}

func normalizeLine(line string) string {
	line = ansiRE.ReplaceAllString(line, "")
	line = strings.ReplaceAll(line, "Âµ", "µ")
	return strings.TrimSpace(line)
}

func parseLogTime(line string, now time.Time) (time.Time, bool) {
	m := timePrefixRE.FindStringSubmatch(line)
	if len(m) != 2 {
		return time.Time{}, false
	}
	value := fmt.Sprintf("%d %s", now.Year(), strings.Join(strings.Fields(m[1]), " "))
	ts, err := time.ParseInLocation("2006 Jan 2 15:04:05.000", value, now.Location())
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func (a *analyzer) extendWindow(ts time.Time) {
	if a.report.WindowStart.IsZero() || ts.Before(a.report.WindowStart) {
		a.report.WindowStart = ts
	}
	if a.report.WindowEnd.IsZero() || ts.After(a.report.WindowEnd) {
		a.report.WindowEnd = ts
	}
}

func (a *analyzer) parseLine(line string, ts time.Time) bool {
	switch {
	case strings.Contains(line, "task pool stats"):
		return a.parseTaskPool(line)
	case strings.Contains(line, "store block worker stats"):
		return a.parseStore(line)
	case strings.Contains(line, "scan stage event stage:"):
		return a.parseScanStage(line)
	case strings.Contains(line, "runtime health stats"):
		return a.parseRuntime(line, ts)
	case strings.Contains(line, "[nodeOperator RPC]"):
		return a.parseNodeRPCStats(line)
	case strings.Contains(line, "fetch full block rpc success"):
		return a.parseFullRPCSuccess(line)
	case strings.Contains(line, "fetch full block rpc failed"):
		return a.parseFullRPCFailed(line)
	case strings.Contains(line, "fetch full block rpc exhausted retries"):
		return a.parseFullRPCExhausted(line)
	case strings.Contains(line, "fetch header failed") || strings.Contains(line, "fetch header by hash failed"):
		return a.parseHeaderFailure(line)
	case strings.Contains(line, "fetch header success") || strings.Contains(line, "fetch header by hash success"):
		return a.parseHeaderSuccess(line)
	case strings.Contains(line, "valid node operators selected"):
		return a.parseValidNodes(line)
	case strings.Contains(line, "body sync start"):
		return a.parseBodySyncStart(line)
	case strings.Contains(line, "body sync success"):
		return a.parseBodySyncSuccess(line)
	case strings.Contains(line, "body sync failed"):
		return a.parseBodySyncFailed(line)
	case strings.Contains(line, "body sync no valid nodes"):
		return a.parseBodyNoValidNodes(line)
	default:
		if duplicateRE.MatchString(line) {
			a.addAnomaly("high", "duplicate_write", "Duplicate write or unique-key conflict logs detected", 1, "Check storage idempotency and duplicate scheduling sources.")
			return true
		}
		if deadlockRE.MatchString(line) {
			a.addAnomaly("high", "db_lock_contention", "Database deadlock or lock-wait timeout detected", 1, "Check store worker concurrency, batch size, and database lock contention.")
			return true
		}
	}
	return false
}

func (a *analyzer) parseTaskPool(line string) bool {
	m := taskPoolRE.FindStringSubmatch(line)
	if len(m) != 33 {
		return false
	}
	vals := parseUintGroups(m[1:])
	s := taskPoolSnapshot{
		total: TaskKindStats{Name: "total", Enqueued: vals[0], Dequeued: vals[4], Succeeded: vals[8], Failed: vals[12], Retried: vals[16], Dropped: vals[20], Tracked: vals[26]},
		body:  TaskKindStats{Name: "body", Enqueued: vals[1], Dequeued: vals[5], Succeeded: vals[9], Failed: vals[13], Retried: vals[17], Dropped: vals[21], Tracked: vals[27]},
		hh:    TaskKindStats{Name: "header_height_sync", Enqueued: vals[2], Dequeued: vals[6], Succeeded: vals[10], Failed: vals[14], Retried: vals[18], Dropped: vals[22], Tracked: vals[28]},
		hs:    TaskKindStats{Name: "header_hash_sync", Enqueued: vals[3], Dequeued: vals[7], Succeeded: vals[11], Failed: vals[15], Retried: vals[19], Dropped: vals[23], Tracked: vals[29]},
	}
	if !a.taskWindow.seen {
		a.taskWindow.first = s
		a.taskWindow.seen = true
	}
	a.taskWindow.samples++
	a.taskWindow.last = s
	a.report.TaskPool.PendingHigh = vals[24]
	a.report.TaskPool.PendingNormal = vals[25]
	a.report.TaskPool.Tracked = vals[26]
	a.report.TaskPool.WorkerCount = vals[30]
	a.report.TaskPool.MaxRetry = vals[31]
	if vals[20] > 0 {
		a.addAnomaly("medium", "taskpool_drop", "Task pool dropped tasks", delta(a.taskWindow.first.total.Dropped, vals[20]), "Check queue capacity, worker count, and RPC throughput.")
	}
	return true
}

func (a *analyzer) parseScanStage(line string) bool {
	m := scanStageRE.FindStringSubmatch(line)
	if len(m) == 0 {
		return false
	}
	stage := m[1]
	stats := a.scanStages[stage]
	if stats == nil {
		stats = &ScanStageStats{Stage: stage}
		a.scanStages[stage] = stats
	}
	stats.Runs++
	if m[4] != "true" {
		stats.Failures++
	}
	stats.LastTarget = m[2]
	stats.LastTargetCount = parseUint(m[3])
	stats.LastDuration = m[5]
	stats.LastError = strings.TrimSpace(m[6])
	if m[4] != "true" {
		a.addAnomaly("medium", "scan_"+stage, "Scan stage failed: "+stage, 1, "Check the latest stage error and upstream task backlog.")
	}
	return true
}

func (a *analyzer) parseStore(line string) bool {
	m := storeStatsRE.FindStringSubmatch(line)
	if len(m) != 11 {
		return false
	}
	vals := parseUintGroups(m[1:])
	s := storeSnapshot{
		Submitted: vals[0], Skipped: vals[1], Succeeded: vals[2], Failed: vals[3], Canceled: vals[4],
		QueuePending: vals[5], Processing: vals[6], SkippedMissingBody: vals[7], SkippedParentNotReady: vals[8], FailedDB: vals[9],
	}
	if !a.storeWindow.seen {
		a.storeWindow.first = s
		a.storeWindow.seen = true
	}
	a.storeWindow.samples++
	a.storeWindow.last = s
	if vals[7] > 0 {
		a.addAnomaly("medium", "store_missing_body", "Store worker skipped blocks with missing bodies", delta(a.storeWindow.first.SkippedMissingBody, vals[7]), "Check whether body fetch is lagging behind branch submission.")
	}
	if vals[8] > 0 {
		a.addAnomaly("medium", "store_parent_not_ready", "Store worker skipped blocks whose parents were not ready", delta(a.storeWindow.first.SkippedParentNotReady, vals[8]), "Check ancestor persistence and branch ordering.")
	}
	if vals[9] > 0 {
		a.addAnomaly("high", "store_db_failed", "Store worker reported database failures", delta(a.storeWindow.first.FailedDB, vals[9]), "Check database error logs, lock waits, and unique-key conflicts.")
	}
	return true
}

func (a *analyzer) parseRuntime(line string, ts time.Time) bool {
	m := runtimeHealthRE.FindStringSubmatch(line)
	if len(m) != 27 {
		return false
	}
	v := parseUintGroups(m[1:])
	s := RuntimeSnapshot{
		Time: ts, RemoteLatest: v[0], NodeReady: v[1], NodeTotal: v[2], BlocktreeStart: v[3], BlocktreeEnd: v[4],
		Linked: v[5], Leaves: v[6], Branches: v[7], Orphans: v[8], OrphanParents: v[9], StagingBlocks: v[10],
		PendingHeaders: v[11], PendingBodies: v[12], CompleteBlocks: v[13], StoredCount: v[14],
		TaskPendingHigh: v[15], TaskPendingNormal: v[16], TaskTracked: v[17], TaskSucceeded: v[18], TaskFailed: v[19], TaskRetried: v[20],
		StoreSubmitted: v[21], StoreSucceeded: v[22], StoreFailed: v[23], StoreSkipped: v[24], StoreQueuePending: v[25],
	}
	a.report.RuntimeSeries = append(a.report.RuntimeSeries, s)
	a.report.RuntimeLatest = s
	if s.NodeTotal > 0 && s.NodeReady < s.NodeTotal {
		a.addAnomaly("medium", "node_unready", "One or more nodes are not ready", s.NodeTotal-s.NodeReady, "Check node rate limits, network errors, or lagging remote height.")
	}
	if s.TaskPendingHigh+s.TaskPendingNormal > 0 {
		a.addAnomaly("low", "task_backlog", "Task pool backlog exists", s.TaskPendingHigh+s.TaskPendingNormal, "Check whether backlog keeps growing over time.")
	}
	return true
}

func (a *analyzer) parseNodeRPCStats(line string) bool {
	m := nodeRPCStatsRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	id := int(parseUint(m[1]))
	node := a.node(id)
	fields := strings.Fields(m[2])
	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}
		method := parts[0]
		count := parseUint(parts[1])
		if node.methodSnapshots == nil {
			node.methodSnapshots = make(map[string]*counterWindow)
		}
		w := node.methodSnapshots[method]
		if w == nil {
			w = &counterWindow{}
			node.methodSnapshots[method] = w
		}
		if !w.seen {
			w.first = count
			w.seen = true
		}
		w.samples++
		w.last = count
	}
	return true
}

func (a *analyzer) parseFullRPCSuccess(line string) bool {
	m := fullRPCSuccessRE.FindStringSubmatch(line)
	if len(m) != 5 {
		return false
	}
	op := m[1]
	nodeID := int(parseUint(m[2]))
	attempts := parseUint(m[3])
	cost := parseUint(m[4])
	a.addNodeMethodEvent(nodeID, op, true, cost)
	if attempts > 1 {
		a.method(nodeID, op).Retries += attempts - 1
		a.methodTotal(op).Retries += attempts - 1
	}
	return true
}

func (a *analyzer) parseFullRPCFailed(line string) bool {
	m := fullRPCFailedRE.FindStringSubmatch(line)
	if len(m) != 5 {
		return false
	}
	op := m[1]
	nodeID := int(parseUint(m[2]))
	a.addNodeMethodEvent(nodeID, op, false, 0)
	a.addNodeFailure(nodeID, op)
	return true
}

func (a *analyzer) parseFullRPCExhausted(line string) bool {
	m := fullRPCExhaustedRE.FindStringSubmatch(line)
	if len(m) != 6 {
		return false
	}
	op := m[1]
	attempts := parseUint(m[2])
	a.methodTotal(op).Retries += attempts
	a.addAnomaly("high", "rpc_exhausted_"+op, "RPC retries exhausted: "+op, 1, "Check node capability, rate limits, and timeouts for this RPC method.")
	return true
}

func (a *analyzer) parseHeaderFailure(line string) bool {
	m := headerFailRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	nodeID := int(parseUint(m[1]))
	op := "FetchBlockHeaderByHeight"
	if strings.Contains(line, "fetch header by hash failed") {
		op = "FetchBlockHeaderByHash"
	}
	a.addNodeMethodEvent(nodeID, op, false, 0)
	a.addNodeFailure(nodeID, classifyError(m[2]))
	return true
}

func (a *analyzer) parseHeaderSuccess(line string) bool {
	m := headerSuccessRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	nodeID := int(parseUint(m[1]))
	op := "FetchBlockHeaderByHeight"
	if strings.Contains(line, "fetch header by hash success") {
		op = "FetchBlockHeaderByHash"
	}
	a.addNodeMethodEvent(nodeID, op, true, durationToMicros(m[2]))
	return true
}

func (a *analyzer) parseValidNodes(line string) bool {
	if !validNodesRE.MatchString(line) {
		return false
	}
	values := keyUintPairs(line)
	selection := &a.report.NodeSelection
	selection.Events++
	selection.ValidNodes += values["valid_nodes"]
	selection.Disabled += values["disabled"]
	selection.NotReady += values["not_ready"]
	selection.Cooldown += values["cooldown"]
	selection.HeightTooLow += values["height_too_low"]
	selection.RemoteUnknown += values["remote_unknown"]
	selection.NilNodes += values["nil_nodes"]
	if values["valid_nodes"] == 0 {
		selection.NoValidNodes++
		a.addAnomaly("high", "node_no_valid_candidates", "Node selection had no valid candidates", 1, "Check node readiness, cooldown state, remote height, and connectivity.")
	}
	if values["height_too_low"] > 0 {
		a.addAnomaly("medium", "node_height_low", "Some nodes were below the target height", values["height_too_low"], "Check lagging nodes or remove long-term stale RPC endpoints.")
	}
	if values["disabled"] > 0 {
		a.addAnomaly("medium", "node_disabled", "Node selection included disabled nodes", values["disabled"], "Check startup chain-info validation failures or manual disable reasons.")
	}
	return true
}

func (a *analyzer) parseBodySyncStart(line string) bool {
	m := bodySyncStartRE.FindStringSubmatch(line)
	if len(m) != 4 {
		return false
	}
	a.report.BodySync.Started++
	a.report.BodySync.LastHeight = parseUint(m[1])
	a.report.BodySync.LastHash = m[2]
	if parseUint(m[3]) == 0 {
		a.addAnomaly("high", "body_no_valid_nodes", "Body sync started without valid nodes", 1, "Check node selection and remote height.")
	}
	return true
}

func (a *analyzer) parseBodySyncSuccess(line string) bool {
	m := bodySyncSuccessRE.FindStringSubmatch(line)
	if len(m) != 6 {
		return false
	}
	body := &a.report.BodySync
	body.Succeeded++
	body.LastHeight = parseUint(m[1])
	body.LastHash = m[2]
	txs := parseUint(m[4])
	cost := parseUint(m[5])
	body.txTotal += txs
	body.txSamples++
	if cost > 0 {
		body.costTotalUS += cost
		body.costSamples++
		if cost > body.MaxCostUS {
			body.MaxCostUS = cost
		}
	}
	return true
}

func (a *analyzer) parseBodySyncFailed(line string) bool {
	m := bodySyncFailedRE.FindStringSubmatch(line)
	if len(m) != 5 {
		return false
	}
	body := &a.report.BodySync
	body.Failed++
	body.LastHeight = parseUint(m[1])
	body.LastHash = m[2]
	cost := parseUint(m[4])
	if cost > 0 {
		body.costTotalUS += cost
		body.costSamples++
		if cost > body.MaxCostUS {
			body.MaxCostUS = cost
		}
	}
	a.addAnomaly("medium", "body_sync_failed", "Body sync failed", 1, "Check full-block RPC failures, exhausted retries, and node health.")
	return true
}

func (a *analyzer) parseBodyNoValidNodes(line string) bool {
	m := bodyNoValidNodesRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	body := &a.report.BodySync
	body.Failed++
	body.LastHeight = parseUint(m[1])
	body.LastHash = m[2]
	a.report.NodeSelection.NoValidNodes++
	a.addAnomaly("high", "body_no_valid_nodes", "Body sync has no valid nodes", 1, "Check node readiness, remote height, and cooldown state.")
	return true
}

func (a *analyzer) addNodeMethodEvent(nodeID int, method string, success bool, costUS uint64) {
	node := a.node(nodeID)
	methodStats := a.method(nodeID, method)
	totalStats := a.methodTotal(method)
	node.Requests++
	methodStats.Requests++
	totalStats.Requests++
	if success {
		node.Successes++
		methodStats.Successes++
		totalStats.Successes++
	} else {
		node.Failures++
		methodStats.Failures++
		totalStats.Failures++
	}
	if costUS > 0 {
		node.costTotalUS += costUS
		node.costSamples++
		if costUS > node.MaxCostUS {
			node.MaxCostUS = costUS
		}
		methodStats.costTotalUS += costUS
		methodStats.costSamples++
		if costUS > methodStats.MaxCostUS {
			methodStats.MaxCostUS = costUS
		}
		totalStats.costTotalUS += costUS
		totalStats.costSamples++
		if costUS > totalStats.MaxCostUS {
			totalStats.MaxCostUS = costUS
		}
	}
}

func (a *analyzer) addNodeFailure(nodeID int, op string) {
	node := a.node(nodeID)
	if node.failuresByOp == nil {
		node.failuresByOp = make(map[string]uint64)
	}
	node.failuresByOp[op]++
	a.addAnomaly("medium", "node_failure_"+strconv.Itoa(nodeID), fmt.Sprintf("Node %d had request failures", nodeID), 1, "Check this node's failed methods and error classes.")
}

func (a *analyzer) node(id int) *NodeStats {
	n := a.nodes[id]
	if n == nil {
		n = &NodeStats{
			ID:              id,
			methods:         make(map[string]*MethodStats),
			methodSnapshots: make(map[string]*counterWindow),
			failuresByOp:    make(map[string]uint64),
			Ready:           "unknown",
		}
		a.nodes[id] = n
	}
	return n
}

func (a *analyzer) method(nodeID int, name string) *MethodStats {
	n := a.node(nodeID)
	m := n.methods[name]
	if m == nil {
		m = &MethodStats{Name: name, NodeID: nodeID}
		n.methods[name] = m
	}
	return m
}

func (a *analyzer) methodTotal(name string) *MethodStats {
	m := a.methodTotals[name]
	if m == nil {
		m = &MethodStats{Name: name, NodeID: -1}
		a.methodTotals[name] = m
	}
	return m
}

func (a *analyzer) addAnomaly(level, category, message string, count uint64, suggestion string) {
	if count == 0 {
		return
	}
	key := level + ":" + category + ":" + message
	item := a.anomalyIndex[key]
	if item == nil {
		item = &Anomaly{Level: level, Category: category, Message: message, Suggestion: suggestion}
		a.anomalyIndex[key] = item
	}
	item.Count += count
}

func (a *analyzer) finish() {
	a.finishTaskPool()
	a.finishStore()
	a.finishBodySync()
	a.finishNodeRPCSnapshots()
	a.finishScanStages()
	a.finishMethods()
	a.finishAnomalies()
	a.finishSummary()
}

func (a *analyzer) finishBodySync() {
	body := &a.report.BodySync
	body.SuccessRate = percent(body.Succeeded, body.Succeeded+body.Failed)
	if body.costSamples > 0 {
		body.AvgCostUS = body.costTotalUS / body.costSamples
	}
	if body.txSamples > 0 {
		body.AvgTxs = body.txTotal / body.txSamples
	}
}

func (a *analyzer) finishTaskPool() {
	if !a.taskWindow.seen {
		return
	}
	first := a.taskWindow.first
	last := a.taskWindow.last
	if a.taskWindow.samples == 1 {
		first = taskPoolSnapshot{}
	}
	total := diffTaskKind(first.total, last.total)
	body := diffTaskKind(first.body, last.body)
	hh := diffTaskKind(first.hh, last.hh)
	hs := diffTaskKind(first.hs, last.hs)
	total.SuccessRate = percent(total.Succeeded, total.Succeeded+total.Failed)
	body.SuccessRate = percent(body.Succeeded, body.Succeeded+body.Failed)
	hh.SuccessRate = percent(hh.Succeeded, hh.Succeeded+hh.Failed)
	hs.SuccessRate = percent(hs.Succeeded, hs.Succeeded+hs.Failed)
	a.report.TaskPool.Totals = total
	a.report.TaskPool.Kinds = []TaskKindStats{body, hh, hs}
}

func (a *analyzer) finishStore() {
	if !a.storeWindow.seen {
		return
	}
	first := a.storeWindow.first
	last := a.storeWindow.last
	if a.storeWindow.samples == 1 {
		first = storeSnapshot{}
	}
	a.report.Store = StoreStats{
		Submitted:             delta(first.Submitted, last.Submitted),
		Skipped:               delta(first.Skipped, last.Skipped),
		Succeeded:             delta(first.Succeeded, last.Succeeded),
		Failed:                delta(first.Failed, last.Failed),
		Canceled:              delta(first.Canceled, last.Canceled),
		QueuePending:          last.QueuePending,
		Processing:            last.Processing,
		SkippedMissingBody:    delta(first.SkippedMissingBody, last.SkippedMissingBody),
		SkippedParentNotReady: delta(first.SkippedParentNotReady, last.SkippedParentNotReady),
		FailedDB:              delta(first.FailedDB, last.FailedDB),
	}
	a.report.Store.SuccessRate = percent(a.report.Store.Succeeded, a.report.Store.Succeeded+a.report.Store.Failed)
}

func (a *analyzer) finishNodeRPCSnapshots() {
	for _, node := range a.nodes {
		for method, w := range node.methodSnapshots {
			if !w.seen {
				continue
			}
			first := w.first
			if w.samples == 1 {
				first = 0
			}
			count := delta(first, w.last)
			if count == 0 {
				continue
			}
			ms := a.method(node.ID, method)
			ms.Requests += count
			a.methodTotal(method).Requests += count
			node.Requests += count
		}
	}
	for _, snap := range a.report.RuntimeSeries {
		for _, node := range a.nodes {
			if node.RemoteHeight == 0 || snap.RemoteLatest > node.RemoteHeight {
				node.RemoteHeight = snap.RemoteLatest
			}
		}
	}
}

func (a *analyzer) finishScanStages() {
	stages := make([]ScanStageStats, 0, len(a.scanStages))
	for _, stage := range a.scanStages {
		stage.SuccessRate = percent(stage.Runs-stage.Failures, stage.Runs)
		stages = append(stages, *stage)
	}
	sort.Slice(stages, func(i, j int) bool { return stages[i].Stage < stages[j].Stage })
	a.report.ScanStages = stages
}

func (a *analyzer) finishMethods() {
	nodes := make([]NodeStats, 0, len(a.nodes))
	for _, node := range a.nodes {
		if node.costSamples > 0 {
			node.AvgCostUS = node.costTotalUS / node.costSamples
		}
		node.FailureRate = percent(node.Failures, node.Requests)
		node.TopFailure = topKey(node.failuresByOp)
		methods := make([]MethodStats, 0, len(node.methods))
		for _, method := range node.methods {
			finalizeMethod(method)
			methods = append(methods, *method)
		}
		sort.Slice(methods, func(i, j int) bool {
			if methods[i].Requests == methods[j].Requests {
				return methods[i].Name < methods[j].Name
			}
			return methods[i].Requests > methods[j].Requests
		})
		node.Methods = methods
		nodes = append(nodes, *node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	a.report.Nodes = nodes

	methods := make([]MethodStats, 0, len(a.methodTotals))
	for _, method := range a.methodTotals {
		finalizeMethod(method)
		methods = append(methods, *method)
	}
	sort.Slice(methods, func(i, j int) bool {
		if methods[i].Requests == methods[j].Requests {
			return methods[i].Name < methods[j].Name
		}
		return methods[i].Requests > methods[j].Requests
	})
	a.report.Methods = methods
}

func finalizeMethod(method *MethodStats) {
	if method.costSamples > 0 {
		method.AvgCostUS = method.costTotalUS / method.costSamples
	}
	method.FailureRate = percent(method.Failures, method.Requests)
}

func (a *analyzer) finishAnomalies() {
	for _, anomaly := range a.anomalyIndex {
		a.report.Anomalies = append(a.report.Anomalies, *anomaly)
	}
	levelRank := map[string]int{"high": 0, "medium": 1, "low": 2}
	sort.Slice(a.report.Anomalies, func(i, j int) bool {
		ri, rj := levelRank[a.report.Anomalies[i].Level], levelRank[a.report.Anomalies[j].Level]
		if ri == rj {
			return a.report.Anomalies[i].Count > a.report.Anomalies[j].Count
		}
		return ri < rj
	})
}

func (a *analyzer) finishSummary() {
	var req, fail uint64
	for _, node := range a.report.Nodes {
		req += node.Requests
		fail += node.Failures
	}
	a.report.Summary.NodeCount = len(a.report.Nodes)
	a.report.Summary.TotalNodeRequests = req
	a.report.Summary.TotalNodeFailures = fail
	a.report.Summary.NodeFailureRate = percent(fail, req)
	a.report.Summary.TaskSucceeded = a.report.TaskPool.Totals.Succeeded
	a.report.Summary.TaskFailed = a.report.TaskPool.Totals.Failed
	a.report.Summary.TaskSuccessRate = percent(a.report.TaskPool.Totals.Succeeded, a.report.TaskPool.Totals.Succeeded+a.report.TaskPool.Totals.Failed)
	a.report.Summary.StoreSucceeded = a.report.Store.Succeeded
	a.report.Summary.StoreFailed = a.report.Store.Failed
	a.report.Summary.StoreSuccessRate = percent(a.report.Store.Succeeded, a.report.Store.Succeeded+a.report.Store.Failed)
	a.report.Summary.RemoteLatest = a.report.RuntimeLatest.RemoteLatest
	a.report.Summary.StoredCount = a.report.RuntimeLatest.StoredCount
	if a.report.RuntimeLatest.RemoteLatest > 0 && a.report.RuntimeLatest.StoredCount > 0 {
		a.report.Summary.Lag = strconv.FormatUint(a.report.RuntimeLatest.RemoteLatest-a.report.RuntimeLatest.StoredCount, 10)
	} else {
		a.report.Summary.Lag = "no data"
	}
}

func parseUintGroups(groups []string) []uint64 {
	vals := make([]uint64, len(groups))
	for i, group := range groups {
		vals[i] = parseUint(group)
	}
	return vals
}

func keyUintPairs(line string) map[string]uint64 {
	result := make(map[string]uint64)
	for _, match := range intPairRE.FindAllStringSubmatch(line, -1) {
		if len(match) != 3 {
			continue
		}
		result[match[1]] = parseUint(match[2])
	}
	return result
}

func parseUint(s string) uint64 {
	if strings.TrimSpace(s) == "" {
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	return v
}

func durationToMicros(s string) uint64 {
	d, err := time.ParseDuration(strings.ReplaceAll(strings.TrimSpace(s), "us", "µs"))
	if err != nil {
		return 0
	}
	return uint64(d.Microseconds())
}

func diffTaskKind(first, last TaskKindStats) TaskKindStats {
	return TaskKindStats{
		Name:      last.Name,
		Enqueued:  delta(first.Enqueued, last.Enqueued),
		Dequeued:  delta(first.Dequeued, last.Dequeued),
		Succeeded: delta(first.Succeeded, last.Succeeded),
		Failed:    delta(first.Failed, last.Failed),
		Retried:   delta(first.Retried, last.Retried),
		Dropped:   delta(first.Dropped, last.Dropped),
		Tracked:   last.Tracked,
	}
}

func delta(first, last uint64) uint64 {
	if last >= first {
		return last - first
	}
	return last
}

func percent(part, total uint64) string {
	if total == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.2f%%", float64(part)*100/float64(total))
}

func topKey(values map[string]uint64) string {
	var key string
	var value uint64
	for k, v := range values {
		if v > value || (v == value && (key == "" || k < key)) {
			key = k
			value = v
		}
	}
	if key == "" {
		return "none"
	}
	return fmt.Sprintf("%s (%d)", key, value)
}

func classifyError(errText string) string {
	s := strings.ToLower(errText)
	switch {
	case strings.Contains(s, "too many requests") || strings.Contains(s, "429"):
		return "rate_limit"
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return "timeout"
	case strings.Contains(s, "connection reset") || strings.Contains(s, "eof"):
		return "network"
	default:
		fields := strings.Fields(strings.TrimSpace(errText))
		if len(fields) == 0 {
			return "unknown"
		}
		if len(fields) > 4 {
			fields = fields[:4]
		}
		return strings.Join(fields, " ")
	}
}

func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return "no data"
			}
			return t.Format("2006-01-02 15:04:05")
		},
	}
}
