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
	Title                 string
	GeneratedAt           time.Time
	Sources               []string
	WindowStart           time.Time
	WindowEnd             time.Time
	LinesTotal            uint64
	LinesParsed           uint64
	Warnings              []string
	Summary               Summary
	Nodes                 []NodeStats
	Methods               []MethodStats
	NodeOperatorMethods   []NodeOperatorMethodStats
	TaskPool              TaskPoolStats
	HeaderSync            HeaderSyncStats
	BodySync              BodySyncStats
	BodyItems             []BodyItemStats
	NodeSelection         NodeSelectionStats
	ScanStages            []ScanStageStats
	Store                 StoreStats
	StoreDataTypes        []StoreDataTypeStats
	FullBlockStore        FullBlockStoreStats
	RuntimeLatest         RuntimeSnapshot
	RuntimeSeries         []RuntimeSnapshot
	BlockTiming           BlockTimingStats
	BlockTimingPhases     []BlockTimingPhaseStats
	BlockTimings          []BlockTimingBlock
	BlockDurationOutliers []BlockTimingBlock
	BodySyncOutliers      []BlockTimingBlock
	SlowBlockTimings      []BlockTimingBlock
	BlockStoreDurations   []BlockStoreDuration
	BlockProgress         []BlockProgressInterval
	BlockProgressOutliers []BlockProgressInterval
	Anomalies             []Anomaly
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
	StoredHeight      uint64
	StoredCount       uint64
	Lag               string
}

type HeaderSyncStats struct {
	Started     uint64
	Succeeded   uint64
	Failed      uint64
	SuccessRate string
	AvgCostUS   uint64
	MaxCostUS   uint64
	LastHeight  uint64
	LastHash    string
	costTotalUS uint64
	costSamples uint64
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

type BodyItemStats struct {
	Item          string
	Blocks        uint64
	Items         uint64
	TotalCostUS   uint64
	AvgCostUS     uint64
	AvgItemCostUS uint64
	MaxCostUS     uint64
	LastHeight    uint64
	LastHash      string
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

type NodeOperatorMethodStats struct {
	NodeID          int
	Method          string
	Calls           uint64
	TotalCostUS     uint64
	AvgCostUS       uint64
	MaxCostUS       uint64
	ArraySamples    uint64
	ArrayItemsTotal uint64
	AvgArrayItems   uint64
	MaxArrayItems   uint64
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
	Totals                       TaskKindStats
	Kinds                        []TaskKindStats
	PendingHigh                  uint64
	PendingNormal                uint64
	Tracked                      uint64
	WorkerCount                  uint64
	MaxRetry                     uint64
	HighQueueCapacity            uint64
	NormalQueueCapacity          uint64
	TrackedOldestAgeUS           uint64
	TrackedAvgAgeUS              uint64
	TrackedBodyOldestAgeUS       uint64
	TrackedHeaderHOldestAgeUS    uint64
	TrackedHeaderHashOldestAgeUS uint64
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
	DurationSamples       uint64
	DurationTotalUS       uint64
	DurationAvgUS         uint64
	DurationLastUS        uint64
	DurationMaxUS         uint64
}

type StoreDataTypeStats struct {
	Type            string
	Writes          uint64
	Tasks           uint64
	Failures        uint64
	RetriedAttempts uint64
	TotalCostUS     uint64
	AvgCostUS       uint64
	AvgWriteCostUS  uint64
	MaxCostUS       uint64
	FailedCostUS    uint64
	AvgFailedCostUS uint64
	MaxFailedCostUS uint64
}

type FullBlockStoreStats struct {
	Count         uint64
	AvgTotalUS    uint64
	MaxTotalUS    uint64
	P95TotalUS    uint64
	AvgDataUS     uint64
	MaxDataUS     uint64
	AvgFinalizeUS uint64
	MaxFinalizeUS uint64
	AvgReadyUS    uint64
	AvgBlockRowUS uint64
}

type RuntimeSnapshot struct {
	Time              time.Time
	RemoteLatest      uint64
	NodeReady         uint64
	NodeTotal         uint64
	BlocktreeStart    uint64
	BlocktreeEnd      uint64
	StoredHeight      uint64
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

type BlockProgressInterval struct {
	Height            uint64
	Hash              string
	TaskCreatedAt     time.Time
	HasTaskCreatedGap bool
	TaskCreatedGapUS  int64
}

type BlockTimingStats struct {
	Blocks                    uint64
	PrevStoreToTaskSamples    uint64
	TaskToHeaderStartSamples  uint64
	HeaderStageSamples        uint64
	HeaderToBodyStartSamples  uint64
	BodyStageSamples          uint64
	BodySyncCostSamples       uint64
	StoreStageSamples         uint64
	BodyToStoreStartSamples   uint64
	StoreCostSamples          uint64
	EndToEndSamples           uint64
	TaskQueueSamples          uint64
	AvgPrevStoreToTaskUS      uint64
	AvgTaskToHeaderStartUS    uint64
	AvgHeaderToBodyStartUS    uint64
	AvgBodyToStoreStartUS     uint64
	AvgFirstTaskToStoreUS     uint64
	P95FirstTaskToStoreUS     uint64
	MaxFirstTaskToStoreUS     uint64
	AvgTaskQueueWaitUS        uint64
	AvgHeaderSyncCostUS       uint64
	AvgBodySyncCostUS         uint64
	AvgStoreCostUS            uint64
	AvgUnattributedOverheadUS uint64
}

type BlockTimingPhaseStats struct {
	Name    string
	Samples uint64
	AvgUS   uint64
	P95US   uint64
	MaxUS   uint64
}

type BlockTimingBlock struct {
	Height              uint64
	Hash                string
	PrevStoreToTaskUS   uint64
	TaskToHeaderStartUS uint64
	HeaderSyncUS        uint64
	HeaderToBodyStartUS uint64
	BodySyncUS          uint64
	BodyToStoreStartUS  uint64
	StoreUS             uint64
	FirstTaskToStoreUS  uint64
}

type blockTimingSegment struct {
	Label string
	Class string
	Value uint64
}

type blockTimingAccumulator struct {
	name   string
	total  uint64
	values []uint64
	max    uint64
}

func (a *blockTimingAccumulator) add(value uint64) {
	a.total += value
	a.values = append(a.values, value)
	if value > a.max {
		a.max = value
	}
}

func (a blockTimingAccumulator) samples() uint64 {
	return uint64(len(a.values))
}

func (a blockTimingAccumulator) avg() uint64 {
	return averageUint(a.total, a.samples())
}

func (a blockTimingAccumulator) stats() BlockTimingPhaseStats {
	return BlockTimingPhaseStats{
		Name:    a.name,
		Samples: a.samples(),
		AvgUS:   a.avg(),
		P95US:   percentile95(a.values),
		MaxUS:   a.max,
	}
}

type BlockStoreDuration struct {
	Height     uint64
	Hash       string
	StoredAt   time.Time
	ReadyUS    uint64
	BlockRowUS uint64
	DataUS     uint64
	FinalizeUS uint64
	TotalUS    uint64
}

type Anomaly struct {
	Level      string
	Category   string
	Message    string
	Count      uint64
	Suggestion string
}

type analyzer struct {
	report                *Report
	nodes                 map[int]*NodeStats
	methodTotals          map[string]*MethodStats
	taskWindow            *taskPoolWindow
	storeWindow           *storeWindow
	scanStages            map[string]*ScanStageStats
	nodeOperatorWindows   map[string]*nodeOperatorMethodWindow
	storeDataTypes        map[string]*StoreDataTypeStats
	bodyItems             map[string]*BodyItemStats
	anomalyIndex          map[string]*Anomaly
	fullBlockStoreSamples []fullBlockStoreSample
	blockProgress         map[uint64]*blockProgressEvent
	blockProgressByHash   map[string]*blockProgressEvent
	storedHeight          uint64
	now                   time.Time
}

type blockProgressEvent struct {
	Height               uint64
	Hash                 string
	FirstTaskCreatedAt   time.Time
	HeaderSyncStartedAt  time.Time
	HeaderSyncedAt       time.Time
	HeaderCostUS         uint64
	BodySyncStartedAt    time.Time
	BodySyncedAt         time.Time
	BodyCostUS           uint64
	StoreStartedAt       time.Time
	StoredAt             time.Time
	StoreTotalUS         uint64
	TaskQueueWaitTotalUS uint64
	TaskQueueWaitSamples uint64
}

type fullBlockStoreSample struct {
	ReadyUS    uint64
	BlockRowUS uint64
	DataUS     uint64
	FinalizeUS uint64
	TotalUS    uint64
}

type counterWindow struct {
	seen    bool
	samples uint64
	first   uint64
	last    uint64
}

type nodeOperatorMethodSnapshot struct {
	Calls           uint64
	TotalCostUS     uint64
	MaxCostUS       uint64
	ArraySamples    uint64
	ArrayItemsTotal uint64
	MaxArrayItems   uint64
}

type nodeOperatorMethodWindow struct {
	NodeID  int
	Method  string
	seen    bool
	samples uint64
	first   nodeOperatorMethodSnapshot
	last    nodeOperatorMethodSnapshot
}

type bodySyncNodeChartSegment struct {
	Method string
	Value  uint64
}

type bodySyncNodeChartRow struct {
	NodeID   int
	TotalUS  uint64
	Segments []bodySyncNodeChartSegment
}

type syncNodeChartSegment struct {
	NodeID int
	Value  uint64
}

type syncNodeChartRow struct {
	Method   string
	TotalUS  uint64
	Segments []syncNodeChartSegment
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
	DurationTotalUS       uint64
	DurationLastUS        uint64
	DurationAvgUS         uint64
	DurationMaxUS         uint64
}

type storeWindow struct {
	seen    bool
	samples uint64
	first   storeSnapshot
	last    storeSnapshot
}

const (
	blockProgressIntervalOutlierUS = int64(10 * time.Second / time.Microsecond)
	blockDurationOutlierUS         = uint64(10 * time.Second / time.Microsecond)
	bodySyncDurationOutlierUS      = uint64(10 * time.Second / time.Microsecond)
)

var (
	ansiRE               = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	timePrefixRE         = regexp.MustCompile(`^([A-Z][a-z]{2}\s+\d{1,2}\s+\d\d:\d\d:\d\d\.\d{3})`)
	intPairRE            = regexp.MustCompile(`([A-Za-z_]+):([0-9]+)`)
	taskPoolRE           = regexp.MustCompile(`task pool stats enqueued:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) dequeued:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) succeeded:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) failed:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) retried:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) dropped:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) pending_high:(\d+) pending_normal:(\d+) tracked:(\d+)\(body:(\d+) hh:(\d+) hs:(\d+)\) workers:(\d+) max_retry:(\d+)`)
	scanStageRE          = regexp.MustCompile(`scan stage event stage:([^ ]+) target:([^ ]*) ?(?:target_count:(\d+) )?success:(true|false) duration:([^ ]+)(?: err:(.*?))?(?: @|$)`)
	nodeRPCStatsRE       = regexp.MustCompile(`\[nodeOperator RPC\] nodeId=(\d+) (.*?)(?: @|$)`)
	nodeOperatorMethodRE = regexp.MustCompile(`\[nodeOperator Method\] nodeId=(\d+) method=([^ ]+) calls=(\d+) total_us=(\d+) avg_us=\d+ max_us=(\d+) array_samples=(\d+) array_items_total=(\d+) avg_array_items=\d+ max_array_items=(\d+)`)
	fullRPCSuccessRE     = regexp.MustCompile(`fetch full block rpc success\. op:([^ ]+) nodeId:(\d+) taskId:[^ ]+ height:[^ ]+ attempts:(\d+) retries:[^ ]+ selected_node_ids:[^ ]* cost_us:(\d+)`)
	fullRPCFailedRE      = regexp.MustCompile(`fetch full block rpc failed\. op:([^ ]+) nodeId:(\d+) taskId:[^ ]+ height:[^ ]+ attempt:(\d+) retries:[^ ]+ err:(.*?)(?: @|$)`)
	fullRPCExhaustedRE   = regexp.MustCompile(`fetch full block rpc exhausted retries\. op:([^ ]+) taskId:[^ ]+ height:[^ ]+ attempts:(\d+) retries:[^ ]+ selected_node_ids:([^ ]*) cost_us:(\d+) err:(.*?)(?: @|$)`)
	headerFailRE         = regexp.MustCompile(`fetch header(?: by hash)? failed\. nodeId:(\d+) .*? error:(.*?)(?: height:| hash:| @|$)`)
	headerSuccessRE      = regexp.MustCompile(`fetch header(?: by hash)? success\. nodeId:(\d+) .*? cost:([^ ]+)`)
	headerSyncSuccessRE  = regexp.MustCompile(`header sync(?: by hash)? success\. height:(\d+) hash:([^ ]+) parent_hash:[^ ]+ cost_us:(\d+)`)
	headerSyncFailedRE   = regexp.MustCompile(`header sync(?: by hash)? failed\. (?:height:(\d+)|hash:([^ ]+)) cost_us:(\d+)`)
	bodySyncStartRE      = regexp.MustCompile(`body sync start\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+)`)
	bodySyncSuccessRE    = regexp.MustCompile(`body sync success\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+) node_ids:[^ ]* txs:(\d+) cost_us:(\d+)`)
	bodySyncFailedRE     = regexp.MustCompile(`body sync failed\. height:(\d+) hash:([^ ]+) valid_nodes:(\d+) node_ids:[^ ]* cost_us:(\d+)`)
	bodyNoValidNodesRE   = regexp.MustCompile(`body sync no valid nodes\. height:(\d+) hash:([^ ]+)`)
	bodyItemCostRE       = regexp.MustCompile(`fetch full block item cost\. height:(\d+) hash:([^ ]+) item:([^ ]+) count:(\d+) cost_us:(\d+)`)
	storeBlockStartRE    = regexp.MustCompile(`store fullblock start\. height:(\d+)(?: hash:([^ ]+))?`)
	storedBlockRE        = regexp.MustCompile(`store fullblock\. height:(\d+)(?: hash:([^ ]+))?`)
	storeDataTypeRE      = regexp.MustCompile(`store data type success\. type:([^ ]+) rows:(\d+) .*? cost:([^ ]+)`)
	storeDataTypeFailRE  = regexp.MustCompile(`store failed err:.*? height:(\d+) type:([^ ]+) task_id:[^ ]+ try_count:(\d+) cost:([^ ]+)`)
	durationFieldRE      = regexp.MustCompile(`([A-Za-z_]+_cost):([^ ]+)`)
	storeDurationFieldRE = regexp.MustCompile(`(store_duration_(?:total|last|avg|max)):([^ ]+)`)
	storeStatsRE         = regexp.MustCompile(`store block worker stats submitted:(\d+) skipped:(\d+) succeeded:(\d+) failed:(\d+) canceled:(\d+) queue_pending:(\d+) processing:(\d+) skipped_missing_body:(\d+) skipped_parent_not_ready:(\d+) failed_db:(\d+)`)
	runtimeHealthRE      = regexp.MustCompile(`runtime health stats remote_latest:(\d+) node_ready:(\d+)/(\d+) blocktree_range:\[(\d+),(\d+)\] linked:(\d+) leaves:(\d+) branches:(\d+) orphans:(\d+) orphan_parents:(\d+) staging_blocks:(\d+) pending_headers:(\d+) pending_bodies:(\d+) complete_blocks:(\d+) stored_count:(\d+) task_pending_high:(\d+) task_pending_normal:(\d+) task_tracked:(\d+) task_succeeded:(\d+) task_failed:(\d+) task_retried:(\d+) store_submitted:(\d+) store_succeeded:(\d+) store_failed:(\d+) store_skipped:(\d+) store_queue_pending:(\d+)`)
	validNodesRE         = regexp.MustCompile(`valid node operators selected\.`)
	duplicateRE          = regexp.MustCompile(`(?i)(duplicate|Error 1062|Duplicate entry)`)
	deadlockRE           = regexp.MustCompile(`(?i)(deadlock|lock wait timeout)`)
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
		nodes:               make(map[int]*NodeStats),
		methodTotals:        make(map[string]*MethodStats),
		taskWindow:          &taskPoolWindow{},
		storeWindow:         &storeWindow{},
		scanStages:          make(map[string]*ScanStageStats),
		nodeOperatorWindows: make(map[string]*nodeOperatorMethodWindow),
		storeDataTypes:      make(map[string]*StoreDataTypeStats),
		bodyItems:           make(map[string]*BodyItemStats),
		anomalyIndex:        make(map[string]*Anomaly),
		blockProgress:       make(map[uint64]*blockProgressEvent),
		blockProgressByHash: make(map[string]*blockProgressEvent),
		now:                 now,
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
	case strings.Contains(line, "task lifecycle event"):
		return a.parseTaskLifecycle(line, ts)
	case strings.Contains(line, "task pool stats"):
		return a.parseTaskPool(line)
	case strings.Contains(line, "store block worker stats"):
		return a.parseStore(line)
	case strings.Contains(line, "store data type success"):
		return a.parseStoreDataType(line)
	case strings.Contains(line, "store failed err:"):
		return a.parseStoreDataTypeFailure(line)
	case strings.Contains(line, "store fullblock start. height:"):
		return a.parseStoreBlockStart(line, ts)
	case strings.Contains(line, "store fullblock. height:"):
		return a.parseStoredBlock(line, ts)
	case strings.Contains(line, "scan stage event stage:"):
		return a.parseScanStage(line)
	case strings.Contains(line, "runtime health stats"):
		return a.parseRuntime(line, ts)
	case strings.Contains(line, "[nodeOperator RPC]"):
		return a.parseNodeRPCStats(line)
	case strings.Contains(line, "[nodeOperator Method]"):
		return a.parseNodeOperatorMethodStats(line)
	case strings.Contains(line, "fetch full block rpc success"):
		return a.parseFullRPCSuccess(line)
	case strings.Contains(line, "fetch full block rpc failed"):
		return a.parseFullRPCFailed(line)
	case strings.Contains(line, "fetch full block rpc exhausted retries"):
		return a.parseFullRPCExhausted(line)
	case strings.Contains(line, "fetch full block item cost"):
		return a.parseBodyItemCost(line)
	case strings.Contains(line, "fetch header failed") || strings.Contains(line, "fetch header by hash failed"):
		return a.parseHeaderFailure(line)
	case strings.Contains(line, "fetch header success") || strings.Contains(line, "fetch header by hash success"):
		return a.parseHeaderSuccess(line)
	case strings.Contains(line, "header sync success") || strings.Contains(line, "header sync by hash success"):
		return a.parseHeaderSyncSuccess(line, ts)
	case strings.Contains(line, "header sync failed") || strings.Contains(line, "header sync by hash failed"):
		return a.parseHeaderSyncFailed(line)
	case strings.Contains(line, "valid node operators selected"):
		return a.parseValidNodes(line)
	case strings.Contains(line, "body sync start"):
		return a.parseBodySyncStart(line, ts)
	case strings.Contains(line, "body sync success"):
		return a.parseBodySyncSuccess(line, ts)
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
	extra := keyUintPairs(line)
	a.report.TaskPool.TrackedOldestAgeUS = extra["tracked_oldest_age_us"]
	a.report.TaskPool.TrackedAvgAgeUS = extra["tracked_avg_age_us"]
	a.report.TaskPool.TrackedBodyOldestAgeUS = extra["tracked_body_oldest_age_us"]
	a.report.TaskPool.TrackedHeaderHOldestAgeUS = extra["tracked_hh_oldest_age_us"]
	a.report.TaskPool.TrackedHeaderHashOldestAgeUS = extra["tracked_hs_oldest_age_us"]
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
	durations := storeDurationPairs(line)
	s := storeSnapshot{
		Submitted: vals[0], Skipped: vals[1], Succeeded: vals[2], Failed: vals[3], Canceled: vals[4],
		QueuePending: vals[5], Processing: vals[6], SkippedMissingBody: vals[7], SkippedParentNotReady: vals[8], FailedDB: vals[9],
		DurationTotalUS: durations["store_duration_total"], DurationLastUS: durations["store_duration_last"], DurationAvgUS: durations["store_duration_avg"], DurationMaxUS: durations["store_duration_max"],
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

func (a *analyzer) parseStoreDataType(line string) bool {
	m := storeDataTypeRE.FindStringSubmatch(line)
	if len(m) != 4 {
		return false
	}
	dataType := m[1]
	stats := a.storeDataTypes[dataType]
	if stats == nil {
		stats = &StoreDataTypeStats{Type: dataType}
		a.storeDataTypes[dataType] = stats
	}
	rows := parseUint(m[2])
	cost := durationToMicros(m[3])
	stats.Writes += rows
	stats.Tasks++
	stats.TotalCostUS += cost
	if cost > stats.MaxCostUS {
		stats.MaxCostUS = cost
	}
	return true
}

func (a *analyzer) parseStoreDataTypeFailure(line string) bool {
	m := storeDataTypeFailRE.FindStringSubmatch(line)
	if len(m) != 5 {
		return false
	}
	dataType := m[2]
	stats := a.storeDataTypes[dataType]
	if stats == nil {
		stats = &StoreDataTypeStats{Type: dataType}
		a.storeDataTypes[dataType] = stats
	}
	tryCount := parseUint(m[3])
	cost := durationToMicros(m[4])
	stats.Failures++
	if tryCount > 1 {
		stats.RetriedAttempts++
	}
	stats.FailedCostUS += cost
	if cost > stats.MaxFailedCostUS {
		stats.MaxFailedCostUS = cost
	}
	return true
}

func (a *analyzer) parseStoreBlockStart(line string, ts time.Time) bool {
	m := storeBlockStartRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	height := parseUint(m[1])
	progress := a.progressEventForHeightHash(height, m[2])
	if m[2] != "" {
		progress.Hash = m[2]
	}
	if !ts.IsZero() && (progress.StoreStartedAt.IsZero() || ts.Before(progress.StoreStartedAt)) {
		progress.StoreStartedAt = ts
	}
	return true
}

func (a *analyzer) parseStoredBlock(line string, ts time.Time) bool {
	m := storedBlockRE.FindStringSubmatch(line)
	if len(m) != 3 {
		return false
	}
	height := parseUint(m[1])
	if height > a.storedHeight {
		a.storedHeight = height
	}
	durations := keyDurationPairs(line)
	totalCost := durations["total_cost"]
	if totalCost > 0 {
		a.fullBlockStoreSamples = append(a.fullBlockStoreSamples, fullBlockStoreSample{
			ReadyUS:    durations["ready_cost"],
			BlockRowUS: durations["block_row_cost"],
			DataUS:     durations["data_cost"],
			FinalizeUS: durations["finalize_cost"],
			TotalUS:    totalCost,
		})
		a.report.BlockStoreDurations = append(a.report.BlockStoreDurations, BlockStoreDuration{
			Height:     height,
			Hash:       m[2],
			StoredAt:   ts,
			ReadyUS:    durations["ready_cost"],
			BlockRowUS: durations["block_row_cost"],
			DataUS:     durations["data_cost"],
			FinalizeUS: durations["finalize_cost"],
			TotalUS:    totalCost,
		})
	}
	progress := a.progressEventForHeightHash(height, m[2])
	if m[2] != "" {
		progress.Hash = m[2]
	}
	if !ts.IsZero() {
		progress.StoredAt = ts
	}
	progress.StoreTotalUS = totalCost
	return true
}

func (a *analyzer) parseTaskLifecycle(line string, ts time.Time) bool {
	fields := colonFields(line)
	if fields["action"] == "" || fields["kind"] == "" {
		return false
	}
	height := parseUint(fields["height"])
	hash := fields["hash"]
	event := a.progressEventForTask(fields["kind"], height, hash, fields["key"])
	if event == nil {
		return true
	}
	if !ts.IsZero() && (event.FirstTaskCreatedAt.IsZero() || ts.Before(event.FirstTaskCreatedAt)) {
		event.FirstTaskCreatedAt = ts
	}
	if fields["action"] == "started" {
		queueWait := parseUint(fields["queue_wait_us"])
		if queueWait > 0 {
			event.TaskQueueWaitTotalUS += queueWait
			event.TaskQueueWaitSamples++
		}
		if !ts.IsZero() && (fields["kind"] == "header_height" || fields["kind"] == "header_hash") && (event.HeaderSyncStartedAt.IsZero() || ts.Before(event.HeaderSyncStartedAt)) {
			event.HeaderSyncStartedAt = ts
		}
	}
	return true
}

func (a *analyzer) progressEvent(height uint64) *blockProgressEvent {
	event := a.blockProgress[height]
	if event == nil {
		event = &blockProgressEvent{Height: height}
		a.blockProgress[height] = event
	}
	return event
}

func (a *analyzer) progressEventByHash(hash string) *blockProgressEvent {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return nil
	}
	event := a.blockProgressByHash[hash]
	if event == nil {
		event = &blockProgressEvent{Hash: hash}
		a.blockProgressByHash[hash] = event
	}
	return event
}

func (a *analyzer) progressEventForTask(kind string, height uint64, hash string, key string) *blockProgressEvent {
	switch kind {
	case "header_height":
		if height == 0 {
			height = parseHeaderHeightTaskKey(key)
		}
		if height == 0 {
			return nil
		}
		return a.progressEvent(height)
	case "body", "header_hash":
		if hash == "" && kind == "body" {
			hash = key
		}
		if hash == "" && kind == "header_hash" {
			hash = strings.TrimPrefix(key, "header_hash:")
		}
		return a.progressEventByHash(hash)
	default:
		return nil
	}
}

func (a *analyzer) progressEventForHeightHash(height uint64, hash string) *blockProgressEvent {
	heightEvent := a.progressEvent(height)
	if hash == "" {
		return heightEvent
	}
	hashEvent := a.progressEventByHash(hash)
	if hashEvent == nil || hashEvent == heightEvent {
		heightEvent.Hash = hash
		a.blockProgressByHash[hash] = heightEvent
		return heightEvent
	}
	a.mergeProgressEvents(heightEvent, hashEvent)
	heightEvent.Hash = hash
	heightEvent.Height = height
	a.blockProgressByHash[hash] = heightEvent
	return heightEvent
}

func (a *analyzer) mergeProgressEvents(dst, src *blockProgressEvent) {
	if dst == nil || src == nil {
		return
	}
	if dst.Hash == "" {
		dst.Hash = src.Hash
	}
	if dst.FirstTaskCreatedAt.IsZero() || (!src.FirstTaskCreatedAt.IsZero() && src.FirstTaskCreatedAt.Before(dst.FirstTaskCreatedAt)) {
		dst.FirstTaskCreatedAt = src.FirstTaskCreatedAt
	}
	if dst.HeaderSyncStartedAt.IsZero() || (!src.HeaderSyncStartedAt.IsZero() && src.HeaderSyncStartedAt.Before(dst.HeaderSyncStartedAt)) {
		dst.HeaderSyncStartedAt = src.HeaderSyncStartedAt
	}
	if dst.HeaderSyncedAt.IsZero() {
		dst.HeaderSyncedAt = src.HeaderSyncedAt
	}
	if dst.HeaderCostUS == 0 {
		dst.HeaderCostUS = src.HeaderCostUS
	}
	if dst.BodySyncStartedAt.IsZero() || (!src.BodySyncStartedAt.IsZero() && src.BodySyncStartedAt.Before(dst.BodySyncStartedAt)) {
		dst.BodySyncStartedAt = src.BodySyncStartedAt
	}
	if dst.BodySyncedAt.IsZero() {
		dst.BodySyncedAt = src.BodySyncedAt
	}
	if dst.BodyCostUS == 0 {
		dst.BodyCostUS = src.BodyCostUS
	}
	if dst.StoreStartedAt.IsZero() || (!src.StoreStartedAt.IsZero() && src.StoreStartedAt.Before(dst.StoreStartedAt)) {
		dst.StoreStartedAt = src.StoreStartedAt
	}
	if dst.StoredAt.IsZero() {
		dst.StoredAt = src.StoredAt
	}
	if dst.StoreTotalUS == 0 {
		dst.StoreTotalUS = src.StoreTotalUS
	}
	dst.TaskQueueWaitTotalUS += src.TaskQueueWaitTotalUS
	dst.TaskQueueWaitSamples += src.TaskQueueWaitSamples
}

func (a *analyzer) parseRuntime(line string, ts time.Time) bool {
	m := runtimeHealthRE.FindStringSubmatch(line)
	if len(m) != 27 {
		return false
	}
	v := parseUintGroups(m[1:])
	s := RuntimeSnapshot{
		Time: ts, RemoteLatest: v[0], NodeReady: v[1], NodeTotal: v[2], BlocktreeStart: v[3], BlocktreeEnd: v[4], StoredHeight: a.storedHeight,
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

func (a *analyzer) parseNodeOperatorMethodStats(line string) bool {
	m := nodeOperatorMethodRE.FindStringSubmatch(line)
	if len(m) != 9 {
		return false
	}
	nodeID := int(parseUint(m[1]))
	method := m[2]
	key := strconv.Itoa(nodeID) + ":" + method
	w := a.nodeOperatorWindows[key]
	if w == nil {
		w = &nodeOperatorMethodWindow{NodeID: nodeID, Method: method}
		a.nodeOperatorWindows[key] = w
	}
	snap := nodeOperatorMethodSnapshot{
		Calls:           parseUint(m[3]),
		TotalCostUS:     parseUint(m[4]),
		MaxCostUS:       parseUint(m[5]),
		ArraySamples:    parseUint(m[6]),
		ArrayItemsTotal: parseUint(m[7]),
		MaxArrayItems:   parseUint(m[8]),
	}
	if !w.seen {
		w.first = snap
		w.seen = true
	}
	w.samples++
	w.last = snap
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

func (a *analyzer) parseHeaderSyncSuccess(line string, ts time.Time) bool {
	m := headerSyncSuccessRE.FindStringSubmatch(line)
	if len(m) != 4 {
		return false
	}
	header := &a.report.HeaderSync
	header.Succeeded++
	header.LastHeight = parseUint(m[1])
	header.LastHash = m[2]
	cost := parseUint(m[3])
	if cost > 0 {
		header.costTotalUS += cost
		header.costSamples++
		if cost > header.MaxCostUS {
			header.MaxCostUS = cost
		}
	}
	progress := a.progressEventForHeightHash(header.LastHeight, header.LastHash)
	if !ts.IsZero() {
		progress.HeaderSyncedAt = ts
	}
	progress.HeaderCostUS = cost
	return true
}

func (a *analyzer) parseHeaderSyncFailed(line string) bool {
	m := headerSyncFailedRE.FindStringSubmatch(line)
	if len(m) != 4 {
		return false
	}
	header := &a.report.HeaderSync
	header.Failed++
	if m[1] != "" {
		header.LastHeight = parseUint(m[1])
	}
	if m[2] != "" {
		header.LastHash = m[2]
	}
	cost := parseUint(m[3])
	if cost > 0 {
		header.costTotalUS += cost
		header.costSamples++
		if cost > header.MaxCostUS {
			header.MaxCostUS = cost
		}
	}
	a.addAnomaly("medium", "header_sync_failed", "Header sync failed", 1, "Check header RPC failures, node health, and chain progress.")
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

func (a *analyzer) parseBodySyncStart(line string, ts time.Time) bool {
	m := bodySyncStartRE.FindStringSubmatch(line)
	if len(m) != 4 {
		return false
	}
	a.report.BodySync.Started++
	a.report.BodySync.LastHeight = parseUint(m[1])
	a.report.BodySync.LastHash = m[2]
	progress := a.progressEventForHeightHash(a.report.BodySync.LastHeight, a.report.BodySync.LastHash)
	progress.Hash = a.report.BodySync.LastHash
	if !ts.IsZero() && (progress.BodySyncStartedAt.IsZero() || ts.Before(progress.BodySyncStartedAt)) {
		progress.BodySyncStartedAt = ts
	}
	if parseUint(m[3]) == 0 {
		a.addAnomaly("high", "body_no_valid_nodes", "Body sync started without valid nodes", 1, "Check node selection and remote height.")
	}
	return true
}

func (a *analyzer) parseBodySyncSuccess(line string, ts time.Time) bool {
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
	progress := a.progressEventForHeightHash(body.LastHeight, body.LastHash)
	progress.Hash = body.LastHash
	if !ts.IsZero() {
		progress.BodySyncedAt = ts
	}
	progress.BodyCostUS = cost
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

func (a *analyzer) parseBodyItemCost(line string) bool {
	m := bodyItemCostRE.FindStringSubmatch(line)
	if len(m) != 6 {
		return false
	}
	itemName := m[3]
	item := a.bodyItems[itemName]
	if item == nil {
		item = &BodyItemStats{Item: itemName}
		a.bodyItems[itemName] = item
	}
	item.Blocks++
	item.Items += parseUint(m[4])
	cost := parseUint(m[5])
	item.TotalCostUS += cost
	if cost > item.MaxCostUS {
		item.MaxCostUS = cost
	}
	item.LastHeight = parseUint(m[1])
	item.LastHash = m[2]
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
	a.finishStoreDataTypes()
	a.finishFullBlockStore()
	a.finishHeaderSync()
	a.finishBodySync()
	a.finishBodyItems()
	a.finishBlockProgress()
	a.finishBlockTiming()
	a.finishNodeRPCSnapshots()
	a.finishNodeOperatorMethods()
	a.finishScanStages()
	a.finishMethods()
	a.finishAnomalies()
	a.finishSummary()
}

func (a *analyzer) finishHeaderSync() {
	header := &a.report.HeaderSync
	header.Started = header.Succeeded + header.Failed
	header.SuccessRate = percent(header.Succeeded, header.Started)
	if header.costSamples > 0 {
		header.AvgCostUS = header.costTotalUS / header.costSamples
	}
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

func (a *analyzer) finishBodyItems() {
	items := make([]BodyItemStats, 0, len(a.bodyItems))
	for _, item := range a.bodyItems {
		if item.Blocks > 0 {
			item.AvgCostUS = item.TotalCostUS / item.Blocks
		}
		if item.Items > 0 {
			item.AvgItemCostUS = item.TotalCostUS / item.Items
		}
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].TotalCostUS == items[j].TotalCostUS {
			return items[i].Item < items[j].Item
		}
		return items[i].TotalCostUS > items[j].TotalCostUS
	})
	a.report.BodyItems = items
}

func (a *analyzer) finishBlockProgress() {
	if len(a.blockProgress) == 0 {
		return
	}
	heights := make([]uint64, 0, len(a.blockProgress))
	for height := range a.blockProgress {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	rows := make([]BlockProgressInterval, 0, len(heights))
	outliers := make([]BlockProgressInterval, 0)
	var prevTaskCreatedAt time.Time
	for _, height := range heights {
		event := a.blockProgress[height]
		row := BlockProgressInterval{
			Height:        event.Height,
			Hash:          event.Hash,
			TaskCreatedAt: event.FirstTaskCreatedAt,
		}
		if !event.FirstTaskCreatedAt.IsZero() && !prevTaskCreatedAt.IsZero() {
			row.HasTaskCreatedGap = true
			row.TaskCreatedGapUS = event.FirstTaskCreatedAt.Sub(prevTaskCreatedAt).Microseconds()
		}
		if !event.FirstTaskCreatedAt.IsZero() {
			prevTaskCreatedAt = event.FirstTaskCreatedAt
		}
		if isBlockProgressOutlier(row) {
			outliers = append(outliers, row)
			continue
		}
		rows = append(rows, row)
	}
	a.report.BlockProgress = rows
	a.report.BlockProgressOutliers = outliers
}

func isBlockProgressOutlier(row BlockProgressInterval) bool {
	return row.HasTaskCreatedGap && row.TaskCreatedGapUS > blockProgressIntervalOutlierUS
}

func (a *analyzer) finishBlockTiming() {
	if len(a.blockProgress) == 0 {
		return
	}
	var stats BlockTimingStats
	prevStoreToTask := blockTimingAccumulator{name: "Prev Store -> Task Created"}
	taskToHeaderStart := blockTimingAccumulator{name: "Task Created -> Header Start"}
	headerSync := blockTimingAccumulator{name: "Header Sync Cost"}
	headerToBodyStart := blockTimingAccumulator{name: "Header Done -> Body Start"}
	bodySync := blockTimingAccumulator{name: "Body Sync Cost"}
	bodyToStoreStart := blockTimingAccumulator{name: "Body Done -> Store Start"}
	storeCost := blockTimingAccumulator{name: "Store Cost"}
	endToEnd := blockTimingAccumulator{name: "Task Created -> Store Done"}
	taskQueue := blockTimingAccumulator{name: "Task Queue Wait"}
	var directCostTotal uint64
	blocks := make([]BlockTimingBlock, 0)
	blockDurationOutliers := make([]BlockTimingBlock, 0)
	bodySyncOutliers := make([]BlockTimingBlock, 0)
	slowBlocks := make([]BlockTimingBlock, 0)
	heights := make([]uint64, 0, len(a.blockProgress))
	for height := range a.blockProgress {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})
	var prevHeight uint64
	var prevStoredAt time.Time
	for _, height := range heights {
		event := a.blockProgress[height]
		if event == nil || event.FirstTaskCreatedAt.IsZero() {
			continue
		}
		headerStartedAt := event.HeaderSyncStartedAt
		bodyStartedAt := event.BodySyncStartedAt
		storeStartedAt := event.StoreStartedAt
		stats.Blocks++
		block := BlockTimingBlock{
			Height: event.Height,
			Hash:   event.Hash,
		}
		if prevHeight+1 == event.Height && !prevStoredAt.IsZero() {
			if d, ok := positiveDurationMicros(prevStoredAt, event.FirstTaskCreatedAt); ok {
				prevStoreToTask.add(d)
				block.PrevStoreToTaskUS = d
			}
		}
		if !event.FirstTaskCreatedAt.IsZero() && !headerStartedAt.IsZero() {
			if d, ok := positiveDurationMicros(event.FirstTaskCreatedAt, headerStartedAt); ok {
				taskToHeaderStart.add(d)
				block.TaskToHeaderStartUS = d
			}
		}
		if event.HeaderCostUS > 0 {
			headerSync.add(event.HeaderCostUS)
			block.HeaderSyncUS = event.HeaderCostUS
		}
		if !event.HeaderSyncedAt.IsZero() && !bodyStartedAt.IsZero() {
			if d, ok := positiveDurationMicros(event.HeaderSyncedAt, bodyStartedAt); ok {
				headerToBodyStart.add(d)
				block.HeaderToBodyStartUS = d
			}
		}
		if event.BodyCostUS > 0 {
			bodySync.add(event.BodyCostUS)
			block.BodySyncUS = event.BodyCostUS
		}
		if !event.BodySyncedAt.IsZero() && !storeStartedAt.IsZero() {
			if d, ok := positiveDurationMicros(event.BodySyncedAt, storeStartedAt); ok {
				bodyToStoreStart.add(d)
				block.BodyToStoreStartUS = d
			}
		}
		if event.StoreTotalUS > 0 {
			storeCost.add(event.StoreTotalUS)
			block.StoreUS = event.StoreTotalUS
		}
		if !event.FirstTaskCreatedAt.IsZero() && !event.StoredAt.IsZero() {
			if d, ok := positiveDurationMicros(event.FirstTaskCreatedAt, event.StoredAt); ok {
				endToEnd.add(d)
				block.FirstTaskToStoreUS = d
				directCostTotal += event.HeaderCostUS + event.BodyCostUS + event.StoreTotalUS
				slowBlocks = append(slowBlocks, block)
			}
		}
		if event.TaskQueueWaitSamples > 0 {
			taskQueue.add(event.TaskQueueWaitTotalUS / event.TaskQueueWaitSamples)
		}
		if block.BodySyncUS > 0 || block.StoreUS > 0 || block.FirstTaskToStoreUS > 0 {
			blocks = append(blocks, block)
		}
		if block.BodySyncUS > bodySyncDurationOutlierUS {
			bodySyncOutliers = append(bodySyncOutliers, block)
		}
		if block.FirstTaskToStoreUS > blockDurationOutlierUS {
			blockDurationOutliers = append(blockDurationOutliers, block)
		}
		if !event.StoredAt.IsZero() {
			prevHeight = event.Height
			prevStoredAt = event.StoredAt
		}
	}
	stats.PrevStoreToTaskSamples = prevStoreToTask.samples()
	stats.TaskToHeaderStartSamples = taskToHeaderStart.samples()
	stats.HeaderStageSamples = headerSync.samples()
	stats.HeaderToBodyStartSamples = headerToBodyStart.samples()
	stats.BodyStageSamples = bodySync.samples()
	stats.BodySyncCostSamples = bodySync.samples()
	stats.BodyToStoreStartSamples = bodyToStoreStart.samples()
	stats.StoreStageSamples = storeCost.samples()
	stats.StoreCostSamples = storeCost.samples()
	stats.EndToEndSamples = endToEnd.samples()
	stats.TaskQueueSamples = taskQueue.samples()
	stats.AvgPrevStoreToTaskUS = prevStoreToTask.avg()
	stats.AvgTaskToHeaderStartUS = taskToHeaderStart.avg()
	stats.AvgHeaderSyncCostUS = headerSync.avg()
	stats.AvgHeaderToBodyStartUS = headerToBodyStart.avg()
	stats.AvgBodySyncCostUS = bodySync.avg()
	stats.AvgBodyToStoreStartUS = bodyToStoreStart.avg()
	stats.AvgFirstTaskToStoreUS = endToEnd.avg()
	stats.P95FirstTaskToStoreUS = percentile95(endToEnd.values)
	stats.MaxFirstTaskToStoreUS = endToEnd.max
	stats.AvgTaskQueueWaitUS = taskQueue.avg()
	stats.AvgStoreCostUS = storeCost.avg()
	if stats.EndToEndSamples > 0 {
		avgDirectCost := directCostTotal / stats.EndToEndSamples
		if stats.AvgFirstTaskToStoreUS > avgDirectCost {
			stats.AvgUnattributedOverheadUS = stats.AvgFirstTaskToStoreUS - avgDirectCost
		}
	}
	a.report.BlockTiming = stats
	a.report.BlockTimingPhases = []BlockTimingPhaseStats{
		prevStoreToTask.stats(),
		taskToHeaderStart.stats(),
		headerSync.stats(),
		headerToBodyStart.stats(),
		bodySync.stats(),
		bodyToStoreStart.stats(),
		storeCost.stats(),
		taskQueue.stats(),
		endToEnd.stats(),
	}
	sort.Slice(slowBlocks, func(i, j int) bool {
		if slowBlocks[i].FirstTaskToStoreUS == slowBlocks[j].FirstTaskToStoreUS {
			return slowBlocks[i].Height < slowBlocks[j].Height
		}
		return slowBlocks[i].FirstTaskToStoreUS > slowBlocks[j].FirstTaskToStoreUS
	})
	if len(slowBlocks) > 10 {
		slowBlocks = slowBlocks[:10]
	}
	if len(blockDurationOutliers) > 0 {
		a.addAnomaly("medium", "block_duration_slow", "Block duration exceeded 10s", uint64(len(blockDurationOutliers)), "Check slow body RPC, node throttling, retries, and store readiness for these blocks.")
	}
	if len(bodySyncOutliers) > 0 {
		a.addAnomaly("medium", "block_body_sync_slow", "Block body sync duration exceeded 10s", uint64(len(bodySyncOutliers)), "Check slow full-block RPC methods, node throttling, and retry behavior for these blocks.")
	}
	a.report.BlockTimings = blocks
	a.report.BlockDurationOutliers = blockDurationOutliers
	a.report.BodySyncOutliers = bodySyncOutliers
	a.report.SlowBlockTimings = slowBlocks
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
		DurationSamples:       delta(first.Succeeded, last.Succeeded) + delta(first.Failed, last.Failed),
		DurationTotalUS:       delta(first.DurationTotalUS, last.DurationTotalUS),
		DurationAvgUS:         last.DurationAvgUS,
		DurationLastUS:        last.DurationLastUS,
		DurationMaxUS:         last.DurationMaxUS,
	}
	a.report.Store.SuccessRate = percent(a.report.Store.Succeeded, a.report.Store.Succeeded+a.report.Store.Failed)
}

func (a *analyzer) finishStoreDataTypes() {
	stats := make([]StoreDataTypeStats, 0, len(a.storeDataTypes))
	for _, item := range a.storeDataTypes {
		if item.Tasks > 0 {
			item.AvgCostUS = item.TotalCostUS / item.Tasks
		}
		if item.Writes > 0 {
			item.AvgWriteCostUS = item.TotalCostUS / item.Writes
		}
		if item.Failures > 0 {
			item.AvgFailedCostUS = item.FailedCostUS / item.Failures
		}
		stats = append(stats, *item)
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Writes == stats[j].Writes {
			return stats[i].Type < stats[j].Type
		}
		return stats[i].Writes > stats[j].Writes
	})
	a.report.StoreDataTypes = stats
}

func (a *analyzer) finishFullBlockStore() {
	if len(a.fullBlockStoreSamples) == 0 {
		return
	}
	var totalSum, dataSum, finalizeSum, readySum, blockRowSum uint64
	var totalValues []uint64
	for _, sample := range a.fullBlockStoreSamples {
		totalSum += sample.TotalUS
		dataSum += sample.DataUS
		finalizeSum += sample.FinalizeUS
		readySum += sample.ReadyUS
		blockRowSum += sample.BlockRowUS
		if sample.TotalUS > a.report.FullBlockStore.MaxTotalUS {
			a.report.FullBlockStore.MaxTotalUS = sample.TotalUS
		}
		if sample.DataUS > a.report.FullBlockStore.MaxDataUS {
			a.report.FullBlockStore.MaxDataUS = sample.DataUS
		}
		if sample.FinalizeUS > a.report.FullBlockStore.MaxFinalizeUS {
			a.report.FullBlockStore.MaxFinalizeUS = sample.FinalizeUS
		}
		totalValues = append(totalValues, sample.TotalUS)
	}
	count := uint64(len(a.fullBlockStoreSamples))
	a.report.FullBlockStore.Count = count
	a.report.FullBlockStore.AvgTotalUS = totalSum / count
	a.report.FullBlockStore.P95TotalUS = percentile95(totalValues)
	a.report.FullBlockStore.AvgDataUS = dataSum / count
	a.report.FullBlockStore.AvgFinalizeUS = finalizeSum / count
	a.report.FullBlockStore.AvgReadyUS = readySum / count
	a.report.FullBlockStore.AvgBlockRowUS = blockRowSum / count
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

func (a *analyzer) finishNodeOperatorMethods() {
	methods := make([]NodeOperatorMethodStats, 0, len(a.nodeOperatorWindows))
	for _, w := range a.nodeOperatorWindows {
		if !w.seen {
			continue
		}
		first := w.first
		if w.samples == 1 {
			first = nodeOperatorMethodSnapshot{}
		}
		calls := delta(first.Calls, w.last.Calls)
		if calls == 0 {
			continue
		}
		arraySamples := delta(first.ArraySamples, w.last.ArraySamples)
		item := NodeOperatorMethodStats{
			NodeID:          w.NodeID,
			Method:          w.Method,
			Calls:           calls,
			TotalCostUS:     delta(first.TotalCostUS, w.last.TotalCostUS),
			MaxCostUS:       w.last.MaxCostUS,
			ArraySamples:    arraySamples,
			ArrayItemsTotal: delta(first.ArrayItemsTotal, w.last.ArrayItemsTotal),
			MaxArrayItems:   w.last.MaxArrayItems,
		}
		if item.Calls > 0 {
			item.AvgCostUS = item.TotalCostUS / item.Calls
		}
		if item.ArraySamples > 0 {
			item.AvgArrayItems = item.ArrayItemsTotal / item.ArraySamples
		}
		methods = append(methods, item)
	}
	sort.Slice(methods, func(i, j int) bool {
		if methods[i].NodeID != methods[j].NodeID {
			return methods[i].NodeID < methods[j].NodeID
		}
		if methods[i].Calls == methods[j].Calls {
			return methods[i].Method < methods[j].Method
		}
		return methods[i].Calls > methods[j].Calls
	})
	a.report.NodeOperatorMethods = methods
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
	a.report.Summary.StoredHeight = a.report.RuntimeLatest.StoredHeight
	a.report.Summary.StoredCount = a.report.RuntimeLatest.StoredCount
	if a.report.RuntimeLatest.RemoteLatest > 0 && a.report.RuntimeLatest.StoredHeight > 0 && a.report.RuntimeLatest.RemoteLatest >= a.report.RuntimeLatest.StoredHeight {
		a.report.Summary.Lag = strconv.FormatUint(a.report.RuntimeLatest.RemoteLatest-a.report.RuntimeLatest.StoredHeight, 10)
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

func colonFields(line string) map[string]string {
	result := make(map[string]string)
	for _, field := range strings.Fields(line) {
		parts := strings.SplitN(field, ":", 2)
		if len(parts) != 2 {
			continue
		}
		result[parts[0]] = parts[1]
	}
	return result
}

func keyDurationPairs(line string) map[string]uint64 {
	result := make(map[string]uint64)
	for _, match := range durationFieldRE.FindAllStringSubmatch(line, -1) {
		if len(match) != 3 {
			continue
		}
		result[match[1]] = durationToMicros(match[2])
	}
	return result
}

func storeDurationPairs(line string) map[string]uint64 {
	result := make(map[string]uint64)
	for _, match := range storeDurationFieldRE.FindAllStringSubmatch(line, -1) {
		if len(match) != 3 {
			continue
		}
		result[match[1]] = durationToMicros(match[2])
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

func parseHeaderHeightTaskKey(key string) uint64 {
	return parseUint(strings.TrimPrefix(strings.TrimSpace(key), "header_height:"))
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

func averageUint(total, samples uint64) uint64 {
	if samples == 0 {
		return 0
	}
	return total / samples
}

func positiveDurationMicros(start, end time.Time) (uint64, bool) {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0, false
	}
	return uint64(end.Sub(start).Microseconds()), true
}

func percent(part, total uint64) string {
	if total == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.2f%%", float64(part)*100/float64(total))
}

func percentile95(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	values = append([]uint64(nil), values...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	idx := (len(values)*95 + 99) / 100
	if idx == 0 {
		return values[0]
	}
	if idx > len(values) {
		idx = len(values)
	}
	return values[idx-1]
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
		"formatClock": func(t time.Time) string {
			if t.IsZero() {
				return "no data"
			}
			return t.Format("15:04:05.000")
		},
		"microsToMillis": func(us uint64) string {
			return fmt.Sprintf("%.3f", float64(us)/1000)
		},
		"formatSignedMicros":          formatSignedMicros,
		"syncProgressChart":           renderSyncProgressChart,
		"blocktreeLagChart":           renderBlocktreeLagChart,
		"blockTimingChart":            renderBlockTimingChart,
		"blockTimingPhaseChart":       renderBlockTimingPhaseChart,
		"taskToStoreDurationChart":    renderTaskToStoreDurationChart,
		"bodySyncDurationChart":       renderBodySyncDurationChart,
		"storeDurationChart":          renderStoreDurationChart,
		"syncNodeOperatorChart":       renderSyncNodeOperatorChart,
		"bodySyncNodeOperatorMethods": bodySyncNodeOperatorMethods,
		"pipelineTimingPhases":        pipelineTimingPhases,
		"summaryTimingPhases":         summaryTimingPhases,
		"nodeSyncChart":               renderNodeSyncChart,
	}
}

func formatSignedMicros(us int64) string {
	sign := ""
	if us < 0 {
		sign = "-"
		us = -us
	}
	switch {
	case us >= 1_000_000:
		return fmt.Sprintf("%s%.3fs", sign, float64(us)/1_000_000)
	case us >= 1_000:
		return fmt.Sprintf("%s%.3fms", sign, float64(us)/1_000)
	default:
		return fmt.Sprintf("%s%dµs", sign, us)
	}
}

func bodySyncNodeOperatorMethods(methods []NodeOperatorMethodStats) []NodeOperatorMethodStats {
	rows := make([]NodeOperatorMethodStats, 0, len(methods))
	for _, method := range methods {
		if isBodySyncNodeOperatorMethod(method.Method) {
			rows = append(rows, method)
		}
	}
	return rows
}

func isBodySyncNodeOperatorMethod(method string) bool {
	return method != "" && method != "FetchBlockHeaderByHeight"
}

func renderSyncNodeOperatorChart(methods []NodeOperatorMethodStats) template.HTML {
	if len(methods) == 0 {
		return ""
	}
	rows, nodeIDs := syncNodeOperatorChartRows(methods)
	if len(rows) == 0 || len(nodeIDs) == 0 {
		return ""
	}

	const (
		width       = 960.0
		left        = 230.0
		right       = 70.0
		top         = 26.0
		bottom      = 42.0
		rowH        = 36.0
		barH        = 18.0
		legendItemW = 80.0
		legendRowH  = 20.0
	)
	legendRows := (len(nodeIDs) + 8) / 9
	legendH := float64(legendRows) * legendRowH
	plotTop := top + legendH + 20
	height := plotTop + float64(len(rows))*rowH + bottom
	plotW := width - left - right

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 %.0f" role="img" aria-label="Sync node remote interface duration ratio">`, height))
	for i, nodeID := range nodeIDs {
		legendX := left + float64(i%9)*legendItemW
		legendY := top + float64(i/9)*legendRowH
		b.WriteString(fmt.Sprintf(`<rect x="%.1f" y="%.1f" width="12" height="12" rx="2" fill="%s"></rect>`, legendX, legendY, template.HTMLEscapeString(syncNodeOperatorNodeColor(nodeID))))
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">N%d</text>`, legendX+18, legendY+10, nodeID))
	}
	for i := 0; i <= 4; i++ {
		x := left + float64(i)*plotW/4
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, plotTop-8, x, plotTop+float64(len(rows))*rowH-8))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%d%%</text>`, x, height-16, i*25))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Cost ratio by request</text>`, left, plotTop-14))

	for i, row := range rows {
		y := plotTop + float64(i)*rowH
		labelY := y + barH - 4
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, left-12, labelY, template.HTMLEscapeString(row.Method)))
		var offset uint64
		for _, segment := range row.Segments {
			if segment.Value == 0 {
				continue
			}
			x1 := left + float64(offset)*plotW/float64(row.TotalUS)
			offset += segment.Value
			x2 := left + float64(offset)*plotW/float64(row.TotalUS)
			segmentW := x2 - x1
			if segmentW <= 0 {
				continue
			}
			b.WriteString(fmt.Sprintf(`<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"></rect>`, x1, y, segmentW, barH, template.HTMLEscapeString(syncNodeOperatorNodeColor(segment.NodeID))))
			if segmentW >= 46 {
				percent := float64(segment.Value) * 100 / float64(row.TotalUS)
				b.WriteString(fmt.Sprintf(`<text class="bar-label" x="%.1f" y="%.1f">N%d %.0f%%</text>`, x1+6, y+barH-4, segment.NodeID, percent))
			}
		}
		b.WriteString(fmt.Sprintf(`<text class="axis-label y-right-label" x="%.1f" y="%.1f">%s</text>`, left+plotW+8, labelY, template.HTMLEscapeString(formatSignedMicros(int64(row.TotalUS)))))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func syncNodeOperatorChartRows(methods []NodeOperatorMethodStats) ([]syncNodeChartRow, []int) {
	byMethod := make(map[string]map[int]uint64)
	methodTotals := make(map[string]uint64)
	nodeSeen := make(map[int]bool)
	for _, method := range methods {
		if method.TotalCostUS == 0 {
			continue
		}
		nodeValues := byMethod[method.Method]
		if nodeValues == nil {
			nodeValues = make(map[int]uint64)
			byMethod[method.Method] = nodeValues
		}
		nodeValues[method.NodeID] += method.TotalCostUS
		methodTotals[method.Method] += method.TotalCostUS
		nodeSeen[method.NodeID] = true
	}

	nodeIDs := make([]int, 0, len(nodeSeen))
	for nodeID := range nodeSeen {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Ints(nodeIDs)

	methodNames := make([]string, 0, len(methodTotals))
	for method := range methodTotals {
		methodNames = append(methodNames, method)
	}
	sort.Slice(methodNames, func(i, j int) bool {
		if methodTotals[methodNames[i]] == methodTotals[methodNames[j]] {
			return methodNames[i] < methodNames[j]
		}
		return methodTotals[methodNames[i]] > methodTotals[methodNames[j]]
	})

	rows := make([]syncNodeChartRow, 0, len(methodNames))
	for _, method := range methodNames {
		nodeValues := byMethod[method]
		row := syncNodeChartRow{Method: method}
		for _, nodeID := range nodeIDs {
			value := nodeValues[nodeID]
			if value == 0 {
				continue
			}
			row.TotalUS += value
			row.Segments = append(row.Segments, syncNodeChartSegment{NodeID: nodeID, Value: value})
		}
		if row.TotalUS > 0 {
			rows = append(rows, row)
		}
	}
	return rows, nodeIDs
}

func renderBodySyncNodeOperatorChart(methods []NodeOperatorMethodStats) template.HTML {
	if len(methods) == 0 {
		return ""
	}
	rows, methodNames := bodySyncNodeOperatorChartRows(methods)
	if len(rows) == 0 || len(methodNames) == 0 {
		return ""
	}

	const (
		width       = 960.0
		left        = 110.0
		right       = 70.0
		top         = 26.0
		bottom      = 42.0
		rowH        = 36.0
		barH        = 18.0
		legendItemW = 230.0
		legendRowH  = 20.0
	)
	legendRows := (len(methodNames) + 2) / 3
	legendH := float64(legendRows) * legendRowH
	plotTop := top + legendH + 20
	height := plotTop + float64(len(rows))*rowH + bottom
	plotW := width - left - right

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 %.0f" role="img" aria-label="Body sync node remote interface duration ratio">`, height))
	for i, method := range methodNames {
		legendX := left + float64(i%3)*legendItemW
		legendY := top + float64(i/3)*legendRowH
		b.WriteString(fmt.Sprintf(`<rect x="%.1f" y="%.1f" width="12" height="12" rx="2" fill="%s"></rect>`, legendX, legendY, template.HTMLEscapeString(bodySyncNodeOperatorMethodColor(method))))
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">%s</text>`, legendX+18, legendY+10, template.HTMLEscapeString(method)))
	}
	for i := 0; i <= 4; i++ {
		x := left + float64(i)*plotW/4
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, plotTop-8, x, plotTop+float64(len(rows))*rowH-8))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%d%%</text>`, x, height-16, i*25))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Cost ratio by node</text>`, left, plotTop-14))

	for i, row := range rows {
		y := plotTop + float64(i)*rowH
		labelY := y + barH - 4
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">N%d</text>`, left-12, labelY, row.NodeID))
		var offset uint64
		for _, segment := range row.Segments {
			if segment.Value == 0 {
				continue
			}
			x1 := left + float64(offset)*plotW/float64(row.TotalUS)
			offset += segment.Value
			x2 := left + float64(offset)*plotW/float64(row.TotalUS)
			segmentW := x2 - x1
			if segmentW <= 0 {
				continue
			}
			b.WriteString(fmt.Sprintf(`<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"></rect>`, x1, y, segmentW, barH, template.HTMLEscapeString(bodySyncNodeOperatorMethodColor(segment.Method))))
			if segmentW >= 58 {
				percent := float64(segment.Value) * 100 / float64(row.TotalUS)
				b.WriteString(fmt.Sprintf(`<text class="bar-label" x="%.1f" y="%.1f">%.0f%%</text>`, x1+6, y+barH-4, percent))
			}
		}
		b.WriteString(fmt.Sprintf(`<text class="axis-label y-right-label" x="%.1f" y="%.1f">%s</text>`, left+plotW+8, labelY, template.HTMLEscapeString(formatSignedMicros(int64(row.TotalUS)))))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func bodySyncNodeOperatorChartRows(methods []NodeOperatorMethodStats) ([]bodySyncNodeChartRow, []string) {
	byNode := make(map[int]map[string]uint64)
	methodTotals := make(map[string]uint64)
	for _, method := range methods {
		if method.TotalCostUS == 0 {
			continue
		}
		nodeMethods := byNode[method.NodeID]
		if nodeMethods == nil {
			nodeMethods = make(map[string]uint64)
			byNode[method.NodeID] = nodeMethods
		}
		nodeMethods[method.Method] += method.TotalCostUS
		methodTotals[method.Method] += method.TotalCostUS
	}
	methodNames := make([]string, 0, len(methodTotals))
	for method := range methodTotals {
		methodNames = append(methodNames, method)
	}
	sort.Slice(methodNames, func(i, j int) bool {
		if methodTotals[methodNames[i]] == methodTotals[methodNames[j]] {
			return methodNames[i] < methodNames[j]
		}
		return methodTotals[methodNames[i]] > methodTotals[methodNames[j]]
	})

	nodeIDs := make([]int, 0, len(byNode))
	for nodeID := range byNode {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Ints(nodeIDs)

	rows := make([]bodySyncNodeChartRow, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodeMethods := byNode[nodeID]
		row := bodySyncNodeChartRow{NodeID: nodeID}
		for _, method := range methodNames {
			value := nodeMethods[method]
			if value == 0 {
				continue
			}
			row.TotalUS += value
			row.Segments = append(row.Segments, bodySyncNodeChartSegment{Method: method, Value: value})
		}
		if row.TotalUS > 0 {
			rows = append(rows, row)
		}
	}
	return rows, methodNames
}

func bodySyncNodeOperatorMethodColor(method string) string {
	palette := []string{
		"#155eef", "#f63d68", "#12b76a", "#f79009", "#7a5af8", "#06aed4",
		"#b54708", "#079455", "#2e90fa", "#d444f1", "#667085", "#b42318",
	}
	known := []string{
		"FetchBalanceNative",
		"FetchErc20BalancesBatch",
		"FetchErc1155BalancesBatch",
		"FetchReceiptsBatch",
		"FetchTransactionsByHashBatch",
		"FetchContractErc20",
		"FetchContractErc721",
		"FetchTokenErc721",
		"FetchInternalTxTracesByBlockHash",
		"FetchBlockHeaderByHash",
	}
	for i, knownMethod := range known {
		if method == knownMethod {
			return palette[i%len(palette)]
		}
	}
	var hash uint32
	for _, r := range method {
		hash = hash*33 + uint32(r)
	}
	return palette[int(hash)%len(palette)]
}

func syncNodeOperatorNodeColor(nodeID int) string {
	palette := []string{
		"#155eef", "#f63d68", "#12b76a", "#f79009", "#7a5af8", "#06aed4",
		"#b54708", "#079455", "#2e90fa", "#d444f1", "#667085", "#b42318",
	}
	if nodeID < 0 {
		nodeID = -nodeID
	}
	return palette[nodeID%len(palette)]
}

func pipelineTimingPhases(phases []BlockTimingPhaseStats) []BlockTimingPhaseStats {
	rows := make([]BlockTimingPhaseStats, 0, len(phases))
	for _, phase := range phases {
		if isSummaryTimingPhase(phase.Name) {
			continue
		}
		rows = append(rows, phase)
	}
	return rows
}

func summaryTimingPhases(phases []BlockTimingPhaseStats) []BlockTimingPhaseStats {
	rows := make([]BlockTimingPhaseStats, 0, 2)
	for _, phase := range phases {
		if isSummaryTimingPhase(phase.Name) {
			rows = append(rows, phase)
		}
	}
	return rows
}

func isSummaryTimingPhase(name string) bool {
	return name == "Task Queue Wait" || name == "Task Created -> Store Done"
}

func renderBlockTimingPhaseChart(phases []BlockTimingPhaseStats) template.HTML {
	if len(phases) == 0 {
		return ""
	}

	maxValue := uint64(0)
	for _, phase := range phases {
		maxValue = maxUint64(maxValue, phase.AvgUS)
		maxValue = maxUint64(maxValue, phase.P95US)
	}
	if maxValue == 0 {
		return ""
	}

	const (
		width  = 960.0
		left   = 210.0
		right  = 34.0
		top    = 34.0
		bottom = 48.0
		rowH   = 58.0
		barH   = 12.0
		gap    = 4.0
	)
	height := top + bottom + float64(len(phases))*rowH
	plotW := width - left - right
	plotH := height - top - bottom
	scaleX := func(value uint64) float64 {
		return left + float64(value)*plotW/float64(maxValue)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 %.0f" role="img" aria-label="Timing phase percentiles">`, height))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		x := left + float64(i)*plotW/4
		value := uint64(float64(maxValue) * float64(i) / 4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, top, x, top+plotH))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, x, height-16, template.HTMLEscapeString(formatSignedMicros(int64(value)))))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Duration</text>`, left, top-10))

	for i, phase := range phases {
		rowTop := top + float64(i)*rowH + 13
		labelY := rowTop + barH + gap + 4
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, left-12, labelY, template.HTMLEscapeString(phase.Name)))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">n=%d</text>`, left-12, labelY+15, phase.Samples))
		writePhaseBar(&b, "phase-avg", phase.AvgUS, left, rowTop, barH, scaleX)
		writePhaseBar(&b, "phase-p95", phase.P95US, left, rowTop+barH+gap, barH, scaleX)
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func writePhaseBar(b *strings.Builder, class string, value uint64, left, y, height float64, scaleX func(uint64) float64) {
	if value == 0 {
		return
	}
	width := scaleX(value) - left
	if width <= 0 {
		return
	}
	b.WriteString(fmt.Sprintf(`<rect class="bar %s" x="%.1f" y="%.1f" width="%.1f" height="%.1f"></rect>`, template.HTMLEscapeString(class), left, y, width, height))
	if width >= 44 {
		b.WriteString(fmt.Sprintf(`<text class="bar-label" x="%.1f" y="%.1f">%s</text>`, left+6, y+height-2, template.HTMLEscapeString(formatSignedMicros(int64(value)))))
	}
}

func renderBlockTimingChart(stats BlockTimingStats) template.HTML {
	if stats.Blocks == 0 {
		return ""
	}

	stageSegments := []blockTimingSegment{
		{Label: "Prev Store -> Task", Class: "prev-store", Value: stats.AvgPrevStoreToTaskUS},
		{Label: "Task -> Header Start", Class: "stage-header", Value: stats.AvgTaskToHeaderStartUS},
		{Label: "Header Sync", Class: "rpc-header", Value: stats.AvgHeaderSyncCostUS},
		{Label: "Header Done -> Body Start", Class: "stage-body", Value: stats.AvgHeaderToBodyStartUS},
		{Label: "Body Sync", Class: "rpc-body", Value: stats.AvgBodySyncCostUS},
		{Label: "Body Done -> Store Start", Class: "stage-store", Value: stats.AvgBodyToStoreStartUS},
		{Label: "Store", Class: "store-write", Value: stats.AvgStoreCostUS},
	}
	costSegments := []blockTimingSegment{
		{Label: "Task Queue", Class: "task-queue", Value: stats.AvgTaskQueueWaitUS},
		{Label: "Header RPC", Class: "rpc-header", Value: stats.AvgHeaderSyncCostUS},
		{Label: "Body RPC", Class: "rpc-body", Value: stats.AvgBodySyncCostUS},
		{Label: "Store Write", Class: "store-write", Value: stats.AvgStoreCostUS},
		{Label: "Other Wait", Class: "other-wait", Value: stats.AvgUnattributedOverheadUS},
	}

	stageTotal := sumSegments(stageSegments)
	costTotal := sumSegments(costSegments)
	maxValue := maxUint64(stageTotal, costTotal)
	maxValue = maxUint64(maxValue, stats.AvgFirstTaskToStoreUS)
	maxValue = maxUint64(maxValue, stats.P95FirstTaskToStoreUS)
	if maxValue == 0 {
		return ""
	}

	const (
		width  = 960.0
		height = 260.0
		left   = 150.0
		right  = 34.0
		top    = 28.0
		bottom = 54.0
		plotW  = width - left - right
		plotH  = height - top - bottom
		barH   = 34.0
	)
	rowY := []float64{76, 152}
	scaleX := func(value uint64) float64 {
		return left + float64(value)*plotW/float64(maxValue)
	}

	var b strings.Builder
	b.WriteString(`<svg class="progress-chart" viewBox="0 0 960 260" role="img" aria-label="Block timing overview with requested block time distribution and direct cost attribution">`)
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		x := left + float64(i)*plotW/4
		value := uint64(float64(maxValue) * float64(i) / 4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, top, x, top+plotH))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, x, height-18, template.HTMLEscapeString(formatSignedMicros(int64(value)))))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Time</text>`, left, top-10))
	b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">Mean requested phases</text>`, left-12, rowY[0]+5))
	b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">Mean direct costs</text>`, left-12, rowY[1]+5))
	writeStackedTimingBar(&b, stageSegments, left, rowY[0]-barH/2, barH, scaleX)
	writeStackedTimingBar(&b, costSegments, left, rowY[1]-barH/2, barH, scaleX)
	if stats.AvgFirstTaskToStoreUS > 0 {
		x := scaleX(stats.AvgFirstTaskToStoreUS)
		b.WriteString(fmt.Sprintf(`<line class="marker mean" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, top, x, top+plotH))
		b.WriteString(fmt.Sprintf(`<text class="axis-label y-right-label" x="%.1f" y="%.1f">mean E2E %sms</text>`, x+6, top+30, template.HTMLEscapeString(fmt.Sprintf("%.3f", float64(stats.AvgFirstTaskToStoreUS)/1000))))
	}
	if stats.P95FirstTaskToStoreUS > 0 {
		x := scaleX(stats.P95FirstTaskToStoreUS)
		b.WriteString(fmt.Sprintf(`<line class="marker p95" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, x, top, x, top+plotH))
		b.WriteString(fmt.Sprintf(`<text class="axis-label y-right-label" x="%.1f" y="%.1f">p95 E2E %sms</text>`, x+6, top+14, template.HTMLEscapeString(fmt.Sprintf("%.3f", float64(stats.P95FirstTaskToStoreUS)/1000))))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func writeStackedTimingBar(b *strings.Builder, segments []blockTimingSegment, left, y, height float64, scaleX func(uint64) float64) {
	var offset uint64
	for _, segment := range segments {
		if segment.Value == 0 {
			continue
		}
		x1 := scaleX(offset)
		offset += segment.Value
		x2 := scaleX(offset)
		width := x2 - x1
		if width <= 0 {
			continue
		}
		b.WriteString(fmt.Sprintf(`<rect class="bar %s" x="%.1f" y="%.1f" width="%.1f" height="%.1f"></rect>`, template.HTMLEscapeString(segment.Class), x1, y, width, height))
		if width >= 86 {
			label := fmt.Sprintf("%s %.1fms", segment.Label, float64(segment.Value)/1000)
			b.WriteString(fmt.Sprintf(`<text class="bar-label" x="%.1f" y="%.1f">%s</text>`, x1+8, y+height/2+4, template.HTMLEscapeString(label)))
		}
	}
}

func sumSegments(segments []blockTimingSegment) uint64 {
	var total uint64
	for _, segment := range segments {
		total += segment.Value
	}
	return total
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func renderBodySyncDurationChart(rows []BlockTimingBlock) template.HTML {
	return renderSingleBlockTimingDurationChart(filterBodySyncDurationRows(rows), "Body sync duration", "body-gap", func(row BlockTimingBlock) uint64 {
		return row.BodySyncUS
	})
}

func renderTaskToStoreDurationChart(rows []BlockTimingBlock) template.HTML {
	return renderSingleBlockTimingDurationChart(filterBlockDurationRows(rows), "Block duration", "end-to-end", func(row BlockTimingBlock) uint64 {
		return row.FirstTaskToStoreUS
	})
}

func renderStoreDurationChart(rows []BlockTimingBlock) template.HTML {
	return renderSingleBlockTimingDurationChart(rows, "Store duration", "store-gap", func(row BlockTimingBlock) uint64 {
		return row.StoreUS
	})
}

func filterBodySyncDurationRows(rows []BlockTimingBlock) []BlockTimingBlock {
	filtered := make([]BlockTimingBlock, 0, len(rows))
	for _, row := range rows {
		if row.BodySyncUS > bodySyncDurationOutlierUS {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func filterBlockDurationRows(rows []BlockTimingBlock) []BlockTimingBlock {
	filtered := make([]BlockTimingBlock, 0, len(rows))
	for _, row := range rows {
		if row.FirstTaskToStoreUS > blockDurationOutlierUS {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func renderSingleBlockTimingDurationChart(rows []BlockTimingBlock, label, lineClass string, selector func(BlockTimingBlock) uint64) template.HTML {
	if len(rows) == 0 {
		return ""
	}

	maxDurationUS := uint64(0)
	for _, row := range rows {
		if value := selector(row); value > maxDurationUS {
			maxDurationUS = value
		}
	}
	if maxDurationUS == 0 {
		return ""
	}

	const (
		width  = 960.0
		height = 280.0
		left   = 82.0
		right  = 20.0
		top    = 24.0
		bottom = 52.0
		plotW  = width - left - right
		plotH  = height - top - bottom
	)
	scaleX := func(i int) float64 {
		if len(rows) == 1 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(rows)-1)
	}
	scaleY := func(value uint64) float64 {
		return top + (float64(maxDurationUS-value) * plotH / float64(maxDurationUS))
	}

	var points strings.Builder
	for i, row := range rows {
		value := selector(row)
		if value == 0 {
			continue
		}
		if points.Len() > 0 {
			points.WriteByte(' ')
		}
		points.WriteString(fmt.Sprintf("%.1f,%.1f", scaleX(i), scaleY(value)))
	}
	if points.Len() == 0 {
		return ""
	}

	escapedLabel := template.HTMLEscapeString(label)
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 280" role="img" aria-label="Per-block %s">`, escapedLabel))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		y := top + float64(i)*plotH/4
		value := maxDurationUS - uint64(float64(maxDurationUS)*float64(i)/4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, y, left+plotW, y))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, left-8, y+4, template.HTMLEscapeString(formatSignedMicros(int64(value)))))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">%s</text>`, left, top-8, escapedLabel))
	b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">%d</text>`, left, height-16, rows[0].Height))
	last := rows[len(rows)-1]
	b.WriteString(fmt.Sprintf(`<text class="axis-label x-end" x="%.1f" y="%.1f">%d</text>`, left+plotW, height-16, last.Height))
	b.WriteString(fmt.Sprintf(`<polyline class="line %s" points="%s"></polyline>`, template.HTMLEscapeString(lineClass), points.String()))
	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}

func renderSingleBlockProgressIntervalChart(rows []BlockProgressInterval, label, lineClass string, selector func(BlockProgressInterval) (int64, bool)) template.HTML {
	if len(rows) == 0 {
		return ""
	}

	maxGapUS := uint64(0)
	for _, row := range rows {
		value, ok := selector(row)
		if ok && value > 0 && uint64(value) > maxGapUS {
			maxGapUS = uint64(value)
		}
	}
	if maxGapUS == 0 {
		return ""
	}

	const (
		width  = 960.0
		height = 280.0
		left   = 82.0
		right  = 20.0
		top    = 24.0
		bottom = 52.0
		plotW  = width - left - right
		plotH  = height - top - bottom
	)
	scaleX := func(i int) float64 {
		if len(rows) == 1 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(rows)-1)
	}
	scaleY := func(value uint64) float64 {
		return top + (float64(maxGapUS-value) * plotH / float64(maxGapUS))
	}

	var points strings.Builder
	for i, row := range rows {
		value, ok := selector(row)
		if !ok {
			continue
		}
		if points.Len() > 0 {
			points.WriteByte(' ')
		}
		points.WriteString(fmt.Sprintf("%.1f,%.1f", scaleX(i), scaleY(uint64(value))))
	}
	if points.Len() == 0 {
		return ""
	}

	escapedLabel := template.HTMLEscapeString(label)
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 280" role="img" aria-label="Per-block %s">`, escapedLabel))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		y := top + float64(i)*plotH/4
		value := maxGapUS - uint64(float64(maxGapUS)*float64(i)/4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, y, left+plotW, y))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%s</text>`, left-8, y+4, template.HTMLEscapeString(formatSignedMicros(int64(value)))))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">%s</text>`, left, top-8, escapedLabel))
	b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">%d</text>`, left, height-16, rows[0].Height))
	last := rows[len(rows)-1]
	b.WriteString(fmt.Sprintf(`<text class="axis-label x-end" x="%.1f" y="%.1f">%d</text>`, left+plotW, height-16, last.Height))
	b.WriteString(fmt.Sprintf(`<polyline class="line %s" points="%s"></polyline>`, template.HTMLEscapeString(lineClass), points.String()))
	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}

func renderNodeSyncChart(nodes []NodeStats) template.HTML {
	if len(nodes) == 0 {
		return ""
	}

	maxRequests := uint64(0)
	maxCostUS := uint64(0)
	for _, node := range nodes {
		if node.Requests > maxRequests {
			maxRequests = node.Requests
		}
		if node.AvgCostUS > maxCostUS {
			maxCostUS = node.AvgCostUS
		}
	}
	if maxRequests == 0 {
		maxRequests = 1
	}
	if maxCostUS == 0 {
		maxCostUS = 1
	}

	const (
		width  = 960.0
		height = 280.0
		left   = 82.0
		right  = 86.0
		top    = 24.0
		bottom = 52.0
		plotW  = width - left - right
		plotH  = height - top - bottom
	)
	barSlot := plotW / float64(len(nodes))
	barW := barSlot * 0.46
	if barW > 42 {
		barW = 42
	}
	scaleReqY := func(value uint64) float64 {
		return top + (float64(maxRequests-value) * plotH / float64(maxRequests))
	}
	scaleCostY := func(value uint64) float64 {
		return top + (float64(maxCostUS-value) * plotH / float64(maxCostUS))
	}

	var costPoints strings.Builder
	var b strings.Builder
	b.WriteString(`<svg class="progress-chart" viewBox="0 0 960 280" role="img" aria-label="Node sync status by success, failure, and average cost">`)
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left+plotW, top, left+plotW, top+plotH))
	for i := 0; i <= 4; i++ {
		y := top + float64(i)*plotH/4
		reqValue := maxRequests - uint64(float64(maxRequests)*float64(i)/4)
		costValueMS := float64(maxCostUS-uint64(float64(maxCostUS)*float64(i)/4)) / 1000
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, y, left+plotW, y))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%d</text>`, left-8, y+4, reqValue))
		b.WriteString(fmt.Sprintf(`<text class="axis-label y-right-label" x="%.1f" y="%.1f">%.1f</text>`, left+plotW+8, y+4, costValueMS))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Requests</text>`, left, top-8))
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-right-title" x="%.1f" y="%.1f">Avg cost (ms)</text>`, left+plotW, top-8))

	baseY := top + plotH
	for i, node := range nodes {
		xCenter := left + float64(i)*barSlot + barSlot/2
		successH := baseY - scaleReqY(node.Successes)
		failureH := baseY - scaleReqY(node.Failures)
		x := xCenter - barW/2
		if successH > 0 {
			b.WriteString(fmt.Sprintf(`<rect class="bar success" x="%.1f" y="%.1f" width="%.1f" height="%.1f"></rect>`, x, baseY-successH, barW, successH))
		}
		if failureH > 0 {
			b.WriteString(fmt.Sprintf(`<rect class="bar failure" x="%.1f" y="%.1f" width="%.1f" height="%.1f"></rect>`, x, baseY-successH-failureH, barW, failureH))
		}
		yCost := scaleCostY(node.AvgCostUS)
		if costPoints.Len() > 0 {
			costPoints.WriteByte(' ')
		}
		costPoints.WriteString(fmt.Sprintf("%.1f,%.1f", xCenter, yCost))
		b.WriteString(fmt.Sprintf(`<circle class="point cost" cx="%.1f" cy="%.1f" r="3.5"></circle>`, xCenter, yCost))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">N%d</text>`, xCenter+8, height-16, node.ID))
	}
	b.WriteString(fmt.Sprintf(`<polyline class="line cost" points="%s"></polyline>`, costPoints.String()))
	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}

func renderSyncProgressChart(series []RuntimeSnapshot) template.HTML {
	if len(series) == 0 {
		return ""
	}

	minHeight, maxHeight := uint64(0), uint64(0)
	for _, snap := range series {
		for _, height := range []uint64{snap.BlocktreeEnd, snap.StoredHeight} {
			if height == 0 {
				continue
			}
			if minHeight == 0 || height < minHeight {
				minHeight = height
			}
			if height > maxHeight {
				maxHeight = height
			}
		}
	}
	if maxHeight == 0 {
		return ""
	}
	if minHeight == maxHeight {
		minHeight--
		maxHeight++
	}

	const (
		width  = 960.0
		height = 280.0
		left   = 82.0
		right  = 20.0
		top    = 24.0
		bottom = 42.0
		plotW  = width - left - right
		plotH  = height - top - bottom
	)
	scaleX := func(i int) float64 {
		if len(series) == 1 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(series)-1)
	}
	scaleHeightY := func(value uint64) float64 {
		return top + (float64(maxHeight-value) * plotH / float64(maxHeight-minHeight))
	}

	var treePoints, storedPoints strings.Builder
	for i, snap := range series {
		x := scaleX(i)
		if snap.BlocktreeEnd > 0 {
			if treePoints.Len() > 0 {
				treePoints.WriteByte(' ')
			}
			treePoints.WriteString(fmt.Sprintf("%.1f,%.1f", x, scaleHeightY(snap.BlocktreeEnd)))
		}
		if snap.StoredHeight > 0 {
			if storedPoints.Len() > 0 {
				storedPoints.WriteByte(' ')
			}
			storedPoints.WriteString(fmt.Sprintf("%.1f,%.1f", x, scaleHeightY(snap.StoredHeight)))
		}
	}

	var b strings.Builder
	b.WriteString(`<svg class="progress-chart" viewBox="0 0 960 280" role="img" aria-label="Sync progress with local block heights">`)
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		y := top + float64(i)*plotH/4
		heightValue := maxHeight - uint64(float64(maxHeight-minHeight)*float64(i)/4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, y, left+plotW, y))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%d</text>`, left-8, y+4, heightValue))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">Block height</text>`, left, top-8))
	if !series[0].Time.IsZero() {
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">%s</text>`, left, height-12, template.HTMLEscapeString(series[0].Time.Format("15:04:05"))))
	}
	last := series[len(series)-1]
	if !last.Time.IsZero() {
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-end" x="%.1f" y="%.1f">%s</text>`, left+plotW, height-12, template.HTMLEscapeString(last.Time.Format("15:04:05"))))
	}
	b.WriteString(fmt.Sprintf(`<polyline class="line tree" points="%s"></polyline>`, treePoints.String()))
	b.WriteString(fmt.Sprintf(`<polyline class="line stored" points="%s"></polyline>`, storedPoints.String()))
	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}

func renderStoredHeightChart(series []RuntimeSnapshot) template.HTML {
	return renderLineChart(series, "Stored block height", "Height", "stored", func(s RuntimeSnapshot) uint64 {
		return s.StoredHeight
	})
}

func renderBlocktreeLagChart(series []RuntimeSnapshot) template.HTML {
	return renderLineChart(series, "BlockTree lag from remote latest", "Blocks behind", "lag", func(s RuntimeSnapshot) uint64 {
		return blocktreeLag(s)
	})
}

func blocktreeLag(s RuntimeSnapshot) uint64 {
	if s.RemoteLatest <= s.BlocktreeEnd {
		return 0
	}
	return s.RemoteLatest - s.BlocktreeEnd
}

func renderLineChart(series []RuntimeSnapshot, ariaLabel, yTitle, lineClass string, selector func(RuntimeSnapshot) uint64) template.HTML {
	if len(series) == 0 {
		return ""
	}

	minValue, maxValue := uint64(0), uint64(0)
	for _, snap := range series {
		value := selector(snap)
		if value == 0 {
			continue
		}
		if minValue == 0 || value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}
	if maxValue == 0 {
		return ""
	}
	if minValue == maxValue {
		minValue--
		maxValue++
	}

	const (
		width  = 960.0
		height = 260.0
		left   = 70.0
		right  = 20.0
		top    = 20.0
		bottom = 42.0
		plotW  = width - left - right
		plotH  = height - top - bottom
	)
	scaleX := func(i int) float64 {
		if len(series) == 1 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(series)-1)
	}
	scaleY := func(value uint64) float64 {
		return top + (float64(maxValue-value) * plotH / float64(maxValue-minValue))
	}
	var points strings.Builder
	for i, snap := range series {
		value := selector(snap)
		if value == 0 {
			continue
		}
		if points.Len() > 0 {
			points.WriteByte(' ')
		}
		points.WriteString(fmt.Sprintf("%.1f,%.1f", scaleX(i), scaleY(value)))
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg class="progress-chart" viewBox="0 0 960 260" role="img" aria-label="%s">`, template.HTMLEscapeString(ariaLabel)))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, top, left, top+plotH))
	for i := 0; i <= 4; i++ {
		y := top + float64(i)*plotH/4
		value := maxValue - uint64(float64(maxValue-minValue)*float64(i)/4)
		b.WriteString(fmt.Sprintf(`<line class="gridline" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"></line>`, left, y, left+plotW, y))
		b.WriteString(fmt.Sprintf(`<text class="axis-label" x="%.1f" y="%.1f">%d</text>`, left-8, y+4, value))
	}
	b.WriteString(fmt.Sprintf(`<text class="axis-title y-left-title" x="%.1f" y="%.1f">%s</text>`, left, top-7, template.HTMLEscapeString(yTitle)))
	if !series[0].Time.IsZero() {
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-start" x="%.1f" y="%.1f">%s</text>`, left, height-12, template.HTMLEscapeString(series[0].Time.Format("15:04:05"))))
	}
	last := series[len(series)-1]
	if !last.Time.IsZero() {
		b.WriteString(fmt.Sprintf(`<text class="axis-label x-end" x="%.1f" y="%.1f">%s</text>`, left+plotW, height-12, template.HTMLEscapeString(last.Time.Format("15:04:05"))))
	}
	b.WriteString(fmt.Sprintf(`<polyline class="line %s" points="%s"></polyline>`, template.HTMLEscapeString(lineClass), points.String()))
	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}
