package logreport

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeParsesScannerLogSummary(t *testing.T) {
	logs := strings.Join([]string{
		`May  4 06:40:43.705 [INFO] [app:scanner_bsc_testnet] [env:prd] task pool stats enqueued:10(body:6 hh:3 hs:1) dequeued:9(body:5 hh:3 hs:1) succeeded:7(body:4 hh:2 hs:1) failed:1(body:1 hh:0 hs:0) retried:2(body:1 hh:1 hs:0) dropped:0(body:0 hh:0 hs:0) pending_high:1 pending_normal:2 tracked:3(body:1 hh:1 hs:1) workers:4 max_retry:2 @task.(*Pool).runTaskStatsReporter /repo/fetch/taskpool/pool.go 398`,
		`May  4 06:40:44.705 [INFO] [app:scanner_bsc_testnet] [env:prd] task pool stats enqueued:15(body:9 hh:4 hs:2) dequeued:14(body:8 hh:4 hs:2) succeeded:12(body:7 hh:3 hs:2) failed:2(body:1 hh:1 hs:0) retried:3(body:1 hh:2 hs:0) dropped:1(body:1 hh:0 hs:0) pending_high:2 pending_normal:3 tracked:4(body:2 hh:1 hs:1) workers:4 max_retry:2 tracked_oldest_age_us:9000 tracked_avg_age_us:5000 tracked_body_oldest_age_us:8000 tracked_hh_oldest_age_us:7000 tracked_hs_oldest_age_us:6000 @task.(*Pool).runTaskStatsReporter /repo/fetch/taskpool/pool.go 398`,
		`May  4 06:40:45.000 [INFO] [app:scanner_bsc_testnet] [env:prd] store block worker stats submitted:5 skipped:1 succeeded:3 failed:1 canceled:0 queue_pending:2 processing:1 skipped_missing_body:1 skipped_parent_not_ready:0 failed_db:1 store_duration_total:12ms store_duration_last:4ms store_duration_avg:3ms store_duration_max:5ms @serialstore.(*Worker).logStats /repo/fetch/serial_store/serial_worker.go 258`,
		`May  4 06:40:45.100 [INFO] [app:scanner_bsc_testnet] [env:prd] scan stage event stage:sync_body target:0xabc target_count:1 success:false duration:12ms err:body sync failed @scan.(*Flow).logScanStageEvent /repo/fetch/scan/flow.go 208`,
		`May  4 06:40:45.120 [DEBUG] [app:scanner_bsc_testnet] [env:prd] store data type success. type:txs rows:4 worker:1 height:106 task_id:10 try_count:1 cost:2ms @store.(*Worker).Run.func1 /repo/fetch/store/worker.go 175`,
		`May  4 06:40:45.125 [ERROR] [app:scanner_bsc_testnet] [env:prd] store failed err:deadlock id:1 height:106 type:txs task_id:10 try_count:2 cost:1ms @store.(*Worker).Run.func1 /repo/fetch/store/worker.go 198`,
		`May  4 06:40:45.130 [DEBUG] [app:scanner_bsc_testnet] [env:prd] store data type success. type:txs rows:6 worker:2 height:106 task_id:11 try_count:1 cost:3ms @store.(*Worker).Run.func1 /repo/fetch/store/worker.go 175`,
		`May  4 06:40:45.150 [INFO] [app:scanner_bsc_testnet] [env:prd] store fullblock. height:106 hash:0xabc block_id:1 message_id:1 txs:1 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:700µs finalize_cost:1ms total_cost:2ms`,
		`May  4 06:40:45.200 [INFO] [app:scanner_bsc_testnet] [env:prd] runtime health stats remote_latest:120 node_ready:1/2 blocktree_range:[100,110] linked:11 leaves:1 branches:1 orphans:0 orphan_parents:0 staging_blocks:4 pending_headers:3 pending_bodies:2 complete_blocks:1 stored_count:105 task_pending_high:2 task_pending_normal:3 task_tracked:4 task_succeeded:12 task_failed:2 task_retried:3 store_submitted:5 store_succeeded:3 store_failed:1 store_skipped:1 store_queue_pending:2 @fetch.(*FetchManager).logRuntimeHealthStats /repo/fetch/fetch_manager.go 340`,
		`May  4 06:40:45.300 [INFO] [app:scanner_bsc_testnet] [env:prd] valid node operators selected. height:100 hash:0xabc valid_nodes:1 node_ids:[1] scores:[100] disabled:1 not_ready:0 cooldown:0 height_too_low:1 remote_unknown:0 nil_nodes:0 @node.(*NodeManager).GetAllValidNodeOperators /repo/fetch/node/manager.go 221`,
		`May  4 06:40:45.400 [INFO] [app:scanner_bsc_testnet] [env:prd] body sync start. height:100 hash:0xabc valid_nodes:1 node_ids:1 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 124`,
		`May  4 06:40:45.500 [INFO] [app:scanner_bsc_testnet] [env:prd] body sync success. height:100 hash:0xabc valid_nodes:1 node_ids:1 txs:7 cost_us:2000 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 139`,
		`May  4 06:40:45.600 [WARN] [app:scanner_bsc_testnet] [env:prd] body sync failed. height:101 hash:0xdef valid_nodes:0 node_ids: cost_us:1000 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 132`,
		`May  4 06:40:46.000 [INFO] [app:scanner_bsc_testnet] [env:prd] [nodeOperator RPC] nodeId=1 FetchBlockHeaderByHeight:eth_getBlockByNumber=4 FetchReceiptsBatch:eth_getTransactionReceipt=10 @node.logNodeOperatorRPCStats /repo/fetch/node/operator_stats.go 124`,
		`May  4 06:40:46.050 [INFO] [app:scanner_bsc_testnet] [env:prd] [nodeOperator Method] nodeId=1 method=FetchReceiptsBatch calls=2 total_us=5000 avg_us=2500 max_us=3000 array_samples=2 array_items_total=20 avg_array_items=10 max_array_items=12 @node.logNodeOperatorRPCStats /repo/fetch/node/operator_stats.go 170`,
		`May  4 06:40:46.100 [INFO] [app:scanner_bsc_testnet] [env:prd] fetch full block rpc success. op:FetchReceiptsBatch nodeId:1 taskId:100 height:100 attempts:2 retries:3 selected_node_ids:1,2 cost_us:3000 @fetcher.withFullBlockRPCRetry /repo/fetch/fetcher/fetcher.go 183`,
		`May  4 06:40:46.120 [DEBUG] [app:scanner_bsc_testnet] [env:prd] fetch full block item cost. height:100 hash:0xabc item:fetch_receipts count:7 cost_us:3000 @fetcher.fetchFullBlock.func2 /repo/fetch/fetcher/fetcher.go 330`,
		`May  4 06:40:46.130 [INFO] [app:scanner_bsc_testnet] [env:prd] header sync success. height:100 hash:0xabc parent_hash:0xparent cost_us:1500 @taskprocess.RuntimeDeps.SyncHeaderByHeight /repo/fetch/task_process/runtime.go 218`,
		`May  4 06:40:46.140 [WARN] [app:scanner_bsc_testnet] [env:prd] header sync failed. height:101 cost_us:500 @taskprocess.RuntimeDeps.SyncHeaderByHeight /repo/fetch/task_process/runtime.go 214`,
		`May  4 06:40:46.200 [WARN] [app:scanner_bsc_testnet] [env:prd] fetch full block rpc failed. op:FetchTransactionsByHashBatch nodeId:1 taskId:100 height:100 attempt:1 retries:3 err:timeout @fetcher.withFullBlockRPCRetry /repo/fetch/fetcher/fetcher.go 196`,
		`May  4 06:40:46.300 [WARN] [app:scanner_bsc_testnet] [env:prd] fetch header failed. nodeId:2 taskId:0 error:429 Too Many Requests height:121 @node.(*NodeOperatorImpl).FetchBlockHeaderByHeight /repo/fetch/node/operation.go 56`,
	}, "\n")

	report, err := Analyze(strings.NewReader(logs), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}

	if report.TaskPool.Totals.Enqueued != 5 || report.TaskPool.Totals.Failed != 1 {
		t.Fatalf("unexpected task pool delta: %+v", report.TaskPool.Totals)
	}
	if len(report.TaskPool.Kinds) != 3 || report.TaskPool.Kinds[0].Name != "body" || report.TaskPool.Kinds[0].Succeeded != 3 {
		t.Fatalf("unexpected task kind stats: %+v", report.TaskPool.Kinds)
	}
	if report.TaskPool.TrackedOldestAgeUS != 9000 || report.TaskPool.TrackedAvgAgeUS != 5000 || report.TaskPool.TrackedBodyOldestAgeUS != 8000 {
		t.Fatalf("unexpected task pool age stats: %+v", report.TaskPool)
	}
	if report.Store.FailedDB != 1 || report.Store.SkippedMissingBody != 1 || report.Store.DurationAvgUS != 3000 || report.Store.DurationMaxUS != 5000 || report.Store.DurationLastUS != 4000 {
		t.Fatalf("unexpected store stats: %+v", report.Store)
	}
	if len(report.ScanStages) != 1 || report.ScanStages[0].Failures != 1 {
		t.Fatalf("unexpected scan stages: %+v", report.ScanStages)
	}
	if report.RuntimeLatest.RemoteLatest != 120 || report.RuntimeLatest.StoredCount != 105 || report.RuntimeLatest.StoredHeight != 106 {
		t.Fatalf("unexpected runtime latest: %+v", report.RuntimeLatest)
	}
	if report.FullBlockStore.Count != 1 || report.FullBlockStore.AvgTotalUS != 2000 || report.FullBlockStore.AvgDataUS != 700 || report.FullBlockStore.AvgFinalizeUS != 1000 {
		t.Fatalf("unexpected fullblock store stats: %+v", report.FullBlockStore)
	}
	if len(report.BlockStoreDurations) != 1 || report.BlockStoreDurations[0].Height != 106 || report.BlockStoreDurations[0].TotalUS != 2000 || report.BlockStoreDurations[0].DataUS != 700 {
		t.Fatalf("unexpected per-block store durations: %+v", report.BlockStoreDurations)
	}
	if len(report.StoreDataTypes) != 1 || report.StoreDataTypes[0].Type != "txs" || report.StoreDataTypes[0].Writes != 10 || report.StoreDataTypes[0].Tasks != 2 || report.StoreDataTypes[0].Failures != 1 || report.StoreDataTypes[0].RetriedAttempts != 1 || report.StoreDataTypes[0].AvgCostUS != 2500 || report.StoreDataTypes[0].AvgWriteCostUS != 500 || report.StoreDataTypes[0].AvgFailedCostUS != 1000 {
		t.Fatalf("unexpected store data type stats: %+v", report.StoreDataTypes)
	}
	if report.BodySync.Started != 1 || report.BodySync.Succeeded != 1 || report.BodySync.Failed != 1 || report.BodySync.AvgCostUS != 1500 || report.BodySync.AvgTxs != 7 {
		t.Fatalf("unexpected body sync stats: %+v", report.BodySync)
	}
	if report.HeaderSync.Started != 2 || report.HeaderSync.Succeeded != 1 || report.HeaderSync.Failed != 1 || report.HeaderSync.AvgCostUS != 1000 || report.HeaderSync.MaxCostUS != 1500 {
		t.Fatalf("unexpected header sync stats: %+v", report.HeaderSync)
	}
	if len(report.BodyItems) != 1 || report.BodyItems[0].Item != "fetch_receipts" || report.BodyItems[0].Items != 7 || report.BodyItems[0].AvgCostUS != 3000 || report.BodyItems[0].AvgItemCostUS != 428 {
		t.Fatalf("unexpected body item stats: %+v", report.BodyItems)
	}
	if report.NodeSelection.Events != 1 || report.NodeSelection.ValidNodes != 1 || report.NodeSelection.Disabled != 1 || report.NodeSelection.HeightTooLow != 1 {
		t.Fatalf("unexpected node selection stats: %+v", report.NodeSelection)
	}
	if len(report.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d: %+v", len(report.Nodes), report.Nodes)
	}
	if report.Nodes[0].ID != 1 || report.Nodes[0].Requests != 16 || report.Nodes[0].Failures != 1 {
		t.Fatalf("unexpected node 1 stats: %+v", report.Nodes[0])
	}
	if report.Nodes[1].ID != 2 || report.Nodes[1].Failures != 1 {
		t.Fatalf("unexpected node 2 stats: %+v", report.Nodes[1])
	}
	if len(report.NodeOperatorMethods) != 1 || report.NodeOperatorMethods[0].Method != "FetchReceiptsBatch" || report.NodeOperatorMethods[0].AvgCostUS != 2500 || report.NodeOperatorMethods[0].AvgArrayItems != 10 {
		t.Fatalf("unexpected node operator method stats: %+v", report.NodeOperatorMethods)
	}
	if methods := bodySyncNodeOperatorMethods([]NodeOperatorMethodStats{{Method: "FetchBlockHeaderByHeight"}, {Method: "FetchBlockHeaderByHash"}, {Method: "FetchReceiptsBatch"}}); len(methods) != 2 || methods[0].Method != "FetchBlockHeaderByHash" || methods[1].Method != "FetchReceiptsBatch" {
		t.Fatalf("unexpected body sync node operator method filtering: %+v", methods)
	}
	chart := string(renderSyncNodeOperatorChart([]NodeOperatorMethodStats{
		{NodeID: 1, Method: "FetchReceiptsBatch", TotalCostUS: 3000},
		{NodeID: 1, Method: "FetchTransactionsByHashBatch", TotalCostUS: 1000},
		{NodeID: 2, Method: "FetchReceiptsBatch", TotalCostUS: 2000},
		{NodeID: 2, Method: "FetchBlockHeaderByHash", TotalCostUS: 1000},
	}))
	if !strings.Contains(chart, "Sync node remote interface duration ratio") || !strings.Contains(chart, "FetchReceiptsBatch") || !strings.Contains(chart, "FetchTransactionsByHashBatch") || !strings.Contains(chart, "FetchBlockHeaderByHash") || !strings.Contains(chart, ">N1<") || !strings.Contains(chart, ">N2<") || !strings.Contains(chart, "N1 60%") {
		t.Fatalf("sync node operator chart missing expected content: %s", chart)
	}
	if len(report.Anomalies) == 0 {
		t.Fatalf("expected anomalies")
	}
}

func TestRenderHTMLSmoke(t *testing.T) {
	report, err := Analyze(strings.NewReader(strings.Join([]string{
		`May  4 06:40:43.705 [INFO] task pool stats enqueued:1(body:1 hh:0 hs:0) dequeued:1(body:1 hh:0 hs:0) succeeded:1(body:1 hh:0 hs:0) failed:0(body:0 hh:0 hs:0) retried:0(body:0 hh:0 hs:0) dropped:0(body:0 hh:0 hs:0) pending_high:0 pending_normal:0 tracked:0(body:0 hh:0 hs:0) workers:1 max_retry:2`,
		`May  4 06:40:43.800 [DEBUG] store data type success. type:txs rows:1 worker:0 height:100 task_id:1 try_count:1 cost:1ms`,
		`May  4 06:40:43.900 [INFO] store fullblock. height:100 hash:0xaaa block_id:1 message_id:1 txs:1 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:300µs finalize_cost:400µs total_cost:1ms`,
		`May  4 06:40:44.000 [INFO] runtime health stats remote_latest:120 node_ready:1/1 blocktree_range:[100,115] linked:16 leaves:1 branches:1 orphans:0 orphan_parents:0 staging_blocks:2 pending_headers:1 pending_bodies:1 complete_blocks:0 stored_count:105 task_pending_high:0 task_pending_normal:0 task_tracked:0 task_succeeded:1 task_failed:0 task_retried:0 store_submitted:1 store_succeeded:1 store_failed:0 store_skipped:0 store_queue_pending:0`,
	}, "\n")), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	var buf bytes.Buffer
	if err := RenderHTML(&buf, report); err != nil {
		t.Fatalf("RenderHTML() error = %v", err)
	}
	html := buf.String()
	if !strings.Contains(html, "scanner_eth Log Analysis Report") || !strings.Contains(html, "Task Pool") || !strings.Contains(html, "Sync Overview") || !strings.Contains(html, "Sync Detail") || !strings.Contains(html, "Store Detail") || !strings.Contains(html, "Header Sync") || !strings.Contains(html, "Body Sync") || !strings.Contains(html, "Node Request Overview") || !strings.Contains(html, "Sync Node Remote Interface Duration") || !strings.Contains(html, "Store Data Type Duration") {
		t.Fatalf("rendered HTML missing expected content: %s", html)
	}
	if strings.Contains(html, "<h2>Sync</h2>") {
		t.Fatalf("rendered HTML should rename Sync section to Sync Overview: %s", html)
	}
	if strings.Contains(html, "<h2>Store</h2>") {
		t.Fatalf("rendered HTML should rename Store section to Store Detail: %s", html)
	}
	if strings.Contains(html, "Per-Block Store Duration") {
		t.Fatalf("rendered HTML should not show per-block store duration table: %s", html)
	}
	if strings.Index(html, "Sync Progress") > strings.Index(html, "Node Request Overview") || strings.Index(html, "Node Request Overview") > strings.Index(html, "Time Distribution") || strings.Index(html, "Time Distribution") > strings.Index(html, "Sync Detail") || strings.Index(html, "Sync Detail") > strings.Index(html, "Store Detail") || strings.Index(html, "Store Detail") > strings.Index(html, "Task Pool") {
		t.Fatalf("rendered HTML should show overall sections before module details: %s", html)
	}
	if strings.Index(html, "Node Request Overview") > strings.Index(html, "Sync Node Remote Interface Duration") || strings.Index(html, "Sync Node Remote Interface Duration") > strings.Index(html, "Time Distribution") {
		t.Fatalf("sync node remote interface duration should render in node request overview: %s", html)
	}
	if strings.Contains(html, "<th>Time</th><th>Remote</th><th>Nodes</th><th>BlockTree</th>") {
		t.Fatalf("rendered HTML should not show runtime sampling table: %s", html)
	}
	if !strings.Contains(html, "BlockTree end") || !strings.Contains(html, "Stored block height") || !strings.Contains(html, "Remote latest - BlockTree end") {
		t.Fatalf("rendered HTML should keep sync progress charts: %s", html)
	}
	if strings.Index(html, "Anomaly Summary") < strings.Index(html, "Store Data Type Duration") {
		t.Fatalf("anomaly summary should render after store details: %s", html)
	}
}

func TestSyncProgressChartExcludesRemoteLatestFromHeightAxis(t *testing.T) {
	html := string(renderSyncProgressChart([]RuntimeSnapshot{
		{
			Time:         time.Date(2026, 5, 5, 1, 2, 3, 0, time.Local),
			RemoteLatest: 1000,
			BlocktreeEnd: 100,
			StoredHeight: 95,
		},
	}))

	if strings.Contains(html, `class="line remote"`) {
		t.Fatalf("sync progress chart should not render a remote latest height line: %s", html)
	}
	if strings.Contains(html, ">1000<") {
		t.Fatalf("sync progress height axis should not include remote latest height: %s", html)
	}
	if strings.Contains(html, `class="line lag"`) || strings.Contains(html, "Blocks behind") {
		t.Fatalf("sync progress chart should not render block lag: %s", html)
	}
	if !strings.Contains(html, `class="line tree"`) || !strings.Contains(html, `class="line stored"`) {
		t.Fatalf("sync progress chart missing expected local progress lines: %s", html)
	}
}

func TestBlocktreeLagChartRendersLagSeparately(t *testing.T) {
	html := string(renderBlocktreeLagChart([]RuntimeSnapshot{
		{
			Time:         time.Date(2026, 5, 5, 1, 2, 3, 0, time.Local),
			RemoteLatest: 1000,
			BlocktreeEnd: 100,
		},
	}))

	if !strings.Contains(html, `class="line lag"`) || !strings.Contains(html, "Blocks behind") {
		t.Fatalf("blocktree lag chart missing expected lag line: %s", html)
	}
}

func TestAnalyzeBuildsPerBlockProgressIntervals(t *testing.T) {
	report, err := Analyze(strings.NewReader(strings.Join([]string{
		`May  4 06:40:00.000 [DEBUG] task lifecycle event action:enqueued kind:header_height key:header_height:100 hash: height:100 priority:1 retry:0 queue_wait_us:0 total_us:0`,
		`May  4 06:40:00.200 [DEBUG] task lifecycle event action:started kind:header_height key:header_height:100 hash: height:100 priority:1 retry:0 queue_wait_us:200000 total_us:0`,
		`May  4 06:40:00.500 [INFO] header sync success. height:100 hash:0xaaa parent_hash:0xparent cost_us:100000`,
		`May  4 06:40:00.800 [INFO] body sync start. height:100 hash:0xaaa valid_nodes:1 node_ids:1`,
		`May  4 06:40:01.000 [INFO] body sync success. height:100 hash:0xaaa valid_nodes:1 node_ids:1 txs:1 cost_us:1000`,
		`May  4 06:40:01.499 [INFO] store fullblock start. height:100 hash:0xaaa`,
		`May  4 06:40:01.500 [INFO] store fullblock. height:100 hash:0xaaa block_id:1 message_id:1 txs:1 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:300µs finalize_cost:400µs total_cost:1ms`,
		`May  4 06:40:02.000 [DEBUG] task lifecycle event action:enqueued kind:header_height key:header_height:101 hash: height:101 priority:1 retry:0 queue_wait_us:0 total_us:0`,
		`May  4 06:40:02.300 [DEBUG] task lifecycle event action:started kind:header_height key:header_height:101 hash: height:101 priority:1 retry:0 queue_wait_us:300000 total_us:0`,
		`May  4 06:40:02.500 [INFO] header sync success. height:101 hash:0xbbb parent_hash:0xparent cost_us:200000`,
		`May  4 06:40:02.800 [INFO] body sync start. height:101 hash:0xbbb valid_nodes:1 node_ids:1`,
		`May  4 06:40:03.000 [INFO] body sync success. height:101 hash:0xbbb valid_nodes:1 node_ids:1 txs:2 cost_us:2000`,
		`May  4 06:40:04.998 [INFO] store fullblock start. height:101 hash:0xbbb`,
		`May  4 06:40:05.000 [INFO] store fullblock. height:101 hash:0xbbb block_id:2 message_id:2 txs:2 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:300µs finalize_cost:400µs total_cost:2ms`,
		`May  4 06:40:17.000 [DEBUG] task lifecycle event action:enqueued kind:body key:0xccc hash:0xccc height:0 priority:2 retry:0 queue_wait_us:0 total_us:0`,
		`May  4 06:40:17.500 [INFO] header sync success. height:102 hash:0xccc parent_hash:0xparent cost_us:300000`,
		`May  4 06:40:17.800 [INFO] body sync start. height:102 hash:0xccc valid_nodes:1 node_ids:1`,
		`May  4 06:40:18.000 [INFO] body sync success. height:102 hash:0xccc valid_nodes:1 node_ids:1 txs:3 cost_us:3000`,
		`May  4 06:40:18.997 [INFO] store fullblock start. height:102 hash:0xccc`,
		`May  4 06:40:19.000 [INFO] store fullblock. height:102 hash:0xccc block_id:3 message_id:3 txs:3 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:300µs finalize_cost:400µs total_cost:3ms`,
	}, "\n")), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.BlockProgress) != 2 {
		t.Fatalf("expected 2 block progress rows, got %d: %+v", len(report.BlockProgress), report.BlockProgress)
	}
	if len(report.BlockProgressOutliers) != 1 || report.BlockProgressOutliers[0].Height != 102 || report.BlockProgressOutliers[0].TaskCreatedGapUS != 15_000_000 {
		t.Fatalf("unexpected block progress outliers: %+v", report.BlockProgressOutliers)
	}
	second := report.BlockProgress[1]
	if second.Height != 101 || !second.HasTaskCreatedGap || second.TaskCreatedGapUS != 2_000_000 {
		t.Fatalf("unexpected second block progress row: %+v", second)
	}
	if report.BlockTiming.EndToEndSamples != 3 || report.BlockTiming.PrevStoreToTaskSamples != 2 || report.BlockTiming.AvgPrevStoreToTaskUS != 6_250_000 || report.BlockTiming.TaskToHeaderStartSamples != 2 || report.BlockTiming.AvgTaskToHeaderStartUS != 250_000 || report.BlockTiming.AvgHeaderSyncCostUS != 200_000 || report.BlockTiming.AvgHeaderToBodyStartUS != 300_000 || report.BlockTiming.AvgBodySyncCostUS != 2_000 || report.BlockTiming.AvgBodyToStoreStartUS != 1_164_666 || report.BlockTiming.AvgStoreCostUS != 2_000 || report.BlockTiming.AvgFirstTaskToStoreUS != 2_166_666 || report.BlockTiming.AvgTaskQueueWaitUS != 250_000 {
		t.Fatalf("unexpected block timing stats: %+v", report.BlockTiming)
	}
	if len(report.BlockTimingPhases) != 9 || report.BlockTimingPhases[0].P95US != 12_000_000 {
		t.Fatalf("unexpected block timing phase stats: %+v", report.BlockTimingPhases)
	}
	phaseChart := string(renderBlockTimingPhaseChart(pipelineTimingPhases(report.BlockTimingPhases)))
	if strings.Contains(phaseChart, "Task Queue Wait") || strings.Contains(phaseChart, "Task Created -&gt; Store Done") || strings.Contains(phaseChart, "Task Created -> Store Done") {
		t.Fatalf("timing phase chart should only render pipeline phases: %s", phaseChart)
	}
	if len(report.SlowBlockTimings) != 3 || report.SlowBlockTimings[0].Height != 101 {
		t.Fatalf("unexpected slow block timing rows: %+v", report.SlowBlockTimings)
	}
	if len(report.BlockTimings) != 3 || report.BlockTimings[1].Height != 101 || report.BlockTimings[1].BodySyncUS != 2000 || report.BlockTimings[1].StoreUS != 2000 {
		t.Fatalf("unexpected per-block timing rows: %+v", report.BlockTimings)
	}
	bodyDurationChart := string(renderBodySyncDurationChart(report.BlockTimings))
	if !strings.Contains(bodyDurationChart, `class="line body-gap"`) || !strings.Contains(bodyDurationChart, "Per-block Body sync duration") {
		t.Fatalf("body sync duration chart missing expected series: %s", bodyDurationChart)
	}
	bodyDurationChart = string(renderBodySyncDurationChart([]BlockTimingBlock{
		{Height: 100, BodySyncUS: 2_000_000},
		{Height: 101, BodySyncUS: 11_000_000},
	}))
	if strings.Contains(bodyDurationChart, "11.000s") || strings.Contains(bodyDurationChart, ">101<") {
		t.Fatalf("body sync duration chart should exclude outliers above 10s: %s", bodyDurationChart)
	}
	taskToStoreDurationChart := string(renderTaskToStoreDurationChart(report.BlockTimings))
	if !strings.Contains(taskToStoreDurationChart, `class="line end-to-end"`) || !strings.Contains(taskToStoreDurationChart, "Per-block Block duration") {
		t.Fatalf("task-to-store duration chart missing expected series: %s", taskToStoreDurationChart)
	}
	taskToStoreDurationChart = string(renderTaskToStoreDurationChart([]BlockTimingBlock{
		{Height: 100, FirstTaskToStoreUS: 2_000_000},
		{Height: 101, FirstTaskToStoreUS: 11_000_000},
	}))
	if strings.Contains(taskToStoreDurationChart, "11.000s") || strings.Contains(taskToStoreDurationChart, ">101<") {
		t.Fatalf("block duration chart should exclude outliers above 10s: %s", taskToStoreDurationChart)
	}
	storeDurationChart := string(renderStoreDurationChart(report.BlockTimings))
	if !strings.Contains(storeDurationChart, `class="line store-gap"`) || !strings.Contains(storeDurationChart, "Per-block Store duration") {
		t.Fatalf("store duration chart missing expected series: %s", storeDurationChart)
	}

	report.RuntimeSeries = []RuntimeSnapshot{{Time: time.Date(2026, 5, 5, 1, 2, 3, 0, time.Local), RemoteLatest: 103, BlocktreeEnd: 102, StoredHeight: 102}}
	var buf bytes.Buffer
	if err := RenderHTML(&buf, report); err != nil {
		t.Fatalf("RenderHTML() error = %v", err)
	}
	if !strings.Contains(buf.String(), "Time Distribution") || !strings.Contains(buf.String(), "Block Duration") || !strings.Contains(buf.String(), "Block duration") || !strings.Contains(buf.String(), "Block Body Sync Duration") || !strings.Contains(buf.String(), "Body sync duration") || !strings.Contains(buf.String(), "Block Store Duration") || !strings.Contains(buf.String(), "Store duration") {
		t.Fatalf("rendered HTML should show duration charts: %s", buf.String())
	}
	if strings.Index(buf.String(), "Block Duration") > strings.Index(buf.String(), "Block Body Sync Duration") {
		t.Fatalf("task-to-store duration chart should render before body sync duration chart: %s", buf.String())
	}
	if strings.Contains(buf.String(), "Per-Block Task Creation Intervals") || strings.Contains(buf.String(), "Task creation interval") || strings.Contains(buf.String(), "Excluded interval outliers") || strings.Contains(buf.String(), "Body Sync Intervals") || strings.Contains(buf.String(), "Per-block Body sync interval") || strings.Contains(buf.String(), "Per-block Store interval") {
		t.Fatalf("rendered HTML should not show interval charts: %s", buf.String())
	}
	if strings.Contains(buf.String(), "Block Timing Overview") || strings.Contains(buf.String(), "Block timing overview with requested block time distribution") {
		t.Fatalf("rendered HTML should not show block timing overview chart: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "Timing Phase Percentiles") || !strings.Contains(buf.String(), "Timing phase percentiles") || !strings.Contains(buf.String(), `class="bar phase-p95"`) {
		t.Fatalf("rendered HTML should show timing phase percentile chart: %s", buf.String())
	}
	if strings.Contains(buf.String(), "Slowest Blocks") {
		t.Fatalf("rendered HTML should not show slowest blocks table: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "Pipeline Phases") || !strings.Contains(buf.String(), "Scheduling And E2E") || !strings.Contains(buf.String(), "Task Queue Wait") || !strings.Contains(buf.String(), "Task Created -&gt; Store Done") {
		t.Fatalf("rendered HTML should split pipeline phases from scheduling/e2e metrics: %s", buf.String())
	}
	if strings.Contains(buf.String(), `class="bar phase-max"`) || strings.Contains(buf.String(), `legend-item phase-max`) {
		t.Fatalf("timing phase percentile chart should not render max bars: %s", buf.String())
	}
	if strings.Contains(buf.String(), "<th>Time</th><th>Remote</th><th>Nodes</th><th>BlockTree</th>") {
		t.Fatalf("rendered HTML should not show runtime sampling table: %s", buf.String())
	}
}

func TestBodySyncDurationOutliersBecomeAnomalies(t *testing.T) {
	report, err := Analyze(strings.NewReader(strings.Join([]string{
		`May  4 06:40:00.000 [DEBUG] task lifecycle event action:enqueued kind:body key:0xaaa hash:0xaaa height:0 priority:2 retry:0 queue_wait_us:0 total_us:0`,
		`May  4 06:40:00.500 [INFO] header sync success. height:100 hash:0xaaa parent_hash:0xparent cost_us:1000`,
		`May  4 06:40:01.000 [INFO] body sync start. height:100 hash:0xaaa valid_nodes:1 node_ids:1`,
		`May  4 06:40:12.000 [INFO] body sync success. height:100 hash:0xaaa valid_nodes:1 node_ids:1 txs:1 cost_us:11000000`,
	}, "\n")), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.BodySyncOutliers) != 1 || report.BodySyncOutliers[0].Height != 100 || report.BodySyncOutliers[0].BodySyncUS != 11_000_000 {
		t.Fatalf("unexpected body sync outliers: %+v", report.BodySyncOutliers)
	}
	var found bool
	for _, anomaly := range report.Anomalies {
		if anomaly.Category == "block_body_sync_slow" && anomaly.Count == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected body sync outlier anomaly: %+v", report.Anomalies)
	}
}

func TestBlockDurationOutliersBecomeAnomalies(t *testing.T) {
	report, err := Analyze(strings.NewReader(strings.Join([]string{
		`May  4 06:40:00.000 [DEBUG] task lifecycle event action:enqueued kind:body key:0xaaa hash:0xaaa height:0 priority:2 retry:0 queue_wait_us:0 total_us:0`,
		`May  4 06:40:00.500 [INFO] header sync success. height:100 hash:0xaaa parent_hash:0xparent cost_us:1000`,
		`May  4 06:40:01.000 [INFO] body sync start. height:100 hash:0xaaa valid_nodes:1 node_ids:1`,
		`May  4 06:40:01.100 [INFO] body sync success. height:100 hash:0xaaa valid_nodes:1 node_ids:1 txs:1 cost_us:100000`,
		`May  4 06:40:12.000 [INFO] store fullblock start. height:100 hash:0xaaa`,
		`May  4 06:40:12.100 [INFO] store fullblock. height:100 hash:0xaaa block_id:1 message_id:1 txs:1 internal_txs:0 event_logs:0 erc20_events:0 erc721_events:0 erc1155_events:0 contracts:0 erc20_contracts:0 erc721_contracts:0 native_balances:0 erc20_balances:0 erc1155_balances:0 tokens_erc721:0 tasks:1 ready_cost:100µs block_row_cost:200µs data_cost:300µs finalize_cost:400µs total_cost:1ms`,
	}, "\n")), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.BlockDurationOutliers) != 1 || report.BlockDurationOutliers[0].Height != 100 || report.BlockDurationOutliers[0].FirstTaskToStoreUS != 12_100_000 {
		t.Fatalf("unexpected block duration outliers: %+v", report.BlockDurationOutliers)
	}
	var found bool
	for _, anomaly := range report.Anomalies {
		if anomaly.Category == "block_duration_slow" && anomaly.Count == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected block duration outlier anomaly: %+v", report.Anomalies)
	}
	var buf bytes.Buffer
	if err := RenderHTML(&buf, report); err != nil {
		t.Fatalf("RenderHTML() error = %v", err)
	}
	if !strings.Contains(buf.String(), "Excluded block duration outliers") || !strings.Contains(buf.String(), "Block Duration (ms)") {
		t.Fatalf("rendered HTML should show block duration outliers: %s", buf.String())
	}
}
