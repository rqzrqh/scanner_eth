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
		`May  4 06:40:44.705 [INFO] [app:scanner_bsc_testnet] [env:prd] task pool stats enqueued:15(body:9 hh:4 hs:2) dequeued:14(body:8 hh:4 hs:2) succeeded:12(body:7 hh:3 hs:2) failed:2(body:1 hh:1 hs:0) retried:3(body:1 hh:2 hs:0) dropped:1(body:1 hh:0 hs:0) pending_high:2 pending_normal:3 tracked:4(body:2 hh:1 hs:1) workers:4 max_retry:2 @task.(*Pool).runTaskStatsReporter /repo/fetch/taskpool/pool.go 398`,
		`May  4 06:40:45.000 [INFO] [app:scanner_bsc_testnet] [env:prd] store block worker stats submitted:5 skipped:1 succeeded:3 failed:1 canceled:0 queue_pending:2 processing:1 skipped_missing_body:1 skipped_parent_not_ready:0 failed_db:1 @serialstore.(*Worker).logStats /repo/fetch/serial_store/serial_worker.go 258`,
		`May  4 06:40:45.100 [INFO] [app:scanner_bsc_testnet] [env:prd] scan stage event stage:sync_body target:0xabc target_count:1 success:false duration:12ms err:body sync failed @scan.(*Flow).logScanStageEvent /repo/fetch/scan/flow.go 208`,
		`May  4 06:40:45.200 [INFO] [app:scanner_bsc_testnet] [env:prd] runtime health stats remote_latest:120 node_ready:1/2 blocktree_range:[100,110] linked:11 leaves:1 branches:1 orphans:0 orphan_parents:0 staging_blocks:4 pending_headers:3 pending_bodies:2 complete_blocks:1 stored_count:105 task_pending_high:2 task_pending_normal:3 task_tracked:4 task_succeeded:12 task_failed:2 task_retried:3 store_submitted:5 store_succeeded:3 store_failed:1 store_skipped:1 store_queue_pending:2 @fetch.(*FetchManager).logRuntimeHealthStats /repo/fetch/fetch_manager.go 340`,
		`May  4 06:40:45.300 [INFO] [app:scanner_bsc_testnet] [env:prd] valid node operators selected. height:100 hash:0xabc valid_nodes:1 node_ids:[1] scores:[100] disabled:1 not_ready:0 cooldown:0 height_too_low:1 remote_unknown:0 nil_nodes:0 @node.(*NodeManager).GetAllValidNodeOperators /repo/fetch/node/manager.go 221`,
		`May  4 06:40:45.400 [INFO] [app:scanner_bsc_testnet] [env:prd] body sync start. height:100 hash:0xabc valid_nodes:1 node_ids:1 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 124`,
		`May  4 06:40:45.500 [INFO] [app:scanner_bsc_testnet] [env:prd] body sync success. height:100 hash:0xabc valid_nodes:1 node_ids:1 txs:7 cost_us:2000 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 139`,
		`May  4 06:40:45.600 [WARN] [app:scanner_bsc_testnet] [env:prd] body sync failed. height:101 hash:0xdef valid_nodes:0 node_ids: cost_us:1000 @taskprocess.RuntimeDeps.fetchBodyByHash /repo/fetch/task_process/runtime.go 132`,
		`May  4 06:40:46.000 [INFO] [app:scanner_bsc_testnet] [env:prd] [nodeOperator RPC] nodeId=1 FetchBlockHeaderByHeight:eth_getBlockByNumber=4 FetchReceiptsBatch:eth_getTransactionReceipt=10 @node.logNodeOperatorRPCStats /repo/fetch/node/operator_stats.go 124`,
		`May  4 06:40:46.100 [INFO] [app:scanner_bsc_testnet] [env:prd] fetch full block rpc success. op:FetchReceiptsBatch nodeId:1 taskId:100 height:100 attempts:2 retries:3 selected_node_ids:1,2 cost_us:3000 @fetcher.withFullBlockRPCRetry /repo/fetch/fetcher/fetcher.go 183`,
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
	if report.Store.FailedDB != 1 || report.Store.SkippedMissingBody != 1 {
		t.Fatalf("unexpected store stats: %+v", report.Store)
	}
	if len(report.ScanStages) != 1 || report.ScanStages[0].Failures != 1 {
		t.Fatalf("unexpected scan stages: %+v", report.ScanStages)
	}
	if report.RuntimeLatest.RemoteLatest != 120 || report.RuntimeLatest.StoredCount != 105 {
		t.Fatalf("unexpected runtime latest: %+v", report.RuntimeLatest)
	}
	if report.BodySync.Started != 1 || report.BodySync.Succeeded != 1 || report.BodySync.Failed != 1 || report.BodySync.AvgCostUS != 1500 || report.BodySync.AvgTxs != 7 {
		t.Fatalf("unexpected body sync stats: %+v", report.BodySync)
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
	if len(report.Anomalies) == 0 {
		t.Fatalf("expected anomalies")
	}
}

func TestRenderHTMLSmoke(t *testing.T) {
	report, err := Analyze(strings.NewReader(`May  4 06:40:43.705 [INFO] task pool stats enqueued:1(body:1 hh:0 hs:0) dequeued:1(body:1 hh:0 hs:0) succeeded:1(body:1 hh:0 hs:0) failed:0(body:0 hh:0 hs:0) retried:0(body:0 hh:0 hs:0) dropped:0(body:0 hh:0 hs:0) pending_high:0 pending_normal:0 tracked:0(body:0 hh:0 hs:0) workers:1 max_retry:2`), "sample.log", Options{
		Now: time.Date(2026, 5, 5, 0, 0, 0, 0, time.Local),
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	var buf bytes.Buffer
	if err := RenderHTML(&buf, report); err != nil {
		t.Fatalf("RenderHTML() error = %v", err)
	}
	if !strings.Contains(buf.String(), "scanner_eth Log Analysis Report") || !strings.Contains(buf.String(), "Task Pool") || !strings.Contains(buf.String(), "Body Sync") {
		t.Fatalf("rendered HTML missing expected content: %s", buf.String())
	}
}
