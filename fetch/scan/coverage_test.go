package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	"testing"
	"time"
)

type doneOnlyContext struct{}

func (doneOnlyContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (doneOnlyContext) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (doneOnlyContext) Err() error    { return nil }
func (doneOnlyContext) Value(any) any { return nil }

func TestCoverageFlowHelpersAndMetrics(t *testing.T) {
	var nilFlow *Flow
	nilFlow.TriggerScan()
	nilFlow.recordScanStageEvent(scanStageEvent{})
	if nilFlow.nodeExists("a") {
		t.Fatal("nil flow should not report node existence")
	}
	if nilFlow.getPendingBody("a") != nil {
		t.Fatal("nil flow should not return pending body")
	}
	if payload := nilFlow.MetricsPayload(); len(payload) != 0 {
		t.Fatalf("nil metrics payload should be empty, got=%+v", payload)
	}
	nilFlow.inspectBlockTreeState("nil")

	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.stagingStore.SetPendingHeader("a", makeTestHeader(1, "a", ""))
	env.stagingStore.SetPendingBody("a", makeTestEventBlockData(1, "a", ""))
	if !env.flow.nodeExists(" a ") {
		t.Fatal("expected normalized node lookup to find node")
	}
	if env.flow.getPendingBody(" a ") == nil {
		t.Fatal("expected normalized pending body lookup")
	}
	env.flow.logScanStageEvent(scanStageEvent{stage: scanStage(99), success: false, target: "x", targetCount: 1, duration: time.Nanosecond, errMsg: "boom"})
	payload := env.flow.MetricsPayload()
	stages := payload["stages"].(map[string]map[string]any)
	if stages["unknown"]["failures"].(uint64) != 1 {
		t.Fatalf("expected failed unknown stage metrics, got=%+v", stages)
	}
	env.flow.inspectBlockTreeState("non_empty")

	worker := NewWorker(nil)
	worker.SetEnabled(true)
	env.flow.scanWorker = worker
	env.flow.TriggerScan()
	if len(worker.TriggerChan()) != 1 {
		t.Fatalf("expected trigger via flow, got queue len %d", len(worker.TriggerChan()))
	}
}

func TestCoverageExpandTreeBoundaries(t *testing.T) {
	var nilFlow *Flow
	nilFlow.RunExpandTreeStage(context.Background())
	nilFlow.GetExpandTreeTargets()
	if nilFlow.latestRemoteHeight() != 0 {
		t.Fatal("nil flow latest remote height should be zero")
	}
	if size, ok := nilFlow.HeaderWindowTargetSize(); ok || size != 0 {
		t.Fatalf("nil flow should have no header window target size, got size=%d ok=%v", size, ok)
	}
	if !nilFlow.ShouldStopHeaderWindowSync(1, 1, 2) {
		t.Fatal("nil flow should stop header window sync")
	}

	env := newTestFlowEnv(t, 2)
	env.setStartHeight(3)
	if got := env.flow.GetHeaderByHeightSyncTargets(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("height wrapper returned unexpected targets: %v", got)
	}
	if ok, msg := env.flow.SyncExpandTreeTarget("bad"); ok || msg == "" {
		t.Fatalf("invalid height target should fail, ok=%v msg=%q", ok, msg)
	}
	ok, msg := env.flow.SyncHeaderByHeightTarget(context.Background(), "4")
	if ok || msg == "" {
		t.Fatalf("expected height sync failure, ok=%v msg=%q", ok, msg)
	}
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
		if height == 4 {
			return makeTestHeader(4, "0x04", "0x03")
		}
		return nil
	}
	env.setLatestRemote(4)
	ok, msg = env.flow.SyncHeaderByHeightTarget(context.Background(), "4")
	if !ok || msg != "" || env.blockTree.Get("0x04") == nil {
		t.Fatalf("expected height sync success, ok=%v msg=%q", ok, msg)
	}
	env.taskPool.TryStartHeaderHeightSync(5)
	env.flow.enqueueExpandTreeTargets([]uint64{5})
	(&Flow{}).enqueueExpandTreeTargets([]uint64{1})
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	env.flow.RunExpandTreeStage(cancelCtx)

	noFetch := newTestFlowEnv(t, 2)
	noFetch.setStartHeight(9)
	noFetch.blockTree.Insert(9, "0x09", "", 1)
	noFetch.setLatestRemote(0)
	if !noFetch.flow.ShouldStopHeaderWindowSync(9, 9, 4) {
		t.Fatal("zero latest remote should stop sync")
	}
	noFetch.setIrreversible(0)
	if targets := noFetch.flow.GetExpandTreeTargets(); targets != nil {
		t.Fatalf("irreversible=0 should have no targets, got=%v", targets)
	}
	noFetch.flow.MarkRootParentReady()

	emptyParent := newTestFlowEnv(t, 2)
	if emptyParent.flow.MarkRootParentReady() {
		t.Fatal("empty tree should not mark root parent")
	}
	emptyParent.blockTree.Insert(1, "root", "", 1)
	if emptyParent.flow.MarkRootParentReady() {
		t.Fatal("root without parent should not mark parent ready")
	}
	noStored := &Flow{blockTree: emptyParent.blockTree}
	if noStored.MarkRootParentReady() {
		t.Fatal("flow without stored state should not mark parent ready")
	}
	fullWindow := newTestFlowEnv(t, 1)
	fullWindow.blockTree.Insert(1, "a", "", 1)
	fullWindow.blockTree.Insert(2, "b", "a", 1)
	fullWindow.setLatestRemote(3)
	if targets := fullWindow.flow.GetExpandTreeTargets(); targets != nil {
		t.Fatalf("full target window should not expand, got=%v", targets)
	}
	atTip := newTestFlowEnv(t, 2)
	atTip.blockTree.Insert(1, "a", "", 1)
	atTip.setLatestRemote(1)
	if targets := atTip.flow.GetExpandTreeTargets(); targets != nil {
		t.Fatalf("tree at latest remote should not expand, got=%v", targets)
	}
}

func TestCoverageFillTreeBoundaries(t *testing.T) {
	var nilFlow *Flow
	nilFlow.RunFillTreeStage(context.Background())
	if targets := nilFlow.GetHeaderByHashSyncTargets(); targets != nil {
		t.Fatalf("nil flow should have no hash targets, got=%v", targets)
	}

	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(1, "a", ""))
	env.blockTree.Insert(2, "b", "missing", 1)
	if got := env.flow.GetHeaderByHashSyncTargets(); len(got) != 1 || got[0] != "missing" {
		t.Fatalf("hash wrapper returned unexpected targets: %v", got)
	}
	env.taskPool.TryStartHeaderHashSync("missing")
	env.flow.enqueueFillTreeTargets([]string{"missing"})
	env.taskPool.FinishHeaderHashSync("missing")
	(&Flow{}).enqueueFillTreeTargets([]string{"x"})
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	env.flow.RunFillTreeStage(cancelCtx)

	if ok, msg := env.flow.SyncFillTreeTarget(""); ok || msg == "" {
		t.Fatalf("empty hash target should fail, ok=%v msg=%q", ok, msg)
	}
	ok, msg := env.flow.SyncHeaderByHashTarget(context.Background(), "missing")
	if ok || msg == "" {
		t.Fatalf("expected hash sync failure, ok=%v msg=%q", ok, msg)
	}
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		if normalizeTestHash(hash) == "missing" {
			return makeTestHeader(2, "missing", "a")
		}
		return nil
	}
	ok, msg = env.flow.SyncHeaderByHashTarget(context.Background(), "missing")
	if !ok || msg != "" || env.blockTree.Get("missing") == nil {
		t.Fatalf("expected hash sync success, ok=%v msg=%q", ok, msg)
	}
}

func TestCoverageRemoteHeaderCandidateBoundaries(t *testing.T) {
	var nilFlow *Flow
	if nilFlow.EnqueueRemoteHeaderCandidate("a", "a", "", "0x1") {
		t.Fatal("nil flow should reject remote header candidate")
	}
	if (&Flow{}).EnqueueRemoteHeaderCandidate("a", "a", "", "0x1") {
		t.Fatal("unbound flow should reject remote header candidate")
	}
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	if env.flow.EnqueueRemoteHeaderCandidate("b", "c", "a", "0x2") {
		t.Fatal("mismatched hashes should reject")
	}
	if env.flow.EnqueueRemoteHeaderCandidate("a", "a", "", "0x1") {
		t.Fatal("existing hash should reject")
	}
	env.taskPool.TryStartHeaderHashSync("b")
	if env.flow.EnqueueRemoteHeaderCandidate("b", "b", "a", "0x2") {
		t.Fatal("already syncing hash should reject")
	}
	env.taskPool.FinishHeaderHashSync("b")
	if env.flow.EnqueueRemoteHeaderCandidate("b", "b", "missing", "0x2") {
		t.Fatal("missing parent should reject")
	}
	if env.flow.EnqueueRemoteHeaderCandidate("b", "b", "a", "bad") {
		t.Fatal("bad height should reject")
	}
	if env.flow.EnqueueRemoteHeaderCandidate("b", "b", "a", "0x3") {
		t.Fatal("non-continuous height should reject")
	}
	if !env.flow.EnqueueRemoteHeaderCandidate("b", "b", "a", "0x2") {
		t.Fatal("continuous candidate should enqueue")
	}
}

func TestCoverageStoreBranchAndBodyBoundaries(t *testing.T) {
	var nilFlow *Flow
	nilFlow.RunStoreBranchesStage(context.Background(), nil)
	if err := nilFlow.SubmitStoreBranches(context.Background(), nil); err == nil {
		t.Fatal("nil flow submit should fail")
	}
	nilFlow.EnqueueMissingBodyTasks(nil)
	if nilFlow.BuildStoreBranches() != nil || nilFlow.CountActionableBodyNodes() != 0 || nilFlow.CountStoredLinkedNodes() != 0 {
		t.Fatal("nil flow store helpers should be empty")
	}
	if ok, msg := nilFlow.SyncBodyBranchTarget(context.Background(), "a"); ok || msg == "" {
		t.Fatalf("nil body branch sync should fail, ok=%v msg=%q", ok, msg)
	}

	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.stored.MarkStored("a")
	env.stagingStore.SetPendingHeader("b", makeTestHeader(2, "b", "a"))
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingBody("b", makeTestEventBlockData(2, "b", "a"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))
	if len(env.flow.BuildStoreBranches()) != 1 {
		t.Fatal("expected one storable branch")
	}
	if env.flow.CountActionableBodyNodes() != env.flow.CountStoreBranchNodes() {
		t.Fatal("compat count wrapper should match store branch count")
	}
	env.attachStoreWorker(func(_ context.Context, _ *fetchstore.EventBlockData) error { return nil })
	if err := env.flow.SubmitStoreBranches(context.Background(), env.flow.BuildStoreBranches()); err != nil {
		t.Fatalf("submit store branches failed: %v", err)
	}
	if !env.stored.IsStored("b") || !env.stored.IsStored("c") {
		t.Fatal("SubmitStoreBranches should store branch")
	}
	if env.flow.CountStoredLinkedNodes() != env.flow.CountStoredLinkedTreeNodes() {
		t.Fatal("compat stored count wrapper should match linked tree count")
	}
	if ok, msg := env.flow.SyncBodyBranchTarget(context.Background(), " "); ok || msg == "" {
		t.Fatalf("blank body target should fail, ok=%v msg=%q", ok, msg)
	}
	noWorker := newTestFlowEnv(t, 2)
	noWorker.blockTree.Insert(1, "a", "", 1)
	noWorker.stagingStore.SetPendingHeader("a", makeTestHeader(1, "a", ""))
	noWorker.stagingStore.SetPendingBody("a", makeTestEventBlockData(1, "a", ""))
	if err := noWorker.flow.SubmitStoreBranches(context.Background(), noWorker.flow.collectStoreBranchesForBodySync()); err == nil {
		t.Fatal("expected submit without store worker to fail")
	}
	noWorker.flow.RunStoreBranchesStage(context.Background(), noWorker.flow.collectStoreBranchesForBodySync())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env.flow.RunStoreBranchesStage(ctx, []fetchserialstore.Branch{{Nodes: []fetchserialstore.BranchNode{{Hash: "x", BlockData: makeTestEventBlockData(1, "x", "")}}}})
	if got := env.flow.serializeStoreBranches([]fetchserialstore.Branch{{Nodes: []fetchserialstore.BranchNode{{Hash: ""}}}}); len(got) != 0 {
		t.Fatalf("empty hashes should serialize to nil, got=%v", got)
	}
	if branches := env.flow.buildStoreBranchesFromTarget(" ; missing "); len(branches) != 0 {
		t.Fatalf("missing target nodes should be skipped, got=%v", branches)
	}
	if branches := env.flow.buildStoreBranchesFromTarget(""); len(branches) != 0 {
		t.Fatalf("blank target should build no branches, got=%v", branches)
	}
	if branches := env.flow.buildStoreBranchesFromTarget(","); len(branches) != 0 {
		t.Fatalf("empty node tokens should build no branches, got=%v", branches)
	}
	nilFlow.waitStoreBranchesWrite(context.Background(), nil)
	cancelled, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	env.flow.waitStoreBranchesWrite(cancelled, []fetchserialstore.Branch{{Nodes: []fetchserialstore.BranchNode{{Hash: "b", ParentHash: "a", BlockData: makeTestEventBlockData(2, "b", "a")}}}})
	if got := env.flow.filterStorableBranchNodes([]fetchserialstore.BranchNode{{Hash: ""}, {Hash: "x"}}); len(got) != 0 {
		t.Fatalf("empty hash should stop storable filtering, got=%v", got)
	}
	if got := nilFlow.filterStorableBranchNodes(nil); len(got) != 0 {
		t.Fatalf("nil flow should have no storable nodes, got=%v", got)
	}
	if got := env.flow.filterStorableBranchNodes(nil); len(got) != 0 {
		t.Fatalf("empty nodes should have no storable nodes, got=%v", got)
	}
	prefixEnv := newTestFlowEnv(t, 2)
	prefixEnv.stored.MarkStored("a")
	if got := prefixEnv.flow.filterStorableBranchNodes([]fetchserialstore.BranchNode{{Hash: "b", ParentHash: "a", BlockData: makeTestEventBlockData(2, "b", "a")}}); len(got) != 1 {
		t.Fatalf("stored parent should make node storable, got=%v", got)
	}
	prefixEnv.stored.MarkStored("b")
	if got := prefixEnv.flow.filterStorableBranchNodes([]fetchserialstore.BranchNode{{Hash: "b", ParentHash: "a"}}); len(got) != 0 {
		t.Fatalf("already stored node should be skipped, got=%v", got)
	}
}

func TestCoveragePruneAndWorkerBoundaries(t *testing.T) {
	var nilFlow *Flow
	nilFlow.RunPruneStage(context.Background())
	PruneRuntimeDeps{}.PruneStoredBlocks(context.Background(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env := newTestFlowEnv(t, 2)
	env.setIrreversible(0)
	env.flow.RunPruneStage(context.Background())
	env.setIrreversible(2)
	env.flow.RunPruneStage(ctx)
	env.flow.RunPruneStage(context.Background())
	if _, _, ok := (PruneRuntimeDeps{}).StoredHeightRangeOnTree(); ok {
		t.Fatal("empty prune deps should not have stored range")
	}
	env.blockTree.Insert(1, "a", "", 1)
	env.stored.MarkStored("a")
	env.flow.pruneRuntime.PruneStoredBlocks(context.Background(), 0)
	env.flow.pruneRuntime.PruneStoredBlocks(ctx, 2)
	env.flow.pruneRuntime.PruneStoredBlocks(context.Background(), 2)
	env.blockTree.Insert(2, "b", "a", 1)
	env.stored.MarkStored("b")
	env.flow.pruneRuntime.PruneStoredBlocks(context.Background(), 2)
	env.blockTree.Insert(3, "c", "b", 1)
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))
	env.stored.MarkStored("c")
	env.taskPool.AddTask("c")
	env.flow.pruneRuntime.PruneStoredBlocks(context.Background(), 1)
	env.flow.pruneRuntime.PruneStoredBlocks(doneOnlyContext{}, 1)

	pruneStage := newTestFlowEnv(t, 1)
	pruneStage.blockTree.Insert(1, "a", "", 1)
	pruneStage.blockTree.Insert(2, "b", "a", 1)
	pruneStage.blockTree.Insert(3, "c", "b", 1)
	pruneStage.stored.MarkStored("a")
	pruneStage.stored.MarkStored("b")
	pruneStage.stored.MarkStored("c")
	pruneStage.flow.RunPruneStage(context.Background())

	pruneFork := newTestFlowEnv(t, 1)
	pruneFork.blockTree.Insert(1, "a", "", 1)
	pruneFork.blockTree.Insert(2, "b", "a", 1)
	pruneFork.blockTree.Insert(3, "c", "b", 1)
	pruneFork.blockTree.Insert(2, "d", "a", 1)
	for _, hash := range []string{"a", "b", "c", "d"} {
		pruneFork.stored.MarkStored(hash)
	}
	pruneFork.flow.RunPruneStage(context.Background())

	var nilWorker *Worker
	nilWorker.Start()
	nilWorker.TriggerAtMicro(1)
	if nilWorker.TriggerChan() != nil || nilWorker.LastScanStartedAtMicro() != 0 || nilWorker.IgnoredTriggerEvents() != 0 {
		t.Fatal("nil worker accessors should be zero")
	}
	nilWorker.recordScanStartedAtMicro(1)
	if !nilWorker.shouldIgnoreTrigger(1) {
		t.Fatal("nil worker should ignore triggers")
	}
	nilWorker.SetEnabled(true)

	worker := NewWorker(env.flow)
	worker.runScanCycle(context.Background())
	if worker.LastScanStartedAtMicro() != 0 {
		t.Fatal("disabled worker should not run scan")
	}
	worker.SetEnabled(true)
	worker.runScanCycle(ctx)
	if worker.LastScanStartedAtMicro() == 0 {
		t.Fatal("enabled worker should record scan start")
	}
	worker.TriggerAtMicro(0)
	if len(worker.TriggerChan()) != 1 {
		t.Fatal("zero trigger timestamp should enqueue current time")
	}
	<-worker.TriggerChan()
	worker.Start()
	worker.Start()
	worker.Stop()

	active := NewWorker(env.flow)
	active.SetEnabled(true)
	active.Start()
	active.Trigger()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if active.LastScanStartedAtMicro() != 0 {
			active.Stop()
			return
		}
		time.Sleep(time.Millisecond)
	}
	active.Stop()
	t.Fatal("worker did not consume trigger")
}

func TestCoverageScanWorkerRunTickerAndIgnoredTrigger(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	ignored := NewWorker(env.flow)
	ignored.SetEnabled(true)
	ignored.recordScanStartedAtMicro(time.Now().UnixMicro() + int64(time.Second/time.Microsecond))
	ignored.TriggerAtMicro(time.Now().UnixMicro())
	if ignored.IgnoredTriggerEvents() == 0 {
		t.Fatal("old trigger should be ignored before enqueue")
	}

	worker := NewWorker(env.flow)
	worker.SetEnabled(true)
	worker.TriggerAtMicro(time.Now().UnixMicro())
	worker.recordScanStartedAtMicro(time.Now().Add(time.Second).UnixMicro())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	worker.wg.Add(1)
	go func() {
		worker.run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if worker.IgnoredTriggerEvents() > 0 {
			cancel()
			<-done
			goto ticker
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("run loop did not ignore buffered old trigger")

ticker:
	tickerWorker := NewWorker(env.flow)
	tickerWorker.SetEnabled(true)
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan struct{})
	tickerWorker.wg.Add(1)
	go func() {
		tickerWorker.run(ctx)
		close(done)
	}()
	deadline = time.Now().Add(scanWorkerPollInterval + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if tickerWorker.LastScanStartedAtMicro() != 0 {
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("run loop did not execute ticker scan")
}

func TestCoverageRunSyncBodyUnavailable(t *testing.T) {
	var nilFlow *Flow
	if branches := nilFlow.RunSyncBodyStage(context.Background()); branches != nil {
		t.Fatalf("nil flow should not sync body, got=%v", branches)
	}
	env := newTestFlowEnv(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if branches := env.flow.RunSyncBodyStage(ctx); branches != nil {
		t.Fatalf("cancelled context should not sync body, got=%v", branches)
	}
}
