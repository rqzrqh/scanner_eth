package fetch

import (
	"expvar"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEnableTaskPoolMetricsPublishesStats(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	name := fmt.Sprintf("fetch_task_pool_test_%d", time.Now().UnixNano())

	fm.EnableTaskPoolMetrics(name)
	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("expected expvar metric to be published")
	}
	if !strings.Contains(v.String(), "\"config\"") || !strings.Contains(v.String(), "\"worker_count\":1") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}
	if !strings.Contains(v.String(), "\"by_kind\"") || !strings.Contains(v.String(), "\"header_hash_sync\"") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}

	storeV := expvar.Get(name + "_store_block")
	if storeV == nil {
		t.Fatalf("expected store block expvar metric to be published")
	}
	if !strings.Contains(storeV.String(), "\"submitted\"") || !strings.Contains(storeV.String(), "\"processing\"") {
		t.Fatalf("unexpected store block metric payload: %v", storeV.String())
	}

	scanV := expvar.Get(name + "_scan")
	if scanV == nil {
		t.Fatalf("expected scan expvar metric to be published")
	}
	if !strings.Contains(scanV.String(), "\"stages\"") {
		t.Fatalf("unexpected scan metric payload: %v", scanV.String())
	}

	runtimeV := expvar.Get(name + "_runtime")
	if runtimeV == nil {
		t.Fatalf("expected runtime expvar metric to be published")
	}
	if !strings.Contains(runtimeV.String(), "\"blocktree\"") || !strings.Contains(runtimeV.String(), "\"nodes\"") {
		t.Fatalf("unexpected runtime metric payload: %v", runtimeV.String())
	}
}
