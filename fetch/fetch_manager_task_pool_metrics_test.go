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
	if !strings.Contains(storeV.String(), "\"submitted\"") || !strings.Contains(storeV.String(), "\"storing\"") {
		t.Fatalf("unexpected store block metric payload: %v", storeV.String())
	}
}
