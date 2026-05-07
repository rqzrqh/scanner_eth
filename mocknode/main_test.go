package mocknode

import (
	"testing"
	"time"
)

func TestNodeConfigByID(t *testing.T) {
	conf := &Config{
		Nodes: []NodeConfig{
			{},
			{Latency: 100 * time.Millisecond, LatencyJitter: 50 * time.Millisecond, DropRate: 0.25},
		},
	}

	if err := validateNodeConfigs(conf); err != nil {
		t.Fatalf("validate node config failed: %v", err)
	}

	node0 := conf.NodeConfig(0)
	if node0.Latency != 0 || node0.LatencyJitter != 0 || node0.DropRate != 0 {
		t.Fatalf("unexpected default node config: %+v", node0)
	}

	node1 := conf.NodeConfig(1)
	if node1.Latency != 100*time.Millisecond || node1.LatencyJitter != 50*time.Millisecond || node1.DropRate != 0.25 {
		t.Fatalf("unexpected configured node config: %+v", node1)
	}
}

func TestValidateNodeConfigsRejectsInvalidDropRate(t *testing.T) {
	conf := &Config{
		Nodes: []NodeConfig{
			{DropRate: 1.1},
		},
	}

	if err := validateNodeConfigs(conf); err == nil {
		t.Fatal("expected invalid drop_rate to fail validation")
	}
}
