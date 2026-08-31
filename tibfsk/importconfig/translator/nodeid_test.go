/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"strings"
	"testing"
)

// A ZooKeeper-mode broker (Kafka 3.9 and earlier) names its identity broker.id.
// FSK reads node.id and refuses to start without it, so the key has to be renamed
// on the way through -- and must not also turn up in unsupported.properties.
func TestBrokerIDBecomesNodeID(t *testing.T) {
	src := "broker.id=0\n" +
		"listeners=PLAINTEXT://localhost:9092\n" +
		"advertised.listeners=PLAINTEXT://localhost:9092\n" +
		"zookeeper.connect=localhost:2181\n"

	out, unsupported := renderProps(t, src)

	if !strings.Contains(out, "\nnode.id=0\n") {
		t.Errorf("node.id=0 not emitted; got:\n%s", out)
	}
	if strings.Contains(out, "broker.id=") {
		t.Errorf("broker.id should have been renamed, not emitted; got:\n%s", out)
	}
	if !strings.Contains(out, "# Node ID: 0 (from broker.id") {
		t.Errorf("missing provenance note in header; got:\n%s", out)
	}
	for _, kv := range unsupported {
		if strings.HasPrefix(kv, "broker.id=") {
			t.Errorf("broker.id reported unsupported though its value was used: %q", kv)
		}
	}
	// zookeeper.* has no FSK equivalent and still belongs in unsupported.
	if !containsKey(unsupported, "zookeeper.connect") {
		t.Errorf("zookeeper.connect should be unsupported; got %v", unsupported)
	}
}

// node.id wins when a file carries both spellings; the stray broker.id is then
// just another unrecognized key.
func TestNodeIDWinsOverBrokerID(t *testing.T) {
	out, unsupported := renderProps(t, "node.id=7\nbroker.id=3\nlisteners=PLAINTEXT://localhost:9092\n")

	if !strings.Contains(out, "\nnode.id=7\n") {
		t.Errorf("node.id=7 not emitted; got:\n%s", out)
	}
	if strings.Contains(out, "# Node ID: 7 (") {
		t.Errorf("header should carry no provenance note; got:\n%s", out)
	}
	if !containsKey(unsupported, "broker.id") {
		t.Errorf("stray broker.id should be unsupported; got %v", unsupported)
	}
}

// The pserver rejects a missing or negative node.id. Kafka permits both -- a KRaft
// file can leave the id to `kafka-storage format`, and broker.id=-1 asks a
// ZooKeeper broker to generate one -- so the tool fills them in, without colliding
// with an id another broker already holds.
func TestEnsureNodeIDsFillsMissingAndNegative(t *testing.T) {
	const listener = "listeners=PLAINTEXT://localhost:9092\n"
	cfgs := []*BrokerConfig{
		parseSrc(t, "broker.id=-1\n"+listener),
		parseSrc(t, "node.id=1\n"+listener),
		parseSrc(t, listener),
	}

	EnsureNodeIDs(cfgs)

	if got := []int{cfgs[0].NodeID, cfgs[1].NodeID, cfgs[2].NodeID}; got[0] != 2 || got[1] != 1 || got[2] != 3 {
		t.Errorf("node ids = %v, want [2 1 3]", got)
	}
	for i, want := range []NodeIDOrigin{NodeIDAssigned, NodeIDFromSource, NodeIDAssigned} {
		if cfgs[i].NodeIDOrigin != want {
			t.Errorf("cfgs[%d].NodeIDOrigin = %q, want %q", i, cfgs[i].NodeIDOrigin, want)
		}
	}
	for i, cfg := range cfgs {
		if got := cfg.Settings["node.id"]; got == "" {
			t.Errorf("cfgs[%d] has no node.id setting to emit", i)
		}
	}
}

// A node.id the tool assigned has to reach the generated file, not just the struct.
func TestAssignedNodeIDIsEmitted(t *testing.T) {
	cfg := parseSrc(t, "listeners=PLAINTEXT://localhost:9092\n")
	EnsureNodeIDs([]*BrokerConfig{cfg})

	out := renderCfg(t, cfg)
	if !strings.Contains(out, "\nnode.id=1\n") {
		t.Errorf("assigned node.id not emitted; got:\n%s", out)
	}
	if !strings.Contains(out, "# Node ID: 1 (assigned by the tool") {
		t.Errorf("missing assigned note in header; got:\n%s", out)
	}
}

func containsKey(unsupported []string, key string) bool {
	for _, kv := range unsupported {
		if strings.HasPrefix(kv, key+"=") {
			return true
		}
	}
	return false
}
