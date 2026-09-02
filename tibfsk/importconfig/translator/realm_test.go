/*
 * Copyright (c) 2026 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

package translator

import (
	"fmt"
	"strings"
	"testing"
)

// shardMembership returns the pserver names of each kof.cluster in the realm, in
// cluster order: ["pserver1,pserver2,pserver3", "pserver4,..."].
func shardMembership(t *testing.T, realm map[string]any) []string {
	t.Helper()
	clusters, ok := realm["clusters"].([]any)
	if !ok {
		t.Fatalf("realm has no clusters list")
	}
	out := make([]string, len(clusters))
	for i, c := range clusters {
		cluster := c.(map[string]any)
		sets := cluster["pserver_sets"].([]any)
		pservers := sets[0].(map[string]any)["pservers"].([]any)
		names := make([]string, len(pservers))
		for j, p := range pservers {
			names[j] = p.(map[string]any)["name"].(string)
		}
		if got, want := cluster["name"].(string), fmt.Sprintf("kof.cluster.%d", i); got != want {
			t.Errorf("cluster %d is named %q, want %q", i, got, want)
		}
		out[i] = strings.Join(names, ",")
	}
	return out
}

// The pserver count is validated in main to be an exact multiple of the replication
// factor, so every shard here is full -- no short final group.
func TestBuildRealm_ShardsFollowReplicationFactor(t *testing.T) {
	cfg := makeCfg(t, minimalBroker)

	cases := []struct {
		name        string
		rf          int
		numPservers int
		want        []string
	}{
		{
			name: "default factor, one shard",
			rf:   3, numPservers: 3,
			want: []string{"pserver1,pserver2,pserver3"},
		},
		{
			name: "default factor, two shards",
			rf:   3, numPservers: 6,
			want: []string{"pserver1,pserver2,pserver3", "pserver4,pserver5,pserver6"},
		},
		{
			name: "default factor, three shards",
			rf:   3, numPservers: 9,
			want: []string{
				"pserver1,pserver2,pserver3",
				"pserver4,pserver5,pserver6",
				"pserver7,pserver8,pserver9",
			},
		},
		{
			name: "factor 5, one shard spanning the primary and aux files",
			rf:   5, numPservers: 5,
			want: []string{"pserver1,pserver2,pserver3,pserver4,pserver5"},
		},
		{
			name: "factor 1, one unreplicated shard per pserver",
			rf:   1, numPservers: 4,
			want: []string{"pserver1", "pserver2", "pserver3", "pserver4"},
		},
		{
			name: "factor 1, a lone standalone pserver",
			rf:   1, numPservers: 1,
			want: []string{"pserver1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			realm := buildRealm(cfg, tc.numPservers, DROpts{}, "auto",
				ClusterOpts{ReplicationFactor: tc.rf})
			got := shardMembership(t, realm)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d shards %v, want %d %v", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("kof.cluster.%d holds %s, want %s", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// A ClusterOpts literal that leaves ReplicationFactor unset -- which every caller
// outside main does -- must behave as the historical hardcoded 3.
func TestBuildRealm_ZeroReplicationFactorDefaultsToThree(t *testing.T) {
	cfg := makeCfg(t, minimalBroker)

	if got := (ClusterOpts{}).RF(); got != 3 {
		t.Errorf("zero-valued ClusterOpts.RF() = %d, want 3", got)
	}

	got := shardMembership(t, buildRealm(cfg, 9, DROpts{}, "auto", ClusterOpts{}))
	want := []string{
		"pserver1,pserver2,pserver3",
		"pserver4,pserver5,pserver6",
		"pserver7,pserver8,pserver9",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d shards %v, want %d", len(got), got, len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("kof.cluster.%d holds %s, want %s", i, got[i], want[i])
		}
	}

	// A single pserver is the one count that is not a multiple of 3 and still
	// reaches here: main resolves the factor to 1 for it, but a zero-valued
	// ClusterOpts does not, so the floor in buildRealm has to hold the shard count
	// at one rather than zero.
	if got := shardMembership(t, buildRealm(cfg, 1, DROpts{}, "auto", ClusterOpts{})); len(got) != 1 || got[0] != "pserver1" {
		t.Errorf("one pserver produced %v, want [pserver1]", got)
	}
}
