package raft

import (
	"testing"
)

type mockNet struct {
	sent []RPCMessage
}

func (m *mockNet) Send(msg RPCMessage) {
	m.sent = append(m.sent, msg)
}

func TestNodeInitialState(t *testing.T) {
	net := &mockNet{}
	inv := NewInvariants()
	node := NewNode(0, []int{1, 2}, net, inv)

	if node.Role != Follower {
		t.Fatalf("expected follower, got %v", node.Role)
	}
	if node.CurrentTerm != 0 {
		t.Fatalf("expected term 0, got %d", node.CurrentTerm)
	}
	if node.LastLogIndex() != 0 {
		t.Fatalf("expected sentinel index 0, got %d", node.LastLogIndex())
	}
}

func TestSingleNodeCluster(t *testing.T) {
	net := &mockNet{}
	inv := NewInvariants()
	node := NewNode(0, nil, net, inv)

	node.StartElection()
	if node.Role != Leader {
		t.Fatalf("single node should immediately become leader")
	}
	if node.CurrentTerm != 1 {
		t.Fatalf("expected term 1, got %d", node.CurrentTerm)
	}

	idx, ok := node.Propose("CMD_1")
	if !ok || idx != 1 {
		t.Fatalf("propose failed: %v, idx=%d", ok, idx)
	}
}

func TestElectionSafetyInvariant(t *testing.T) {
	inv := NewInvariants()
	if err := inv.CheckElectionSafety(1, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same leader in same term is valid
	if err := inv.CheckElectionSafety(1, 0); err != nil {
		t.Fatalf("re-electing same leader in term should be ok: %v", err)
	}
	// Different leader in same term MUST fail invariant
	if err := inv.CheckElectionSafety(1, 2); err == nil {
		t.Fatalf("expected error on split brain invariant check")
	}
}
