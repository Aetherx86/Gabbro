package simulator

import (
	"fmt"
	"testing"

	"github.com/Aetherx86/Gabbro/pkg/raft"
)

func TestLeaderElection(t *testing.T) {
	sim := New(5, 42)

	// Node 0 starts election
	sim.Nodes[0].StartElection()

	// Run simulation until queue drains or 50 ticks
	sim.RunUntil(50)

	leader := sim.GetLeader()
	if leader == nil {
		t.Fatalf("expected a leader to be elected")
	}
	if leader.ID != 0 {
		t.Fatalf("expected node 0 to be elected, got node %d", leader.ID)
	}
	if leader.Role != raft.Leader {
		t.Fatalf("expected role Leader, got %v", leader.Role)
	}
}

func TestLogReplication(t *testing.T) {
	sim := New(5, 1337)
	sim.Nodes[0].StartElection()
	sim.RunUntil(50)

	leader := sim.GetLeader()
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	// Propose 3 commands
	cmds := []string{"SET x=10", "SET y=20", "SET z=30"}
	for _, cmd := range cmds {
		idx, ok := leader.Propose(cmd)
		if !ok {
			t.Fatalf("failed to propose %s", cmd)
		}
		if idx == 0 {
			t.Fatalf("invalid log index")
		}
	}

	// Run ticks to replicate
	sim.RunUntil(150)

	// Check that majority committed
	for id, n := range sim.Nodes {
		if n.CommitIndex < 3 {
			t.Errorf("node %d commit index %d < 3", id, n.CommitIndex)
		}
	}

	if err := sim.CheckLogConsistency(); err != nil {
		t.Fatalf("log consistency failed: %v", err)
	}
}

func TestNetworkPartitionAndHeal(t *testing.T) {
	sim := New(5, 9999)
	sim.Nodes[0].StartElection()
	sim.RunUntil(50)

	if sim.GetLeader() == nil {
		t.Fatalf("initial leader election failed")
	}

	// Partition: {0, 1} vs {2, 3, 4}
	sim.SetPartition([]int{0, 1}, []int{2, 3, 4})

	// Leader 0 (in minority {0, 1}) attempts to propose command
	sim.Nodes[0].Propose("UNCOMMITTED_IN_MINORITY")
	sim.RunUntil(100)

	// Majority {2, 3, 4} starts election
	sim.Nodes[2].StartElection()
	sim.RunUntil(150)

	newLeader := sim.GetLeader()
	if newLeader == nil || (newLeader.ID != 2 && newLeader.ID != 3 && newLeader.ID != 4) {
		t.Fatalf("expected majority group {2, 3, 4} to elect a new leader, got %+v", newLeader)
	}

	// Propose on new leader in majority
	newLeader.Propose("COMMITTED_IN_MAJORITY")
	sim.RunUntil(200)

	if newLeader.CommitIndex < 1 {
		t.Fatalf("expected majority leader to commit")
	}

	// Heal partition
	sim.ClearPartitions()

	// Trigger heartbeat to sync logs
	newLeader.BroadcastHeartbeats()
	sim.RunUntil(300)

	if err := sim.CheckLogConsistency(); err != nil {
		t.Fatalf("post-heal log divergence: %v", err)
	}
}

func TestChaosDropAndJitter(t *testing.T) {
	sim := New(5, 777)
	sim.DropRate = 0.10     // 10% packet drop
	sim.MaxDelayTicks = 4   // Random 1..4 tick latency

	sim.Nodes[0].StartElection()
	sim.RunUntil(50)

	leader := sim.GetLeader()
	if leader == nil {
		// Retry election if dropped
		sim.Nodes[1].StartElection()
		sim.RunUntil(100)
		leader = sim.GetLeader()
	}

	if leader != nil {
		for i := 0; i < 10; i++ {
			leader.Propose(fmt.Sprintf("CHAOS_OP_%d", i))
		}
		// Periodic heartbeats during chaos
		for tick := uint64(0); tick < 300; tick += 20 {
			leader.BroadcastHeartbeats()
			sim.RunUntil(sim.CurrentTick + 20)
		}

		if err := sim.CheckLogConsistency(); err != nil {
			t.Fatalf("chaos divergence: %v", err)
		}
	}
}
