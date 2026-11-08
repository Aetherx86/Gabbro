package simulator

import (
	"container/heap"
	"fmt"
	"math/rand"

	"github.com/Aetherx86/Gabbro/pkg/raft"
)

type Event struct {
	DeliveryTick uint64
	Msg          raft.RPCMessage
	index        int
}

// PriorityQueue implements heap.Interface for Events sorted by DeliveryTick.
type PriorityQueue []*Event

func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].DeliveryTick < pq[j].DeliveryTick }
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Event)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type Simulator struct {
	CurrentTick uint64
	Nodes       map[int]*raft.Node
	Queue       PriorityQueue
	RNG         *rand.Rand
	Inv         *raft.Invariants

	// Chaos parameters
	DropRate       float64
	MaxDelayTicks  uint64
	Partitions     map[int]map[int]bool // From -> To -> IsBlocked
	TotalDelivered uint64
	TotalDropped   uint64
}

func New(nodeCount int, seed int64) *Simulator {
	s := &Simulator{
		CurrentTick:   0,
		Nodes:         make(map[int]*raft.Node),
		Queue:         make(PriorityQueue, 0),
		RNG:           rand.New(rand.NewSource(seed)),
		Inv:           raft.NewInvariants(),
		DropRate:      0.0,
		MaxDelayTicks: 5,
		Partitions:    make(map[int]map[int]bool),
	}
	heap.Init(&s.Queue)

	allIDs := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		allIDs[i] = i
	}

	for i := 0; i < nodeCount; i++ {
		peers := make([]int, 0, nodeCount-1)
		for _, id := range allIDs {
			if id != i {
				peers = append(peers, id)
			}
		}
		s.Nodes[i] = raft.NewNode(i, peers, s, s.Inv)
	}

	return s
}

// Send enqueues an RPC message with simulated latency or chaos drops.
func (s *Simulator) Send(msg raft.RPCMessage) {
	// 1. Partition Check
	if s.isBlocked(msg.From, msg.To) {
		s.TotalDropped++
		return
	}

	// 2. Randomized Drop Check
	if s.DropRate > 0 && s.RNG.Float64() < s.DropRate {
		s.TotalDropped++
		return
	}

	// 3. Jitter Delay
	delay := uint64(1)
	if s.MaxDelayTicks > 1 {
		delay = 1 + uint64(s.RNG.Intn(int(s.MaxDelayTicks)))
	}

	ev := &Event{
		DeliveryTick: s.CurrentTick + delay,
		Msg:          msg,
	}
	heap.Push(&s.Queue, ev)
}

func (s *Simulator) isBlocked(from, to int) bool {
	if blockedTo, ok := s.Partitions[from]; ok {
		if blockedTo[to] {
			return true
		}
	}
	return false
}

// SetPartition isolates group A from group B bidirectionally.
func (s *Simulator) SetPartition(groupA, groupB []int) {
	for _, a := range groupA {
		if s.Partitions[a] == nil {
			s.Partitions[a] = make(map[int]bool)
		}
		for _, b := range groupB {
			s.Partitions[a][b] = true
			if s.Partitions[b] == nil {
				s.Partitions[b] = make(map[int]bool)
			}
			s.Partitions[b][a] = true
		}
	}
}

// ClearPartitions restores full network connectivity.
func (s *Simulator) ClearPartitions() {
	s.Partitions = make(map[int]map[int]bool)
}

// Step advances to the next scheduled event in virtual time.
func (s *Simulator) Step() bool {
	if s.Queue.Len() == 0 {
		return false
	}

	ev := heap.Pop(&s.Queue).(*Event)
	if ev.DeliveryTick > s.CurrentTick {
		s.CurrentTick = ev.DeliveryTick
	}

	targetNode, exists := s.Nodes[ev.Msg.To]
	if !exists || targetNode.IsDead {
		return true
	}

	s.TotalDelivered++

	switch payload := ev.Msg.Payload.(type) {
	case raft.RequestVoteArgs:
		reply := targetNode.HandleRequestVote(payload)
		s.Send(raft.RPCMessage{
			From:    ev.Msg.To,
			To:      ev.Msg.From,
			Payload: reply,
		})

	case raft.RequestVoteReply:
		targetNode.HandleVoteReply(payload, ev.Msg.From)

	case raft.AppendEntriesArgs:
		reply := targetNode.HandleAppendEntries(payload)
		s.Send(raft.RPCMessage{
			From:    ev.Msg.To,
			To:      ev.Msg.From,
			Payload: reply,
		})

	case raft.AppendEntriesReply:
		targetNode.HandleAppendEntriesReply(payload, ev.Msg.From)
	}

	return true
}

// RunUntil runs events until the queue is drained or maxTicks is reached.
func (s *Simulator) RunUntil(maxTicks uint64) {
	for s.CurrentTick < maxTicks && s.Queue.Len() > 0 {
		s.Step()
	}
}

// GetLeader returns the currently elected leader with the highest term, if any.
func (s *Simulator) GetLeader() *raft.Node {
	var bestLeader *raft.Node
	for _, n := range s.Nodes {
		if n.Role == raft.Leader && !n.IsDead {
			if bestLeader == nil || n.CurrentTerm > bestLeader.CurrentTerm {
				bestLeader = n
			}
		}
	}
	return bestLeader
}

// GetLeaderForGroup returns the leader within a specified set of node IDs.
func (s *Simulator) GetLeaderForGroup(ids []int) *raft.Node {
	var bestLeader *raft.Node
	for _, id := range ids {
		n := s.Nodes[id]
		if n.Role == raft.Leader && !n.IsDead {
			if bestLeader == nil || n.CurrentTerm > bestLeader.CurrentTerm {
				bestLeader = n
			}
		}
	}
	return bestLeader
}

// CheckLogConsistency verifies that all nodes with applied logs agree on the exact sequence.
func (s *Simulator) CheckLogConsistency() error {
	var referenceLog []raft.LogEntry
	var refNode int = -1

	for id, node := range s.Nodes {
		if node.IsDead {
			continue
		}
		if refNode == -1 {
			referenceLog = node.Log
			refNode = id
			continue
		}

		minLen := len(referenceLog)
		if len(node.Log) < minLen {
			minLen = len(node.Log)
		}

		for i := 0; i < minLen; i++ {
			if referenceLog[i].Term != node.Log[i].Term || referenceLog[i].Command != node.Log[i].Command {
				return fmt.Errorf("log divergence between node %d and node %d at index %d", refNode, id, i)
			}
		}
	}
	return nil
}
