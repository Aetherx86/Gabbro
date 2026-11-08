package raft

import (
	"fmt"
	"sync"
)

type NetworkInterface interface {
	Send(msg RPCMessage)
}

type Node struct {
	mu sync.Mutex

	ID    int
	Peers []int
	Net   NetworkInterface
	Inv   *Invariants

	CurrentTerm uint64
	VotedFor    int
	Log         []LogEntry

	CommitIndex uint64
	LastApplied uint64

	Role Role

	// Leader state
	NextIndex  map[int]uint64
	MatchIndex map[int]uint64

	VotesReceived int
	IsDead        bool
}

func NewNode(id int, peers []int, net NetworkInterface, inv *Invariants) *Node {
	n := &Node{
		ID:          id,
		Peers:       peers,
		Net:         net,
		Inv:         inv,
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         make([]LogEntry, 0),
		Role:        Follower,
		NextIndex:   make(map[int]uint64),
		MatchIndex:  make(map[int]uint64),
	}
	// Index 0 sentinel entry
	n.Log = append(n.Log, LogEntry{Index: 0, Term: 0, Command: "SENTINEL"})
	return n
}

func (n *Node) LastLogIndex() uint64 {
	return n.Log[len(n.Log)-1].Index
}

func (n *Node) LastLogTerm() uint64 {
	return n.Log[len(n.Log)-1].Term
}

// StartElection triggers a candidate election cycle for the node.
func (n *Node) StartElection() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.IsDead {
		return
	}

	n.Role = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	n.VotesReceived = 1 // Vote for self

	lastIdx := n.LastLogIndex()
	lastTerm := n.LastLogTerm()

	// Check single-node cluster election
	if len(n.Peers) == 0 {
		n.becomeLeader()
		return
	}

	for _, peer := range n.Peers {
		args := RequestVoteArgs{
			Term:         n.CurrentTerm,
			CandidateID:  n.ID,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		n.Net.Send(RPCMessage{
			From:    n.ID,
			To:      peer,
			Payload: args,
		})
	}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsDead {
		return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: false}
	}

	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.Role = Follower
		n.VotedFor = -1
	}

	reply := RequestVoteReply{
		Term:        n.CurrentTerm,
		VoteGranted: false,
	}

	if args.Term < n.CurrentTerm {
		return reply
	}

	canVote := (n.VotedFor == -1 || n.VotedFor == args.CandidateID)

	// Leader log up-to-date check (§5.4.1)
	lastIdx := n.LastLogIndex()
	lastTerm := n.LastLogTerm()
	logUpToDate := false
	if args.LastLogTerm > lastTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx {
		logUpToDate = true
	}

	if canVote && logUpToDate {
		reply.VoteGranted = true
		n.VotedFor = args.CandidateID
		n.Role = Follower
	}

	return reply
}

// HandleVoteReply receives a vote result and evaluates quorum.
func (n *Node) HandleVoteReply(reply RequestVoteReply, from int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsDead || n.Role != Candidate {
		return
	}

	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.Role = Follower
		n.VotedFor = -1
		return
	}

	if reply.Term == n.CurrentTerm && reply.VoteGranted {
		n.VotesReceived++
		totalNodes := len(n.Peers) + 1
		majority := (totalNodes / 2) + 1
		if n.VotesReceived >= majority {
			n.becomeLeader()
		}
	}
}

func (n *Node) becomeLeader() {
	n.Role = Leader
	if n.Inv != nil {
		if err := n.Inv.CheckElectionSafety(n.CurrentTerm, n.ID); err != nil {
			panic(fmt.Sprintf("[INVARIANT BREACH] %v", err))
		}
	}

	lastIdx := n.LastLogIndex()
	for _, p := range n.Peers {
		n.NextIndex[p] = lastIdx + 1
		n.MatchIndex[p] = 0
	}

	n.BroadcastHeartbeats()
}

// BroadcastHeartbeats sends empty AppendEntries RPC to all peers.
func (n *Node) BroadcastHeartbeats() {
	if n.Role != Leader || n.IsDead {
		return
	}

	lastIdx := n.LastLogIndex()
	lastTerm := n.LastLogTerm()

	for _, peer := range n.Peers {
		args := AppendEntriesArgs{
			Term:         n.CurrentTerm,
			LeaderID:     n.ID,
			PrevLogIndex: lastIdx,
			PrevLogTerm:  lastTerm,
			Entries:      nil,
			LeaderCommit: n.CommitIndex,
		}
		n.Net.Send(RPCMessage{
			From:    n.ID,
			To:      peer,
			Payload: args,
		})
	}
}

// HandleAppendEntries handles log replication and heartbeats from the leader.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{
		Term:       n.CurrentTerm,
		Success:    false,
		MatchIndex: 0,
	}

	if n.IsDead {
		return reply
	}

	if args.Term < n.CurrentTerm {
		return reply
	}

	if args.Term > n.CurrentTerm || n.Role == Candidate {
		n.CurrentTerm = args.Term
		n.Role = Follower
		n.VotedFor = -1
	}

	// Term match verified, reply term is current term
	reply.Term = n.CurrentTerm

	// Check log matching (§5.3)
	if args.PrevLogIndex >= uint64(len(n.Log)) || n.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return reply
	}

	// Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx < uint64(len(n.Log)) {
			if n.Log[idx].Term != entry.Term {
				n.Log = n.Log[:idx]
				n.Log = append(n.Log, entry)
			}
		} else {
			n.Log = append(n.Log, entry)
		}
	}

	// Update commit index
	if args.LeaderCommit > n.CommitIndex {
		lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNewIndex {
			n.CommitIndex = args.LeaderCommit
		} else {
			n.CommitIndex = lastNewIndex
		}
		if n.CommitIndex > n.LastApplied {
			n.LastApplied = n.CommitIndex
		}
	}

	reply.Success = true
	reply.MatchIndex = n.LastLogIndex()
	return reply
}

// HandleAppendEntriesReply updates leader's nextIndex and matchIndex.
func (n *Node) HandleAppendEntriesReply(reply AppendEntriesReply, from int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsDead || n.Role != Leader {
		return
	}

	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.Role = Follower
		n.VotedFor = -1
		return
	}

	if reply.Success {
		if reply.MatchIndex > n.MatchIndex[from] {
			n.MatchIndex[from] = reply.MatchIndex
			n.NextIndex[from] = reply.MatchIndex + 1
		}
		oldCommit := n.CommitIndex
		n.updateCommitIndex()
		if n.CommitIndex > oldCommit {
			n.BroadcastHeartbeats()
		}
	} else {
		if n.NextIndex[from] > 1 {
			n.NextIndex[from]--
		}
		prevIdx := n.NextIndex[from] - 1
		var entries []LogEntry
		if n.LastLogIndex() >= n.NextIndex[from] {
			entries = n.Log[n.NextIndex[from]:]
		}
		n.Net.Send(RPCMessage{
			From: n.ID,
			To:   from,
			Payload: AppendEntriesArgs{
				Term:         n.CurrentTerm,
				LeaderID:     n.ID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  n.Log[prevIdx].Term,
				Entries:      entries,
				LeaderCommit: n.CommitIndex,
			},
		})
	}
}

// Propose appends a client command to the leader's log and broadcasts replication.
func (n *Node) Propose(cmd string) (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Leader || n.IsDead {
		return 0, false
	}

	newIndex := n.LastLogIndex() + 1
	entry := LogEntry{
		Index:   newIndex,
		Term:    n.CurrentTerm,
		Command: cmd,
	}
	n.Log = append(n.Log, entry)

	// Replicate to peers
	for _, peer := range n.Peers {
		prevIdx := n.NextIndex[peer] - 1
		var entries []LogEntry
		if newIndex >= n.NextIndex[peer] {
			entries = n.Log[n.NextIndex[peer]:]
		}

		args := AppendEntriesArgs{
			Term:         n.CurrentTerm,
			LeaderID:     n.ID,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  n.Log[prevIdx].Term,
			Entries:      entries,
			LeaderCommit: n.CommitIndex,
		}
		n.Net.Send(RPCMessage{
			From:    n.ID,
			To:      peer,
			Payload: args,
		})
	}

	return newIndex, true
}

func (n *Node) updateCommitIndex() {
	totalNodes := len(n.Peers) + 1
	majority := (totalNodes / 2) + 1

	for idx := n.LastLogIndex(); idx > n.CommitIndex; idx-- {
		if n.Log[idx].Term != n.CurrentTerm {
			continue
		}
		matches := 1 // Self
		for _, peer := range n.Peers {
			if n.MatchIndex[peer] >= idx {
				matches++
			}
		}
		if matches >= majority {
			n.CommitIndex = idx
			if n.CommitIndex > n.LastApplied {
				n.LastApplied = n.CommitIndex
			}
			break
		}
	}
}
