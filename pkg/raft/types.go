package raft

import (
	"fmt"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command string
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	MatchIndex uint64
}

type RPCMessage struct {
	From int
	To   int
	Payload any
}

// Invariants tracks Raft consensus guarantees across terms and nodes.
type Invariants struct {
	// Term -> Elected Leader ID
	LeadersPerTerm map[uint64]int
}

func NewInvariants() *Invariants {
	return &Invariants{
		LeadersPerTerm: make(map[uint64]int),
	}
}

// CheckElectionSafety asserts at most one leader can be elected in a term.
func (inv *Invariants) CheckElectionSafety(term uint64, leaderID int) error {
	if existing, found := inv.LeadersPerTerm[term]; found {
		if existing != leaderID {
			return fmt.Errorf("election safety violation: term %d elected both node %d and node %d", term, existing, leaderID)
		}
	} else {
		inv.LeadersPerTerm[term] = leaderID
	}
	return nil
}
