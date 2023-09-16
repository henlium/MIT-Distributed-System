package raft

/*
 * All structures and interfaces for Raft RPC calls go here
 */

type Termer interface {
	getTerm() int
}

type TermInt struct {
	Term int
}

func (t TermInt) getTerm() int {
	return t.Term
}

type RequestVoteArgs struct {
	TermInt
	Candidate int
}

type RequestVoteReply struct {
	TermInt
	Granted bool
}

type AppendEntriesArgs struct {
	TermInt
	Leader int
}

type AppendEntriesReply struct {
	TermInt
}
