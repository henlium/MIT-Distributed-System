package raft

import (
	"testing"

	"6.5840/labrpc"
)

func TestMakeAppendEntriesArgs(t *testing.T) {
	nilPeers := make([]*labrpc.ClientEnd, 3)
	me := 0
	raft := makeRaft(nilPeers, me, MakePersister(), nil)
	raft.becomeLeader()
	raft.appendLog(nil)
	raft.appendLog(nil)

	args := raft.makeAppendEntriesArgs(raft.term, me)

	if len(args.Entries) != 2 {
		t.Fatalf("Args should have 2 log entry, but got %v", len(args.Entries))
	}
}
