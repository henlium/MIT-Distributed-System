package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type state int

const (
	follower state = iota
	candidate
	leader
)

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu should be used during serving an RPC, when an election timer expired and when an election is passed
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// All following fields should be guarded by mu for both reads and writes
	term        int
	state       state
	leaderAlive bool
	vote        int
	logs        []Log
	applyCh     chan ApplyMsg

	// Volatile states
	commitIndex int
	lastApplied int

	// Volatile leader states
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == leader
}

// 1-index accessor to Raft.logs
func (rf *Raft) logAt(i int) Log {
	return rf.logs[i-1]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) call(server int, svcMeth string, args Termer, reply Termer) bool {
	ok := rf.peers[server].Call(svcMeth, args, reply)
	if ok {
		rf.mu.Lock()
		rf.checkTerm(reply.getTerm())
		rf.mu.Unlock()
	}
	return ok
}

// Return if this server's term is ahead.
// If this server's term is behind, it will become follower
// and update its term.
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.term {
		rf.becomeFollower(term)
	}
	return rf.term > term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	termAhead := rf.checkTerm(args.Term)
	reply.Term = rf.term
	if termAhead {
		// Do not bother with request from server of older term
		return
	}
	if rf.vote == -1 || rf.vote == args.Candidate {
		reply.Granted = true
		rf.vote = args.Candidate
	} else {
		// println(rf.me, "rejected vote from", args.Candidate, ", already voted for", rf.vote)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.call(server, "Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	termAhead := rf.checkTerm(args.Term)
	reply.Term = rf.term
	if termAhead {
		// Do not bother with request from server of older term
		return
	}
	rf.leaderAlive = true

	noPrevLog := args.PrevLogIndex > len(rf.logs)
	prevTermMismatch := args.PrevLogIndex > 0 && rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm
	if noPrevLog || prevTermMismatch {
		return
	}

	if len(rf.logs) > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex]
	}

	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		for index := rf.commitIndex + 1; index <= args.LeaderCommit; index++ {
			rf.commit(index)
		}
	}
	reply.Success = true
}

// Adds the term number to args inside. No need to do it elsewhere
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.call(server, "Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if (!reply.Success) && rf.state == leader && rf.nextIndex[server] > 1 {
		rf.nextIndex[server] -= 1
	}
	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}
	rf.mu.Unlock()
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return
	}
	isLeader = true

	index, term = rf.appendLog(command)

	return
}

// Append a command to the log entries.
// No conditions will be checked, and lock needs to be acquired before calling
func (rf *Raft) appendLog(command interface{}) (index int, term int) {
	index = len(rf.logs) + 1
	term = rf.term
	rf.logs = append(rf.logs, Log{index, term, command})
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == leader {
			term := rf.term
			rf.mu.Unlock()
			rf.replicateLogs(term)
			go rf.tryCommit()
			time.Sleep(time.Duration(80) * time.Millisecond)
			continue
		}

		if !rf.leaderAlive {
			newTerm := rf.becomeCandidate()
			go rf.newElection(newTerm)
		}
		rf.leaderAlive = false
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Replicate logs to other servers
func (rf *Raft) replicateLogs(term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := rf.makeAppendEntriesArgs(term, i)
		go func(server int) {
			rf.sendAppendEntries(
				server,
				&args,
				&AppendEntriesReply{})
		}(i)
	}
}

func (rf *Raft) makeAppendEntriesArgs(term int, server int) (args AppendEntriesArgs) {
	args.Term = term
	args.Leader = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
	}
	args.Entries = append(args.Entries, rf.logs[args.PrevLogIndex:]...)
	args.LeaderCommit = rf.commitIndex
	return
}

func (rf *Raft) tryCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return
	}
	for index := rf.commitIndex + 1; index <= len(rf.logs); index++ {
		if !rf.canCommit(index) {
			break
		}
		rf.commit(index)
		rf.commitIndex = index
	}
}

// Return true when the majority of peers has replicated the log at index
func (rf *Raft) canCommit(index int) bool {
	replicated := 0
	for i := range rf.peers {
		if i == rf.me || rf.matchIndex[i] >= index {
			replicated += 1
		}
	}

	return replicated*2 > len(rf.peers)
}

func (rf *Raft) commit(index int) {
	rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logAt(index).Command, CommandIndex: index}
	rf.commitIndex = index
}

// transits to candidate state and returns the new term number
func (rf *Raft) becomeCandidate() int {
	rf.state = candidate
	rf.term++
	rf.vote = rf.me
	return rf.term
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.term = term
	rf.vote = -1
}

func (rf *Raft) becomeLeader() {
	if rf.state == leader {
		return
	}
	rf.state = leader
	nextIndex := len(rf.logs) + 1
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) newElection(term int) {
	args := RequestVoteArgs{TermInt{term}, rf.me}
	c := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(
				server,
				&args,
				&reply)
			if !ok {
				reply.Granted = false
			}
			c <- reply
		}(i)
	}
	finished := 1
	votes := 1
	for finished < len(rf.peers) {
		reply := <-c
		finished++
		rf.mu.Lock()
		rf.checkTerm(reply.Term)
		if term == rf.term &&
			rf.state == candidate &&
			reply.Granted {
			votes++
			if votes*2 >= len(rf.peers) {
				rf.becomeLeader()
				rf.replicateLogs(term)
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := makeRaft(peers, me, persister, applyCh)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func makeRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.vote = -1
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	return rf
}
