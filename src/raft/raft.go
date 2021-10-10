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

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Role int64

var (
	Leader    Role = 1
	Candidate Role = 2
	Follower  Role = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	log       []LogEntry

	role    Role
	active  bool
	term    int
	voteFor int

	lastLogIndex int
	lastLogTerm  int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	hasReplies int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term    int
	Content int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoterId     int
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoterId = rf.me
	if rf.term < args.Term {
		rf.active = true
		rf.role = Follower
		rf.term = args.Term
		rf.voteFor = -1
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	} else if rf.term == args.Term {
		reply.Term = rf.term
		if (rf.voteFor != -1 && rf.voteFor != args.CandidateId) || rf.lastLogTerm > args.LastLogTerm ||
			(rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
		} else {
			rf.active = true
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
		}
	} else {
		reply.Term = rf.term
		reply.VoteGranted = false
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
	reply *RequestVoteReply, state *sendAllRequestVoteState) {
	rf.mu.Lock()
	args.Term = rf.term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			state.mu.Lock()
			if !state.replies[reply.VoterId] {
				state.replies[reply.VoterId] = true
				state.count++
			}
			state.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.active = true
		reply.Success = false
		reply.Term = rf.term
		return
	}

	for i := 0; i < len(args.Entries); i++ {
		rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}
	rf.active = true
	rf.lastLogIndex = args.PrevLogIndex + len(args.Entries)
	rf.lastLogTerm = args.Term
	reply.Success = true
	reply.Term = rf.term
}

var RetryTime = 3

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var i int
	for i = RetryTime; i > 0; i-- {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			rf.mu.Lock()
			rf.hasReplies++
			rf.mu.Unlock()
			break
		} else {
			time.Sleep(time.Duration(HeartbeatTimeout/50) * time.Millisecond)
		}
	}
	if i == 0 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		if !reply.Success {
			if reply.Term > rf.term {
				rf.role = Follower
				rf.term = args.Term
				rf.voteFor = -1
			} else {
				rf.nextIndex[server]--
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}
		} else {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

var (
	ElectionTimeoutBase float32 = 600
	HeartbeatTimeout            = 350
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(int64(rf.me)<<10 + time.Now().Unix())
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			go rf.sendHeartbeats()
			time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
			rf.mu.Lock()

			if rf.hasReplies+1 < len(rf.peers)-(len(rf.peers)-1)/2 {
				rf.role = Follower
			}
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		count := ElectionTimeoutBase + ElectionTimeoutBase*rand.Float32()
		for count > 0 {
			rf.mu.Lock()
			if rf.active {
				rf.active = false
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			count -= 10
			time.Sleep(10 * time.Millisecond)
		}
		if count <= 0 {
			rf.mu.Lock()
			rf.role = Candidate
			rf.term++
			rf.voteFor = rf.me
			rf.mu.Unlock()
			rf.sendAllRequestVote()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.hasReplies = 0
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.term,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		}
		if rf.lastLogIndex >= rf.nextIndex[server] {
			args.Entries = []LogEntry{rf.log[rf.nextIndex[server]]}
		}

		go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
	}
}

type sendAllRequestVoteState struct {
	mu      sync.Mutex
	replies []bool
	count   int
}

func (rf *Raft) sendAllRequestVote() {
	state := &sendAllRequestVoteState{
		count: 1,
	}
	rf.mu.Lock()
	state.replies = make([]bool, len(rf.peers))
	electionTerm := rf.term
	for server := 0; server < len(state.replies); server++ {
		if server == rf.me {
			continue
		}
		go rf.sendRequestVote(server, &RequestVoteArgs{}, &RequestVoteReply{}, state)
	}
	rf.mu.Unlock()

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.term != electionTerm || rf.role != Candidate {
			rf.mu.Unlock()
			break
		}
		state.mu.Lock()
		if float32(state.count) > float32(len(state.replies))/2 {
			state.mu.Unlock()
			rf.mu.Unlock()
			rf.leaderInit()
			return
		}
		for server := 0; server < len(state.replies); server++ {
			if server == rf.me {
				continue
			}
			if !state.replies[server] {
				go rf.sendRequestVote(server, &RequestVoteArgs{}, &RequestVoteReply{}, state)
			}
		}
		state.mu.Unlock()
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderInit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.active = true
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.role = Follower
	rf.log = make([]LogEntry, 1)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
