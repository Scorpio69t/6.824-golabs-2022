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
	"sort"
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
	log       []*LogEntry

	role    Role
	active  bool
	term    int
	voteFor int

	lastLogIndex int
	lastLogTerm  int

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	nextIndex  []int
	matchIndex []int

	replies int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.term
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
		if rf.lastLogTerm > args.LastLogTerm ||
			(rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
		} else {
			rf.active = true
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
		}
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
		state.mu.Lock()
		if reply.VoteGranted {
			if !state.replies[reply.VoterId] {
				state.replies[reply.VoterId] = true
				state.count++
			}
		} else {
			state.quit = true
		}
		state.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("me: %v, leaderId:%v, term:%v, lastLogIndex: %v, PrevLogIndex: %v\n",
	//	rf.me, args.LeaderId, rf.term, rf.lastLogIndex, args.PrevLogIndex)
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	if rf.lastLogIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.active = true
		reply.Success = false
		reply.Term = rf.term
		return
	}

	rewrites := rf.lastLogIndex - args.PrevLogIndex
	// fmt.Printf("me: %v, prelogindex: %v, rewrites: %v, loglength:%v, len(Entry):%v\n",
	//	rf.me, args.PrevLogIndex, rewrites, len(rf.log), len(args.Entries))

	i := 0
	for ; i < rewrites && i < len(args.Entries); i++ {
		rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:len(args.Entries)]...)
	}
	rf.active = true
	rf.lastLogIndex = args.PrevLogIndex + len(args.Entries)
	rf.lastLogTerm = args.Term

	current := rf.commitIndex
	if args.LeaderCommit > rf.lastLogIndex {
		rf.commitIndex = rf.lastLogIndex
	} else {
		rf.commitIndex = args.LeaderCommit
	}

	if current < rf.commitIndex {
		for i := current + 1; i <= rf.commitIndex; i++ {
			// fmt.Printf("me:%v, i:%v, apply command:%v\n", rf.me, i, rf.log[i].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
	}

	reply.Success = true
	reply.Term = rf.term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		rf.replies++
		defer rf.mu.Unlock()
	} else {
		return
	}

	if rf.role == Leader {
		if !reply.Success {
			if reply.Term > rf.term {
				rf.role = Follower
				rf.term = args.Term
				rf.voteFor = -1
			} else {
				if rf.nextIndex[server]-1 < 1 {
					rf.nextIndex[server] = 1
				} else {
					rf.nextIndex[server] -= 1
				}
			}
		} else {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			// fmt.Printf("server: %v, append success matchindex:%v\n", server, rf.matchIndex)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			// fmt.Printf("server: %v, append success nextIndex:%v\n", server, rf.nextIndex)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 幂等性
	if rf.role != Leader {
		return 0, 0, false
	}
	if command == rf.log[rf.lastLogIndex].Command {
		return rf.lastLogIndex, rf.lastLogTerm, true
	}
	rf.lastLogIndex++
	rf.matchIndex[rf.me] = rf.lastLogIndex
	term := rf.term
	rf.lastLogTerm = term
	rf.log = append(rf.log, &LogEntry{
		Term:    term,
		Command: command,
	})

	// fmt.Printf("leader me:%v, get index:%v,term:%v,command:%v\n", rf.me, rf.lastLogIndex, term, command)
	/**
	for _, l := range rf.log {
		fmt.Printf("log:%+v", l)
	}
	**/
	return rf.lastLogIndex, term, true
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
	ElectionTimeoutBase float32 = 500
	HeartbeatTimeout            = 50
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
			rf.leaderRoutine()
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
			// fmt.Printf("me:%v, counting %v!\n", rf.me, count-10)
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

func (rf *Raft) leaderRoutine() {
	go rf.sendHeartbeats()
	time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.replies+1 < len(rf.peers)-(len(rf.peers)-1)/2 {
		rf.active = true
		rf.role = Follower
		return
	}
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	rf.mu.Unlock()
	sort.Ints(tmp)
	rf.mu.Lock()
	current := rf.commitIndex
	if rf.commitIndex < tmp[len(tmp)/2] || rf.log[tmp[len(tmp)/2]].Term == rf.term {
		rf.commitIndex = tmp[len(tmp)/2]
	}
	// fmt.Printf("leaderRoutine matchindex:%v\n", rf.matchIndex)
	// fmt.Printf("leaderRoutine current:%v, commitindex:%v\n", current, rf.commitIndex)
	if current < rf.commitIndex {
		for i := current + 1; i <= rf.commitIndex; i++ {
			// fmt.Printf("leaderRoutine apply me:%v, i:%v, command:%v\n", rf.me, i, rf.log[i].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.replies = 0
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
			args.Entries = rf.log[rf.nextIndex[server] : rf.lastLogIndex+1]
		}

		// fmt.Printf("%v(role:%v) sending to %v! args:%+v\n", rf.me, rf.role, server, args)
		go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
	}
}

type sendAllRequestVoteState struct {
	mu      sync.Mutex
	replies []bool
	count   int
	quit    bool
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
		if state.quit {
			rf.role = Follower
			rf.active = true
			state.mu.Unlock()
			rf.mu.Unlock()
			return
		}

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
	// fmt.Printf("become leader! me:%v\n", rf.me)
	rf.active = true
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.lastLogIndex + 1
		} else {
			rf.nextIndex[i] = -1
		}
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
	rf.log = make([]*LogEntry, 1)
	rf.log[0] = &LogEntry{}
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
