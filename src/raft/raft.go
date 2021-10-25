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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
	Log       []*LogEntry
	Offset    int
	applyMu   sync.Mutex

	role    Role
	active  bool
	Term    int
	VoteFor int

	LastLogIndex int
	LastLogTerm  int

	commitIndex int
	LastApplied int
	applyCh     chan ApplyMsg

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Term:%d,Command:%d}", l.Term, l.Command)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.Term
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
func (rf *Raft) persistL() {
	rf.persister.SaveRaftState(rf.encodeRaftStateL())
}

func (rf *Raft) encodeRaftStateL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Term)
	e.Encode(rf.LastApplied)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	e.Encode(rf.Offset)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Log []*LogEntry
	var VoteFor int
	var Term int
	var LastApplied int
	var LastLogTerm int
	var LastLogIndex int
	var Offset int
	if d.Decode(&Log) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Term) != nil || d.Decode(&LastApplied) != nil ||
		d.Decode(&LastLogIndex) != nil || d.Decode(&LastLogTerm) != nil || d.Decode(&Offset) != nil {
		log.Fatalf("readPersist Decode fucked!\n")
	} else {
		rf.Log = Log
		rf.VoteFor = VoteFor
		rf.Term = Term
		rf.LastApplied = LastApplied
		rf.LastLogIndex = LastLogIndex
		rf.LastLogTerm = LastLogTerm
		rf.Offset = Offset
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	if rf != nil && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%v | CondInstallSnapshot | lastIncludedTerm:%v, lastIncludedIndex:%v\n", rf.me, lastIncludedTerm, lastIncludedIndex)
		if rf.Log[0].Term <= lastIncludedTerm && rf.Offset < lastIncludedIndex && rf.LastLogIndex <= lastIncludedIndex {
			log.Printf("%v | CondInstallSnapshot | discard all log\n", rf.me)
			rf.Log = []*LogEntry{{Term: lastIncludedTerm, Command: lastIncludedIndex}}
			rf.Offset = lastIncludedIndex
			rf.LastLogIndex = lastIncludedIndex
			rf.LastLogTerm = lastIncludedTerm
			rf.persister.SaveStateAndSnapshot(rf.encodeRaftStateL(), snapshot)
			return true
		} else if rf.Log[0].Term <= lastIncludedTerm && rf.Offset < lastIncludedIndex && rf.LastLogIndex > lastIncludedIndex {
			newLog := []*LogEntry{}
			newLog = append(newLog, &LogEntry{Term: lastIncludedTerm, Command: lastIncludedIndex})
			if rf.Log[lastIncludedIndex-rf.Offset].Term == lastIncludedTerm {
				newLog = append(newLog, rf.Log[lastIncludedIndex-rf.Offset:rf.LastLogIndex-rf.Offset+1]...)
			} else {
				rf.LastLogIndex = lastIncludedIndex
				rf.LastLogTerm = lastIncludedTerm
			}
			rf.Log = newLog
			rf.Offset = lastIncludedIndex
			log.Printf("%v | CondInstallSnapshot | maybe preserve some log:%+v\n", rf.me, rf.Log)
			rf.persister.SaveStateAndSnapshot(rf.encodeRaftStateL(), snapshot)
			return true
		} else {
			log.Printf("%v | CondInstallSnapshot | cannot install snapshot, lastIncludedTerm:%v, lastIncludedIndex:%v, snapshotTerm:%v, lastLogIndex:%v, offset:%v, rf.Log[0].Command:%v\n",
				rf.me, lastIncludedTerm, lastIncludedIndex, rf.Log[0].Term, rf.LastLogIndex, rf.Offset, rf.Log[0].Command)
			return false
		}
	} else {
		return false
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf != nil && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		newLog := []*LogEntry{{Term: rf.Log[index-rf.Offset].Term, Command: index}}
		if len(rf.Log) > index-rf.Offset+1 {
			newLog = append(newLog, rf.Log[index-rf.Offset+1:len(rf.Log)]...)
		}

		rf.Log = newLog
		log.Printf("%v | Snapshot | new log:%+v\n", rf.me, rf.Log)
		rf.Offset = index
		rf.persister.SaveStateAndSnapshot(rf.encodeRaftStateL(), snapshot)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	VoterId     int
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoterId = rf.me
	if rf.Term < args.Term {
		rf.role = Follower
		rf.Term = args.Term
		rf.VoteFor = -1
		rf.persistL()
	}
	if rf.Term <= args.Term {
		if (rf.Term == args.Term && rf.VoteFor != -1 && rf.VoteFor != args.CandidateId) || rf.LastLogTerm > args.LastLogTerm ||
			(rf.LastLogTerm == args.LastLogTerm && rf.LastLogIndex > args.LastLogIndex) {
			log.Printf("%v | RequestVote | %v got the same term but not at least up-to-date\n", rf.me, args.CandidateId)
			reply.Term = rf.Term
			reply.VoteGranted = false
		} else {
			log.Printf("%v | RequestVote | vote for %v which is at least up-to-date\n", rf.me, args.CandidateId)
			rf.active = true
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateId
			rf.persistL()
		}
	} else {
		log.Printf("%v | RequestVote | deny %v which got lower term\n", rf.me, args.CandidateId)
		reply.Term = rf.Term
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		state.mu.Lock()
		defer state.mu.Unlock()
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term == rf.Term {
			if rf.Term < reply.Term || !reply.VoteGranted {
				log.Printf("%v | sendRequestVote | fail to become leader\n", rf.me)
				rf.role = Follower
				rf.active = true
				return
			} else {
				if rf.role == Candidate {
					log.Printf("%v | sendRequestVote | %v vote for me\n", rf.me, reply.VoterId)
					state.count++
					if float32(state.count) > float32(len(rf.peers))/2 {
						log.Printf("%v | sendRequestVote | become leader\n", rf.me)
						rf.leaderInitL()
						return
					}
				}
			}
		}
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
	Term         int
	Success      bool
	ConflitTerm  int
	ConflitIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v | AppendEntries | get AppendEntries request %+v\n", rf.me, args)
	if args.Term > rf.Term {
		log.Printf("%v | AppendEntries | term lag behind\n", rf.me)
		rf.active = true
		reply.Success = false
		if rf.LastLogIndex > rf.Offset {
			reply.ConflitIndex = rf.LastLogIndex
		} else {
			reply.ConflitIndex = rf.Offset + 1
		}
		reply.Term = rf.Term
		rf.role = Follower
		rf.Term = args.Term
		rf.VoteFor = args.LeaderId
		rf.persistL()
		return
	}
	if args.Term < rf.Term {
		log.Printf("%v | AppendEntries | deny AppendEntries request from lower term\n", rf.me)
		reply.Success = false
		reply.Term = rf.Term
		return
	}

	// same term
	if rf.VoteFor != args.LeaderId {
		log.Printf("%v | AppendEntries | deny AppendEntries request from unknown leader\n", rf.me)
		reply.Success = false
		reply.Term = rf.Term
		if rf.LastLogIndex > rf.Offset {
			reply.ConflitIndex = rf.LastLogIndex
		} else {
			reply.ConflitIndex = rf.Offset + 1
		}
		return
	}

	if rf.LastLogIndex < args.PrevLogIndex {
		log.Printf("%v | AppendEntries | mismatch, LastLogIndex:%v, PrevLogIndex:%v\n", rf.me, rf.LastLogIndex, args.PrevLogIndex)
		rf.active = true
		reply.Success = false
		if rf.LastLogIndex > rf.Offset {
			reply.ConflitIndex = rf.LastLogIndex
		} else {
			reply.ConflitIndex = rf.Offset + 1
		}
		reply.Term = rf.Term
		return
	}

	if args.PrevLogIndex-rf.Offset > 0 && rf.Log[args.PrevLogIndex-rf.Offset].Term != args.PrevLogTerm {
		rf.active = true
		reply.Success = false
		reply.ConflitTerm = rf.Log[args.PrevLogIndex-rf.Offset].Term
		for i, t := range rf.Log {
			if t.Term >= reply.ConflitTerm {
				reply.ConflitIndex = i - 1 + rf.Offset
				if reply.ConflitIndex < 1+rf.Offset {
					reply.ConflitIndex = 1 + rf.Offset
				}
				break
			}
		}
		reply.Term = rf.Term
		log.Printf("%v | AppendEntries | mismatch, conflict term:%v, conflict index:%v\n", rf.me, reply.Term, reply.ConflitIndex)
		return
	}

	rewrites := rf.LastLogIndex - args.PrevLogIndex

	index := 0
	for ; index < rewrites && index < len(args.Entries); index++ {
		log.Printf("%v | AppendEntries | rewrite rf.log[%v] from %v to %v\n", rf.me, args.PrevLogIndex+1+index-rf.Offset, rf.Log[args.PrevLogIndex+1+index-rf.Offset], args.Entries[index])
		rf.Log[args.PrevLogIndex+1+index-rf.Offset] = args.Entries[index]
	}
	if index != 0 {
		rf.LastLogIndex = args.PrevLogIndex + index
	}

	if index < len(args.Entries) {
		for j := rf.LastLogIndex + 1 - rf.Offset; j < len(rf.Log) && index < len(args.Entries); index, j = index+1, j+1 {
			log.Printf("%v | AppendEntries | rewrite stale rf.log[%v] from %v to %v\n", rf.me, j, rf.Log[j], args.Entries[index])
			rf.Log[j] = args.Entries[index]
			rf.LastLogIndex++
		}
		log.Printf("%v | AppendEntries | append rf.log, entries:%v\n", rf.me, args.Entries[index:len(args.Entries)])
		rf.Log = append(rf.Log, args.Entries[index:len(args.Entries)]...)
		rf.LastLogIndex += len(args.Entries) - index
	}
	log.Printf("%v | AppendEntries | current logs:%+v, lastLogIndex:%v\n", rf.me, rf.Log, rf.LastLogIndex)
	rf.active = true
	rf.LastLogTerm = rf.Log[rf.LastLogIndex-rf.Offset].Term
	rf.persistL()

	if args.LeaderCommit > rf.LastLogIndex {
		log.Printf("%v | AppendEntries | commit index update from %v to lastLogIndex %v\n", rf.me, rf.commitIndex, rf.LastLogIndex)
		rf.commitIndex = rf.LastLogIndex
	} else {
		log.Printf("%v | AppendEntries | commit index update from %v to LeaderCommit %v\n", rf.me, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
	}
	reply.Success = true
	reply.Term = rf.Term
	// avoid state change in unreliable situation
	rf.applyL()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, replies *int) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		*replies++
		defer rf.mu.Unlock()
	} else {
		return
	}

	if reply.ConflitIndex > 0 && reply.ConflitIndex < rf.Offset {
		rf.sendInstallSnapshotL(server, &InstallSnapshotArgs{}, &InstallSnapshotReply{})
		return
	}

	if rf.role == Leader && args.Term == rf.Term {
		if !reply.Success {
			if reply.Term > rf.Term {
				log.Printf("%v | sendAppendEntries | send AppendEntries to server %v with larger term\n", rf.me, server)
				rf.role = Follower
				rf.Term = args.Term
				rf.VoteFor = -1
				rf.persistL()
				return
			} else if reply.ConflitTerm > 0 {
				log.Printf("%v | sendAppendEntries | send AppendEntries to server %v mismatch, ConflitTerm is %v, ConflitIndex is %v\n", rf.me, server, reply.ConflitTerm, reply.ConflitIndex)
				if rf.nextIndex[server] > reply.ConflitIndex {
					rf.nextIndex[server] = reply.ConflitIndex
				} else {
					for i, t := range rf.Log {
						if t.Term >= reply.ConflitTerm {
							if rf.nextIndex[server] > i-1+rf.Offset {
								rf.nextIndex[server] = i - 1 + rf.Offset
							} else {
								rf.nextIndex[server] -= 1
							}
							break
						}
					}
				}
			} else if rf.nextIndex[server] > reply.ConflitIndex {
				log.Printf("%v | sendAppendEntries | send AppendEntries to server %v mismatch, ConflitIndex is %v\n", rf.me, server, reply.ConflitIndex)
				if rf.nextIndex[server] > reply.ConflitIndex {
					rf.nextIndex[server] = reply.ConflitIndex
				} else {
					rf.nextIndex[server] -= 1
				}
			} else {
				rf.nextIndex[server] -= 1
			}
			if rf.nextIndex[server] <= rf.Offset {
				log.Printf("%v | sendAppendEntries | nextIndex lower than Offset+1, %v needs to install snapshot\n", rf.me, server)
				rf.sendInstallSnapshotL(server, &InstallSnapshotArgs{}, &InstallSnapshotReply{})
				rf.nextIndex[server] = rf.Offset + 1
			}
			log.Printf("%v | sendAppendEntries | send AppendEntries to server %v mismatch, now nextIndex is %v\n", rf.me, server, rf.nextIndex[server])
		} else {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			log.Printf("%v | sendAppendEntries | matchIndex of server %v is now %v\n", rf.me, server, rf.matchIndex[server])
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			log.Printf("%v | sendAppendEntries | nextIndex of server %v is now %v\n", rf.me, server, rf.nextIndex[server])
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

	if rf.role != Leader {
		return 0, 0, false
	}

	// idempotence
	if command == rf.Log[rf.LastLogIndex-rf.Offset].Command && rf.Term == rf.LastLogTerm {
		return rf.LastLogIndex, rf.LastLogTerm, true
	}

	rf.LastLogIndex++
	rf.matchIndex[rf.me] = rf.LastLogIndex
	term := rf.Term
	rf.LastLogTerm = term
	newEntry := &LogEntry{
		Term:    term,
		Command: command,
	}
	if rf.LastLogIndex < len(rf.Log) {
		rf.Log[rf.LastLogIndex-rf.Offset] = newEntry
	} else {
		rf.Log = append(rf.Log, newEntry)
	}
	rf.persistL()
	log.Printf("%v | Start | get new command index:%v, term:%v, command:%v\n", rf.me, rf.LastLogIndex, term, command)
	log.Printf("%v | Start | current log:%+v\n", rf.me, rf.Log)
	return rf.LastLogIndex, term, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

var (
	ElectionTimeoutBase float32 = 150
	ElectionTimeoutRand float32 = 150
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
			log.Printf("%v | ticker | go leaderRoutine\n", rf.me)
			rf.mu.Unlock()
			rf.leaderRoutine()
			continue
		}
		rf.mu.Unlock()
		count := ElectionTimeoutBase + ElectionTimeoutRand*rand.Float32()
		for count > 0 {
			rf.mu.Lock()
			if rf.active {
				log.Printf("%v | ticker | reset ticker\n", rf.me)
				rf.active = false
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			count -= 1
			time.Sleep(1 * time.Millisecond)
		}
		if count <= 0 {
			rf.mu.Lock()
			log.Printf("%v | ticker | ticker timeout\n", rf.me)
			rf.role = Candidate
			rf.Term++
			rf.VoteFor = rf.me
			rf.persistL()
			rf.mu.Unlock()
			rf.sendAllRequestVote()
		}
	}
}

func (rf *Raft) leaderRoutine() {
	repliesValue := 1
	replies := &repliesValue
	go rf.sendHeartbeats(replies)
	time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if *replies+1 < len(rf.peers)-(len(rf.peers)-1)/2 {
		log.Printf("%v | leaderRoutine | only get %v replies\n", rf.me, *replies)
		return
	}
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	if rf.commitIndex < tmp[len(tmp)/2] && (tmp[len(tmp)/2]-rf.Offset <= 0 || rf.Log[tmp[len(tmp)/2]-rf.Offset].Term == rf.Term) {
		log.Printf("%v | leaderRoutine | commit index update from %v to %v\n", rf.me, rf.commitIndex, tmp[len(tmp)/2])
		rf.commitIndex = tmp[len(tmp)/2]
	}
	rf.applyL()
}

func (rf *Raft) applyL() {
	if rf.LastApplied < rf.commitIndex {
		command := []interface{}{}
		applyCh := rf.applyCh
		start := rf.LastApplied + 1
		if rf.LastApplied+1 <= rf.Offset {
			start = rf.Offset + 1
		}
		for i := start - rf.Offset; i <= rf.commitIndex-rf.Offset; i++ {
			log.Printf("%v | apply | apply command of index %v, command %v\n", rf.me, i+rf.Offset, rf.Log[i].Command)
			command = append(command, rf.Log[i].Command)
		}
		rf.LastApplied = rf.commitIndex
		rf.persistL()
		// cannot lock when apply
		rf.mu.Unlock()
		rf.applyMu.Lock()
		for _, c := range command {
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      c,
				CommandIndex: start,
			}
			start++
		}
		rf.applyMu.Unlock()
		rf.mu.Lock()
	}
}

func (rf *Raft) sendHeartbeats(replies *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me || rf.nextIndex[server]-1-rf.Offset < 0 {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.Term,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.Log[rf.nextIndex[server]-1-rf.Offset].Term,
		}
		if rf.LastLogIndex >= rf.nextIndex[server] {
			args.Entries = rf.Log[rf.nextIndex[server]-rf.Offset : rf.LastLogIndex+1-rf.Offset]
		}

		go rf.sendAppendEntries(server, args, &AppendEntriesReply{}, replies)
	}
}

type sendAllRequestVoteState struct {
	mu    sync.Mutex
	count int
}

func (rf *Raft) sendAllRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := &sendAllRequestVoteState{
		count: 1,
	}

	args := &RequestVoteArgs{
		Term:         rf.Term,
		CandidateId:  rf.me,
		LastLogIndex: rf.LastLogIndex,
		LastLogTerm:  rf.LastLogTerm,
	}
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go rf.sendRequestVote(server, args, &RequestVoteReply{}, state)
	}
}

func (rf *Raft) leaderInitL() {
	rf.active = true
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.LastLogIndex + 1
		} else {
			rf.nextIndex[i] = -1
		}
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.Offset
	}

	/** no-op
	if rf.LastLogIndex != 0 {
		rf.LastLogIndex++
		rf.matchIndex[rf.me] = rf.LastLogIndex
		rf.LastLogTerm = rf.Term
		newEntry := &LogEntry{
			Term:    rf.Term,
			Command: 0,
		}
		if rf.LastLogIndex-rf.Offset < len(rf.Log) {
			rf.Log[rf.LastLogIndex-rf.Offset] = newEntry
		} else {
			rf.Log = append(rf.Log, newEntry)
		}
		rf.persistL()
	}
	**/
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
	log.SetFlags(log.Lmicroseconds)
	// log.SetOutput(ioutil.Discard)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.VoteFor = -1
	rf.role = Follower
	rf.Log = make([]*LogEntry, 1)
	rf.Log[0] = &LogEntry{Command: 0}
	rf.applyCh = applyCh
	rf.Offset = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Printf("%v | Make | resume persist state, log:%+v, term:%v, voteFor:%v, lastApplied:%v, lastLogIndex:%v, lastLogTerm:%v\n",
		rf.me, rf.Log, rf.Term, rf.VoteFor, rf.LastApplied, rf.LastLogIndex, rf.LastLogTerm)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v | InstallSnapshot | args:%+v\n", rf.me, args)
	if rf.LastApplied > args.LastIncludedIndex || args.Term < rf.Term {
		log.Printf("%v | InstallSnapshot | can't not install, LastApplied:%v, rf.Term:%v\n", rf.me, rf.LastApplied, rf.Term)
		return
	}
	rf.applyMu.Lock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyMu.Unlock()
}

func (rf *Raft) sendInstallSnapshotL(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	args.Term = rf.Term
	if rf.Offset != 0 {
		args.LastIncludedIndex = rf.Offset
	}
	args.LastIncludedTerm = rf.Log[0].Term
	args.Snapshot = rf.persister.ReadSnapshot()
	log.Printf("%v | sendInstallSnapshotL | send snapshot to %v, args:%+v\n", rf.me, server, args)
	rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}
