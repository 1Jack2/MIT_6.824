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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type ServerState int

const (
	FOLLOWER ServerState = iota
	CANDIDATE
	LEADER
)

const (
	// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	// The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader
	ElectionTimeoutBase = 500 * time.Millisecond

	LeaderHeartbeatInterval = 100 * time.Millisecond
)

const initialTerm = 0
const nobody = -1

func getElectionTimeout() time.Duration {
	return ElectionTimeoutBase + time.Duration(rand.Int31n(6)*200)*time.Millisecond
}

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
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm   int
	votedFor      int
	votedPeers    []bool
	log           []LogEntry
	snapshotIndex int // last snapshot index(absolute)
	snapshotTerm  int
	snapshot      []byte

	// Volatile state on all servers:
	commitIndex         int
	lastApplied         int // last applied log index
	lastAppliedSnapshot int // last applied snapshot index(absolute)
	state               ServerState
	lastHeartbeat       time.Time
	applyCh             chan ApplyMsg

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == LEADER
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	Debug(dPersist, "S%d persist START, term=%d, votedFor=%d, sIdx=%d, sTerm=%d, len(log)=%d",
		rf.me, rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.snapshotTerm, len(rf.log))
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.votedPeers)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	Debug(dPersist, "S%d persist END, term=%d, votedFor=%d, sIdx=%d, sTerm=%d, len(log)=%d, len(data)=%d",
		rf.me, rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.snapshotTerm, len(rf.log), len(data))
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersistedState(data []byte) {
	Debug(dPersist, "S%d start reading persist, term=%d, votedFor=%d, sIdx=%d, sTerm=%d, log=%v", rf.me,
		rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.snapshotTerm, rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var votePeers []bool
	var logs []LogEntry
	var snapshotIndex, snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&votePeers) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		log.Fatalf("S%d readPersistedState error\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.votedPeers = votePeers
		rf.log = logs
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
	Debug(dPersist, "S%d end reading persist, term=%d, votedFor=%d, sIdx=%d, sTerm=%d, log=%v", rf.me,
		rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.snapshotTerm, rf.log)
}

// restore previously persisted snapshot.
func (rf *Raft) readPersistedSnapshot(snapshot []byte) {
	rf.snapshot = snapshot
}

// restore from previously persist.
func (rf *Raft) readPersist(persister *Persister) {
	rf.readPersistedState(persister.ReadRaftState())
	rf.readPersistedSnapshot(persister.ReadSnapshot())
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	relIndex := relLogIndex(rf.snapshotIndex, logIn(index))
	if snapshot == nil || len(snapshot) < 1 || relIndex < 0 {
		Debug(dSnap, "S%d illegal Snapshot size: %d index: %d rf.sidx: %d relIndex: %d", rf.me, len(snapshot), index, rf.snapshotIndex, relIndex)
		return
	}
	Debug(dSnap, "S%d before Snapshot index: %d snapshotIndex: %d snapshotTerm: %d nextIndex: %v commitIndex: %d lastApplied: %d lastAppliedSnapshot: %d logLen: %d",
		rf.me, index, rf.snapshotIndex, rf.snapshotTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied, rf.lastAppliedSnapshot, len(rf.log))

	for i := range rf.nextIndex {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = relLogIndex(logIn(index), absLogIndex(rf.snapshotIndex, rf.nextIndex[i]))
		rf.matchIndex[i] = relLogIndex(logIn(index), absLogIndex(rf.snapshotIndex, rf.matchIndex[i]))
	}
	rf.commitIndex = relLogIndex(logIn(index), absLogIndex(rf.snapshotIndex, rf.commitIndex))
	rf.lastApplied = relLogIndex(logIn(index), absLogIndex(rf.snapshotIndex, rf.lastApplied))
	rf.snapshotTerm = rf.log[relIndex].Term
	rf.snapshotIndex = logIn(index)
	rf.lastAppliedSnapshot = rf.snapshotIndex
	rf.log = rf.log[relIndex+1:]
	rf.snapshot = snapshot
	rf.persist()

	Debug(dSnap, "S%d after Snapshot index: %d snapshotIndex: %d snapshotTerm: %d nextIndex: %v commitIndex: %d lastApplied: %d lastAppliedSnapshot: %d logLen: %d",
		rf.me, index, rf.snapshotIndex, rf.snapshotTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied, rf.lastAppliedSnapshot, len(rf.log))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		Debug(dVote, "S%d <- S%d, deny: T%d > T%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		Debug(dTerm, "S%d <- S%d Term is higher, updating term(%d > %d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.becomeFollower(args.Term, nobody)
	}
	if rf.currentTerm != args.Term {
		log.Panicf("RequestVote assert fail: S%d <- S%d rf.currentTerm is %d, while should be %d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}

	if rf.state == FOLLOWER {
		if (rf.votedFor == nobody || rf.votedFor == args.CandidateId) && rf.candidateLogUpToDate(args) {
			rf.voteFor(args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			Debug(dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, args.Term)
		}
	}
}

func (rf *Raft) voteFor(candidateId int) {
	rf.votedFor = candidateId
	rf.persist()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) candidateLogUpToDate(args *RequestVoteArgs) bool {
	if rf.lastAbsLogTerm() < args.LastLogTerm {
		return true
	}
	if rf.lastAbsLogTerm() > args.LastLogTerm {
		return false
	}
	return rf.lastAbsLogIndex() <= args.LastLogIndex
}

func (rf *Raft) installSnapshotLogUptoDate(args *InstallSnapshotArgs) bool {
	if rf.snapshotTerm < args.LastIncludedTerm {
		return true
	}
	if rf.snapshotTerm > args.LastIncludedTerm {
		return false
	}
	return rf.snapshotIndex < args.LastIncludedIndex
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
	Term                     int
	Success                  bool
	FirstConflictingLogIndex int
	FirstConflictingLogTerm  int
	LogLen                   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.lastHeartbeat = time.Now()
	rf.becomeFollower(args.Term, args.LeaderId)

	// log is too short
	if relLogIndex(rf.snapshotIndex, args.PrevLogIndex) >= len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.LogLen = absLogIndex(rf.snapshotIndex, len(rf.log))
		return
	}
	// prevLogTerm matches
	if relLogIndex(rf.snapshotIndex, args.PrevLogIndex) < 0 ||
		rf.log[relLogIndex(rf.snapshotIndex, args.PrevLogIndex)].Term == args.PrevLogTerm {
		Debug(dLog, "S%d[T%d] <- S%d[T%d] Accept PLI: %d PLT: %d N: %d LC: %d rf.sidx: %d - %v", rf.me, rf.currentTerm,
			args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.snapshotIndex, args.Entries)
		reply.Term = rf.currentTerm
		reply.Success = true

		// find first conflicted entry
		conflicted := false
		logAppended := false
		i := relLogIndex(rf.snapshotIndex, args.PrevLogIndex)
		for _, entry := range args.Entries {
			i += 1
			// snapshot is not conflicted
			if i < 0 {
				continue
			} else if i >= len(rf.log) {
				// append new entry
				rf.log = append(rf.log, entry)
				logAppended = true
			} else if entry.Term != rf.log[i].Term {
				// replace conflicted entry
				rf.log[i] = entry
				conflicted = true
			}
		}
		// if conflicted, delete all entry following it
		if conflicted {
			rf.log = rf.log[:i+1]
		}
		if logAppended || conflicted {
			rf.persist()
		}

		if absLogIndex(rf.snapshotIndex, rf.commitIndex) < args.LeaderCommit {
			// commitIndex = min(leaderCommit, index of last new entry)
			rf.commitIndex = relLogIndex(rf.snapshotIndex, min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
			Debug(dLog, "S%d[T%d] <- S%d[T%d] Update commitIndex: %d, len(log)=%d", rf.me, rf.currentTerm,
				args.LeaderId, args.Term, rf.commitIndex, len(rf.log))
		}
		return
	}
	// conflicting
	// now we have 0 <= relLogIndex(rf.snapshotIndex, args.PrevLogIndex) < len(rf.log)
	Debug(dDrop, "S%d <- S%d Deny PLI: %d PLT: %d N: %d LC: %d rf.sidx: %d - %v", rf.me, args.LeaderId, args.PrevLogIndex,
		args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.snapshotIndex, args.Entries)
	firstConflictingLogTerm := rf.log[relLogIndex(rf.snapshotIndex, args.PrevLogIndex)].Term
	var i int
	for i = relLogIndex(rf.snapshotIndex, args.PrevLogIndex); i >= 0; i-- {
		if rf.log[i].Term != firstConflictingLogTerm {
			break
		}
	}
	reply.FirstConflictingLogIndex = absLogIndex(rf.snapshotIndex, i+1)
	reply.FirstConflictingLogTerm = firstConflictingLogTerm
	reply.LogLen = -1
	reply.Term = rf.currentTerm
	reply.Success = false
}

// not thread safe
func (rf *Raft) becomeFollower(term int, votedFor int) {
	rf.becomeFollowerWithoutPersist(term, votedFor)
	rf.persist()
}

func (rf *Raft) becomeFollowerWithoutPersist(term int, votedFor int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = votedFor
	rf.votedPeers = make([]bool, len(rf.peers))
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	rf.lastHeartbeat = time.Now()
	rf.becomeFollower(args.Term, args.LeaderId)

	if rf.installSnapshotLogUptoDate(args) {
		Debug(dLog2, "S%d <- S%d receive snapshot T: %d LIT: %d LII: %d size: %d",
			rf.me, args.LeaderId, args.Term, args.LastIncludedTerm, args.LastIncludedIndex, len(args.Snapshot))
		Debug(dSnap, "S%d before InstallSnapshot snapshotIndex: %d snapshotTerm: %d nextIndex: %v commitIndex: %d lastApplied: %d lastAppliedSnapshot: %d logLen: %d",
			rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied, rf.lastAppliedSnapshot, len(rf.log))
		relLogNewStartIndex := relLogIndex(rf.snapshotIndex, args.LastIncludedIndex) + 1
		if relLogNewStartIndex >= len(rf.log) {
			rf.log = make([]LogEntry, 0)
			rf.commitIndex = -1
			rf.lastApplied = -1
		} else {
			rf.log = rf.log[relLogNewStartIndex:]
		}
		rf.commitIndex = max(-1, relLogIndex(args.LastIncludedIndex, absLogIndex(rf.snapshotIndex, rf.commitIndex)))
		rf.lastApplied = max(-1, relLogIndex(args.LastIncludedIndex, absLogIndex(rf.snapshotIndex, rf.lastApplied)))
		rf.snapshotTerm = args.LastIncludedTerm
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshot = args.Snapshot
		rf.persist()
		Debug(dSnap, "S%d after InstallSnapshot snapshotIndex: %d snapshotTerm: %d nextIndex: %v commitIndex: %d lastApplied: %d lastAppliedSnapshot: %d logLen: %d",
			rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied, rf.lastAppliedSnapshot, len(rf.log))
	} else {
		Debug(dDrop, "S%d <- S%d drop snapshot T: %d LIT: %d LII: %d size: %d rf.snapTerm: %d, rf.snapIndex: %d",
			rf.me, args.LeaderId, args.Term, args.LastIncludedTerm, args.LastIncludedIndex, len(args.Snapshot), rf.snapshotTerm, rf.snapshotIndex)
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
// todo
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
	Debug(dVote, "S%d -> S%d requestVote T: %d, PLI: %d, PLT: %d",
		args.CandidateId, server, args.Term, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(dLog, "S%d -> S%d Sending AppendEntries T: %d PLI: %d PLT: %d N: %d LC: %d - %v", args.LeaderId, server, args.Term,
		args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dLog2, "S%d -> S%d Sending InstallSnapshot T: %d LIT: %d LII: %d size: %d", args.LeaderId, server, args.Term,
		args.LastIncludedTerm, args.LastIncludedIndex, len(args.Snapshot))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return index, term, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	Debug(dClient, "S%d apply Start: %v", rf.me, command)
	rf.persist()
	index = logOut(absLogIndex(rf.snapshotIndex, len(rf.log)-1))
	term = rf.currentTerm

	currentTerm := rf.currentTerm
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
	rf.broadcastLog(currentTerm)

	return index, term, isLeader
}

// invariant: enter without locking, and exit without locking
func (rf *Raft) broadcastLog(currentTerm int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, term int) {
			rf.mu.Lock()

			if rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			// if term doesn't change, state must be leader
			rf.assertIsLeader()

			var prevLogIndex, prevLogTerm int
			relPrevLogIndex := rf.nextIndex[server] - 1
			if relPrevLogIndex >= 0 {
				prevLogIndex = absLogIndex(rf.snapshotIndex, relPrevLogIndex)
				prevLogTerm = rf.log[relPrevLogIndex].Term
			} else {
				prevLogIndex = rf.snapshotIndex
				prevLogTerm = rf.snapshotTerm
			}

			// InstallSnapshot, ignore the case: no snapshot, no log
			if relLogIndex(rf.snapshotIndex, prevLogIndex) < 0 && len(rf.snapshot) > 0 && len(rf.log) > 0 {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotIndex,
					LastIncludedTerm:  rf.snapshotTerm,
					Snapshot:          cloneByte(rf.snapshot),
				}
				var reply InstallSnapshotReply

				rf.mu.Unlock()
				ok := rf.sendInstallSnapshot(server, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if !rf.checkAfterAppendEntriesOrInstallSnapshot(term, reply.Term) {
					return
				}
				if rf.snapshotIndex != args.LastIncludedIndex || rf.snapshotTerm != args.LastIncludedTerm {
					Debug(dLog2, "S%d snapshotIndex changed when waiting InstallSnapshot response: before: %d, after: %d",
						rf.me, args.LastIncludedIndex, rf.snapshotIndex)
					rf.mu.Unlock()
					return
				}
				// InstallSnapshot RPC already success
				if rf.nextIndex[server] > 0 {
					rf.mu.Unlock()
					return
				}

				rf.nextIndex[server] = 0
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				Debug(dLeader, "S%d <- S%d Reinit nextIndex and commitIndex after InstallSnapshot rf.snapShotIdx=%d",
					rf.me, server, rf.snapshotIndex)
				prevLogIndex = args.LastIncludedIndex
				prevLogTerm = args.LastIncludedTerm
				if relLogIndex(rf.snapshotIndex, prevLogIndex) != -1 {
					log.Fatalf("rf.snapshotIndx(%d) != prevLogIndex(%d)", rf.snapshotIndex, prevLogIndex)
				}
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      cloneLog(rf.log[relLogIndex(rf.snapshotIndex, prevLogIndex+1):]),
				LeaderCommit: absLogIndex(rf.snapshotIndex, rf.commitIndex),
			}
			var reply AppendEntriesReply

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			if !rf.checkAfterAppendEntriesOrInstallSnapshot(term, reply.Term) {
				return
			}
			if rf.nextIndex[server] != relLogIndex(rf.snapshotIndex, args.PrevLogIndex+1) {
				Debug(dLeader, "S%d <- S%d nextIndex when waiting AppendEntries response nextIndex: %d args's nextIndex: %d",
					rf.me, server, rf.nextIndex[server], absLogIndex(rf.snapshotIndex, args.PrevLogIndex+1))
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				rf.nextIndex[server] = relLogIndex(rf.snapshotIndex, args.PrevLogIndex+1+len(args.Entries))
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				Debug(dLeader, "S%d <- S%d Update after AppendEntries nextIdx(%d) and matchIdx(%d), rf.snapShotIdx=%d", rf.me, server, rf.nextIndex[server], rf.matchIndex[server], rf.snapshotIndex)

				// commit
				if rf.matchIndex[server] >= 0 &&
					rf.log[rf.matchIndex[server]].Term == rf.currentTerm && rf.matchIndex[server] > rf.commitIndex {
					cnt := 0
					for i, matchIndex := range rf.matchIndex {
						if rf.me == i || matchIndex >= rf.matchIndex[server] {
							cnt += 1
						}
					}
					if cnt >= rf.majorityNumber() {
						rf.commitIndex = rf.matchIndex[server]
					}
				}
			} else { // log inconsistency, retry
				// reply log len < arg log len
				nextIndex := rf.nextIndex[server] - 1
				// follower's log is too short
				if reply.LogLen >= 0 {
					nextIndex = relLogIndex(rf.snapshotIndex, reply.LogLen)
				} else {
					// leader has XTerm
					var j int
					for j = min(len(rf.log)-1, rf.nextIndex[server]); j >= 0; j-- {
						if rf.log[j].Term == reply.FirstConflictingLogTerm {
							nextIndex = j
							break
						}
					}
					// leader doesn't have XTerm
					if j < 0 {
						nextIndex = relLogIndex(rf.snapshotIndex, reply.FirstConflictingLogIndex)
					}
				}
				rf.nextIndex[server] = nextIndex
			}

			rf.mu.Unlock()
		}(i, currentTerm)
	}
}

func (rf *Raft) checkAfterAppendEntriesOrInstallSnapshot(beforeSendTerm, replyTerm int) bool {
	if rf.currentTerm < replyTerm {
		rf.becomeFollower(replyTerm, nobody)
		rf.mu.Unlock()
		return false
	}
	if rf.currentTerm != beforeSendTerm {
		rf.mu.Unlock()
		return false
	}
	rf.assertIsLeader()
	return true
}

func (rf *Raft) assertIsLeader() {
	if rf.state != LEADER {
		log.Fatalf("S%d should be leader at T%d, but is %v", rf.me, rf.currentTerm, rf.state)
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		//Debug(dTimer, "S%d ticker", rf.me)
		rf.applyLogAndSnapshot()
		if rf.state == LEADER && time.Now().Sub(rf.lastHeartbeat) > LeaderHeartbeatInterval {
			currentTerm := rf.currentTerm
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			rf.broadcastLog(currentTerm)
		} else if rf.state != LEADER && time.Now().Sub(rf.lastHeartbeat) > getElectionTimeout() {
			rf.startElection()
			Debug(dTimer, "S%d start Election at T%d", rf.me, rf.currentTerm)

			voteArgs := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: absLogIndex(rf.snapshotIndex, len(rf.log)-1),
				LastLogTerm:  rf.lastAbsLogTerm(),
			}
			voteReply := RequestVoteReply{}
			rf.mu.Unlock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
					ok := rf.sendRequestVote(server, &args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()

					if rf.currentTerm < reply.Term {
						rf.becomeFollower(reply.Term, nobody)
						rf.mu.Unlock()
						return
					}
					// next election
					if rf.currentTerm != reply.Term {
						rf.mu.Unlock()
						return
					}
					// already became leader
					if rf.state != CANDIDATE {
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted && rf.currentTerm == args.Term {
						Debug(dVote, "S%d <- S%d Got vote at T%d", args.CandidateId, server, args.Term)
						rf.getVoteFrom(server)
						// become leader
						if rf.voteCount() >= rf.majorityNumber() {
							Debug(dLeader, "S%d Achieved Majority for T%d (%d), converting to Leader", rf.me, rf.currentTerm, rf.voteCount())
							rf.state = LEADER
							rf.lastHeartbeat = time.Now()
							rf.initNextIndexAndMatchIndex()
							rf.mu.Unlock()
							rf.broadcastLog(args.Term)
							return
						}
					}

					rf.mu.Unlock()
				}(i, voteArgs, voteReply)
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// NOT threadsafe
func (rf *Raft) applyLogAndSnapshot() {
	// don't send stale snapshot, because tests don't check this, see config.go:ingestSnap()
	// must send when snapshotIndex=lastApplied, because we keep rf.lastApplied >= -1 for simplicity
	if rf.snapshotIndex > rf.lastAppliedSnapshot && rf.snapshotIndex >= absLogIndex(rf.snapshotIndex, rf.lastApplied) {
		Debug(dClient, "S%d apply snapshot SI: %d, ST: %d, size: %d", rf.me, rf.snapshotIndex, rf.snapshotTerm, len(rf.snapshot))
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: logOut(rf.snapshotIndex),
		}
		rf.lastAppliedSnapshot = rf.snapshotIndex
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
	for !(rf.snapshotIndex > rf.lastAppliedSnapshot && rf.snapshotIndex >= absLogIndex(rf.snapshotIndex, rf.lastApplied)) &&
		rf.commitIndex > rf.lastApplied {
		Debug(dClient, "S%d applying rf.lastApplied: %d rf.commitIndex: %d logLen: %d, rf.sIdx: %d",
			rf.me, rf.lastApplied, rf.commitIndex, len(rf.log), rf.snapshotIndex)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied+1].Command,
			CommandTerm:  rf.log[rf.lastApplied+1].Term,
			CommandIndex: logOut(absLogIndex(rf.snapshotIndex, rf.lastApplied+1)),
		}
		Debug(dClient, "S%d applied log[%d]: %v applyMsg: %v", rf.me, rf.lastApplied+1, rf.log[rf.lastApplied+1], applyMsg)
		rf.lastApplied += 1
		// must release lock, because receiver may call rf.Snapshot, which cause deadlock
		rf.mu.Unlock()
		// NOT threadsafe: recheck if need sending snapshot
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
}

// not thread safe
func (rf *Raft) startElection() {
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.lastHeartbeat = time.Now()
	rf.votedFor = rf.me
	rf.votedPeers = make([]bool, len(rf.peers))
	rf.votedPeers[rf.me] = true

	rf.persist()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollowerWithoutPersist(initialTerm, nobody)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.snapshotIndex = -1
	rf.lastAppliedSnapshot = -1
	rf.snapshotTerm = initialTerm
	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister)
	rf.initNextIndexAndMatchIndex()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// reinitialized after election
func (rf *Raft) initNextIndexAndMatchIndex() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) majorityNumber() int {
	return (len(rf.peers) + 1) / 2
}

func (rf *Raft) getVoteFrom(server int) {
	rf.votedPeers[server] = true
	rf.persist()
}

func (rf *Raft) voteCount() int {
	cnt := 0
	for _, granted := range rf.votedPeers {
		if granted {
			cnt += 1
		}
	}
	return cnt
}

func (rf *Raft) lastAbsLogIndex() int {
	return rf.snapshotIndex + len(rf.log)
}

func (rf *Raft) lastAbsLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func absLogIndex(snapshotLogIndex, relLogIndex int) int {
	return snapshotLogIndex + 1 + relLogIndex
}

func relLogIndex(snapshotLogIndex, absLogIndex int) int {
	return absLogIndex - snapshotLogIndex - 1
}

// service layer <-- raft layer
func logOut(raftAbsLogIndex int) int {
	return raftAbsLogIndex + 1
}

// service layer --> raft layer
func logIn(serviceLayerLogIndex int) int {
	return serviceLayerLogIndex - 1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func cloneLog(orig []LogEntry) []LogEntry {
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
}

func cloneByte(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}
