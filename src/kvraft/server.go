package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	CommitTimeOut  = 500 * time.Millisecond
	TickerInterval = 5 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   string
	ClerkId  string
	ClerkSeq int64
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state              map[string]string           // kv table
	clerkResult        map[string]map[int64]string // Clerk RPC result
	lastCommitLogTerm  int
	lastCommitLogIndex int
	lastSnapshotTerm   int
	lastSnapshotIndex  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:      args.Key,
		OpType:   GET,
		ClerkId:  args.ClerkId,
		ClerkSeq: args.ClerkSeq,
	}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	before := time.Now()
	for time.Now().Sub(before) < CommitTimeOut {
		kv.mu.Lock()
		if kv.leaderChanged(term) {
			kv.mu.Unlock()
			reply.Err = ErrWrongLeader
			return
		}
		value, ok := kv.getClerkReq(op.ClerkId, op.ClerkSeq)
		if ok {
			reply.Err = OK
			reply.Value = value
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
		time.Sleep(TickerInterval)
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !(args.Op == PUT || args.Op == APPEND) {
		log.Fatalf("failure: KV PutAppend wrong args: %v", args)
	}
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		ClerkId:  args.ClerkId,
		ClerkSeq: args.ClerkSeq,
	}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	before := time.Now()
	for time.Now().Sub(before) < CommitTimeOut {
		kv.mu.Lock()
		if kv.leaderChanged(term) {
			kv.mu.Unlock()
			reply.Err = ErrWrongLeader
			return
		}
		_, ok := kv.getClerkReq(op.ClerkId, op.ClerkSeq)
		if ok {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
		time.Sleep(TickerInterval)
	}
	reply.Err = ErrWrongLeader
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.clerkResult = make(map[string]map[int64]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.ticker()

	return kv
}

func (kv *KVServer) applier() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid == false {
			if !applyMsg.SnapshotValid {
				continue
			}
			kv.mu.Lock()
			if kv.isStaleSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex) {
				kv.mu.Unlock()
				continue
			}
			kv.readSnapshot(applyMsg.Snapshot)
			kv.mu.Unlock()
		} else {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			_, ok := kv.getClerkReq(op.ClerkId, op.ClerkSeq)
			if ok {
				DPrintf("KV%d ignore duplicate op: %v value: %v", kv.me, op, kv.get(op.Key))
				kv.mu.Unlock()
				continue
			}
			//DPrintf("KV%d before apply op: %v value: %v", kv.me, op, kv.get(op.Key))
			switch op.OpType {
			case PUT:
				kv.put(op.Key, op.Value)
			case APPEND:
				kv.append(op.Key, op.Value)
			}
			kv.putClerkReq(op.ClerkId, op.ClerkSeq, kv.get(op.Key))
			kv.lastCommitLogTerm = applyMsg.CommandTerm
			kv.lastCommitLogIndex = applyMsg.CommandIndex
			DPrintf("KV%d after apply op: %v value: %v", kv.me, op, kv.get(op.Key))
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) get(key string) string {
	return kv.state[key]
}

func (kv *KVServer) put(key, value string) {
	kv.state[key] = value
}

func (kv *KVServer) append(key, value string) {
	DPrintf("KV%d apply key: %v value: %v", kv.me, key, value)
	kv.put(key, kv.get(key)+value)
}

func (kv *KVServer) getClerkReq(clerkId string, seq int64) (value string, ok bool) {
	m, ok := kv.clerkResult[clerkId]
	if !ok {
		return "", ok
	}
	op, ok := m[seq]
	return op, ok
}

// todo: clean ClerkReq
func (kv *KVServer) putClerkReq(clerkId string, seq int64, value string) {
	m, ok := kv.clerkResult[clerkId]
	if !ok {
		kv.clerkResult[clerkId] = make(map[int64]string)
		m = kv.clerkResult[clerkId]
	}
	m[seq] = value
}

func (kv *KVServer) leaderChanged(term int) bool {
	return kv.lastCommitLogTerm > term
}

// change state
// return snapshot data
func (kv *KVServer) snapshot() (index int, snapshot []byte) {

	kv.lastSnapshotIndex = kv.lastCommitLogIndex
	kv.lastSnapshotTerm = kv.lastCommitLogTerm

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.clerkResult)
	e.Encode(kv.lastCommitLogTerm)
	e.Encode(kv.lastCommitLogIndex)
	e.Encode(kv.lastSnapshotTerm)
	e.Encode(kv.lastSnapshotIndex)
	return kv.lastSnapshotIndex, w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var state map[string]string
	var clerkResult map[string]map[int64]string
	var lastCommitLogTerm, lastCommitLogIndex int
	var lastSnapshotTerm, lastSnapshotIndex int
	if d.Decode(&state) != nil ||
		d.Decode(&clerkResult) != nil ||
		d.Decode(&lastCommitLogTerm) != nil ||
		d.Decode(&lastCommitLogIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&lastSnapshotIndex) != nil {
		log.Fatalf("KV%d readSnapshot error\n", kv.me)
	} else {
		kv.state = state
		kv.clerkResult = clerkResult
		kv.lastCommitLogTerm = lastCommitLogTerm
		kv.lastCommitLogIndex = lastCommitLogIndex
		kv.lastSnapshotTerm = lastSnapshotTerm
		kv.lastSnapshotIndex = lastSnapshotIndex
	}
}

func (kv *KVServer) ticker() {
	for {
		kv.mu.Lock()
		if kv.maxraftstate > -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			index, snapshot := kv.snapshot()
			kv.mu.Unlock()
			DPrintf("KV%d snapshot: lastSnapshotIndex: %d", kv.me, index)
			kv.rf.Snapshot(index, snapshot)
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(TickerInterval)
	}
}

func (kv *KVServer) isStaleSnapshot(lastSnapshotTerm int, lastSnapshotIndex int) bool {
	if kv.lastSnapshotTerm > lastSnapshotTerm {
		return true
	}
	if kv.lastSnapshotTerm < lastSnapshotTerm {
		return false
	}

	return kv.lastSnapshotIndex >= lastSnapshotIndex
}
