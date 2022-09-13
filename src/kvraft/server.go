package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state       map[string]string           // kv table
	ClerkResult map[string]map[int64]string // Clerk RPC result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:      args.Key,
		OpType:   GET,
		ClerkId:  args.ClerkId,
		ClerkSeq: args.ClerkSeq,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	before := time.Now()
	for time.Now().Sub(before) < CommitTimeOut {
		kv.mu.Lock()
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
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	before := time.Now()
	for time.Now().Sub(before) < CommitTimeOut {
		kv.mu.Lock()
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
	kv.ClerkResult = make(map[string]map[int64]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ticker()

	return kv
}

func (kv *KVServer) ticker() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			DPrintf("KV%d before apply op: %v value: %v", kv.me, op, kv.get(op.Key))
			switch op.OpType {
			case PUT:
				kv.put(op.Key, op.Value)
			case APPEND:
				kv.append(op.Key, op.Value)
			}
			kv.putClerkReq(op.ClerkId, op.ClerkSeq, kv.get(op.Key))
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

func (kv *KVServer) getClerkReq(clerkId string, seq int64) (string, bool) {
	m, value := kv.ClerkResult[clerkId]
	if !value {
		return "", value
	}
	op, value := m[seq]
	return op, value
}

// todo: clean ClerkReq
func (kv *KVServer) putClerkReq(clerkId string, seq int64, value string) {
	m, ok := kv.ClerkResult[clerkId]
	if !ok {
		kv.ClerkResult[clerkId] = make(map[int64]string)
		m = kv.ClerkResult[clerkId]
	}
	m[seq] = value
}
