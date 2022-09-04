package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id  string
	seq atomic.Int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = randomString(10)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	seq := ck.nextSeq()
	// You will have to modify this function.
	for i := range ck.servers {
		args := GetArgs{
			Key:      key,
			ClerkId:  ck.id,
			ClerkSeq: seq,
		}
		reply := GetReply{}
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
		default:
			log.Fatalf("Clerk-%v Get args:%v reply: %v", ck.id, args, reply)
		}
	}
	return ""
}

func (ck *Clerk) nextSeq() int64 {
	return ck.seq.Add(1)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := ck.nextSeq()
	for {
		for i := range ck.servers {
			args := PutAppendArgs{
				Key:      key,
				Value:    value,
				Op:       op,
				ClerkId:  ck.id,
				ClerkSeq: seq,
			}
			reply := PutAppendReply{}
			ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
			case ErrNoKey:
				log.Fatalf("Clerk-%v PutAppend args:%v reply: %v", ck.id, args, reply)
			default:
				log.Fatalf("Clerk-%v PutAppend args:%v reply: %v", ck.id, args, reply)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}
