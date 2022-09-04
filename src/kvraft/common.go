package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClerkId  string
	ClerkSeq int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClerkId  string
	ClerkSeq int64
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)
