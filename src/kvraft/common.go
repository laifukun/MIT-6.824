package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId   int64
	SequenceId int64
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId   int64
	Key        string
	SequenceId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
/*
type SnapShot struct {
	LastIndex int
	LastTerm  int
	KeyValue  []byte
}
*/