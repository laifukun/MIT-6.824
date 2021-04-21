package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
import (
	"hash/fnv"

	"../shardmaster"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfig = "ErrWrongConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId   int64
	SequenceId int64
	GID        int
	Shard      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId   int64
	SequenceId int64

	GID   int
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardRequestArgs struct {
	Shard        int
	SourceGID    int
	TargetGID    int
	Config       shardmaster.Config
	RequestPhase int
}

type ShardRequestReply struct {
	Err         Err
	KVData      map[string]string
	ShardRecord map[string]string
}

func hash(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64())
}
