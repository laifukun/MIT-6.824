package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	mu         sync.Mutex
	servers    []*labrpc.ClientEnd
	leaderId   int
	clientId   int64
	sequenceId int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.sequenceId = 0
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceId++
	args := GetArgs{Key: key, ClientId: ck.clientId, SequenceId: ck.sequenceId}

	//DPrintf("Client %d get value of %s", ck.clientId, key)
	for {
		//DPrintf("Client %d get to Server %d:", ck.clientId, ck.leaderId)
		var reply GetReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		} 
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// You will have to modify this function.
	ck.sequenceId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SequenceId: ck.sequenceId}
	//DPrintf("Client input Put Append, %s, %s", key, value)
	//DPrintf("Client %s, %s, %s, leaderId: %d", op, args.Key, args.Value, ck.leaderId)

	for {
		//DPrintf("Client %d put/append to Server %d:", ck.clientId, ck.leaderId)
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		//DPrintf("OK? %v", ok)
		if ok && reply.Err == OK  {
			return
		}
		if !ok || (reply.Err == ErrWrongLeader) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)		
			//DPrintf("Try next server %d", ck.leaderId)	
		} 
		time.Sleep(10 * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
