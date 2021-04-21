package kvraft

import (
	"sync/atomic"
	"crypto/rand"
	"math/big"
	//"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	//mu         sync.Mutex
	servers    []*labrpc.ClientEnd
	leaderId   int32
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

	//time1 := time.Now()
	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	args := GetArgs{Key: key, ClientId: ck.clientId, SequenceId: seqId}

	leader := atomic.LoadInt32(&ck.leaderId)
	for {
		//DPrintf("Client %d get to Server %d:", ck.clientId, ck.leaderId)
		var reply GetReply
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			//DPrintf("Client %d get to Server %d, args: %v, reply: %v", ck.clientId, ck.leaderId, args, reply)
			//DPrintf("Get Time: %v", time.Since(time1))
			atomic.StoreInt32(&ck.leaderId, leader)
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		} 
		
		//if !ok || reply.Err == ErrWrongLeader {
		leader = (leader + 1) % int32(len(ck.servers))
		//ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		//}
		time.Sleep(100 * time.Millisecond)
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
	//ck.mu.Lock()
	//defer ck.mu.Unlock()
	// You will have to modify this function.
	//time1 := time.Now()
	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SequenceId: seqId}
	leader := atomic.LoadInt32(&ck.leaderId)
	for {
		//DPrintf("Client %d put/append to Server %d:", ck.clientId, ck.leaderId)
		var reply PutAppendReply
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		//DPrintf("Client %d put/append to Server %d, args: %v, reply: %v", ck.clientId, ck.leaderId, args, reply)
		if ok && reply.Err == OK  {
			//DPrintf("Put Time: %v", time.Since(time1))
			atomic.StoreInt32(&ck.leaderId, leader)
			return
		}

		//ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		//if !ok || (reply.Err == ErrWrongLeader) {
		leader = (leader + 1) % int32(len(ck.servers))		
		//} 
		time.Sleep(100 * time.Millisecond)
	}

	

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
