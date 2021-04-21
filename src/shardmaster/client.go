package shardmaster

//
// Shardmaster clerk.
//

import (
	"sync/atomic"
	"../labrpc"
	"time"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId   int32
	clientId   int64
	sequenceId int64
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}

	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)
	args.ClientId = ck.clientId
	args.SequenceId = seqId
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderId, leader)
			return reply.Config
		}
		leader = (leader + 1) % int32(len(ck.servers))	
		/*
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)
	args.ClientId = ck.clientId
	args.SequenceId = seqId

	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardMaster.Join", args, &reply)

		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderId, leader)
			return
		}
		leader = (leader + 1) % int32(len(ck.servers))	
		/*
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)
	args.ClientId = ck.clientId
	args.SequenceId = seqId

	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardMaster.Leave", args, &reply)

		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderId, leader)
			return
		}
		leader = (leader + 1) % int32(len(ck.servers))	
		/*
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	seqId := atomic.AddInt64(&ck.sequenceId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)
	args.ClientId = ck.clientId
	args.SequenceId = seqId

	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardMaster.Move", args, &reply)

		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderId, leader)
			return
		}
		leader = (leader + 1) % int32(len(ck.servers))	
		/*
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		time.Sleep(100 * time.Millisecond)
	}
}
