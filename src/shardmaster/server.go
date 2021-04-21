package shardmaster

import (
	//"sort"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.

	opChannel     map[int]chan Op
	requestRecord map[int64]int64

	configs []Config // indexed by config num
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type Op struct {
	OpId        int64
	ClientId    int64
	OpType      string
	Index       int
	Term        int
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int

	Error Err
	// Your data here.
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	// assert leader of this server
	if !sm.assertLeader() {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	// check whether the request is duplicate
	if sm.isDuplicateRequest(args.ClientId, args.SequenceId) {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}

	sm.mu.Unlock()

	// contruct Op and send to raft for concensus
	op := Op{OpId: args.SequenceId, OpType: JOIN, ClientId: args.ClientId, JoinServers: args.Servers}
	var isLeader bool
	op.Index, op.Term, isLeader = sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	appliedOp := sm.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}

	log.Printf("Join request from Client %d, Servers %v", args.ClientId, args.Servers)
	//log.Printf("Server states: %v", nextConfig.Groups)

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	// assert leader of this server
	if !sm.assertLeader() {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()

	// check whether the request is duplicate
	if sm.isDuplicateRequest(args.ClientId, args.SequenceId) {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}

	sm.mu.Unlock()

	// contruct Op and send to raft for concensus
	op := Op{OpId: args.SequenceId, OpType: LEAVE, ClientId: args.ClientId, LeaveGIDs: args.GIDs}
	var isLeader bool
	op.Index, op.Term, isLeader = sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	appliedOp := sm.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
	log.Printf("Leave request from Client %d, Servers %v", args.ClientId, args.GIDs)
	//log.Printf("Server states: %v", nextConfig.Groups)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// assert leader of this server
	if !sm.assertLeader() {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()

	// check whether the request is duplicate
	if sm.isDuplicateRequest(args.ClientId, args.SequenceId) {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}

	sm.mu.Unlock()
	// contruct Op and send to raft for concensus
	op := Op{OpId: args.SequenceId, OpType: MOVE, ClientId: args.ClientId, MoveShard: args.Shard, MoveGID: args.GID}
	var isLeader bool
	op.Index, op.Term, isLeader = sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//DPrintf("StateSignal: %v", kv.stateSignal)
	appliedOp := sm.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}

	log.Printf("Move request from Client %d, Shard %d, Group %d", args.ClientId, args.Shard, args.GID)
}

func equalOperation(op1 Op, op2 Op) bool {

	return op1.OpId == op2.OpId && op1.ClientId == op2.ClientId && op1.OpType == op2.OpType && op1.Index == op2.Index && op1.Term == op2.Term
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	// assert leader of this server
	if !sm.assertLeader() {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	// check whether the request is duplicate
	if sm.isDuplicateRequest(args.ClientId, args.SequenceId) {
		if args.Num == -1 || args.Num >= sm.configs[len(sm.configs)-1].Num {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		reply.Err = OK
		sm.mu.Unlock()
		return
	}
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
		reply.Err = OK
		sm.mu.Unlock()
		return
	}

	sm.mu.Unlock()

	// contruct Op and send to raft for concensus
	op := Op{OpId: args.SequenceId, OpType: QUERY, ClientId: args.ClientId}
	var isLeader bool
	op.Index, op.Term, isLeader = sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	appliedOp := sm.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num == -1 || args.Num >= sm.configs[len(sm.configs)-1].Num {
			reply.Config = sm.configs[len(sm.configs)-1]
			reply.Err = OK
			return
		}
		reply.Config = sm.configs[args.Num]
		reply.Err = OK
	}
	reply.WrongLeader = true

	log.Printf("Server %d, Query request from client %d", sm.me, args.ClientId)
}

//assert leadership of current server
func (sm *ShardMaster) assertLeader() bool {
	var isLeader bool
	_, isLeader = sm.rf.GetState()
	if !isLeader {
		return false
	}
	return true
}

//detect duplicate Reqest
func (sm *ShardMaster) isDuplicateRequest(clientId int64, seqId int64) bool {

	clientSeq, ok := sm.requestRecord[clientId]
	if ok && clientSeq >= seqId {
		return true
	}
	return false
}

// create next configuration from current configuration
func (sm *ShardMaster) createNextConfig(currentConfig Config) Config {
	nextConfig := Config{}
	nextConfig.Num = currentConfig.Num + 1
	nextConfig.Groups = make(map[int][]string)
	nextConfig.Shards = [NShards]int{}

	for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
		nextConfig.Groups[gid] = servers
	}
	for i, gid := range sm.configs[len(sm.configs)-1].Shards {
		nextConfig.Shards[i] = gid
	}
	return nextConfig
}

// assign shards to servers

func (sm *ShardMaster) assignShards(nextCfg *Config, opType string, gid int) {

	if len(nextCfg.Groups) < 1 {
		return
	}
	gidShardsMap := make(map[int][]int)

	for gid, _ := range nextCfg.Groups {
		gidShardsMap[gid] = []int{}
	}
	for i := 0; i < len(nextCfg.Shards); i++ {
		val, _ := gidShardsMap[nextCfg.Shards[i]]
		val = append(val, i)
		gidShardsMap[nextCfg.Shards[i]] = val
	}

	avg := len(nextCfg.Shards) / len(nextCfg.Groups)

	switch opType {
	case JOIN:
		jShards := []int{}
		for i := 0; i < avg; i++ {
			max := 0
			maxGid := 0
			for xgid, shards := range gidShardsMap {
				if len(shards) > max {
					max = len(shards)
					maxGid = xgid
				}
			}
			mShards := gidShardsMap[maxGid]
			jShards = append(jShards, mShards[len(mShards)-1])
			mShards = mShards[0 : len(mShards)-1]
			gidShardsMap[maxGid] = mShards
		}

		gidShardsMap[gid] = jShards

	case LEAVE:
		leaveShards := gidShardsMap[gid]
		delete(gidShardsMap, gid)
		for _, shard := range leaveShards {
			min := len(nextCfg.Shards) + 1
			minGid := 0
			for xgid, xshards := range gidShardsMap {
				if len(xshards) < min {
					min = len(xshards)
					minGid = xgid
				}
			}
			mShards := gidShardsMap[minGid]
			mShards = append(mShards, shard)
			gidShardsMap[minGid] = mShards
		}
	}

	for gid, shardList := range gidShardsMap {
		for _, shard := range shardList {
			nextCfg.Shards[shard] = gid
		}
	}

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// wait and receive concensus result from raft state thread
func (kv *ShardMaster) getAppliedOperation(index int) Op {

	kv.mu.Lock()
	ch := kv.getChannel(index)
	kv.mu.Unlock()
	op := Op{}
	select {
	case appliedOp := <-ch:
		close(ch)
		op = appliedOp

	case <-time.After(1000 * time.Millisecond):

	}
	kv.mu.Lock()
	kv.removeChannel(index)
	kv.mu.Unlock()

	return op
}

func (sm *ShardMaster) serverStateThread() {

	for !sm.killed() {

		select {
		case applyMsg := <-sm.applyCh:

			if applyMsg.CommandValid {
				sm.mu.Lock()
				sm.handleStateMessage(applyMsg)
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) handleStateMessage(applyMsg raft.ApplyMsg) {

	op, ok := applyMsg.Command.(Op)

	if ok {
		op.Index = applyMsg.CommandIndex
		op.Term = applyMsg.CommandTerm
		if !sm.isDuplicateRequest(op.ClientId, op.OpId) {
			sm.updateSeviceState(op)
		}
		sm.sendStateSignal(op)
	}
}

// update Configuration based Operation from Raft based on the OpType
func (sm *ShardMaster) updateSeviceState(op Op) {

	sm.requestRecord[op.ClientId] = op.OpId
	if op.OpType == QUERY {
		return
	}
	currentConfig := sm.configs[len(sm.configs)-1]
	nextConfig := sm.createNextConfig(currentConfig)

	switch op.OpType {

	case JOIN:

		for gid, servers := range op.JoinServers {

			newServers := make([]string, len(servers))
			copy(newServers, servers)
			nextConfig.Groups[gid] = newServers
			sm.assignShards(&nextConfig, op.OpType, gid)
		}

	case LEAVE:

		for _, gid := range op.LeaveGIDs {
			delete(nextConfig.Groups, gid)
			sm.assignShards(&nextConfig, op.OpType, gid)
		}

	case MOVE:

		for i := 0; i < NShards; i++ {
			nextConfig.Shards[i] = currentConfig.Shards[i]
		}
		nextConfig.Shards[op.MoveShard] = op.MoveGID
	}

	sm.configs = append(sm.configs, nextConfig)

	//log.Printf("Server %d configs: %v", sm.me, sm.configs[len(sm.configs)-1])
}

func (sm *ShardMaster) sendStateSignal(op Op) {

	ch, ok := sm.opChannel[op.Index]

	if ok {
		ch <- op
	}

}

func (sm *ShardMaster) getChannel(Id int) chan Op {

	ch, ok := sm.opChannel[Id]
	if !ok {
		ch = make(chan Op, 1)
		sm.opChannel[Id] = ch
	}
	return ch

}
func (sm *ShardMaster) removeChannel(Id int) {

	delete(sm.opChannel, Id)

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.opChannel = make(map[int]chan Op)
	sm.requestRecord = make(map[int64]int64)

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	log.Printf("Shard Master Server %d start....", sm.me)

	go sm.serverStateThread()
	// Your code here.

	return sm
}
