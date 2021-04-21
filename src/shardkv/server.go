package shardkv

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const Debug = 1

//var f, err = os.Create("../logfile")

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.SetOutput(f)
		log.Printf(format, a...)
	}
	return
}

const (
	PUT                  = "Put"
	GET                  = "Get"
	APPEND               = "Append"
	CFG_UPDATE           = "Config_Update"
	SAVE_INWARD_SHARD    = "Save_Shard"
	COMMIT_INWARD_SHARD  = "Commit_Shard"
	DELETE_INWARD_STATUS = "Delete_Status"
	DELETE_OUTWARD_SHARD = "DELETE_Shard"
)

// incomming shard data status, two phase commiting is used
const (
	WAIT     = "Waiting"
	COMMITED = "Committed"
)

type Op struct {
	OpId     int64
	ClientId int64
	OpType   string
	Key      string
	Value    string
	Index    int
	Term     int
	Shard    int
	GID      int
	Config   shardmaster.Config

	KVData      map[string]string
	ShardRecord map[string]string

	Error Err
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

/*
type OpSeq struct {
	Id int64
	//Value string
	Error Err
}
*/
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	sm        *shardmaster.Clerk
	curConfig shardmaster.Config

	// Your definitions here.
	dead int32

	shardData     Shardstore     //store shard key value pair
	departData    Shardstore     //store KVs that waiting for other groups to fetch, first key is config+shard
	pullinStatus  map[int]string //status of incomming shard data
	serviceShards map[int]bool   //shards currently in services

	requestRecord Shardstore //request record to this group

	lastAppliedIndex int
	lastAppliedTerm  int

	opChannel map[int]chan Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if !kv.assertGID(args.GID) {
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.assertLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.assertShard(args.Shard) {
		reply.Err = ErrWrongGroup
		return
	}

	// detect duplicated request
	if kv.isDuplicateRequest(args.Shard, args.ClientId, args.SequenceId) {
		kv.mu.Lock()
		reply.Value, reply.Err = kv.shardData.Get(strconv.Itoa(args.Shard), args.Key)
		kv.mu.Unlock()
		return
	}

	// send to raft for agreement
	kv.mu.Lock()
	op := Op{OpId: args.SequenceId, OpType: GET, ClientId: args.ClientId, Config: kv.curConfig, Key: args.Key}
	op.Shard = args.Shard
	op.GID = args.GID
	kv.mu.Unlock()

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for response from raft
	appliedOp := kv.getAppliedOperation(op.Index)

	//appliedOp bring out Error message if it does not successfully applied to the state machine
	//because the op might be started to Raft, but it could lose the shard before it is applied to state machine.
	// i.e., a config update op might be before the current op.
	// this applies to PutAppend operations too
	if equalOperation(op, appliedOp) {
		reply.Value = appliedOp.Value
		reply.Err = appliedOp.Error
	} else {
		reply.Err = ErrWrongGroup
	}

	//DPrintf("Get request from Client %d, request record: %v", args.ClientId, kv.requestRecord)
	DPrintf("Get request from Client %d, Key: %s, Value: %s, GID: %d, Shard: %d, op: %v", args.ClientId, args.Key, appliedOp.Value, kv.gid, args.Shard, appliedOp)
	//DPrintf("Shard Data %v", kv.shardData)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// Your code here.
	if !kv.assertGID(args.GID) {
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.assertLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.assertShard(args.Shard) {
		reply.Err = ErrWrongGroup
		return
	}

	// check whether the request is duplicate
	if kv.isDuplicateRequest(args.Shard, args.ClientId, args.SequenceId) {
		reply.Err = OK
		return
	}

	// send to raft for agreement
	kv.mu.Lock()
	op := Op{OpId: args.SequenceId, OpType: args.Op, ClientId: args.ClientId, Key: args.Key, Value: args.Value, Config: kv.curConfig}
	op.Shard = args.Shard
	op.GID = args.GID
	kv.mu.Unlock()

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	appliedOp := kv.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		reply.Err = appliedOp.Error
	} else {
		reply.Err = ErrWrongGroup
	}

	DPrintf("     PutAppend request from Client %d, op: %v, applied of: %v", args.ClientId, op, appliedOp)
}

func equalOperation(op1 Op, op2 Op) bool {

	return (op1.OpId == op2.OpId && op1.ClientId == op2.ClientId && op1.OpType == op2.OpType && op1.Key == op2.Key && op1.Index == op2.Index && op1.Term == op2.Term && op1.Shard == op2.Shard && op1.GID == op2.GID)
}

func (kv *ShardKV) RequestShardRPC(args *ShardRequestArgs, reply *ShardRequestReply) {

	if !kv.assertGID(args.TargetGID) {
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.assertLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	DPrintf("Group %d Server %d received request shard %d from Group %d, Phase: %d", kv.gid, kv.me, args.Shard, args.SourceGID, args.RequestPhase)
	DPrintf("     Received ConfigId: %d, current config Id: %d", args.Config.Num, kv.curConfig.Num)

	// first phase to pull data
	if args.RequestPhase == 0 && args.Config.Num <= kv.curConfig.Num {

		shardRd := make(map[string]string)
		srdRd, _ := kv.requestRecord.GetShard(strconv.Itoa(args.Shard))
		for ky, val := range srdRd {
			shardRd[ky] = val
		}

		cfgShard := strconv.Itoa(args.Config.Num) + "-" + strconv.Itoa(args.Shard)

		srdData, ok := kv.departData.GetShard(cfgShard)
		if ok {
			kvData := make(map[string]string)
			for key, val := range srdData {
				kvData[key] = val
			}
			reply.KVData = kvData
			reply.ShardRecord = shardRd
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	}
	kv.mu.Unlock()

	// second phase to delete depart data from the current group
	if args.RequestPhase == 1 {

		op := Op{OpType: DELETE_OUTWARD_SHARD, Shard: args.Shard, Config: args.Config}
		clientId := hash(strconv.Itoa(kv.gid) + strconv.Itoa(args.Shard) + op.OpType)
		op.ClientId = clientId
		op.OpId = int64(args.Config.Num)

		if !kv.isDuplicateRequest(op.Shard, op.ClientId, op.OpId) {
			op.Index, op.Term, _ = kv.rf.Start(op)
		}
		reply.Err = OK
	}
}

//assert leadership of current server
func (kv *ShardKV) assertLeader() bool {
	var isLeader bool
	_, isLeader = kv.rf.GetState()

	return isLeader
}

// assert GID and Shard
func (kv *ShardKV) assertGID(GID int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.gid != GID {
		return false
	}

	return true
}

// assert the shard is inservice in this group
func (kv *ShardKV) assertShard(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.curConfig.Shards[shard] != kv.gid {
		return false
	}
	if _, ok := kv.serviceShards[shard]; !ok {
		return false
	}
	return true
}

//detect duplicate Reqest

func (kv *ShardKV) isDuplicateRequest(shard int, clientId int64, seqId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientSeq, ok := kv.requestRecord.Get(strconv.Itoa(shard), strconv.FormatInt(clientId, 10))
	maxSeq, _ := strconv.ParseInt(clientSeq, 10, 64)
	if ok == OK && maxSeq >= seqId {
		return true
	}
	return false
}

// get a channel for the index
func (kv *ShardKV) getChannel(Id int) chan Op {

	ch, ok := kv.opChannel[Id]
	if !ok {
		ch = make(chan Op, 1)
		kv.opChannel[Id] = ch
	}
	return ch
}

func (kv *ShardKV) removeChannel(Id int) {
	delete(kv.opChannel, Id)
}

// get applied Op from state machine that was sent from Raft
func (kv *ShardKV) getAppliedOperation(index int) Op {

	kv.mu.Lock()
	ch := kv.getChannel(index)
	kv.mu.Unlock()
	op := Op{}
	select {
	case appliedOp := <-ch:
		op = appliedOp

	case <-time.After(1000 * time.Millisecond):
	}

	kv.mu.Lock()
	kv.removeChannel(index)
	kv.mu.Unlock()

	return op
}

// handle snapshot from raft machine
func (kv *ShardKV) updateServiceSnapshot(applyMsg raft.ApplyMsg) {

	// if the current states is more advanced than the snapshot, then reject the snapshot
	if kv.lastAppliedIndex >= applyMsg.LastIncludedIndex {
		return
	}
	kv.installSnapshot(applyMsg.Snapshot)

	kv.lastAppliedIndex = applyMsg.LastIncludedIndex
	kv.lastAppliedTerm = applyMsg.LastIncludedTerm

}

// put data to database, make sure the group is still serve the shard, if not, reply ErrWrongGroup
func (kv *ShardKV) putToDatabase(op *Op) {

	if _, ok := kv.serviceShards[op.Shard]; !ok {
		op.Error = ErrWrongGroup
		return
	}
	kv.shardData.Put(strconv.Itoa(op.Shard), op.Key, op.Value)
	op.Value, op.Error = kv.shardData.Get(strconv.Itoa(op.Shard), op.Key)
}

func (kv *ShardKV) getFromDatabase(op *Op) {

	//DPrintf("Group %d, Serve %d, service shard: %v",kv.gid, kv.me, kv.serviceShards)
	if _, ok := kv.serviceShards[op.Shard]; !ok {
		op.Error = ErrWrongGroup
		return
	}

	op.Value, op.Error = kv.shardData.Get(strconv.Itoa(op.Shard), op.Key)
}

func (kv *ShardKV) appendToDatabase(op *Op) {

	if _, ok := kv.serviceShards[op.Shard]; !ok {
		op.Error = ErrWrongGroup
		return
	}
	kv.shardData.Append(strconv.Itoa(op.Shard), op.Key, op.Value)
	op.Value, op.Error = kv.shardData.Get(strconv.Itoa(op.Shard), op.Key)
}

// install the incomming shard to database, make sure the current shard is in WAIT status
func (kv *ShardKV) saveShard(op *Op) {

	if kv.pullinStatus[op.Shard] == WAIT {
		//deep copy
		shardRd := make(map[string]string)
		for key, val := range op.ShardRecord {
			shardRd[key] = val
		}
		kv.requestRecord.PutShard(strconv.Itoa(op.Shard), shardRd)

		kvData := make(map[string]string)
		for key, val := range op.KVData {
			kvData[key] = val
		}
		kv.shardData.PutShard(strconv.Itoa(op.Shard), kvData)

		kv.serviceShards[op.Shard] = true
		kv.pullinStatus[op.Shard] = COMMITED
		op.Error = OK
	}
}

// delete shard from current group
func (kv *ShardKV) deleteDepartShard(op *Op) {
	cfgShard := strconv.Itoa(op.Config.Num) + "-" + strconv.Itoa(op.Shard)
	kv.departData.RemoveShard(cfgShard)
	kv.requestRecord.RemoveShard(strconv.Itoa(op.Shard))
	op.Error = OK
}

// delete the shard from pullin status
func (kv *ShardKV) deletePullinStatus(op *Op) {
	delete(kv.pullinStatus, op.Shard)
	op.Error = OK
}

// update request record
func (kv *ShardKV) updateRequestRecord(op *Op) {
	if op.Error != OK {
		return
	}
	kv.requestRecord.Put(strconv.Itoa(op.Shard), strconv.FormatInt(op.ClientId, 10), strconv.FormatInt(op.OpId, 10))
}

// update service state by the operation from Raft
func (kv *ShardKV) updateServiceState(op *Op) {

	if kv.lastAppliedIndex >= op.Index {
		return
	}
	kv.lastAppliedIndex = op.Index
	kv.lastAppliedTerm = op.Term
	DPrintf("Group %d, Server %d, op: %v", kv.gid, kv.me, op)
	switch op.OpType {
	case PUT:
		kv.putToDatabase(op)
	case APPEND:
		kv.appendToDatabase(op)
	case GET:
		kv.getFromDatabase(op)
	case CFG_UPDATE:
		kv.updateConfig(op)
	case SAVE_INWARD_SHARD:
		kv.saveShard(op)
	case DELETE_INWARD_STATUS:
		kv.deletePullinStatus(op)
	case DELETE_OUTWARD_SHARD:
		kv.deleteDepartShard(op)
	}

	kv.updateRequestRecord(op)

	//	DPrintf("shardData %v", kv.shardData)
}

// send state signal to Get/Put/Append request
func (kv *ShardKV) sendOpSignal(op Op) {

	ch, ok := kv.opChannel[op.Index]
	if ok {
		if len(ch) > 0 {
			<-ch
		}
		ch <- op
	}
}

// update config and service shards, set up pullin status, and depart data
func (kv *ShardKV) updateConfig(op *Op) {
	newConfig := op.Config
	if newConfig.Num != kv.curConfig.Num+1 {
		return
	}

	prevConfig := kv.curConfig
	kv.curConfig = newConfig
	configId := kv.curConfig.Num
	kv.serviceShards = make(map[int]bool)
	for shard, newGID := range kv.curConfig.Shards {
		if configId == 1 || (prevConfig.Shards[shard] == kv.gid && newGID == kv.gid) {
			kv.serviceShards[shard] = true
		} else if prevConfig.Shards[shard] != kv.gid && newGID == kv.gid {
			kv.pullinStatus[shard] = WAIT
		} else if prevConfig.Shards[shard] == kv.gid && newGID != kv.gid {
			outshardData := kv.shardData.RemoveShard(strconv.Itoa(shard))
			cfgShard := strconv.Itoa(configId) + "-" + strconv.Itoa(shard)
			kv.departData.PutShard(cfgShard, outshardData)
		}
	}
	op.Error = OK
}

// request shards from the target group
func (kv *ShardKV) requestShards() {

	configId := kv.curConfig.Num
	prevCfg := kv.sm.Query(configId - 1)

	// if shard is still in pullinStatus
	for shard, status := range kv.pullinStatus {

		rGID := prevCfg.Shards[shard]
		switch status {

		// if the shard status is WAITING
		case WAIT:

			args := ShardRequestArgs{SourceGID: kv.gid, TargetGID: rGID, Shard: shard, RequestPhase: 0, Config: kv.curConfig}
			go func(vgid int, vshard int) {

				DPrintf("Group %d Send to Group %d for shards %d", kv.gid, vgid, vshard)
				for _, server := range prevCfg.Groups[rGID] {

					srv := kv.make_end(server)
					var reply ShardRequestReply
					ok := kv.sendRequestShard(srv, &args, &reply)

					// if reply is OK, send to raft for agreement
					if ok && reply.Err == OK {
						op := Op{OpType: SAVE_INWARD_SHARD, KVData: reply.KVData, ShardRecord: reply.ShardRecord, Shard: vshard, Config: args.Config}
						clientId := hash(strconv.Itoa(kv.gid) + strconv.Itoa(vshard) + op.OpType)
						op.ClientId = clientId
						op.OpId = int64(configId)

						if !kv.isDuplicateRequest(op.Shard, op.ClientId, op.OpId) {
							DPrintf("Group %d, Server %s, Send to Raft", kv.gid, server)
							op.Index, op.Term, _ = kv.rf.Start(op)
						}
					}
				}
			}(rGID, shard)

		// if it is commited, send commit phase to target group
		case COMMITED:

			args := ShardRequestArgs{SourceGID: kv.gid, TargetGID: rGID, Shard: shard, RequestPhase: 1, Config: kv.curConfig}

			go func(vgid int, vshard int) {

				for _, server := range prevCfg.Groups[rGID] {

					srv := kv.make_end(server)
					var reply ShardRequestReply
					ok := kv.sendRequestShard(srv, &args, &reply)

					// if reply is OK, send to Raft for agreement and so that the pullinStatus is deleted
					if ok && reply.Err == OK {
						op := Op{OpType: DELETE_INWARD_STATUS, Shard: vshard, Config: args.Config}

						clientId := hash(strconv.Itoa(kv.gid) + strconv.Itoa(vshard) + op.OpType)
						op.ClientId = clientId
						op.OpId = int64(configId)
						if !kv.isDuplicateRequest(op.Shard, op.ClientId, op.OpId) {
							op.Index, op.Term, _ = kv.rf.Start(op)
						}
					}
				}
			}(rGID, shard)
		}
	}
}

func (kv *ShardKV) sendRequestShard(server *labrpc.ClientEnd, args *ShardRequestArgs, reply *ShardRequestReply) bool {
	ok := server.Call("ShardKV.RequestShardRPC", args, reply)
	return ok
}

// install snapshot to server
func (kv *ShardKV) installSnapshot(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	kv.decodeState(data)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// monitor and receive concensus agreement state
func (kv *ShardKV) serverStateThread() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:

			if !applyMsg.CommandValid {

				//DPrintf("Group %d, Server %d, applyMsg snapshot id: %d", kv.gid, kv.me, applyMsg.LastIncludedIndex)
				kv.mu.Lock()
				kv.updateServiceSnapshot(applyMsg)
				kv.mu.Unlock()

			} else {
				op, ok := applyMsg.Command.(Op)
				//DPrintf("Group %d, Server %d, op in thread: %v, index: %d", kv.gid, kv.me, op, applyMsg.CommandIndex)
				//DPrintf("Group %d, Server %d, last Applied Index: %d", kv.gid, kv.me, kv.lastAppliedIndex)
				if ok {
					op.Index = applyMsg.CommandIndex
					op.Term = applyMsg.CommandTerm
					if op.Index <= kv.lastAppliedIndex {
						continue
					}
					if !kv.isDuplicateRequest(op.Shard, op.ClientId, op.OpId) {
						kv.mu.Lock()
						kv.updateServiceState(&op)
						kv.mu.Unlock()
					}
					kv.mu.Lock()
					kv.sendOpSignal(op)
					kv.mu.Unlock()
				}
				kv.mu.Lock()
				kv.snapshot()
				kv.mu.Unlock()
			}
		}
	}
}

// snapshot when raft state size is more than max
func (kv *ShardKV) snapshot() {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	snapshot := kv.encodeState()
	go kv.rf.Snapshot(snapshot, kv.lastAppliedIndex, kv.lastAppliedTerm)
}

/*
// monitor state and send snapshot request
func (kv *ShardKV) snapshotThread() {

	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {

			snapshot := kv.encodeState()
			//DPrintf("Group %d, server %d, Length of snapshot: %d, length of raft: %d", kv.gid, kv.me, len(snapshot), kv.persister.RaftStateSize())
			go kv.rf.Snapshot(snapshot, kv.lastAppliedIndex, kv.lastAppliedTerm)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}
*/

func (kv *ShardKV) encodeState() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.requestRecord)
	e.Encode(kv.shardData)
	e.Encode(kv.departData)
	e.Encode(kv.pullinStatus)
	e.Encode(kv.serviceShards)
	e.Encode(kv.curConfig)
	encode := w.Bytes()
	return encode
}

func (kv *ShardKV) decodeState(data []byte) {

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	reqMap, kvData, outData := Shardstore{}, Shardstore{}, Shardstore{}
	inShardStatus := make(map[int]string)
	srvShards := make(map[int]bool)
	curCfg := shardmaster.Config{}
	d.Decode(&reqMap)
	d.Decode(&kvData)
	d.Decode(&outData)
	d.Decode(&inShardStatus)
	d.Decode(&srvShards)
	d.Decode(&curCfg)

	kv.requestRecord = reqMap
	kv.shardData = kvData
	kv.departData = outData
	kv.pullinStatus = inShardStatus
	kv.serviceShards = srvShards
	kv.curConfig = curCfg
}

func (kv *ShardKV) checkNewConfig() (bool, shardmaster.Config) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	nextConfig := kv.sm.Query(kv.curConfig.Num + 1)

	// if pull in Status is not empty, that means, previous config update is still in process
	// and it should not proceed to update the new config
	if len(kv.pullinStatus) == 0 && nextConfig.Num > kv.curConfig.Num {
		return true, nextConfig
	}
	return false, kv.curConfig
}

//Threads detect config update
func (kv *ShardKV) configUpdateThread() {

	// every operation from the group leader itself need a client Id for Op struct.
	// It is used to detect operation duplication, because the same config might have multiple Op send to Raft
	clientId := hash(strconv.Itoa(kv.gid) + CFG_UPDATE)
	for !kv.killed() {

		if kv.assertLeader() {

			if ok, newCfg := kv.checkNewConfig(); ok {

				DPrintf("Group %d, Server %d, New Config: %v, ", kv.gid, kv.me, newCfg)
				op := Op{OpType: CFG_UPDATE, Config: newCfg}
				op.ClientId = clientId
				op.OpId = int64(newCfg.Num)

				if !kv.isDuplicateRequest(op.Shard, op.ClientId, op.OpId) {
					op.Index, op.Term, _ = kv.rf.Start(op)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// threads to continously monitor the inward shard status and if not commited to pull data from its group in old config
func (kv *ShardKV) shardsUpdateThread() {
	for !kv.killed() {

		if kv.assertLeader() {

			kv.mu.Lock()
			DPrintf("Group %d, Leader %d, Config Num: %d, ", kv.gid, kv.me, kv.curConfig.Num)
			DPrintf("    Inward status: %v, Service Shards %v", kv.pullinStatus, kv.serviceShards)

			if len(kv.pullinStatus) > 0 {
				kv.requestShards()
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister

	kv.sm = shardmaster.MakeClerk(masters)
	kv.curConfig = kv.sm.Query(0)
	// Your initialization code here.

	//kv.shardData = make(map[int]kvraft.Datastore)
	kv.shardData = Shardstore{}
	kv.shardData.Init()

	kv.requestRecord = Shardstore{}
	kv.requestRecord.Init()

	kv.departData = Shardstore{}
	kv.departData.Init()
	kv.pullinStatus = make(map[int]string)
	kv.serviceShards = make(map[int]bool)

	kv.opChannel = make(map[int]chan Op)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.installSnapshot(kv.persister.ReadSnapshot())

	go kv.serverStateThread()

	go kv.configUpdateThread()
	go kv.shardsUpdateThread()
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	log.Printf("Shard KV Group %d server %d start....", kv.gid, kv.me)

	return kv
}
