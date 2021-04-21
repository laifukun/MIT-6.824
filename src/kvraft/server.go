package kvraft

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
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.SetOutput(raft.f)
		log.Printf(format, a...)
	}
	return
}

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

type Op struct {
	OpId     int64
	ClientId int64
	OpType   string
	Key      string
	Value    string
	Index    int
	Term     int

	Error Err
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	kvData           Datastore
	lastAppliedIndex int
	lastAppliedTerm  int

	opChannel     map[int]chan Op
	requestRecord Datastore

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if !kv.assertLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// check if the request has been processed
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.SequenceId) {
		reply.Value, reply.Err = kv.kvData.Get(args.Key)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send to raft for agreement
	op := Op{OpId: args.SequenceId, OpType: GET, Key: args.Key, ClientId: args.ClientId}

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//DPrintf("Server %d, Get request from Client %d, op: %v", kv.me, args.ClientId, op)
	appliedOp := kv.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		kv.mu.Lock()
		reply.Value, reply.Err = kv.kvData.Get(op.Key)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}

	//DPrintf("Server %d, Get request from Client %d, op: %v, reply: %v", kv.me, args.ClientId, appliedOp, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	if !kv.assertLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.SequenceId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send to raft for agreement
	op := Op{OpId: args.SequenceId, OpType: args.Op, ClientId: args.ClientId, Key: args.Key, Value: args.Value}
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("Server %d PutAppend request from Client %d, op: %v", kv.me, args.ClientId, op)

	appliedOp := kv.getAppliedOperation(op.Index)

	if equalOperation(op, appliedOp) {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	//DPrintf("PutAppend request from Client %d, key: %s, value: %s, op: %v", args.ClientId, args.Key, op.Value, appliedOp)
}

func equalOperation(op1 Op, op2 Op) bool {

	return (op1.OpId == op2.OpId && op1.ClientId == op2.ClientId && op1.OpType == op2.OpType && op1.Key == op2.Key && op1.Index == op2.Index && op1.Term == op2.Term)
}

//assert leadership of current server
func (kv *KVServer) assertLeader() bool {
	var isLeader bool
	_, isLeader = kv.rf.GetState()
	if !isLeader {
		return false
	}
	return true
}

//detect duplicate Reqest

func (kv *KVServer) isDuplicateRequest(clientId int64, seqId int64) bool {
	clientSeq, ok := kv.requestRecord.Get(strconv.FormatInt(clientId, 10))
	maxSeq, _ := strconv.ParseInt(clientSeq, 10, 64)
	if ok == OK && maxSeq >= seqId {
		return true
	}
	return false
}

func (kv *KVServer) getChannel(Id int) chan Op {

	ch, ok := kv.opChannel[Id]
	if !ok {
		ch = make(chan Op, 1)
		kv.opChannel[Id] = ch
	}
	return ch
}

func (kv *KVServer) removeChannel(Id int) {
	delete(kv.opChannel, Id)
}

// wait and receive concensus result from raft state thread
func (kv *KVServer) getAppliedOperation(index int) Op {

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

// handle apply message from state machine
func (kv *KVServer) handleStateMessage(applyMsg raft.ApplyMsg) {

	op, ok := applyMsg.Command.(Op)

	if ok {
		op.Index = applyMsg.CommandIndex
		op.Term = applyMsg.CommandTerm
		if !kv.isDuplicateRequest(op.ClientId, op.OpId) {
			kv.updateServiceState(&op)
		}
		kv.sendStateSignal(op)
	}
}

// handle snapshot from raft machine
func (kv *KVServer) updateServiceSnapshot(applyMsg raft.ApplyMsg) {

	// old snapshot should not installed
	if applyMsg.LastIncludedIndex < kv.lastAppliedIndex {
		return
	}
	kv.installSnapshot(applyMsg.Snapshot)

	kv.lastAppliedIndex = applyMsg.LastIncludedIndex
	kv.lastAppliedTerm = applyMsg.LastIncludedTerm

}

// update service state from raft
func (kv *KVServer) updateServiceState(op *Op) {

	switch op.OpType {
	case PUT:
		kv.kvData.Put(op.Key, op.Value)
	case APPEND:
		kv.kvData.Append(op.Key, op.Value)
	}

	kv.requestRecord.Put(strconv.FormatInt(op.ClientId, 10), strconv.FormatInt(op.OpId, 10))
	kv.lastAppliedIndex = op.Index
	kv.lastAppliedTerm = op.Term
}

// send state signal to Get/Put/Append request
func (kv *KVServer) sendStateSignal(op Op) {

	ch, ok := kv.opChannel[op.Index]
	if ok {
		if len(ch) > 0 {
			<-ch
		}
		ch <- op
	}

}

// monitor and receive concensus agreement state
func (kv *KVServer) serverStateThread() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			if !applyMsg.CommandValid {

				kv.updateServiceSnapshot(applyMsg)
			} else {
				kv.handleStateMessage(applyMsg)
				kv.snapshot()
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) snapshot() {

	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.requestRecord)
	e.Encode(kv.kvData)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.lastAppliedTerm)
	snapshot := w.Bytes()

	go kv.rf.Snapshot(snapshot, kv.lastAppliedIndex, kv.lastAppliedTerm)

}

/*
// monitor state and send snapshot request
func (kv *KVServer) snapshotThread() {

	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.requestRecord)
			e.Encode(kv.kvData)
			e.Encode(kv.lastAppliedIndex)
			e.Encode(kv.lastAppliedTerm)

			snapshot := w.Bytes()
			go kv.rf.Snapshot(snapshot, kv.lastAppliedIndex, kv.lastAppliedTerm)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}
*/

// install snapshot to server
func (kv *KVServer) installSnapshot(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	reqMap, kvDat := Datastore{}, Datastore{}
	d.Decode(&reqMap)
	d.Decode(&kvDat)
	var lastAppliedId, lastAppliedTm int
	d.Decode(&lastAppliedId)
	d.Decode(&lastAppliedTm)
	kv.lastAppliedIndex = lastAppliedId
	kv.lastAppliedTerm = lastAppliedTm
	kv.requestRecord = reqMap
	kv.kvData = kvDat
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.kvData = Datastore{}
	kv.kvData.Init()
	kv.opChannel = make(map[int]chan Op)
	kv.requestRecord = Datastore{}
	kv.requestRecord.Init()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.installSnapshot(kv.persister.ReadSnapshot())

	go kv.serverStateThread()

	// You may need initialization code here.

	return kv
}
