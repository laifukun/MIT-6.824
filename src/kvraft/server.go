package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"

	"../labgob"
	"../labrpc"
	"../raft"

)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.SetOutput(f)
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
	//SequenceId int64
	OpType string
	Key    string
	Value  string
	Index  int
	Term   int

	Error Err
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type OpSeq struct {
	Id int64
	Value string
	Error Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()


	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	kvData        Datastore
	lastAppliedIndex int
	lastAppliedTerm  int

	raftSignal  map[int]chan Op
	//tempKVStore map[int64]KVPair
	//requestMap  map[int64]int64
	requestMap map[int64]OpSeq
	//requestCount int64
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	
	op := Op{OpId: args.SequenceId, OpType: GET, ClientId: args.ClientId}

	var isLeader bool
	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	//client := strconv.FormatInt(args.ClientId, 10) + "-" + strconv.FormatInt(args.SequenceId, 10)
	//seqId, ok := kv.requestMap[args.ClientId]
	clientSeq, ok := kv.requestMap[args.ClientId]
	if ok && clientSeq.Id >= args.SequenceId {
		if clientSeq.Id == args.SequenceId && clientSeq.Error == OK {
			reply.Value = clientSeq.Value
			reply.Err = clientSeq.Error
		} else {
			reply.Value, reply.Err = kv.kvData.Get(args.Key)
		}		
		
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op.Index, op.Term, isLeader = kv.rf.Start(op)

	//DPrintf("Get from Server %d is Leader:  %v", kv.me, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d, clientId: %d, sequence ID: %d ", kv.me, args.ClientId, args.SequenceId)
	kv.mu.Lock()

	ch := kv.getChannel(op.Index)
	kv.mu.Unlock()

	//DPrintf("Server %d, has KV data: %v", kv.me, kv.kvData)
	//kv.mu.Unlock()
	select {
	case rfOp := <-ch:

		//kv.mu.Lock()
		//DPrintf("returned op index: %d, desired Index: %d", rfOp.Index, op.Index)
		if rfOp.Term != op.Term || rfOp.Index != op.Index {
			DPrintf("Returned op index: %d, desired Index: %d", rfOp.Index, op.Index)
			reply.Err = ErrWrongLeader
		} else {
			//op.Value, op.Error = kv.kvData.Get(op.Key)
			reply.Value = op.Value
			reply.Err = op.Error
			//kv.kvData.Get(args.Key)
		}
		//DPrintf("Server %d, has KV data: %v", kv.me, kv.kvData)

		//kv.mu.Unlock()

	case <-time.After(2000 * time.Millisecond):
		reply.Err = ErrWrongLeader

	}
	kv.mu.Lock()
	kv.removeChannel(op.Index)

	//DPrintf("Server %d, KV: %v", kv.me, kv.kvData)
	//kv.lastRaftIndex = op.Index
	//kv.lastRaftTerm = op.Term
	//kv.snapshot()
	//DPrintf("Server has last raft Index: %d", kv.lastRaftIndex)
	//DPrintf("After Get Server %d, has KV data: %v, signal: %v", kv.me, kv.kvData, kv.raftSignal)
	kv.mu.Unlock()

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{OpId: args.SequenceId, OpType: args.Op, ClientId: args.ClientId, Key: args.Key, Value: args.Value}
	var isLeader bool
	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//client := strconv.FormatInt(args.ClientId, 10) + "-" + strconv.FormatInt(args.SequenceId, 10)

	//DPrintf("where is the problem server, %d", kv.me)
	kv.mu.Lock()

	//seqId, ok := kv.requestMap[args.ClientId]
	clientSeq, ok := kv.requestMap[args.ClientId]
	DPrintf("Server %d, request Map: %v",kv.me, kv.requestMap)
	if ok && clientSeq.Id >= args.SequenceId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op.Index, op.Term, isLeader = kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf("Put Append to server %d, Key: %s, Value: %s, clientID: %d, Seq Id: %d", kv.me, args.Key, args.Value, args.ClientId, args.SequenceId)
	//DPrintf("The desired Index is: %d", op.Index)
	//DPrintf("Server %d is Leader:  %v", kv.me, isLeader)
	//kv.mu.Lock()
	//kv.tempKVStore[requestId] = KVPair{Key: args.Key, Value: args.Value}
	ch := kv.getChannel(op.Index)
	kv.mu.Unlock()

	//DPrintf("Server %d, has KV data: %v", kv.me, kv.kvData)
	//kv.mu.Lock()

	select {
	case rfOp := <-ch:

		//kv.mu.Lock()
		//DPrintf("returned op index: %d, desired Index: %d", rfOp.Index, op.Index)
		if rfOp.Term != op.Term || rfOp.Index != op.Index {
			DPrintf("Returned op index: %d, desired Index: %d", rfOp.Index, op.Index)
			reply.Err = ErrWrongLeader

		} else {

			reply.Err = OK
		}
		//kv.mu.Unlock()

	case <-time.After(2000 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	//DPrintf("Server %d, KV store: %v", kv.me, kv.kvData)
	
	//kv.lastRaftIndex = op.Index
	//kv.lastRaftTerm = op.Term
	//kv.snapshot()
	//DPrintf("Server has last raft Index: %d", kv.lastRaftIndex)
	kv.removeChannel(op.Index)
	//DPrintf("Server %d, has KV data: %v", kv.me, kv.kvData)

	kv.mu.Unlock()
	

}

func (kv *KVServer) getChannel(Id int) chan Op {

	ch, ok := kv.raftSignal[Id]
	if !ok {
		ch = make(chan Op, 1)
		kv.raftSignal[Id] = ch
	}

	//DPrintf("Raft %d get channel Signal: %v", kv.me, kv.raftSignal)
	return ch

}
func (kv *KVServer) removeChannel(Id int) {
	//DPrintf("Server %d, delete Id: %d", kv.me, Id)
	delete(kv.raftSignal, Id)
	//DPrintf("After Deleting: %v", kv.raftSignal)
}

//

func (kv *KVServer) raftStateThread() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:

			if !applyMsg.CommandValid {
				DPrintf("Server %d received snapshot, last included index: %d, last included term: %d", kv.me,applyMsg.LastIncludedIndex,applyMsg.LastIncludedTerm)
				installed := kv.rf.CondInstallSnapshot(applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm, applyMsg.Snapshot) 
				
				kv.mu.Lock()
				if installed {
					DPrintf("Read snapshot")
					kv.installSnapshot(applyMsg.Snapshot)
					if kv.lastAppliedIndex < applyMsg.LastIncludedIndex {
						kv.lastAppliedIndex = applyMsg.LastIncludedIndex
					}
					if kv.lastAppliedTerm < applyMsg.LastIncludedTerm {
						kv.lastAppliedTerm = applyMsg.LastIncludedTerm
					}
				}
				kv.mu.Unlock();
			} else {

				kv.mu.Lock()
				op, ok1 := applyMsg.Command.(Op)
				//DPrintf("Server %d, Signal from raft, opID: %d, OK: %v", kv.me, op.OpId, ok1)
				
				if ok1 {

					clientSeq, ok2 := kv.requestMap[op.ClientId]
					if !ok2 || op.OpId > clientSeq.Id {
						
						if op.OpType == PUT {
							kv.kvData.Put(op.Key, op.Value)
						} else if op.OpType == APPEND {
							kv.kvData.Append(op.Key, op.Value)
						} else if op.OpType == GET {
							op.Value, op.Error = kv.kvData.Get(op.Key)
						}
						//if op.OpType == PUT || op.OpType == APPEND {
						//	DPrintf("Server %d, KV: %v", kv.me, kv.kvData)
						//}
						op.Index = applyMsg.CommandIndex
						op.Term = applyMsg.CommandTerm
						//DPrintf("Raft %d returned, opId: %d,kv %s, %s, opIndex: %d ", kv.me, op.OpId, op.Key, op.Value, op.Index)
						kv.requestMap[op.ClientId] = OpSeq {Id: op.OpId, Value: op.Value, Error: op.Error}
						kv.lastAppliedIndex = op.Index
						kv.lastAppliedTerm = op.Term
						//kv.snapshot();
					}
					ch, ok2 := kv.raftSignal[op.Index]

					if ok2 {
						//DPrintf("Signal request exist, Index: %d", op.Index)

						ch <- op
					} 
				}

				kv.mu.Unlock()
			}
			

			/*
		case <-time.After(100 * time.Millisecond):

			kv.mu.Lock()
			for _, ch := range kv.raftSignal {

				ch <- Op{}
			}
			kv.mu.Unlock()
			*/
		}

		//if kv.isLeader {
		//	DPrintf("Server %d, KV store: %v", kv.me, kv.kvData)
		//}
		
		//time.Sleep(10*time.Millisecond)
	}
}

func (kv *KVServer) snapshotThread() {

	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {

			//snapshot := kv.kvData.GetSnapshot(kv.lastAppliedIndex, kv.lastAppliedTerm)

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			//e.Encode(kv.lastAppliedIndex)
			//e.Encode(kv.lastAppliedTerm)
			e.Encode(kv.requestMap)
			e.Encode(kv.kvData)
			
			snapshot := w.Bytes()
			//DPrintf("Server %d Call snapshot, lastRaftIndex: %d", kv.me, kv.lastAppliedIndex)
			go kv.rf.Snapshot(snapshot, kv.lastAppliedIndex, kv.lastAppliedTerm)
		}
		kv.mu.Unlock()
		time.Sleep(100*time.Millisecond)
	}

}

func (kv *KVServer) installSnapshot(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//var lastAppliedId int
	//var lastAppliedTm int
	//d.Decode(&lastAppliedId)
	//d.Decode(&lastAppliedTm)
	//if kv.lastAppliedIndex < lastAppliedId {
	//	kv.lastAppliedIndex = lastAppliedId
	//}
	//if kv.lastAppliedTerm < lastAppliedTm {
	//	kv.lastAppliedTerm = lastAppliedTm
	//}
	
	reqMap := make(map[int64]OpSeq)
	var kvDat Datastore
	//kvDat := new(Datastore) 
	
	//kv.kvData.Init()
	d.Decode(&reqMap)
	d.Decode(&kvDat)

	kv.requestMap = reqMap
	kv.kvData = kvDat
	DPrintf("Read snapshot: KV: %v", kv.kvData.KeyValue)
	DPrintf("Read snapshot: Map: %v", kv.requestMap)
	//kv.lastAppliedIndex, kv.lastAppliedTerm = kv.kvData.PutSnapshot(data)
}

/*
func (kv *KVServer) getSnapshot() SnapShot {
	snapshot := SnapShot{
		LastIndex: kv.lastRaftIndex,
		LastTerm:  kv.lastRaftTerm,
		KeyValue:  kv.kvData.GetSnapshot(LastIndex, LastTerm),
	}
	return snapshot
}
*/
/*
func (kv *KVServer) clientRequestThread() {
	for !kv.killed() {

	}
}
*/
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

	/*
	kv.mu.Lock()
			for _, ch := range kv.raftSignal {

				ch <- Op{}
			}
			kv.applyCh <- raft.ApplyMsg{}
	kv.mu.Unlock()
	*/
	// Your code here, if desired.
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
	kv.raftSignal = make(map[int]chan Op)
	//kv.tempKVStore = make(map[int64]KVPair)
	kv.requestMap = make(map[int64]OpSeq)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg,1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//kv.readSnapshot(persister.ReadSnapshot())
	go kv.raftStateThread()
	go kv.snapshotThread()
	//time.Sleep(time.Second)
	// You may need initialization code here.

	return kv
}
