package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex // Lock to protect shared access to this peer's state
	stateChanged *sync.Cond
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()

	//logs []LogEntry // index start from 1, the 0th entry is empty
	logs Log
	//lastLogId int
	//newEntries bool

	lastSnapshotIndex int
	lastSnapshotTerm int
	currentTerm int
	state       ServerState
	votedFor    int

	requestVoteDone bool
	commitId        int
	lastApplied     int

	//termHeartBeat chan int
	stateSignal       *StateBroadcaster
	startElectionAt   time.Time
	nextHeartbeatTime time.Time

	//lastReceived time.Time
	nextId  []int  // for each server, index of the next log entry to send to the server for replication
	matchId []int  //for each server, index of highest log entry know to be replicated on server
	hasVote []bool // for each server, vote result
	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) getLastLogId() int {

	return rf.logs.getLastIndex()
	//return len(rf.logs) - 1
}

/*
func (rf *Raft) getLogEntry(logId int) (LogEntry, error) {
	if logId <= rf.getLastLogId() {
		return rf.logs[logId], nil
	}
	return LogEntry{}, errors.New("Index out of bound")
}
*/
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.state == LEADER)

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	//e.Encode(rf.getStartIndex())
	//DPrintf("Server %d persist: start index: %d, last index: %d", rf.me, rf.logs.getStartIndex(),rf.logs.getLastIndex())
	//DPrintf("Logs %v", rf.logs)
	e.Encode(rf.logs.getEntries(rf.logs.getStartIndex(), rf.logs.getLastIndex()))

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("Server %d read from storage......", rf.me)
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	if d.Decode(&term) != nil || d.Decode(&voted) != nil {
		DPrintf("Error reading state")
	} else {
		rf.currentTerm = term
		rf.votedFor = voted
	}

	var lastSnapshotId int
	var lastSnapshotTm int

	if d.Decode(&lastSnapshotId) != nil || d.Decode(&lastSnapshotTm) != nil {
		DPrintf("Error reading state")
	} else {
		rf.lastSnapshotIndex = lastSnapshotId
		rf.lastSnapshotTerm = lastSnapshotTm
	}

	rf.lastApplied = rf.lastSnapshotIndex
	rf.commitId = rf.lastSnapshotIndex
	var tempLog []Entry
	d.Decode(&tempLog)
	rf.logs.replicateEntries(tempLog)
	rf.logs.setSnapshotParameter(lastSnapshotId,lastSnapshotTm)

	DPrintf("Server %d read state: last snapshot index: %d, last log index: %d", rf.me, rf.lastSnapshotIndex, rf.logs.getLastIndex())
	for i := 0; i < len(rf.peers); i++ {
		rf.nextId[i] = rf.getLastLogId() + 1
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d received voting request from candidate %d", rf.me, args.CandidateId)

	DPrintf("   Server %d, Last Log Term: %d, current Term %d, votedFor: %d, lastLogIndex: %d", rf.me, rf.logs.getLastLogTerm(), rf.currentTerm, rf.votedFor, rf.getLastLogId())

	DPrintf("   Candidate %d, candidate term: %d, Last Log Term: %d, LastLogIndex: %d,", args.CandidateId, args.CandidateTerm, args.LastLogTerm, args.LastLogId)

	//DPrintf("    Server %d, logs: %v", rf.me, rf.logs)
	//rf.mu.Lock()

	// log compare
	//logIsOk := (args.LastLogTerm > rf.logs[rf.getLastLogId()].Term) || (rf.logs[rf.getLastLogId()].Term == args.LastLogTerm && args.LastLogId >= rf.getLastLogId())
	logIsOk := args.LastLogTerm > rf.logs.getLastLogTerm() || (rf.logs.getLastLogTerm() == args.LastLogTerm && args.LastLogId >= rf.getLastLogId())

	// term compare
	if args.CandidateTerm > rf.currentTerm {
		rf.stepDownToFollower(args.CandidateTerm)
		DPrintf("   Server %d convert to Follower.", rf.me)
	}

	if args.CandidateTerm == rf.currentTerm {
		if logIsOk && rf.votedFor == -1 {
			rf.stepDownToFollower(args.CandidateTerm)
			//rf.setElectionTimer()
			rf.votedFor = args.CandidateId
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = (args.CandidateTerm == rf.currentTerm && rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		DPrintf("     Server %d voted for %d", rf.me, args.CandidateId)
	}
	rf.setElectionTimer()
	//rf.mu.Unlock()
	go rf.stateSignal.NotifyAll()
	//rf.setElectionTimer()
	//
}

//AppendEntries RPC handler

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d Received Entries/HeartBeat from Leader: %d", rf.me, args.LeaderId)
	//DPrintf("    Server Term: %d, Leader term, %d", rf.currentTerm, args.Term)
	DPrintf("    Server %d last log ID: %d, last snapshot Id: %d, Received prevLogId: %d", rf.me, rf.getLastLogId(), rf.lastSnapshotIndex, args.PrevLogId)
	
	DPrintf("    Server %d logs: %v", rf.me, rf.logs)
	//rf.mu.Unlock()

	
	//rf.termHeartBeat <- args.Term
	

	
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LastLogId = rf.getLastLogId()
	reply.XTerm = -1
	reply.XIndex = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	}

	rf.stepDownToFollower(args.Term)
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll()
	//rf.lastReceived = time.Now()

	if args.PrevLogId > rf.getLastLogId() {
		return
	}

	if args.PrevLogId < rf.commitId {
		reply.CommitId = rf.commitId
		//reply.XTerm = rf.logs.getLogTerm(rf.commitId + 1)
		return
	}
	DPrintf("    Server %d Log Term: %d, Received prevLog Term: %d", rf.me, rf.logs.getLogTerm(args.PrevLogId), args.PrevLogTerm)
	if rf.logs.getLogTerm(args.PrevLogId) != args.PrevLogTerm {
		reply.XTerm = rf.logs.getLogTerm(args.PrevLogId)
		for id := args.PrevLogId; id >= rf.lastSnapshotIndex; id-- {
			if rf.logs.getLogTerm(id) != reply.XTerm {
				reply.XIndex = id + 1
				break
			}
		}
		return
	}

	reply.LastLogId = args.PrevLogId
	
	reply.Success = true

	id := args.PrevLogId

	/* only if the log entries after the current snapshot index should be installed. 
	Note that the lastSnapshotIndex >= commitId, it seems that the PrevLogId from lead is always more than commitId.
	However, these exist a case that the server have commited but not yet reply to the leader, and the leader not update its
	nextId table. The leader still uses old nextId to send the entries, but the current server already takes the snapshot.
	This case will overwrite the snapshot logs and cause error
	*/
	if args.Entries != nil && args.PrevLogId + 1 > rf.lastSnapshotIndex {
		rf.logs.removeEntriesAfterIndex(id)
		rf.logs.replicateEntries(args.Entries)
		
	} 

	reply.LastLogId = rf.getLastLogId()
	

	if rf.commitId < args.LeaderCommitId {
		if args.LeaderCommitId <= rf.getLastLogId() {
			rf.commitId = args.LeaderCommitId
		} else {
			rf.commitId = rf.getLastLogId()
		}
	}

	DPrintf("    Server %d last log ID: %d, commit ID: %d", rf.me, rf.getLastLogId(), rf.commitId)
	//DPrintf("    Server %d last log ID: %d, commit ID: %d, logs: %v", rf.me, rf.getLastLogId(), rf.commitId, rf.logs)
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll()

}

// install snapshot

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d Received Install Snapshot from Leader: %d, Server Term: %d, leader term: %d", rf.me, args.LeaderId, rf.currentTerm, args.Term)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	}

	rf.stepDownToFollower(args.Term)
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll()

	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	} 
	rf.sendSnapshotToServices(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll()


	
	//DPrintf("Server %d Save snapshot, raft state size: %d, lastIndex: %d", rf.me, rf.persister.RaftStateSize(), args.LastIncludedIndex)
	//rf.saveStateAndSnapshot(args.Data, args.LastIncludedIndex)
	//DPrintf("    New raft state size: %d", rf.persister.RaftStateSize())
	//rf.setElectionTimer()
	//rf.mu.Unlock()
	//go rf.stateSignal.NotifyAll()
	//rf.setElectionTimer()
	//
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("   Send Vote Request to %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// call request vote

func (rf *Raft) sendToVote() {

	if rf.state != CANDIDATE {
		return
	}
	//defer rf.mu.Unlock()
	//DPrintf("Candidate %d sent vote to server %d: last Log Id: %d", rf.me, server, rf.getLastLogId())
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogId:     rf.getLastLogId(),
		LastLogTerm:   rf.logs.getLastLogTerm(),
	}

	//rf.mu.Unlock()

	rf.setElectionTimer()
	for server, _ := range rf.peers {

		if server == rf.me {
			continue
		}
		//DPrintf("Voting: Candidate %d: Send to %d: ", rf.me, server)
		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			//DPrintf("      Server %d Reply is %v, VoteGranted: %v", server, ok, reply.VoteGranted)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != CANDIDATE {
				//rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDownToFollower(reply.Term)
				rf.setElectionTimer()
			} else {

				rf.hasVote[server] = reply.VoteGranted
				if rf.voteQuorum() {
					rf.becomeLeader()
				}
			}
			go rf.stateSignal.NotifyAll()
			//rf.mu.Unlock()

		}(server)

	}

	//rf.requestVoteDone = true
	//go rf.stateSignal.NotifyAll()
	//return reply.VoteGranted

}

func (rf *Raft) voteQuorum() bool {
	count := 0
	for _, result := range rf.hasVote {
		if result {
			count++
		}
	}

	if count > len(rf.peers)/2 {
		return true
	}
	return false
}

// send heart beat as a leader

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendLogEntries() {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//DPrintf("Leader %d next Id: %v, matchID: %v", rf.me, rf.nextId, rf.matchId)
	//DPrintf("Leader logs %v: ", rf.logs)

	//DPrintf("Leader %d Send logs to server %d", rf.me, server)

	DPrintf("........Leader %d Send Logs to Servers, next ID: %v, match Id: %v, Last Snapshot Index: %d", rf.me, rf.nextId, rf.matchId, rf.lastSnapshotIndex)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		if rf.nextId[server] <= rf.lastSnapshotIndex  {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			testLogId := rf.nextId[server]

			
			lastLogId := rf.getLastLogId()

			if testLogId-1 > lastLogId {
				rf.mu.Unlock()
				return
			}
			args := AppendEntryArgs{
				Term:        rf.currentTerm,
				LeaderId:    rf.me,
				PrevLogId:   testLogId - 1,
				PrevLogTerm: rf.logs.getLogTerm(testLogId - 1),
			}
			//if testLogId <= lastLogId {
				args.Entries = rf.logs.getEntries(testLogId, lastLogId) //rf.logs[testLogId : lastLogId+1]
				//DPrintf("Log entries: %v", args.Entries)
			//}
			args.LeaderCommitId = rf.commitId
			rf.mu.Unlock()

			var reply AppendEntryReply
			ok := rf.sendAppendEntry(server, &args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			//DPrintf(" Server %d,  Reply: %v, Server %d Test Log Id %d, reply lastlogId %d, reply.Success: %v", server,reply.Success, server, testLogId, reply.LastLogId, reply.Success)

			if rf.state != LEADER {
				return
			}
			if reply.Term > rf.currentTerm {

				rf.stepDownToFollower(reply.Term)
				rf.setElectionTimer()

			} else {

				if reply.Success {
					if rf.matchId[server] < reply.LastLogId {
						rf.matchId[server] = reply.LastLogId						
					}
					rf.nextId[server] = rf.matchId[server] + 1
					rf.advanceCommitIndex()
					
				} else {

					if rf.nextId[server] > rf.lastSnapshotIndex + 1 {
						rf.nextId[server]--
					}

					if (rf.nextId[server] <= reply.CommitId) {
						rf.nextId[server] = reply.CommitId + 1
					} else if rf.nextId[server] > rf.lastSnapshotIndex + 1 && reply.XTerm != -1 {
						for id := rf.nextId[server]; id > rf.lastSnapshotIndex; id-- {
							if rf.logs.getLogTerm(id) == reply.XTerm {
								rf.nextId[server] = id + 1
								break
							}
							if rf.logs.getLogTerm(id) < reply.XTerm {
								rf.nextId[server] = reply.XIndex
							}
						}
					} else if rf.nextId[server] > reply.LastLogId+1 {
						rf.nextId[server] = reply.LastLogId + 1
					}
				}
			}
			go rf.stateSignal.NotifyAll()

		}(server)
	}

	return

}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot() {


	var snapshot = make([]byte, len(rf.persister.ReadSnapshot()))
	copy(snapshot, rf.persister.ReadSnapshot())
	args := SnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              snapshot,
	}

	

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		
		if rf.nextId[server] > rf.lastSnapshotIndex  {
			continue
		}
			DPrintf("........Leader %d Send Snapshop to Server %d, last snapshot index %d", rf.me, server,rf.lastSnapshotIndex)
			var reply SnapshotReply
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
			ok := rf.sendInstallSnapshot(server, &args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

				
			if rf.state != LEADER {
				return
			}
			if reply.Term > rf.currentTerm {

				rf.stepDownToFollower(reply.Term)
				rf.setElectionTimer()

			} else {
				rf.matchId[server] = rf.lastSnapshotIndex
				rf.nextId[server] = rf.lastSnapshotIndex + 1
			}
			go rf.stateSignal.NotifyAll()

		}(server)
		
	}
}

// operate as leader
func (rf *Raft) performLeaderOperation() time.Time {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if rf.nextHeartbeatTime.Before(time.Now()) {
		rf.nextHeartbeatTime = time.Now().Add(time.Duration(BroadcastTime) * time.Millisecond)
		
		rf.sendSnapshot()
		rf.sendLogEntries()
	}
	go rf.stateSignal.NotifyAll()
	return rf.nextHeartbeatTime

}

func (rf *Raft) advanceCommitIndex() {

	DPrintf("Leader %d: Match ID: %v,commited ID: %d, next ID: %v", rf.me, rf.matchId, rf.commitId, rf.nextId)
	if rf.state != LEADER {
		return
	}

	matched := make([]int, len(rf.peers))
	copy(matched, rf.matchId)

	sort.Ints(matched)

	newCommitId := matched[(len(rf.peers)-1)/2]
	//DPrintf("Leader %d, logs: %v",rf.me, rf.logs)
	//DPrintf("Last snapshot index: %d", rf.lastSnapshotIndex)
	DPrintf("Leader %d, New CommitID: %d, new commit ID term: %d, current term, %d", rf.me, newCommitId,rf.logs.getLogTerm(newCommitId),rf.currentTerm)
	
	if rf.logs.getLogTerm(newCommitId) != rf.currentTerm {
		return
	}
	if rf.commitId >= newCommitId {
		return
	}
	rf.commitId = newCommitId

	DPrintf("Leader %d: Match ID: %v,commited ID: %d, next ID: %v", rf.me, rf.matchId, rf.commitId, rf.nextId)

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	//rf.stateSignal.NotifyAll()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startNewElection() {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	for i := 0; i < len(rf.hasVote); i++ {
		rf.hasVote[i] = false
	}
	rf.hasVote[rf.me] = true
	rf.state = CANDIDATE

	//go rf.stateSignal.NotifyAll()
	//rf.mu.Unlock()
	DPrintf("Candidate %d Start new election with Term %d", rf.me, rf.currentTerm)

	//voteResult := make(chan bool)
	rf.setElectionTimer()
	rf.sendToVote()

	rf.requestVoteDone = true
	go rf.stateSignal.NotifyAll()

}

func (rf *Raft) setElectionTimer() {

	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(ElectionTimeout)
	rf.startElectionAt = time.Now().Add(time.Duration(ElectionTimeout+n) * time.Millisecond)
	//DPrintf("     Server %d reset election timer to: %v", rf.me, rf.startElectionAt)
	//go rf.stateSignal.NotifyAll()

}
func (rf *Raft) timerThread() {

	stateChanged := rf.stateSignal.Subscribe()
	DPrintf("Timer thread started: %v", rf.me)
	for !rf.killed() {

		rf.mu.Lock()
		waitUntil := rf.startElectionAt
		rf.mu.Unlock()
		select {
		case <-stateChanged:
			rf.mu.Lock()
			waitUntil = rf.startElectionAt
			rf.mu.Unlock()
		case currentTime := <-time.After(time.Until(waitUntil)):
			rf.mu.Lock()
			DPrintf("Time Out, server %d start new election at: %v, currentTime: %v", rf.me, rf.startElectionAt, currentTime)
			
			rf.startNewElection()
			rf.mu.Unlock()
		}
	}
	//rf.stateSignal.Close()
}

func (rf *Raft) serverStateThread() {

	stateChanged := rf.stateSignal.Subscribe()

	waitUntil := time.Now().Add(1 * time.Millisecond)
	for !rf.killed() {

		//DPrintf("Server %d is %s at Term %d", rf.me, rf.state, rf.currentTerm)

		//DPrintf("waitUntil: %v, next heartbeat time: %v", waitUntil, rf.nextHeartbeatTime)
		select {
		case <-stateChanged:
			rf.mu.Lock()
			//state := rf.state
			//rf.mu.Unlock()
			switch rf.state {

			case FOLLOWER:
				waitUntil = MaxTimePoint
			case CANDIDATE:
				//rf.mu.Lock()
				//requestVoteDone := rf.requestVoteDone
				
				if !rf.requestVoteDone {
					rf.startNewElection()
				} else {
					waitUntil = MaxTimePoint
				}
				//rf.mu.Unlock()
			case LEADER:
				//	DPrintf("Next Heart Beat Time: %v", rf.nextHeartbeatTime)
				//rf.mu.Lock()
				waitUntil = rf.performLeaderOperation()
				//rf.mu.Unlock()
				//	DPrintf("WaitUntil: %v", waitUntil)

				//rf.mu.Unlock()
			}
			rf.mu.Unlock()
		case <-time.After(time.Until(waitUntil)):
			go rf.stateSignal.NotifyAll()
			//DPrintf("Server %d WaitUntil %v, now: %v", rf.me, waitUntil, time.Now())
			//break
			//default:
			//	time.Sleep(1 * time.Millisecond)

		}
		//DPrintf("server state thread %d sleep", rf.me)
	}
}

func (rf *Raft) stateMachineThread() {

	waitUntil := MaxTimePoint
	stateChanged := rf.stateSignal.Subscribe()
	for !rf.killed() {
		select {
		case <-stateChanged:
			rf.mu.Lock()

			for rf.commitId > rf.lastApplied && rf.lastApplied < rf.getLastLogId() {
				rf.lastApplied++
				entry := rf.logs.getEntry(rf.lastApplied)
				msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index, CommandTerm: entry.Term}
				//msg.IsSnapshot = false
				if rf.state == LEADER {
					//DPrintf("Server %d, logs: %v", rf.me, rf.logs)
					DPrintf("Server %d send to KV services,last applied Id, %d, Index: %d,", rf.me, rf.lastApplied, entry.Index)
					
				}
				rf.applyCh <- msg
				
			}

			
			rf.mu.Unlock()

		case <-time.After(time.Until(waitUntil)):
			DPrintf("State machine time out")
			break

		}
	}
}

func (rf *Raft) logSaveThread() {
	waitUntil := MaxTimePoint
	stateChanged := rf.stateSignal.Subscribe()
	for !rf.killed() {
		//DPrintf("LogSave")
		select {
		case <-stateChanged:
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
		case <-time.After(time.Until(waitUntil)):
			break
		}
		//DPrintf("Server log save thread %d sleep", rf.me)
	}

}
func (rf *Raft) stepDownToFollower(Term int) {

	if Term > rf.currentTerm {
		rf.votedFor = -1
		rf.requestVoteDone = false
		rf.currentTerm = Term
	}
	if rf.state != FOLLOWER {
		DPrintf("Server %d become Follower at Term %d", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
	}

	//rf.setElectionTimer()

}

func (rf *Raft) becomeLeader() {

	rf.state = LEADER

	for i := 0; i < len(rf.nextId); i++ {
		rf.nextId[i] = rf.getLastLogId() + 1
		//rf.matchId[i] = 0
	}
	rf.matchId[rf.me] = rf.getLastLogId()
	rf.startElectionAt = MaxTimePoint

	DPrintf("Server %d becomes new leader at Term %d. Commited Id: %d, Last Log Id: %d, last applied Id: %d ", rf.me, rf.currentTerm, rf.commitId, rf.getLastLogId(), rf.lastApplied)

	rf.sendLogEntries()

	go rf.stateSignal.NotifyAll()
	//DPrintf("Exit becomeleader function")
	//
	return

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := (rf.state == LEADER)
	if !isLeader {
		return index, term, false
	}
	//isLeader := (rf.state == LEADER)

	term = rf.currentTerm
	/*
	if rf.logs.isEmpty() {
		index = rf.lastSnapshotIndex + 1
	} else {
		index = rf.logs.getLastIndex() + 1
	}
*/
	index = rf.logs.nextIndex()
	rf.logs.appendEntry(Entry{Command: command, Term: term, Index: index})
	//rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
	//rf.lastLogId++
	//index = rf.getLastLogId()

	rf.matchId[rf.me] = rf.getLastLogId()
	//DPrintf("   Leader %d, logs: %v", rf.me, rf.logs)

	DPrintf("Start Agreement Leader %d: current Term %d, lastlog ID: %d, commitId: %d", rf.me, rf.currentTerm, rf.getLastLogId(), rf.commitId)

	go rf.stateSignal.NotifyAll()
	return index, term, isLeader
}

func (rf *Raft) Snapshot(snapshot []byte, lastIndex int, lastTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//index := -1
	//term := -1

	//if rf.state != LEADER {
	//	return index, false
	//}
	//isLeader := (rf.state == LEADER)

	if lastIndex < rf.lastSnapshotIndex {
		return
	}
	DPrintf("Server %d Save snapshot, raft state size: %d, log last index: %d, lastIncludedIndex: %d, lastIncludedTerm: %d", rf.me, rf.persister.RaftStateSize(), rf.logs.getLastIndex(), lastIndex, lastTerm)
	//DPrintf("Logs: %v", rf.logs)
	rf.lastSnapshotIndex = lastIndex
	rf.lastSnapshotTerm = lastTerm
	rf.saveStateAndSnapshot(snapshot, lastIndex, lastTerm)
	//DPrintf("Logs: %v", rf.logs)
	
	DPrintf("    New raft state size: %d", rf.persister.RaftStateSize())
	//go rf.sendSnapshotToFollowers(snapshot, lastIndex)
	//return index, isLeader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if lastIncludedIndex <= rf.lastSnapshotIndex {
	//	return false
	//}

	DPrintf("Cond Install SnapShot Server %d Save snapshot, raft state size: %d, log last index: %d, lastIncludedIndex: %d, lastIncludedTerm: %d", rf.me, rf.persister.RaftStateSize(), rf.logs.getLastIndex(), lastIncludedIndex,lastIncludedTerm)
	rf.saveStateAndSnapshot(snapshot, lastIncludedIndex, lastIncludedTerm)
	DPrintf("    New raft state size: %d", rf.persister.RaftStateSize())
	rf.lastSnapshotIndex = lastIncludedIndex
	rf.lastSnapshotTerm = lastIncludedTerm
	if rf.lastApplied < lastIncludedIndex {
		rf.lastApplied = lastIncludedIndex
	}
	if rf.commitId < lastIncludedIndex {
		rf.commitId = lastIncludedIndex
	}
	
	return true;

}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte, lastIndex int, lastTerm int) {

	rf.logs.snapshot(lastIndex, lastTerm)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)

	e.Encode(lastIndex)
	e.Encode(lastTerm)

	e.Encode(rf.logs.getEntries(rf.logs.getStartIndex(), rf.logs.getLastIndex()))

	stateData := w.Bytes()

	rf.persister.SaveStateAndSnapshot(stateData, snapshot)
}

func (rf *Raft) sendSnapshotToServices(snapshot [] byte, lastIncludedId int,lastIncludedTerm int) {
	msg := ApplyMsg{}
	msg.LastIncludedIndex = lastIncludedId
	msg.LastIncludedTerm = lastIncludedTerm
	msg.Snapshot = snapshot
	msg.CommandValid = false

	rf.applyCh <- msg
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.state = FOLLOWER
	//rf.logs = append(rf.logs, LogEntry{Term: -1, Command: -1})
	//rf.logs = Log{startIndex: 1, lastIndex: 0}
	rf.logs = Log{}
	//rf.lastLogId = 0

	rf.votedFor = -1
	rf.nextId = make([]int, len(peers))
	rf.matchId = make([]int, len(peers))
	rf.hasVote = make([]bool, len(peers))
	rf.lastApplied = 0
	rf.commitId = 0

	//rf.lastReceived = time.Now()

	//rf.startElectionAt = time.Now().Add(time.Duration(rand.Intn(ElectionTimeout)) * time.Millisecond)
	rf.stateSignal = NewStateBroadcaster()

	rf.applyCh = applyCh
	DPrintf("%d Start program...", rf.me)
	//go rf.TransitionToFollower()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.sendSnapshotToServices(persister.ReadSnapshot(), rf.lastSnapshotIndex, rf.lastSnapshotTerm)

	rf.mu.Lock()
	rf.setElectionTimer()
	rf.mu.Unlock()

	go rf.serverStateThread()
	go rf.timerThread()
	go rf.stateMachineThread()
	go rf.logSaveThread()

	go rf.stateSignal.NotifyAll()

	return rf
}
