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
	//"fmt"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

	logs              Log
	lastSnapshotIndex int
	lastSnapshotTerm  int
	currentTerm       int
	state             ServerState
	votedFor          int

	requestVoteDone bool
	commitId        int
	lastApplied     int

	stateSignal       *StateBroadcaster //a channel for state change that related to election and heart beat, raft state, etc
	startElectionAt   time.Time
	nextHeartbeatTime time.Time

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

}

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
	//DPrintf("Server %d read from storage......", rf.me)

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
	rf.logs.setSnapshotParameter(lastSnapshotId, lastSnapshotTm)

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

	// log compare
	logIsOk := args.LastLogTerm > rf.logs.getLastLogTerm() || (rf.logs.getLastLogTerm() == args.LastLogTerm && args.LastLogId >= rf.getLastLogId())

	// term compare
	if args.CandidateTerm > rf.currentTerm {
		rf.stepDownToFollower(args.CandidateTerm)
		DPrintf("   Server %d convert to Follower.", rf.me)
	}

	if args.CandidateTerm == rf.currentTerm {
		if logIsOk && rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.stepDownToFollower(args.CandidateTerm)
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = (args.CandidateTerm == rf.currentTerm && rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		DPrintf("     Server %d voted for %d", rf.me, args.CandidateId)
	}
}

//AppendEntries RPC handler

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {

	DPrintf("Server %d Received Entries/HeartBeat from Leader: %d", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("    Server Term: %d, Leader term, %d", rf.currentTerm, args.Term)
	DPrintf("    Server %d last log ID: %d, last snapshot Id: %d, Received prevLogId: %d", rf.me, rf.getLastLogId(), rf.lastSnapshotIndex, args.PrevLogId)

	reply.Term = rf.currentTerm
	reply.Success = false

	reply.XTerm = -1  //conflit term
	reply.XIndex = -1 //conflit index

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	}

	rf.stepDownToFollower(args.Term)

	if args.PrevLogId > rf.getLastLogId() {
		reply.XIndex = rf.getLastLogId()
		return
	}

	DPrintf("    Server %d Log Term: %d, Received prevLog Term: %d", rf.me, rf.logs.getLogTerm(args.PrevLogId), args.PrevLogTerm)
	if args.PrevLogId >= rf.lastSnapshotIndex && args.PrevLogId <= rf.getLastLogId() && rf.logs.getLogTerm(args.PrevLogId) != args.PrevLogTerm {
		reply.XTerm = rf.logs.getLogTerm(args.PrevLogId)
		/*for id := args.PrevLogId; id >= rf.lastSnapshotIndex; id-- {
			if rf.logs.getLogTerm(id) != reply.XTerm {
				reply.XIndex = id
				break
			}
		}*/
		// Bineary Search to find the first index with XTerm
		leftId := rf.lastSnapshotIndex
		rightId := args.PrevLogId
		for leftId <= rightId {
			midId := leftId + (rightId-leftId)/2
			if rf.logs.getLogTerm(midId) >= reply.XTerm {
				rightId = midId - 1
			} else {
				leftId = midId + 1
			}
		}
		reply.XIndex = rightId
		return
	}

	reply.Success = true

	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		term := args.Entries[i].Term

		if index <= rf.getLastLogId() && term == rf.logs.getLogTerm(index) {
			continue
		} else {
			rf.logs.removeEntriesAfterIndex(index - 1)
			rf.logs.replicateEntries(args.Entries[i:])
			break
		}
	}

	rf.persist()
	rf.advanceCommitIndex(args.LeaderCommitId)
	DPrintf("    Server %d last log ID: %d, commit ID: %d", rf.me, rf.getLastLogId(), rf.commitId)
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))

}

// install snapshot from leader

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

	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}
	rf.saveStateAndSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm

	if rf.lastApplied > args.LastIncludedIndex {
		return
	}
	rf.sendSnapshotToServices(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.lastApplied = args.LastIncludedIndex
	if rf.commitId < rf.lastApplied {
		rf.commitId = rf.lastApplied
	}
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
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

	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogId:     rf.getLastLogId(),
		LastLogTerm:   rf.logs.getLastLogTerm(),
	}

	rf.setElectionTimer()
	for server := range rf.peers {

		if server == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != CANDIDATE {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDownToFollower(reply.Term)
				return
			}
			rf.hasVote[server] = reply.VoteGranted
			if rf.voteQuorum() {
				rf.becomeLeader()
			}

		}(server)
	}
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

func (rf *Raft) leaderOperation() {

	DPrintf("Leader %d Send Logs to Servers, next ID: %v, match Id: %v, Last Snapshot Index: %d", rf.me, rf.nextId, rf.matchId, rf.lastSnapshotIndex)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		if rf.nextId[server] <= rf.lastSnapshotIndex {
			go rf.sendSnapshot(server)
		} else {
			go rf.sendLogEntries(server)
		}
	}
	rf.setNextHeartBeatTime()
}

// send heart beat as a leader

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendLogEntries(server int) {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := AppendEntryArgs{
		Term:        rf.currentTerm,
		LeaderId:    rf.me,
		PrevLogId:   rf.nextId[server] - 1,
		PrevLogTerm: rf.logs.getLogTerm(rf.nextId[server] - 1),
	}

	args.Entries = rf.logs.getEntries(rf.nextId[server], rf.getLastLogId())
	args.LeaderCommitId = rf.commitId
	rf.mu.Unlock()

	var reply AppendEntryReply
	ok := rf.sendAppendEntry(server, &args, &reply)

	//DPrintf("Server %d appendEntry reply, args: %v, reply: %v", server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		return
	}
	if reply.Success {
		rf.matchId[server] = args.PrevLogId + len(args.Entries)
		rf.nextId[server] = rf.matchId[server] + 1
		newCommitId := rf.matchQuorum()
		rf.advanceCommitIndex(newCommitId)
		return
	}
	nextId := reply.XIndex + 1
	if reply.XTerm != -1 {
		/*for id := rf.nextId[server]; id >= rf.lastSnapshotIndex; id-- {
			if rf.logs.getLogTerm(id) == reply.XTerm {
				nextId = id+1
				break
			}
		}
		*/
		// bineary search to find the first index with XTerm
		leftId := rf.lastSnapshotIndex
		rightId := reply.XIndex
		for leftId <= rightId {
			midId := leftId + (rightId-leftId)/2
			if rf.logs.getLogTerm(midId) <= reply.XTerm {
				leftId = midId + 1
			} else {
				rightId = midId - 1
			}
		}
		nextId = leftId
	}
	rf.nextId[server] = nextId

}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int) {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	var snapshot = make([]byte, len(rf.persister.ReadSnapshot()))
	copy(snapshot, rf.persister.ReadSnapshot())
	args := SnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              snapshot,
	}
	//one check here
	DPrintf("Leader %d Send Snapshot to Server %d, next Id: %v, match Id: %v, last snapshot index %d", rf.me, server, rf.nextId, rf.matchId, rf.lastSnapshotIndex)
	if rf.nextId[server] > rf.lastSnapshotIndex {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	var reply SnapshotReply

	ok := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
	} else if rf.matchId[server] < rf.lastSnapshotIndex {
		rf.matchId[server] = rf.lastSnapshotIndex
		rf.nextId[server] = rf.matchId[server] + 1
	}
}

func (rf *Raft) matchQuorum() int {
	matched := make([]int, len(rf.peers))
	copy(matched, rf.matchId)
	sort.Ints(matched)
	return matched[(len(rf.peers)-1)/2]
}

func (rf *Raft) advanceCommitIndex(newCommitId int) {

	if rf.logs.getLogTerm(newCommitId) != rf.currentTerm {
		return
	}
	if rf.commitId >= newCommitId {
		return
	}
	if newCommitId > rf.getLastLogId() {
		newCommitId = rf.getLastLogId()
	}
	rf.commitId = newCommitId
	rf.applyStateMachine()
	DPrintf("Leader %d: Match ID: %v,commited ID: %d, next ID: %v, Last Snapshot ID: %d", rf.me, rf.matchId, rf.commitId, rf.nextId, rf.lastSnapshotIndex)

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

	//rf.stateSignal.Close()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startNewElection() {

	rf.currentTerm++
	rf.votedFor = rf.me
	for i := 0; i < len(rf.hasVote); i++ {
		rf.hasVote[i] = false
	}
	rf.hasVote[rf.me] = true
	rf.state = CANDIDATE

	DPrintf("Candidate %d Start new election with Term %d", rf.me, rf.currentTerm)

	rf.setElectionTimer()
	rf.sendToVote()
	rf.requestVoteDone = true
	rf.persist()

	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
}

func (rf *Raft) setElectionTimer() {

	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(ElectionTimeout)
	rf.startElectionAt = time.Now().Add(time.Duration(ElectionTimeout+n) * time.Millisecond)
}

func (rf *Raft) setNextHeartBeatTime() {
	rf.nextHeartbeatTime = time.Now().Add(time.Duration(BroadcastTime) * time.Millisecond)
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
	rf.setElectionTimer()
	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
	rf.persist()
}

func (rf *Raft) becomeLeader() {

	rf.state = LEADER

	for i := 0; i < len(rf.nextId); i++ {
		rf.nextId[i] = rf.getLastLogId() + 1
	}
	rf.matchId[rf.me] = rf.getLastLogId()
	rf.leaderOperation()
	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
	rf.persist()
	DPrintf("Server %d becomes new leader at Term %d. Commited Id: %d, Last Log Id: %d, last applied Id: %d ", rf.me, rf.currentTerm, rf.commitId, rf.getLastLogId(), rf.lastApplied)
}

/* raft state to captch state change. The main state include: election time, heart beat time, server state change. Whenever a state changes,
a statechanged signal should send to the channel.
*/
func (rf *Raft) raftStateThread2() {

	stateChanged := rf.stateSignal.Subscribe()

	waitUntil := time.Now().Add(1 * time.Millisecond)
	for !rf.killed() {
		select {
		// when state changed, wait time is updated for all server state
		case <-stateChanged:
			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				waitUntil = rf.startElectionAt
			case CANDIDATE:
				if !rf.requestVoteDone {
					rf.startNewElection()
				} else {
					waitUntil = rf.startElectionAt
				}
			case LEADER:
				waitUntil = rf.nextHeartbeatTime
			}
			rf.mu.Unlock()

		// when time is out, either election time or heartbeat time, perform server state function
		case <-time.After(time.Until(waitUntil)):

			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				rf.startNewElection()
				waitUntil = rf.startElectionAt
			case CANDIDATE:
				rf.startNewElection()
				waitUntil = rf.startElectionAt
			case LEADER:
				rf.leaderOperation()
				waitUntil = rf.nextHeartbeatTime
			}
			rf.mu.Unlock()
		}
	}
}

/*
func (rf *Raft) raftStateThread() {

	stateChanged := rf.stateSignal.Subscribe()

	waitUntil := time.Now().Add(1 * time.Millisecond)
	for !rf.killed() {
		//DPrintf("Server %d is %s at Term %d", rf.me, rf.state, rf.currentTerm)
		//DPrintf("waitUntil: %v, next heartbeat time: %v", waitUntil, rf.nextHeartbeatTime)
		select {
		case <-stateChanged:

			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				waitUntil = MaxTimePoint
			case CANDIDATE:
				if !rf.requestVoteDone {
					rf.startNewElection()
				} else {
					waitUntil = MaxTimePoint
				}
			case LEADER:
				waitUntil = rf.performLeaderOperation()
			}
			rf.mu.Unlock()

		case <-time.After(time.Until(waitUntil)):
			go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
		}
		//DPrintf("server state thread %d sleep", rf.me)
	}
}
*/
func (rf *Raft) applyStateMachine() {

	if rf.commitId < rf.lastSnapshotIndex {
		rf.commitId = rf.lastSnapshotIndex
	}
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex
	}

	for rf.commitId > rf.lastApplied && rf.lastApplied < rf.getLastLogId() {
		rf.lastApplied++
		entry := rf.logs.getEntry(rf.lastApplied)
		msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index, CommandTerm: entry.Term}
		rf.applyCh <- msg
	}
}

/*
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
				rf.applyCh <- msg
				//DPrintf("Server %d, msg: %v", rf.me, msg)
				//fmt.Printf("Server %d, msg: %v", rf.me, msg)
			}
			rf.mu.Unlock()
		case <-time.After(time.Until(waitUntil)):
			DPrintf("State machine time out")
			break
		}
	}
}*/
/*
func (rf *Raft) logSaveThread() {
	waitUntil := MaxTimePoint
	stateChanged := rf.stateSignal.Subscribe()
	for !rf.killed() {
		//DPrintf("LogSave")
		select {
		case newState := <-stateChanged:
			if !newState {
				return
			}
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
		case <-time.After(time.Until(waitUntil)):
			break
		}
		//DPrintf("Server log save thread %d sleep", rf.me)
	}

}
*/

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
	DPrintf("Server %d Start Agreement %v", rf.me, command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := (rf.state == LEADER)

	if !isLeader {
		return index, term, false
	}

	term = rf.currentTerm
	index = rf.logs.nextIndex()
	rf.logs.appendEntry(Entry{Command: command, Term: term, Index: index})

	rf.matchId[rf.me] = rf.getLastLogId()

	rf.leaderOperation()
	rf.persist()
	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))
	DPrintf("Start Agreement Leader %d: current Term %d, lastlog ID: %d, commitId: %d, cmd: %v", rf.me, rf.currentTerm, rf.getLastLogId(), rf.commitId, command)
	return index, term, isLeader
}

// Server call this function to snapshot the data
func (rf *Raft) Snapshot(snapshot []byte, lastIndex int, lastTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIndex < rf.lastSnapshotIndex {
		return
	}
	DPrintf("Server %d Save snapshot, raft state size: %d, log last index: %d, lastIncludedIndex: %d, lastIncludedTerm: %d", rf.me, rf.persister.RaftStateSize(), rf.logs.getLastIndex(), lastIndex, lastTerm)

	rf.lastSnapshotIndex = lastIndex
	rf.lastSnapshotTerm = lastTerm
	rf.saveStateAndSnapshot(snapshot, lastIndex, lastTerm)

}

/* The way to call CondInstallSnapshot from KV service might cause blocking. When KV service call CondInstallSnapsht,
The KV server is blocked, which means applyCh can't accept message. But CondInstallSnapshot need to acquire a lock, if send log entries
requires to get ApplyCh, then, the system is blocked.
*/
/*
func (rf *Raft) CondInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied > lastIncludedIndex {
		return false
	}
	DPrintf("Cond Install SnapShot Server %d Save snapshot, raft state size: %d, log last index: %d, lastIncludedIndex: %d, lastIncludedTerm: %d", rf.me, rf.persister.RaftStateSize(), rf.logs.getLastIndex(), lastIncludedIndex, lastIncludedTerm)
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
	return true

}
*/
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

func (rf *Raft) sendSnapshotToServices(snapshot []byte, lastIncludedId int, lastIncludedTerm int) {

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
	rf.logs = Log{}

	rf.votedFor = -1
	rf.nextId = make([]int, len(peers))
	rf.matchId = make([]int, len(peers))
	rf.hasVote = make([]bool, len(peers))
	rf.lastApplied = 0
	rf.commitId = 0

	rf.stateSignal = NewStateBroadcaster()

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.setElectionTimer()
	rf.mu.Unlock()

	DPrintf("%d Start Raft...", rf.me)

	go rf.raftStateThread2()
	//go rf.stateMachineThread()
	//go rf.logSaveThread()

	go rf.stateSignal.NotifyAll(atomic.LoadInt32(&rf.dead))

	return rf
}
