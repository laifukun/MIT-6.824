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
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	NEWSERVER = "new"
)

const (
	WIN = iota
	LOST
	TIE
	NORESULT
)

const electionTimeout = 400
const broadcastTime = 200

type ElectionResult int

type ServerState string

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logs      []LogEntry // index start from 1, the 0th entry is empty
	lastLogId int
	//newEntries bool

	currentTerm int
	state       ServerState
	votedFor    int

	commitId    int
	lastApplied int

	termHeartBeat chan int

	lastReceived time.Time
	nextId       []int // for each server, index of the next log entry to send to the server for replication
	matchId      []int //for each server, index of highest log entry know to be replicated on server
	applyCh      chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.Unlock()
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

	DPrintf("Server %d save to storage......", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Server %d Save state: %v", rf.me, rf.logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)

	e.Encode(rf.logs)
	/*
		for _, log := range rf.logs {
			e.Encode(log.Term)
			e.Encode(log.Command)
		}
	*/
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	var tempLog []LogEntry
	d.Decode(&tempLog)
	rf.logs = tempLog
	rf.lastLogId = len(rf.logs) - 1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextId[i] = rf.lastLogId + 1
	}
	/*
		for {
			var logTerm int
			var logCom interface{}
			if d.Decode(&logTerm) == nil && d.Decode(&logCom) == nil {
				rf.logs = append(rf.logs, LogEntry{Term: logTerm, Command: logCom})
			} else {
				return
			}
			DPrintf("Server %d read logs: %v", rf.me, rf.logs)
		}
	*/
	//DPrintf("Server %d read logs: %v", rf.me, rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int
	LastLogId     int
	LastLogTerm   int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntryArgs struct {
	Term           int
	LeaderId       int
	PrevLogId      int
	PrevLogTerm    int
	Entries        []LogEntry
	LeaderCommitId int
}

type AppendEntryReply struct {
	Term           int
	LogConsistency bool
	ContainEntry   bool
	Success        bool
	LastLogId      int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("Server %d received voting request from candidate %d", rf.me, args.CandidateId)

	DPrintf("   Server %d, Last Log Term: %d, current Term %d, votedFor: %d, lastLogIndex: %d", rf.me, rf.logs[rf.lastLogId].Term, rf.currentTerm, rf.votedFor, rf.lastLogId)

	DPrintf("   Candidate %d, candidate term: %d, Last Log Term: %d, LastLogIndex: %d,", args.CandidateId, args.CandidateTerm, args.LastLogTerm, args.LastLogId)
	rf.mu.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log compare
	logIsOk := (args.LastLogTerm > rf.logs[rf.lastLogId].Term) || (rf.logs[rf.lastLogId].Term == args.LastLogTerm && args.LastLogId >= rf.lastLogId)

	// term compare
	if args.CandidateTerm > rf.currentTerm {
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.currentTerm = args.CandidateTerm
		DPrintf("   Server %d conver to Follower.", rf.me)
	}

	if args.CandidateTerm == rf.currentTerm {
		if logIsOk && rf.votedFor == -1 {
			rf.state = FOLLOWER
			rf.votedFor = args.CandidateId
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = args.CandidateTerm == rf.currentTerm && rf.votedFor == args.CandidateId
}

//AppendEntries RPC handler

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	DPrintf("Server %d Received Entries/HeartBeat from Leader: %d", rf.me, args.LeaderId)
	DPrintf("    Server Term: %d, Leader term, %d", rf.currentTerm, args.Term)
	DPrintf("    Server %d last log ID: %d, Received prevLogId: %d", rf.me, rf.lastLogId, args.PrevLogId)
	rf.mu.Unlock()
	//DPrintf("    Server %d logs: %v, appended logs, %v", rf.me, rf.logs, args.Entries)
	//DPrintf("    Server %d Log Term: %d, Received prevLog Term: %d", rf.me, rf.logs[args.PrevLogId].Term, args.PrevLogTerm)
	//rf.termHeartBeat <- args.Term
	rf.mu.Lock()

	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LastLogId = rf.lastLogId

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	}

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.lastReceived = time.Now()

	if args.PrevLogId > rf.lastLogId {
		return
	}

	if args.PrevLogId >= 0 && rf.logs[args.PrevLogId].Term != args.PrevLogTerm {
		return
	}

	reply.Success = true

	id := args.PrevLogId

	for i := 0; i < len(args.Entries); i++ {
		id += 1
		entry := args.Entries[i]

		if rf.lastLogId >= id {
			if rf.logs[id].Term == entry.Term {
				continue
			}
			rf.logs = rf.logs[0 : id-1]
			rf.lastLogId = len(rf.logs) - 1
		}

		for i < len(args.Entries) {
			entry := args.Entries[i]
			rf.logs = append(rf.logs, entry)
			i += 1
		}

	}
	rf.lastLogId = len(rf.logs) - 1
	reply.LastLogId = rf.lastLogId

	if rf.commitId < args.LeaderCommitId {
		rf.commitId = args.LeaderCommitId
	}
	rf.lastReceived = time.Now()
	//go rf.persist()
	/*
		DPrintf("     Server %d start term match", rf.me)
		if !rf.TermMatch(args, reply) {
			DPrintf("     Server %d term match fail", rf.me)
			return
		}

		DPrintf("     Server %d after term match", rf.me)
		if rf.LogConsistency(args, reply) {
			if args.Entries != nil {
				rf.mu.Lock()
				rf.logs = append(rf.logs, args.Entries...)
				rf.lastLogId = len(rf.logs) - 1
				rf.mu.Unlock()
			}
			if !rf.CommitMatch(args, reply) {
				rf.applyToStateMachine()
			}
		}

		DPrintf("    Server %d return from entry append, Log Consistency, %v, ContainEntry: %v", rf.me, reply.LogConsistency, reply.ContainEntry)
		return
	*/
}

func (rf *Raft) TermMatch(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//reply.ContainEntry = false
		//reply.LogConsistency = false
		reply.Term = rf.currentTerm
		return false
	}
	DPrintf("     Server %d, current term: %d", rf.me, rf.currentTerm)
	rf.lastReceived = time.Now()
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}

	return true
}
func (rf *Raft) LogConsistency(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastLogId < args.PrevLogId {
		reply.ContainEntry = false
		reply.LogConsistency = false
		return false
	}

	if rf.lastLogId >= args.PrevLogId && rf.logs[args.PrevLogId].Term != args.PrevLogTerm {
		rf.logs = rf.logs[0:args.PrevLogId]
		rf.lastLogId = args.PrevLogId - 1
		reply.ContainEntry = false
		reply.LogConsistency = false
		return false
	}
	if rf.lastLogId == args.PrevLogId && rf.logs[args.PrevLogId].Term == args.PrevLogTerm {
		reply.LogConsistency = true
		reply.ContainEntry = true
		return true
	}
	if rf.lastLogId > args.PrevLogId && rf.logs[args.PrevLogId].Term == args.PrevLogTerm {
		//rf.logs = rf.logs[0:args.PrevLogId]
		//rf.lastLogId = args.PrevLogId - 1
		reply.ContainEntry = true
		reply.LogConsistency = false
		return false
	}

	DPrintf("    Server %d Log consistency return", rf.me)
	return false
}

func (rf *Raft) CommitMatch(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderCommitId > rf.commitId {
		if args.LeaderCommitId < rf.lastLogId {
			rf.commitId = args.LeaderCommitId
		} else {
			rf.commitId = rf.lastLogId
		}
		DPrintf("    Server %d, New Last Log %d, New Commited ID: %d", rf.me, rf.lastLogId, rf.commitId)
		return false

	}
	DPrintf("      Server %d return from commit match", rf.me)
	return true
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

func (rf *Raft) sendToVote(server int) bool {

	rf.mu.Lock()

	DPrintf("Candidate %d sent vote to server %d: last Log Id: %d", rf.me, server, rf.lastLogId)
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogId:     rf.lastLogId,
		LastLogTerm:   rf.logs[rf.lastLogId].Term,
	}
	rf.mu.Unlock()
	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}
	DPrintf("      Server %d Reply is %v, VoteGranted: %v", server, ok, reply.VoteGranted)
	rf.mu.Lock()

	if !reply.VoteGranted && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	rf.mu.Unlock()
	return reply.VoteGranted

}

// send heart beat as a leader

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	//DPrintf("Send logs to server %d..............", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendLogEntries(server int) bool {

	//rf.persist()
	//logConsistency := false
	rf.mu.Lock()
	lastLogId := rf.lastLogId
	nextIdLow := rf.matchId[server] + 1
	nextIdHigh := lastLogId + 1
	//rf.nextId[server] = lastLogId + 1

	DPrintf("Send logs to server %d", server)
	//fmt.Println()
	//firstSentTime := time.Now()
	//for nextIdLow <= nextIdHigh {

	//	if time.Since(firstSentTime) > 0.9*broadcastTime*time.Millisecond {
	//		return false
	//	}

	//testLogId := (nextIdLow + nextIdHigh) / 2
	testLogId := rf.nextId[server]
	if testLogId-1 > len(rf.logs)-1 {
		return false
	}
	DPrintf("Server %d TestLogId %d, High, %d, Low, %d,  Last LogId: %d", server, testLogId, nextIdHigh, nextIdLow, lastLogId)

	args := AppendEntryArgs{
		Term:           rf.currentTerm,
		LeaderId:       rf.me,
		PrevLogId:      testLogId - 1,
		PrevLogTerm:    rf.logs[testLogId-1].Term,
		LeaderCommitId: rf.commitId,
	}

	if testLogId <= lastLogId {
		args.Entries = rf.logs[testLogId : lastLogId+1]
	}

	rf.mu.Unlock()

	var reply AppendEntryReply
	ok := rf.sendAppendEntry(server, &args, &reply)
	if !ok {
		DPrintf("Sent to server %d error", server)
		return false
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()
	DPrintf("    Reply: %v, Server %d Test Log Id %d, ContainEntry: %v, Log Consistency: %v", reply.Success, server, testLogId, reply.ContainEntry, reply.LogConsistency)

	if reply.Term > rf.currentTerm {

		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1

	} else {
		if reply.Success {
			if rf.matchId[server] < reply.LastLogId {
				rf.matchId[server] = reply.LastLogId
				rf.updateLeaderCommitId()
			}
			rf.nextId[server] = rf.matchId[server] + 1
		} else {
			if rf.nextId[server] > 1 {
				rf.nextId[server]--
			}
			if rf.nextId[server] > reply.LastLogId+1 {
				rf.nextId[server] = reply.LastLogId + 1
			}
		}

	}

	return true
	/*
		if reply.LogConsistency {
			rf.mu.Lock()

			rf.matchId[server] = lastLogId
			rf.nextId[server] = lastLogId + 1
			rf.mu.Unlock()
			return true
		}

		if !reply.ContainEntry {
			rf.mu.Lock()
			rf.nextId[server]--
			rf.mu.Unlock()
			return false
			//nextIdLow = testLogId + 1
		}
	*/
	//else {
	//nextIdHigh = testLogId
	//}

	/*
		if !reply.Success {
			rf.mu.Lock()
			//defer rf.mu.Unlock()
			DPrintf("Leader %d, to server  %d, Reply not success, nextID %d, reply Term: %d, current Term: %d", rf.me, server, rf.nextId[server], reply.Term, rf.currentTerm)
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.mu.Unlock()
				return false
			}

			if rf.state == LEADER {
				nextId = testLogId
				replySuccess = false
			} else {
				rf.mu.Unlock()
				return false
			}
			rf.mu.Unlock()

		} else {
			//rf.mu.Lock()
			//rf.nextId[server] = lastLogId + 1
			matchId = testLogId + 1
			replySuccess = true
			//rf.mu.Unlock()

		}
		if matchId >= nextId {
			rf.mu.Lock()
			rf.matchId[server] = testLogId - 1
			rf.nextId[server] = testLogId
			rf.mu.Unlock()
			return true
		}
	*/

	//}

	//	return false

}

// operate as leader
func (rf *Raft) performLeaderOperation() {

	//go rf.persist()
	appendResult := make(chan bool)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			appendResult <- rf.sendLogEntries(server)
		}(server)

	}

	go rf.handleLogReplication(appendResult)
	//rf.applyToStateMachine()

	//rf.mu.Unlock()
	//DPrintf(" **********************************Start to update commitID***********")
	//DPrintf("End entry sending")
}

func (rf *Raft) handleLogReplication(appendResult chan bool) {
	successCount := 1
	finishedReturn := 1
	for {
		select {
		case sR := <-appendResult:
			finishedReturn++
			if sR {
				successCount++
			}
			if successCount > len(rf.peers)/2 || finishedReturn == len(rf.peers) {
				go rf.updateLeaderCommitId()
				return
			}
		case <-time.After(broadcastTime * time.Millisecond):
			return

		}
	}
}
func (rf *Raft) updateLeaderCommitId() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	N := rf.lastLogId
	DPrintf("Leader %d, Current Term: %d, Logs term: %d", rf.me, rf.currentTerm, rf.logs[N].Term)
	for N > rf.commitId && rf.logs[N].Term == rf.currentTerm {
		DPrintf("Leader %d, Current Term: %d, Logs term: %d", rf.me, rf.currentTerm, rf.logs[N].Term)
		matchCount := 0
		for server, _ := range rf.peers {
			if rf.matchId[server] >= N {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			rf.commitId = N
			rf.applyToStateMachine()
			return
		}
		N--
	}
	DPrintf("Leader %d: Match ID: %v,commited ID: %d", rf.me, rf.matchId, rf.commitId)

}

func (rf *Raft) applyToStateMachine() {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	for rf.commitId > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
	}
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
	index := -1
	term := -1
	rf.mu.Lock()

	if rf.state != LEADER {
		rf.mu.Unlock()
		return index, term, false
	}
	isLeader := (rf.state == LEADER)

	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
	rf.lastLogId++
	index = rf.lastLogId

	for i := 0; i < len(rf.peers); i++ {
		rf.nextId[i] = rf.lastLogId + 1
	}
	rf.matchId[rf.me] = rf.lastLogId
	//DPrintf("   Leader %d, logs: %v", rf.me, rf.logs)

	DPrintf("Start Agreement Leader %d: current Term %d, lastlog ID: %d, commitId: %d", rf.me, rf.currentTerm, rf.lastLogId, rf.commitId)
	//DPrintf("Logs in Leader %d: %v", rf.me, rf.logs)
	//rf.newEntries <- true
	//go rf.logReplication()
	// Your code here (2B).

	rf.mu.Unlock()
	go rf.persist()
	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) KickOffLeaderElection() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	DPrintf("Candidate %d Kick off election with Term %d", rf.me, rf.currentTerm)

	voteResult := make(chan bool)

	for server, _ := range rf.peers {

		if server == rf.me {
			continue
		}
		//DPrintf("Voting: Candidate %d: Send to %d: ", rf.me, server)
		go func(server int) {
			voteResult <- rf.sendToVote(server)
		}(server)
	}

	rf.handleVoteResult(voteResult)
}

func (rf *Raft) handleVoteResult(voteResult chan bool) {
	voteCount := 1
	finishedVote := 1
	for {
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}
		select {
		case vR := <-voteResult:
			if vR {
				voteCount++
				finishedVote++
			} else {
				finishedVote++
			}
			if voteCount > len(rf.peers)/2 {
				rf.TransitionToLeader()
				return
			}
			if finishedVote == len(rf.peers) && voteCount < len(rf.peers)/2 {
				rf.TransitionToFollower()
				return
			}

		case <-time.After(electionTimeout * time.Millisecond):
			return

		}
	}
}

func (rf *Raft) monitorState() {

	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		state := rf.state
		DPrintf("Server %d is %s at Term %d", rf.me, state, rf.currentTerm)
		rf.mu.Unlock()

		switch state {
		case FOLLOWER:

			rf.mu.Lock()
			timeOut := (time.Since(rf.lastReceived) > electionTimeout*time.Millisecond)
			rf.mu.Unlock()

			if timeOut {
				DPrintf("Server %d: no heart Beat from Leader, convert to candidate", rf.me)
				rf.TransitionToCandidate()
			}
			time.Sleep(electionTimeout * time.Millisecond)

			/*
				select {
				case leaderTerm := <-rf.termHeartBeat:

					rf.mu.Lock()
					if leaderTerm != rf.currentTerm {
						rf.currentTerm = leaderTerm
					}
					rf.mu.Unlock()
					//time.Sleep(electionTimeout / 2 * time.Millisecond)
					DPrintf("Server %d received heart beat from leader", rf.me)

				case <-time.After(electionTimeout * time.Millisecond):
					DPrintf("Leader is dead, Server %d with Term %d tranisition to candidate", rf.me, rf.currentTerm)
					go rf.TransitionToCandidate()
					return
				}*/
		case CANDIDATE:

			rf.mu.Lock()
			if rf.state == FOLLOWER {

				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			rf.KickOffLeaderElection()

			rand.Seed(time.Now().UnixNano())
			n := rand.Intn(electionTimeout)
			time.Sleep(time.Duration(n) * time.Millisecond)
			/*
				switch electionResult {
				case WIN:

					//rf.state = LEADER
					rf.TransitionToLeader()
					//return
				case LOST:
					rf.TransitionToFollower()
					//return
				default:
					rf.TransitionToCandidate()
				}
			*/
			/*
				select {
				case leaderTerm := <-rf.termHeartBeat:

					rf.mu.Lock()
					if leaderTerm >= rf.logs[rf.lastLogId].Term || rf.state == FOLLOWER {
						rf.currentTerm = leaderTerm
						rf.mu.Unlock()
						go rf.TransitionToFollower()
						return
					}
					rf.mu.Unlock()

				default:

					DPrintf("Server %d start election", rf.me)
					electionResult := rf.KickOffLeaderElection()
					switch electionResult {
					case WIN:
						go rf.TransitionToLeader()
						return
					case LOST:
						go rf.TransitionToFollower()
						return
					}

				}
			*/

		case LEADER:

			DPrintf("Send Log Entries/Heart Beat.......")
			go rf.performLeaderOperation()
			time.Sleep(broadcastTime * time.Millisecond)
		/*
			select {
			case leaderTerm := <-rf.termHeartBeat:
				rf.mu.Lock()

				if leaderTerm > rf.currentTerm || rf.state == FOLLOWER {
					rf.mu.Unlock()
					go rf.TransitionToFollower()
					return
				}
				rf.mu.Unlock()

			default:

				DPrintf("Send Log Entries/Heart Beat.......")
				go rf.logReplication()
				time.Sleep(broadcastTime * time.Millisecond)

			}
		*/
		default:
			rf.TransitionToFollower()
		}
	}
}

/*
func (rf *Raft) MonitorHeartBeat() {

	for {
		rf.mu.Lock()
		if rf.state != FOLLOWER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		select {
		case <-rf.termHeartBeat:

			time.Sleep(electionTimeout / 2 * time.Millisecond)

		case <-time.After(electionTimeout * time.Millisecond):
			//if rf.killed() {
			//	return
			//}
			DPrintf("Leader is dead, Server %d with Term %d tranisition to candidate", rf.me, rf.currentTerm)
			defer rf.TransitionToCandidate()
			return
		}
		if rf.killed() {
			return
		}
	}

}
*/

func (rf *Raft) TransitionToFollower() {

	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.mu.Unlock()

	DPrintf("Server %d become Follower", rf.me)
	//go rf.MonitorHeartBeat()
	//go rf.monitorState()

}
func (rf *Raft) TransitionToCandidate() {

	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.mu.Unlock()
	DPrintf("Server %d becomes candidate", rf.me)
	//go rf.monitorState()
	/*
		go func() {
			for {
				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(electionTimeout - broadcastTime)
				time.Sleep(time.Duration(n+broadcastTime) * time.Millisecond)

				rf.mu.Lock()
				if rf.state != CANDIDATE {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				select {
				case leaderTerm := <-rf.termHeartBeat:

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if leaderTerm >= rf.currentTerm || rf.state == FOLLOWER {
						//rf.mu.Unlock()
						go rf.TransitionToFollower()
						return
					}
					//rf.mu.Unlock()

				default:

					electionResult := rf.KickOffLeaderElection()
					switch electionResult {
					case WIN:
						go rf.TransitionToLeader()
						return
					case LOST:
						go rf.TransitionToFollower()
						return
					}

				}
				if rf.killed() {
					return
				}
			}
		}()
	*/
}

func (rf *Raft) TransitionToLeader() {

	rf.mu.Lock()

	rf.state = LEADER

	//rf.votedFor = rf.me

	for i := 0; i < len(rf.nextId); i++ {
		rf.nextId[i] = rf.lastLogId + 1
		//rf.matchId[i] = 0
	}
	rf.matchId[rf.me] = rf.lastLogId
	rf.mu.Unlock()
	//go rf.monitorState()
	//rf.newEntries <- true
	DPrintf("Server %d becomes new leader", rf.me)

	return

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
	rf.logs = append(rf.logs, LogEntry{Term: -1, Command: -1})
	rf.lastLogId = 0

	rf.votedFor = -1
	rf.nextId = make([]int, len(peers))
	rf.matchId = make([]int, len(peers))
	rf.lastApplied = 0
	rf.commitId = 0

	rf.lastReceived = time.Now()
	rf.termHeartBeat = make(chan int)

	rf.applyCh = applyCh
	DPrintf("%d Start program...", rf.me)
	//go rf.TransitionToFollower()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.monitorState()

	return rf
}
