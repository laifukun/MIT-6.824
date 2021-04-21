package raft

import (
	//"strconv"
	//"math/rand"
	"log"
	"time"
	//"os"
)

// Debugging
const Debug = 0
//var Filename string = strconv.Itoa(rand.Int())

//var f, err = os.Create("./res/"+Filename)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.SetOutput(f)
		log.Printf(format, a...)
	}
	return
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
	Entries        []Entry
	LeaderCommitId int
}

type AppendEntryReply struct {
	Term      int
	XTerm     int //Term of the conflit
	XIndex    int // First Index of the conflit term
	Success   bool
}

type SnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type SnapshotReply struct {
	Term int
}

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
	CommandValid      bool
	Command           interface{}
	CommandIndex      int
	CommandTerm       int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot []byte
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	NEWSERVER = "new"
)

const StateMachineUpdateBackoff = 1000
const ElectionTimeout = 300
const NetworkTimeout = 300
const BroadcastTime = 100

var MaxTimePoint = time.Unix(10000000000, 0)

type ServerState string
