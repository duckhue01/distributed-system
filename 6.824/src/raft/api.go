package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logRecord struct {
	statement string
	term      int
}

// RaftState captures the state of a Raft node: Follower, Candidate, Leader,
// or Shutdown.
type state int

const (
	// Follower is the initial state of a Raft node.
	Follower state = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	// Shutdown
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// rwLock    sync.RWMutex
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile state
	commitIndex int // Index of hightest log entry know to be committed
	lastApplied int // Index of hightest log entry applied to state machine

	// Persistent state
	currentTerm int
	votedFor    int
	log         []logRecord

	// timeout
	electionTimeout time.Duration

	// lastHeartbeat time.Time
	heartbeatCh chan interface{}
	leaderCh    chan interface{}
	followerCh  chan interface{}

	state state
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
	if rf.state == Follower {
		rf.heartbeatCh <- struct{}{}
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debug(dVote, "RequestVote Rejected to S%d", args.CandidateId)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// If votedFor is null or candidateld, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.debug(dVote, "RequestVote Accepted to S%d", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PevLogTerm   int
	// entries []
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.debug(dTimer, "Received Heartbeat from S%d with Term %d", args.LeaderId, args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.heartbeatCh <- struct{}{}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Success = true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	state := rf.state
	rf.mu.Unlock()

	return term, state == Leader
}

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }
