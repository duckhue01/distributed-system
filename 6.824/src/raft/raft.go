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
	//	"bytes"

	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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
func (rf *Raft) broadcastRequestVote() {
	votes := 1
	repCh := make(chan RequestVoteReply, len(rf.peers))

	for i := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(server int, repCh chan<- RequestVoteReply) {
			// send request in parallel and keep information about voted receivers
			// increase term when timeout elapsed
			// only send vote for havent voted yet client
			rep := RequestVoteReply{}

			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()

			ok := rf.peers[server].Call("Raft.RequestVote", &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: -1,
				LastLogTerm:  -1,
			}, &rep,
			)

			if ok {
				repCh <- rep
			}

		}(i, repCh)

	}

	// rf.debug(dVote, "S%d RequestVote to S%d with Term %d", rf.me, server, rf.currentTerm)

	for rep := range repCh {
		if rep.VoteGranted {
			votes++
		}

		rf.mu.Lock()
		if rep.Term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = rep.Term
		}

		rf.mu.Unlock()

		if votes >= (len(rf.peers)+1)/2 {
			rf.leaderCh <- struct{}{}
			break
		}
	}

	// rf.debug(dVote, "S%d Received %d vote(s) over %d", rf.me, votes, len(rf.peers))

	// return ok
}

func (rf *Raft) broadcastHeartbeat() {
	repCh := make(chan AppendEntriesReply, len(rf.peers)-1)
	// defer close(repCh)
	for i := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(server int, repCh chan<- AppendEntriesReply) {
			// send request in parallel and keep information about voted receivers
			// increase term when timeout elapsed
			// only send vote for havent voted yet client

			defer func() {
				if recover() != nil {
					rf.debug(dTimer, "Channel's been closed")
				}
			}()
			rep := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: i,
				// PevLogTerm:   0,
				// LeaderCommit: 0,
			}, &rep)

			if ok {
				repCh <- rep
			}

		}(i, repCh)

	}

	for rep := range repCh {
		if rep.Term > rf.currentTerm {
			rf.debug(dTimer, "Term is stale => Convert to Follower")
			rf.mu.Lock()
			rf.state = Follower
			rf.followerCh <- struct{}{}
			rf.votedFor = -1
			rf.currentTerm = rep.Term
			rf.mu.Unlock()
			break
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			t := time.NewTicker(rf.electionTimeout)
			defer t.Stop()
			select {
			case <-rf.heartbeatCh:
				t.Reset(rf.electionTimeout)
				rf.debug(dTimer, "Reset Timeout")

			case <-t.C:
				rf.debug(dTimer, "Election Timeout")
				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				rf.debug(dTimer, "Become Candidate")
			}

		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
			rf.mu.Unlock()

			go rf.broadcastRequestVote()

			select {

			case <-time.After(rf.electionTimeout):
				continue

			case <-rf.heartbeatCh:
				rf.state = Follower
				rf.debug(dTimer, "Become Follower")

			case <-rf.leaderCh:
				rf.mu.Lock()
				rf.state = Leader
				rf.mu.Unlock()
				rf.debug(dLeader, "Become Leader")
			}

		case Leader:
			done := false
			for !done {
				rf.debug(dTimer, "Checking Heartbeat")
				go rf.broadcastHeartbeat()

				select {
				case <-time.After(50 * time.Millisecond):
					continue
				case <-rf.followerCh:
					done = true
				}
			}

		}
	}

	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.heartbeatCh = make(chan interface{})
	rf.leaderCh = make(chan interface{})
	rf.followerCh = make(chan interface{})

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
