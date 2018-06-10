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
	"math/rand"
	"sync"
	"time"

	"github.com/yogyrahmawan/learn_distributed_system/labrpc"
)

// import "bytes"
// import "labgob"

const (
	followerState uint = iota
	candidateState
	leaderState
)

//
// ApplyMsg as each Raft peer becomes aware that successive log entries are
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

// Log represent raft log
type Log struct {
	Term    int
	Command interface{}
}

//
// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persitent state on all servers
	currentTerm uint
	votedFor    int
	log         []Log
	leader      int
	state       uint

	// volatile state on all servers
	commitIndex uint
	lastApplied uint

	// volatile state on leaders
	nextIndex  map[uint]uint
	matchIndex map[uint]uint

	// additional data goes here
	heartbeatRun      bool
	appendEntriesChan chan *AppendEntriesArgs
	leaderChan        chan struct{}
	stopHeartbeatChan chan struct{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.leader == rf.me
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
}

//
// restore previously persisted state.
//
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

// AppendEntriesArgs invoked by leader to replicate log entries
// Also used as heartbeat
type AppendEntriesArgs struct {
	Term         uint // leader's term
	LeaderID     int  // so follower can redirect clients
	PrevLogIndex int

	PrevLogTerm   int
	Entries       []Log
	LeadersCommit int // leaders commit index
}

// AppendEntriesReply handle reply of append entries
type AppendEntriesReply struct {
	Term    uint // current term
	Success bool // true if follower contained entry matching prevLogIndex and PrevLogTerm
}

// AppendEntries implement append entries rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.leader = args.LeaderID

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

	}
	rf.state = followerState
	rf.mu.Unlock()

	rf.appendEntriesChan <- args
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint
	CandidateID  uint
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint
	VoteGranted bool
}

//
// RequestVote example RequestVote RPC handler.
// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	if rf.votedFor > 0 && rf.votedFor != int(args.CandidateID) {
		return
	}

	// check whether is log up to date
	if len(rf.log) != 0 {
		currentLogData := rf.log[len(rf.log)-1]
		if (currentLogData.Term != args.LastLogTerm) || (currentLogData.Term == args.LastLogTerm && int(args.LastLogIndex) != len(rf.log)-1) {
			return
		}
	}

	reply.VoteGranted = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Start the service using Raft (e.g. a k/v server) wants to start
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// Kill the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// Make , the service or tester wants to create a Raft server. the ports
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
	rf.state = followerState
	rf.leader = -1
	// Your initialization code here (2A, 2B, 2C).
	rf.appendEntriesChan = make(chan *AppendEntriesArgs)
	rf.stopHeartbeatChan = make(chan struct{}, 2)
	rand.Seed(time.Now().Unix())
	go rf.loop()
	//go rf.sendHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) getServerState() uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) sendHeartbeatImmediately() {
	if rf.leader != rf.me || rf.state != leaderState {
		return
	}

	//done := make(chan int, len(rf.peers))
	//var wg sync.WaitGroup
	for k := 0; k < len(rf.peers); k++ {
		if k == rf.me {
			continue
		}
		prevLogIndex, prevLogTerm := -1, -1
		if len(rf.log) > 0 {
			prevLogIndex = len(rf.log) - 1
			prevLogTerm = rf.log[len(rf.log)-1].Term
		}

		go func(k int, currentTerm uint, leaderID int, prevLogIndex, prevLogTerm int) {
			//defer wg.Done()
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,

				// TODO entries and leaders commit
			}

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(k, &args, &reply)
			//if reply.

		}(k, rf.currentTerm, rf.leader, prevLogIndex, prevLogTerm)
	}

}

func (rf *Raft) setState(state uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) loop() {
	for {
		switch rf.getServerState() {
		case leaderState:
			rf.leaderLoop()
		case candidateState:
			rf.candidateLoop()
		case followerState:
			rf.followerLoop()
		}
	}
}

func (rf *Raft) followerLoop() {
	for {
		select {
		case <-rf.appendEntriesChan:
		case <-time.After(time.Duration(random(400, 500)) * time.Millisecond):
			rf.setState(candidateState)
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	voteAccepted := 0
	replyChan := rf.startElection()
	for rf.getServerState() == candidateState {
		select {
		case <-rf.appendEntriesChan:
		case reply := <-replyChan:

			// a candidate wins an election if it receives votes from a majority of the servers in the full cluseter for same term
			// each server will vote at most 1 canditate in given term
			if reply.VoteGranted {
				voteAccepted++
			}

			if voteAccepted >= (len(rf.peers)/2)+1 {
				rf.mu.Lock()
				if rf.leader == -1 {
					rf.leader = rf.me
					rf.state = leaderState
					rf.sendHeartbeatImmediately()
				}

				rf.mu.Unlock()
			}
		case <-time.After(time.Duration(random(400, 500)) * time.Millisecond):
			return
		}
	}
}

func (rf *Raft) leaderLoop() {
	for rf.getServerState() == leaderState {
		rf.mu.Lock()
		rf.sendHeartbeatImmediately()
		rf.mu.Unlock()
		time.Sleep(120 * time.Millisecond)

	}
}

func (rf *Raft) startElection() chan *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidateState

	replyChan := make(chan *RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			replyChan <- &RequestVoteReply{
				Term:        rf.currentTerm,
				VoteGranted: true,
			}
			continue
		}

		lastLogIdx, lastLogTerm := -1, -1
		if len(rf.log) != 0 {
			lastLogIdx = len(rf.log) - 1
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		go func(i, lastLogIdx, lastLogTerm int, currentTerm uint, votedFor int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  uint(votedFor),
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}

			reply := RequestVoteReply{}

			rf.sendRequestVote(i, &args, &reply)

			replyChan <- &reply
		}(i, lastLogIdx, lastLogTerm, rf.currentTerm, rf.votedFor)
	}

	return replyChan

}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
