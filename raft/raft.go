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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
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

const (
	triggerAppendCommand = "triggerAppendCommand"
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
	appendEntriesChan chan *AppendEntriesArgs
	heartbeatChan     chan struct{}
	commandChan       chan string
	heartbeatRun      bool
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

	Target uint
}

// AppendEntriesReply handle reply of append entries
type AppendEntriesReply struct {
	Term    uint // current term
	Success bool // true if follower contained entry matching prevLogIndex and PrevLogTerm
}

// AppendEntries implement append entries rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	logOk := args.PrevLogIndex == 0 ||
		(args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term)

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && !logOk) {
		fmt.Printf("returning, logOk = %v , args.PrevLogIndex = %d , len(rf.log) = %d \n", logOk, args.PrevLogIndex, len(rf.log))
		rf.mu.Unlock()
		return
	}

	reply.Success = true

	rf.leader = args.LeaderID

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

	}

	if rf.state != followerState {
		rf.state = followerState
	}

	// if existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if len(rf.log) != 0 && len(args.Entries) > 0 {
		fmt.Printf("before entries , %d at : %d \n", len(rf.log), rf.me)
		if len(rf.log)-1 == args.PrevLogIndex &&
			rf.currentTerm != uint(args.PrevLogTerm) {
			fmt.Printf("before truncating, len = %d at : %d \n", len(rf.log), rf.me)
			rf.log = rf.log[:len(rf.log)-1]
			fmt.Printf("truncating , %d at : %d \n", len(rf.log), rf.me)
		}
		rf.log = append(rf.log, args.Entries...)
	} else if len(rf.log) == 0 {
		fmt.Printf("before entries at zero log , %d at : %d \n", len(rf.log), rf.me)
		rf.log = append(rf.log, args.Entries...)
		fmt.Printf("after entries at zero log , %d at : %d \n", len(rf.log), rf.me)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if uint(args.LeadersCommit) > rf.commitIndex {
		rf.commitIndex = uint(math.Min(float64(args.LeadersCommit), float64(len(rf.log))))
	} else {
		rf.commitIndex = uint(args.LeadersCommit)
	}
	rf.mu.Unlock()

	rf.heartbeatChan <- struct{}{}
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

	// Each server will vote for at most one candidate in a given term,
	// on a first-come-first-served basis (note: Sec- tion 5.4 adds an additional restriction on votes)
	if (args.Term == rf.currentTerm && rf.votedFor != -1) || (rf.votedFor == int(args.CandidateID)) {
		return
	}
	/*
		if rf.votedFor == int(args.CandidateID) {
			return
		}*/

	if len(rf.log) > 0 {
		currentLogData := rf.log[len(rf.log)-1]
		if (args.LastLogTerm < currentLogData.Term) || (currentLogData.Term != args.LastLogTerm && int(args.LastLogIndex) < len(rf.log)) {
			fmt.Println("invalid log")
			return
		}
	}
	rf.votedFor = int(args.CandidateID)

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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == leaderState
	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}
	index = len(rf.log)
	rf.log = append(rf.log, Log{Term: term, Command: command})
	term = int(rf.currentTerm)

	fmt.Println(index)
	fmt.Println(term)
	fmt.Println("is leader")

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
	rf.appendEntriesChan = make(chan *AppendEntriesArgs, 3) // make non blocking
	rf.heartbeatChan = make(chan struct{}, 3)
	rf.commandChan = make(chan string, 3)
	rf.stopHeartbeatChan = make(chan struct{}, 2)

	rand.Seed(time.Now().Unix())

	rf.matchIndex = make(map[uint]uint)
	rf.nextIndex = make(map[uint]uint)

	go rf.loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) getServerState() uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) buildAndSendAppendEntry(k int, currentTerm uint, leaderID int, prevLogIndex, prevLogTerm int, entries []Log, commitIndex uint) {
	args := AppendEntriesArgs{
		Term:          currentTerm,
		LeaderID:      leaderID,
		PrevLogIndex:  prevLogIndex,
		PrevLogTerm:   prevLogTerm,
		LeadersCommit: int(commitIndex),
		Entries:       entries,
	}

	reply := AppendEntriesReply{}

	timeoutChan := make(chan bool)
	go func(timeOutChan chan bool, reply *AppendEntriesReply) {
		ok := rf.sendAppendEntries(k, &args, reply)
		timeOutChan <- ok
	}(timeoutChan, &reply)

	select {
	case <-timeoutChan:
		if reply.Success {
			rf.appendEntriesChan <- &args
		} else {
			rf.heartbeatChan <- struct{}{}
		}
	case <-time.After(200 * time.Millisecond):
		// do nothing
	}
}

func (rf *Raft) sendHeartbeatImmediately() {
	if rf.leader != rf.me || rf.state != leaderState {
		return
	}

	for k := 0; k < len(rf.peers); k++ {
		if k == rf.me {
			continue
		}
		prevLogIndex := int(rf.nextIndex[uint(k)]) - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		go rf.buildAndSendAppendEntry(k, rf.currentTerm, rf.leader, int(prevLogIndex), prevLogTerm, []Log{}, rf.commitIndex)
	}
}

func (rf *Raft) triggerAppendEntries() {
	for k := 0; k < len(rf.peers); k++ {
		if k == rf.me {
			continue
		}

		prevLogIndex := int(rf.nextIndex[uint(k)]) - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		lastEntry := uint(math.Min(float64(len(rf.log)-1), float64(rf.nextIndex[uint(k)]-1)))

		go rf.buildAndSendAppendEntry(k, rf.currentTerm, rf.leader, int(prevLogIndex), prevLogTerm, rf.log[int(lastEntry):len(rf.log)], rf.commitIndex)
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
		case <-rf.heartbeatChan:
		case <-time.After(time.Duration(random(300, 400)) * time.Millisecond):
			rf.setState(candidateState)
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	var voteAccepted int32
	replyChan := rf.startElection()
	for rf.getServerState() == candidateState {
		select {
		case <-rf.heartbeatChan:
		case reply := <-replyChan:
			// suspect to be

			// a candidate wins an election if it receives votes from a majority of the servers in the full cluseter for same term
			// each server will vote at most 1 canditate in given term
			if reply.VoteGranted {
				//voteAccepted++
				atomic.AddInt32(&voteAccepted, int32(voteAccepted)+1)
				//fmt.Printf("vote granted at : %d, voted : %d\n", rf.me, atomic.LoadInt32(&voteAccepted))

			}

			if rf.getServerState() != candidateState {
				return
			}

			//fmt.Printf("vote accepted : %d at me : %d, len(peers) : %d \n", voteAccepted, rf.me, len(rf.peers))
			if atomic.LoadInt32(&voteAccepted) >= int32((len(rf.peers)/2)+1) {
				rf.mu.Lock()
				if rf.leader == -1 {
					rf.leader = rf.me
					rf.state = leaderState

					// transition to be leader
					// set index
					fmt.Printf("got leader %d \n", rf.me)
					rf.updateIndexForFollower()
					//rf.sendHeartbeatImmediately()
				}

				rf.mu.Unlock()
				return
			}
		case <-time.After(time.Duration(random(300, 400)) * time.Millisecond):
			return
		}
	}
}

// update matchindex and nextindex
func (rf *Raft) updateIndexForFollower() {
	for k := range rf.peers {
		if k != rf.me {
			rf.matchIndex[uint(k)] = 0
			rf.nextIndex[uint(k)] = uint(len(rf.log) + 1)
		}
	}
}

func (rf *Raft) leaderLoop() {
	go rf.runHeartbeat()
	rf.setHearbeatStatus(false)
	for rf.getServerState() == leaderState {

		select {
		case <-rf.heartbeatChan:
			// stepdown to follower
		case args := <-rf.appendEntriesChan:
			//fmt.Println("get append entries chan")
			rf.handleAppendEntriesAndQuorum(args)
		case cmd := <-rf.commandChan:
			fmt.Println("hanle command")
			rf.handleCommand(cmd)

		}
	}
}

func (rf *Raft) handleCommand(cmd string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch cmd {
	case triggerAppendCommand:
		fmt.Println("trigger append entries")
		rf.triggerAppendEntries()
	}
}

func (rf *Raft) handleAppendEntriesAndQuorum(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[uint(args.Target)] = uint(args.PrevLogIndex + len(args.Entries))
	rf.nextIndex[uint(args.Target)] = uint(args.PrevLogIndex + len(args.Entries) + 1)

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	lastIdx := uint(len(rf.log))
	for N := rf.commitIndex; N < lastIdx; N++ {
		quorum := 0
		for p := range rf.matchIndex {
			if p == uint(rf.me) {
				quorum++
				continue
			}

			val := rf.matchIndex[p]
			if val >= N {
				quorum++
			}
		}

		//fmt.Printf("quorum : %d \n", quorum)
		//fmt.Printf("committedIndex : %d \n", rf.commitIndex)
		//fmt.Printf("logTerm : %d, currentTerm : %d \n", rf.log[N-1].Term, rf.currentTerm)

		if uint(quorum) > N/2+1 {
			//fmt.Printf("len(log) = %d \n", len(rf.log))
			if N <= 0 {
				rf.commitIndex = uint(len(rf.log))
			} else {
				fmt.Printf("term = %d, currentTerm = %d \n", rf.log[N-1].Term, rf.currentTerm)
				if uint(rf.log[N-1].Term) == rf.currentTerm {
					rf.commitIndex = N
				}
			}
		}
	}
}

func (rf *Raft) setHearbeatStatus(stop bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if stop {
		rf.heartbeatRun = false
	} else {
		rf.heartbeatRun = true
	}
}

func (rf *Raft) getHeartbeatStatus() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.heartbeatRun
}

func (rf *Raft) runHeartbeat() {
	defer func() {
		fmt.Printf("stopping heartbeat at %d \n", rf.me)
	}()
	for {
		select {
		case <-rf.stopHeartbeatChan:
			fmt.Printf("heartbeat stopped at %d \n", rf.me)
			return
		case <-time.After(120 * time.Millisecond):
			rf.mu.Lock()
			rf.sendHeartbeatImmediately()
			rf.mu.Unlock()
		}
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

			//rf.sendRequestVote(i, &args, &reply)

			//replyChan <- &reply

			timeoutChan := make(chan bool)
			go func(timeOutChan chan bool, reply *RequestVoteReply) {
				ok := rf.sendRequestVote(i, &args, reply)
				timeOutChan <- ok
			}(timeoutChan, &reply)

			select {
			case <-timeoutChan:
				replyChan <- &reply
			case <-time.After(500 * time.Millisecond):
				replyChan <- &RequestVoteReply{
					VoteGranted: false,
				}
			}
		}(i, lastLogIdx, lastLogTerm, rf.currentTerm, rf.votedFor)
	}

	return replyChan

}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
