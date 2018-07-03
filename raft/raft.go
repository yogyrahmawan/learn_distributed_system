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
	lastApplied uint32 // for atomic support

	// volatile state on leaders
	nextIndex  map[uint]uint
	matchIndex map[uint]uint

	// additional data goes here
	heartbeatChan     chan struct{}
	commandChan       chan string
	heartbeatRun      bool
	stopHeartbeatChan chan struct{}
	applyChan         chan ApplyMsg
	requestVoteChan   chan struct{}
	commitChan        chan struct{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.getLeader() == rf.me
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

	CommitIndex int
}

// AppendEntries implement append entries rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	logOk := (args.PrevLogIndex == 0) ||
		(args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) &&
			args.PrevLogTerm == rf.log[args.PrevLogIndex-1].Term)

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.CommitIndex = int(rf.commitIndex)

	if args.Term < rf.currentTerm || (args.Term >= rf.currentTerm && !logOk) {
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
		if rf.heartbeatRun {
			rf.stopHeartbeatChan <- struct{}{}
		}
	}
	rf.heartbeatChan <- struct{}{}

	// if existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if len(rf.log) > 0 && len(args.Entries) > 0 {
		index := args.PrevLogIndex + 1
		var newLog []Log
		for i, entry := range args.Entries {
			if index > len(rf.log) {
				newLog = args.Entries[i:]
				break
			}

			if rf.log[index-1].Term != entry.Term {
				rf.log = rf.log[:index-1]
				newLog = args.Entries[i:]
				break
			}
			index++
		}

		rf.log = append(rf.log, newLog...)
	} else if len(rf.log) == 0 {
		rf.log = append(rf.log, args.Entries...)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if uint(args.LeadersCommit) > rf.commitIndex {
		rf.commitIndex = uint(math.Min(float64(args.LeadersCommit), float64(len(rf.log))))

	}
	rf.mu.Unlock()
	rf.commitChan <- struct{}{}
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	// If the logs end with the same term,
	// then whichever log is longer is more up-to-date.

	shouldVote := false
	if rf.getCurrentTerm() <= args.Term {
		//Candidate in higher term
		lenLog := rf.getLenOfLog()
		if lenLog > 0 {

			lastLog := rf.getLogAt(lenLog - 1)
			if lastLog.Term > args.LastLogTerm {
				shouldVote = false
			} else if lastLog.Term == args.LastLogTerm &&
				lenLog > args.LastLogIndex {
				shouldVote = false
			} else {
				shouldVote = true
			}
		} else {
			if args.LastLogIndex < lenLog {
				shouldVote = false
			} else {
				shouldVote = true
			}
		}

		if rf.getCurrentTerm() == args.Term {
			if rf.getVotedFor() == -1 {
				shouldVote = true
			} else {
				shouldVote = false
			}
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.getCurrentTerm()
	if shouldVote {
		reply.VoteGranted = true
		rf.setVotedFor(int(args.CandidateID))
		rf.requestVoteChan <- struct{}{}
	}
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
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
	rf.log = append(rf.log, Log{Term: int(rf.currentTerm), Command: command})
	index = len(rf.log)
	term = int(rf.currentTerm)

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
	rf.heartbeatChan = make(chan struct{}, 3)
	rf.commandChan = make(chan string, 3)
	rf.stopHeartbeatChan = make(chan struct{}, 2)
	rf.applyChan = applyCh
	rf.requestVoteChan = make(chan struct{}, 2)
	rf.commitChan = make(chan struct{}, 5)

	rand.Seed(time.Now().UTC().UnixNano())

	rf.matchIndex = make(map[uint]uint)
	rf.nextIndex = make(map[uint]uint)

	go rf.loop()
	go rf.runApplyMessage()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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
	rf.setVotedFor(-1)
	for {
		select {
		case <-rf.requestVoteChan:
		case <-rf.heartbeatChan:
			DPrintf("follower loop got heartbeat at %d \n", rf.me)
		case <-time.After(time.Duration(random(500, 900)+200) * time.Millisecond):
			rf.setState(candidateState)
			rf.setLeader(-1)
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	var voteAccepted int32
	rf.incrementTerm()
	replyChan := rf.startElection()
	for rf.getServerState() == candidateState {
		select {
		case <-rf.requestVoteChan:
		case <-rf.heartbeatChan:
		case reply := <-replyChan:

			// a candidate wins an election if it receives votes from a majority of the servers in the full cluseter for same term
			// each server will vote at most 1 canditate in given term

			if reply.VoteGranted {
				atomic.AddInt32(&voteAccepted, int32(voteAccepted)+1)
			} else {
				rf.setCandidateCurrentTermByPeer(reply.Term)
			}

			if rf.getServerState() != candidateState {
				return
			}

			if atomic.LoadInt32(&voteAccepted) >= int32((len(rf.peers)/2)+1) {
				atomic.StoreInt32(&voteAccepted, 0)
				rf.mu.Lock()
				if rf.leader == -1 {
					rf.leader = rf.me
					rf.state = leaderState
					// transition to be leader
					// set index
					rf.updateIndexForFollower()
				}

				rf.mu.Unlock()
				return
			}
		case <-time.After(time.Duration(random(500, 900)+200) * time.Millisecond):
			return
		}
	}
}

func (rf *Raft) leaderLoop() {
	go rf.runHeartbeat()
	rf.setHearbeatStatus(false)

	for rf.getServerState() == leaderState {
		select {
		case <-rf.requestVoteChan:
		case <-rf.heartbeatChan:
		case cmd := <-rf.commandChan:
			rf.handleCommand(cmd)
		}
	}
}

func (rf *Raft) startElection() chan *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = -1
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

		lastLogIdx, lastLogTerm := 0, 0
		if len(rf.log) > 0 {
			lastLogIdx = len(rf.log)
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

			timeoutChan := make(chan bool)
			go func(timeOutChan chan bool, args *RequestVoteArgs, reply *RequestVoteReply) {
				ok := rf.sendRequestVote(i, args, reply)
				timeOutChan <- ok
			}(timeoutChan, &args, &reply)

			select {
			case <-timeoutChan:
				replyChan <- &reply
			case <-time.After(100 * time.Millisecond):
				replyChan <- &RequestVoteReply{
					VoteGranted: false,
					Term:        currentTerm,
				}
				return
			}
		}(i, lastLogIdx, lastLogTerm, rf.currentTerm, rf.votedFor)
	}

	return replyChan

}

func (rf *Raft) handleCommand(cmd string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch cmd {
	case triggerAppendCommand:
		rf.triggerAppendEntries()
	}
}

func (rf *Raft) runApplyMessage() {
	for {
		<-rf.commitChan
		rf.sendApplyMessage(rf.getCurrentLog(), uint32(rf.getCommitIndex()))
	}
}

func (rf *Raft) getCurrentLog() []Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.log
}

func (rf *Raft) getCommitIndex() uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.commitIndex
}

func (rf *Raft) sendApplyMessage(log []Log, commitIndex uint32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	idx := rf.lastApplied
	for i := idx; i < commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: int(i + 1),
			Command:      log[i].Command,
		}

		rf.applyChan <- applyMsg
		atomic.AddUint32(&rf.lastApplied, 1)
	}
}

func (rf *Raft) getServerState() uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setNextIndex(peer, k uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[peer] = k
}

func (rf *Raft) getLogFromUntilEnd(from int) []Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[from:len(rf.log)]
}

func (rf *Raft) triggerAppendEntries() {
	doneChan := make(chan struct{}, len(rf.peers))
	for k := 0; k < len(rf.peers); k++ {
		if k == rf.me {
			doneChan <- struct{}{}
			continue
		}

		prevLogIndex := int(rf.getNextIndexAt(uint(k))) - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.getLogAt(prevLogIndex - 1).Term
		}

		lastEntry := uint(math.Min(float64(rf.getLenOfLog()), float64(rf.getNextIndexAt(uint(k))-1)))

		go rf.buildAndSendAppendEntry(k, rf.getCurrentTerm(),
			rf.getLeader(), int(prevLogIndex),
			prevLogTerm, rf.log[int(lastEntry):rf.getLenOfLog()],
			uint(math.Min(float64(rf.getCommitIndex()), float64(lastEntry))), doneChan)
	}

	for k := 0; k < len(rf.peers); k++ {
		<-doneChan
	}

	lastIdx := uint(rf.getLenOfLog())
	lenMatchIndex := rf.getLenMatchIndex()
	for ci := rf.getCommitIndex(); ci < lastIdx; ci++ {
		N := ci + 1
		quorum := 1
		for p := range rf.matchIndex {

			val := rf.getMatchIndexAt(p)
			if val >= N {
				quorum++
			}
		}

		if quorum > lenMatchIndex/2 {
			if N <= 0 {
				//rf.commitIndex = uint(len(rf.log))
				rf.setCommitIndex(lastIdx)
			} else {
				//if uint(rf.log[N-1].Term) == rf.getCurrentTerm() {
				if uint(rf.getLogAt(int(N)-1).Term) == rf.getCurrentTerm() {
					//rf.commitIndex = N
					rf.setCommitIndex(N)
				}
			}

			rf.commitChan <- struct{}{}
		}
	}
}

func (rf *Raft) setCommitIndex(cmdx uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = cmdx
}

func (rf *Raft) getLenMatchIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.matchIndex)
}

func (rf *Raft) getLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leader
}

func (rf *Raft) getLenOfLog() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) getLogAt(index int) Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[index]
}

func (rf *Raft) getNextIndexAt(peer uint) uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[peer]
}

func (rf *Raft) getMatchIndexAt(peer uint) uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[peer]
}

func (rf *Raft) buildAndSendAppendEntry(k int, currentTerm uint,
	leaderID int, prevLogIndex, prevLogTerm int,
	entries []Log, commitIndex uint, doneChan chan struct{}) {
	defer func() {
		doneChan <- struct{}{}
	}()

	args := AppendEntriesArgs{
		Term:          currentTerm,
		LeaderID:      leaderID,
		PrevLogIndex:  prevLogIndex,
		PrevLogTerm:   prevLogTerm,
		LeadersCommit: int(commitIndex),
		Entries:       entries,
		Target:        uint(k),
	}

	reply := AppendEntriesReply{}
	timeoutChan := make(chan bool)
	fnSendWithTimeout := func(timeoutChan chan bool, reply *AppendEntriesReply, args *AppendEntriesArgs) {
		ok := rf.sendAppendEntries(k, args, reply)
		timeoutChan <- ok
	}

	go fnSendWithTimeout(timeoutChan, &reply, &args)
	select {
	case <-timeoutChan:
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.setNextIndex(uint(k), uint(args.PrevLogIndex+len(args.Entries)+1))
				rf.setMatchIndex(uint(k), uint(args.PrevLogIndex+len(args.Entries)))
			}
		} else {
			if currentTerm >= reply.Term {
				if prevLogIndex <= 0 {

					if reply.Term > rf.getCurrentTerm() {
						rf.setTerm(reply.Term)
						rf.stepDownLeader()
						rf.setLeader(-1)
						return
					}
				}

				if prevLogIndex > reply.CommitIndex {
					prevLogIndex = reply.CommitIndex
				} else {
					prevLogIndex = prevLogIndex - 1
				}
				rf.setNextIndex(uint(k), uint(math.Max(float64(prevLogIndex), float64(1))))

			}

			if reply.Term > rf.getCurrentTerm() {
				rf.setTerm(reply.Term)
				rf.stepDownLeader()
				rf.setLeader(-1)
				return
			}

		}
	case <-time.After(time.Duration(200) * time.Millisecond):

		return
	}

}

func (rf *Raft) setMatchIndex(peer, val uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[peer] = val
}

func (rf *Raft) handleAppendEntriesAndQuorum(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		rf.matchIndex[uint(args.Target)] = uint(args.PrevLogIndex + len(args.Entries))
		rf.nextIndex[uint(args.Target)] = uint(args.PrevLogIndex + len(args.Entries) + 1)
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	lastIdx := uint(len(rf.log))
	for ci := rf.commitIndex; ci < lastIdx; ci++ {
		N := ci + 1
		quorum := 1
		for p := range rf.matchIndex {

			val := rf.matchIndex[p]
			if val >= N {
				quorum++
			}
		}

		if quorum > len(rf.matchIndex)/2 {
			if N <= 0 {
				rf.commitIndex = uint(len(rf.log))
			} else {
				if uint(rf.log[N-1].Term) == rf.currentTerm {
					rf.commitIndex = N
				}
			}

			rf.commitChan <- struct{}{}
		}
	}
}

func (rf *Raft) setState(state uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = votedFor
}

func (rf *Raft) getCurrentTerm() uint {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) setLeader(leader int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = leader
}

func (rf *Raft) setCandidateCurrentTermByPeer(peerTerm uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < peerTerm {
		rf.currentTerm = peerTerm
	}
}

func (rf *Raft) incrementTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
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

func (rf *Raft) setTerm(term uint) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < term {
		rf.currentTerm = term
	}
}

func (rf *Raft) stepDownLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = followerState
	if rf.heartbeatRun {
		rf.stopHeartbeatChan <- struct{}{}
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
	rf.triggerAppendEntries()

	for {
		select {
		case <-rf.stopHeartbeatChan:
			return
		case <-time.After(110 * time.Millisecond):
			rf.triggerAppendEntries()
		}
	}
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
