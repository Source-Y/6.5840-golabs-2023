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
	rand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3

	tickInterval     = 50 * time.Millisecond
	heartBeatTimeout = 150 * time.Millisecond

	baseElectionTimeout = 300
	None                = -1
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int // 节点状态
	currentTerm int // 当前任期
	votedFor    int // 投票给谁

	heartBeatTimeout time.Duration
	electionTimeout  time.Duration
	lastHeartBeat    time.Time
	lastElectionTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntriesArgs struct {
	LeaderId     int
	LeaderTerm   int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []ApplyMsg
}

type RequestAppendEntriesReply struct {
	FollowerTerm int
	Success      bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.becomeFollower(args.Term)

	reply.Term = rf.currentTerm
	if rf.votedFor == None || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeout()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state // mu锁保护state
		rf.mu.Unlock()
		switch state {
		case Follower:
			// follower什么也不用做
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.StartElection()
			}
		case Leader:
			if rf.pastHeartbeatTimeout() {
				rf.resetHeartbeatTimeout()
			}
			rf.StartAppendEntries()
		}

		time.Sleep(tickInterval)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.InitState()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) InitState() {
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower
	rf.heartBeatTimeout = heartBeatTimeout
}

// 是否已经达到选举超时时间
func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElectionTime) > rf.electionTimeout
}

// 是否已经超过了上一次的心跳时间
func (rf *Raft) pastHeartbeatTimeout() bool {
	return time.Since(rf.lastHeartBeat) > rf.heartBeatTimeout
}

// 重置随机超时选举时间
func (rf *Raft) resetElectionTimeout() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElectionTime = time.Now()
}

func (rf *Raft) resetHeartbeatTimeout() {
	rf.lastHeartBeat = time.Now()
}

// 选举
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.resetElectionTimeout()
	rf.becomeCandidate()
	rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	readerToLeader := false // 标记已经成为Leader
	receiveVote := 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverId int) {
			reply := new(RequestVoteReply)
			ok := rf.sendRequestVote(serverId, args, reply)
			if !ok || !reply.VoteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			receiveVote += 1
			if readerToLeader || receiveVote <= len(rf.peers)/2 {
				return
			}
			readerToLeader = true
			if rf.state != Candidate {
				return
			}
			rf.state = Leader
			go rf.StartAppendEntries()
		}(i)
	}
}

// 重置为candidate
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func (rf *Raft) StartAppendEntries() {
	rf.mu.Lock()
	rf.resetElectionTimeout()
	args := &RequestAppendEntriesArgs{
		LeaderId:   rf.me,
		LeaderTerm: rf.currentTerm,
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, args)
	}
}

func (rf *Raft) AppendEntries(serverId int, args *RequestAppendEntriesArgs) {
	reply := new(RequestAppendEntriesReply)
	rf.sendRequestAppendEntries(serverId, args, reply)
}

// 接受方
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	// 发送方任期比自己的要低
	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.mu.Lock()
	rf.resetElectionTimeout()
	if args.LeaderTerm > rf.currentTerm {
		rf.becomeFollower(args.LeaderTerm)
		reply.FollowerTerm = rf.currentTerm
	}
	rf.mu.Unlock()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.votedFor = None
	rf.currentTerm = term
}
