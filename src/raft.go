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
	"sync"
	"sync/atomic"
	"../labrpc"
	"math/rand"
	"time"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	log []LogEntry
	commitIndex int
	lastApplied int
	hBeatCh chan bool
	applyCh chan ApplyMsg
	kill chan bool
	winElection chan bool
	state int
	voteCount int

	nextIndex []int
	matchIndex []int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//helper
func (rf *Raft) Lock(){
	rf.mu.Lock()
}

//helper
func (rf *Raft) Unlock(){
	rf.mu.Unlock()
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM int
	CANDIDATEID int
	LASTLOGINDEX int
	LASTLOGTERM int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM int
	VOTEGRANTED bool
}

func Max(x, y int) int {
    if x < y {
        return y
    }
    return x
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("RequestVote called\n")

	rf.Lock()
	defer rf.Unlock()

	reply.VOTEGRANTED = false
	reply.TERM = Max(args.TERM, rf.currentTerm)

	//refresh for vote in new term
	if args.TERM > rf.currentTerm{
		rf.currentTerm = args.TERM
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	if rf.votedFor == -1{
		rf.votedFor = args.CANDIDATEID		
		reply.VOTEGRANTED = true
		rf.state = FOLLOWER
		// DPrintf("%d votes aye for %d term %d!\n", rf.me, rf.votedFor, args.TERM)

	}

}

func (rf *Raft) HeartBeat(dummy_args *RequestVoteArgs, dummy_reply *RequestVoteReply) {
	select{
	case rf.hBeatCh <- true:
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.Lock()
	defer rf.Unlock()

	if !ok ||
		args.TERM != rf.currentTerm ||
		rf.state != CANDIDATE{ 
		return 
	}

	if reply.TERM > rf.currentTerm{
		rf.currentTerm = reply.TERM
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.VOTEGRANTED{
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE{
			rf.winElection <- true
		}
	}

	return
}

func (rf *Raft) broadcastHeartBeat(){
	rf.Lock()
	defer rf.Unlock()

	n := len(rf.peers)
	var replys = make([]RequestVoteReply, n)
	var args RequestVoteArgs

	for i:=0;i<n;i++{
		if i == rf.me { continue }
		ok := rf.peers[i].Call("Raft.HeartBeat", &args, &replys[i])
		if !ok { DPrintf("HeartBeat fail at server (%d -> %d)", rf.me, i) }
	}
}

func (rf *Raft) broadcastEntries(){

}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs

	rf.Lock()
	args.TERM = rf.currentTerm
	args.CANDIDATEID = rf.me
	args.LASTLOGINDEX = rf.log[len(rf.log)-1].LogIndex
	args.LASTLOGTERM = rf.log[len(rf.log)-1].LogTerm
	n := len(rf.peers)
	rf.Unlock()

	for i:=0;i<n;i++{
		if rf.state != CANDIDATE { break }
		if rf.me == i { continue }

		var replys = make([]RequestVoteReply, n)
		go rf.sendRequestVote(i, &args, &replys[i])
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
// start will be called for multiple times
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()
	defer rf.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (2B).


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

func (rf *Raft) run() {
	for{
		switch rf.state{

		case FOLLOWER:
			select{
			case <- rf.hBeatCh:
				//prevents it from election
				// DPrintf("follower %d heartbeats!", rf.me)
			case <- rf.kill:
				return
			case <- time.After(time.Duration(rand.Intn(150)+250) * time.Millisecond):
				rf.Lock()
				rf.state = CANDIDATE
				rf.Unlock()
			}

		case CANDIDATE:
			rf.Lock()
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.currentTerm++
			rf.persist()
			rf.Unlock()

			go rf.broadcastRequestVote()

			select{
			case <- rf.winElection:
				rf.Lock()
				
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i:=0;i<len(rf.peers);i++{
					rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1
					rf.matchIndex[i] = 0
				}

				rf.Unlock()
			case <- rf.hBeatCh:
				rf.Lock()
				rf.state = FOLLOWER
				rf.Unlock()
			case <- rf.kill:
				return
			case <- time.After(time.Duration(rand.Intn(150)+250) * time.Millisecond):
			}


		case LEADER:
			go rf.broadcastHeartBeat()
			go rf.broadcastEntries()
			time.Sleep(time.Millisecond*100)

		

		}
	}
}

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0}) //index start at 1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER

	//use multiple channels instead of multiplexer
	rf.hBeatCh = make(chan bool, 9)
	rf.applyCh = applyCh

	rf.winElection = make(chan bool, 9)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()
	return rf
}

type LogEntry struct{
	LogTerm int
	LogIndex int
}