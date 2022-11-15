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

import "sync"
import "labrpc"
import "math/rand"
import "time"
//import "fmt"

import "bytes"
import "labgob"
//import "log"
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
type logEntry struct{
	Command interface{}
	Term int
}
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	//------------------
	log  []logEntry
	currentTerm int
	votedFor int
	numberOfVotesReceived int
	numberOfAckReceived int
	//------------------
	commitIndex int
	lastApplied int
	//------------------
	nextIndex  []int
	matchIndex []int
	state string
	currentLeaderId int
	recievedVoteRequest chan int
	recievedAppendEntries chan int
	sendIamLeader chan int
	resetEvent bool
	needApply bool
	timer *time.Timer
	ApplyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	defer rf.mu.Unlock()
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = false
    if(rf.state == "Leader"){
		isleader = true
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	// // Your code here (2C).
	// // Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var log []logEntry
	// if d.Decode(&currentTerm) != nil ||
	//    d.Decode(&votedFor) != nil || d.Decode(&log) !=nil {
	// 	 return
	// } else {
	//   rf.currentTerm= currentTerm
	//   rf.votedFor = votedFor
	//   rf.log = log
	// }
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}
// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries [] logEntry
	LeaderCommit int
}
// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term int
	Success bool
	UniqueAck bool
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if(args.Term < rf.currentTerm){
		reply.VoteGranted = false
		return
	}
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollower(rf.currentTerm)
	}
	if (rf.votedFor == -1  && (args.LastLogIndex < 0 && args.LastLogTerm == 0 ) || rf.log[args.LastLogIndex].Term == args.LastLogTerm){
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.resetEvent = true
		rf.recievedVoteRequest <-1
		//rf.resetTimer()
		return
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.recievedAppendEntries <-1
	defer rf.persist()
	if(len(rf.log) < args.PrevLogIndex ){
		currentIndex := len(rf.log)-1
		if(rf.log[currentIndex].Term == args.Entries[currentIndex].Term){
			index := currentIndex+1
			for index <= args.PrevLogIndex+1{
				rf.log= append(rf.log, args.Entries[index])
				applyingMessage := ApplyMsg {true, rf.log[index].Command, index}
				rf.ApplyCh <-applyingMessage
				DPrintf("Follower  %v append new log entry: %v\n", rf.me, rf.log)
				index++
			}
			DPrintf("rf: %v  entry: %v", rf.me, rf.log)
		}
	
	}

	if (args.Term < rf.currentTerm){
		reply.Success=false
		return
	}
	if(args.PrevLogIndex > 0 && args.PrevLogIndex >= len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm){
		reply.Success=false
		return
	}

	if(args.Term > rf.currentTerm){
		rf.currentTerm = args.Term
		if(rf.state !="Follower"){
			rf.becomeFollower(args.Term)
		}
	}

	if(args.Term == rf.currentTerm && (args.PrevLogIndex == -1) || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm){
		reply.Success=true
		reply.Term = args.Term
		rf.currentLeaderId = args.LeaderId
		rf.currentTerm = args.Term
		reply.UniqueAck= rf.addEntryIfDoesNotExist(args.PrevLogIndex, len(rf.log), args.Entries)
	}

	if(args.LeaderCommit > rf.commitIndex){
		if(args.LeaderCommit < len(rf.log)-1){
			rf.commitIndex = args.LeaderCommit
		}else{
			rf.commitIndex = len(rf.log)-1
		}
		applyingMessage := ApplyMsg {true, rf.log[rf.commitIndex].Command, rf.commitIndex}
		rf.ApplyCh <-applyingMessage
		DPrintf("Follower  %v append new log entry: %v\n", rf.me, rf.log)
	}

}
func (rf *Raft) addEntryIfDoesNotExist(prevIndex int, lengthLogRe int, logSender [] logEntry)bool{
	var entry logEntry
	ret:= false
	if(lengthLogRe < len(logSender)){
		entry.Term= logSender[prevIndex+1].Term
		entry.Command = logSender[prevIndex+1].Command
		rf.log= append(rf.log, entry)
		ret=  true
	}
	return ret
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
	if(reply.Term > rf.currentTerm){
		//DPrintf("Stale candidate Server %v current Term: %v reply term: %v\n ", rf.me, rf.currentTerm, reply.Term)
		rf.becomeFollower(reply.Term)
		return false
	} 
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if(reply.VoteGranted == true){
		rf.numberOfVotesReceived= rf.numberOfVotesReceived + 1
		//DPrintf("server: %v vote recieved:%v from %v term%v\n ", rf.me, rf.numberOfVotesReceived, server, rf.currentTerm)
		if(rf.numberOfVotesReceived > len(rf.peers)/2){
			rf.state = "Leader"
			rf.becomeLeader()
			rf.sendIamLeader <-1
		}
	}
	return ok
}
func (rf *Raft) sendAppendRpc(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if(reply.Term > rf.currentTerm){
		rf.becomeFollower(reply.Term)
		return false
	}
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.mu.Lock()
	if(reply.Success){
		if(reply.UniqueAck){
			rf.numberOfAckReceived +=1
			DPrintf("NumberofAckReceived %v", rf.numberOfAckReceived)
			if(rf.numberOfAckReceived>5){
				rf.numberOfAckReceived=0
			}
		}
		if(rf.numberOfAckReceived > (len(rf.peers)/2 +1)){
				rf.commitIndex +=1
				applyingMessage := ApplyMsg {true, rf.log[len(rf.log)-1].Command, len(rf.log)-1}
				rf.ApplyCh <-applyingMessage
		}
	}
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool){
    defer rf.mu.Unlock()
	rf.mu.Lock()
	is_leader := true
	if(rf.state != "Leader"){
		return rf.currentTerm, -1, false
	}
	var newEntry logEntry
	newEntry.Term = rf.currentTerm
	newEntry.Command = command
	rf.log = append(rf.log,  newEntry)
	DPrintf("The log leader %v after apending the clients request: %v", rf.me,rf.log)
	return (len(rf.log)-1), rf.currentTerm, is_leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.state="Killed"
	DPrintf("The server is killed %v", rf.me)
}



//
// the service or tester wants to *create a Raft server*. the ports
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
	rf.state="Follower"
	rf.currentTerm = 0
	rf.log = nil
	rf.votedFor = -1
	rf.currentLeaderId=-1
	rf.numberOfVotesReceived=0
	rf.resetEvent=false
	rf.log = []logEntry{}
	var dummy logEntry
	dummy.Term = 0
	rf.log = append(rf.log, dummy)
	rf.nextIndex=[]int{}
	rf.matchIndex=[]int{}
	rf.commitIndex=0
	rf.lastApplied=0
	rf.recievedAppendEntries = make(chan int , 100)
	rf.recievedVoteRequest = make(chan int, 100)
	rf.sendIamLeader = make(chan int, 100)
	rf.ApplyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Actions to run the server (Follower/Candidate/Leader)
	go runElection(me, rf)

	return rf
}

func runElection(server int, rf *Raft){
		for{
				//DPrintf("Server %v number of vote recieved %v", rf.me, rf.numberOfVotesReceived)
				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()
				if(state == "Leader"){
					 rf.becomeLeader()
				}else{
					rf.timer = time.NewTimer(generateRandomMilliSc()* time.Millisecond)
				select{
					case <-rf.sendIamLeader:
					case <-rf.recievedVoteRequest:
						rf.resetTimer()
					case <-rf.recievedAppendEntries:
						rf.resetTimer()
					case <-rf.timer.C:
						rf.beginElectionProcess(server)
				}	
				}		
		}
}
func (rf *Raft) resetTimer(){
		rf.mu.Lock()
		rf.timer.Reset(generateRandomMilliSc()* time.Millisecond)
		rf.mu.Unlock()
}
func (rf *Raft) beginElectionProcess(server int){
	defer rf.mu.Unlock()
	rf.mu.Lock()
	rf.state="Candidate"
	rf.currentTerm= rf.currentTerm+1
	rf.votedFor=rf.me
	//rf.timer = time.NewTimer(generateRandomMilliSc()* time.Millisecond)
	var args RequestVoteArgs
	var reply RequestVoteReply
	//DPrintf("Start the election Server %v state: %v", rf.me, rf.state)
	args.Term = rf.currentTerm
	args.CandidateId =rf.me
	for serverIndex, _ := range rf.peers {
		if(serverIndex != rf.me){
			go rf.sendRequestVote(serverIndex, &args, &reply)
		}
	}
}
func (rf *Raft) becomeLeader(){
		time.Sleep(300* time.Millisecond)
		var args AppendEntriesArgs
		var replyargs AppendEntriesReply
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex=len(rf.log)-2
		if(len(rf.log)-2 < 0){
			args.PrevLogTerm = 0
		}else{
			args.PrevLogTerm = rf.log[len(rf.log)-2].Term
		}
		args.LeaderCommit=rf.commitIndex
		args.Entries=rf.log
		for serverIndex, _ := range rf.peers{
			if(rf.me != serverIndex){
				go rf.sendAppendRpc(serverIndex, &args, &replyargs)
			}
		}
}
func(rf *Raft) becomeFollower(term int){
  rf.currentTerm = term
  rf.numberOfVotesReceived=0
  rf.state="Follower"
  rf.votedFor = -1
}
func generateRandomMilliSc() time.Duration{
	min := 500
	max := 800
	return time.Duration(rand.Intn(max-min)+min)
}