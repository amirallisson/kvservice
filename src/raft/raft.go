package raft

import (
	"bytes"
	"encoding/gob"

	"math/rand"
	"src/labrpc"
	"sync"
	"time"
)

const ElectionTimeout = 1000 * time.Millisecond

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       int // follower = 0, candidate = 1, leader = 2
	timer       *time.Timer
	applyCh     chan ApplyMsg
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Ok       bool
	ReqTerm  int
	ReqIndex int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == 2
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// no mutex lock, because modifications of fields occur while it is locked
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	enc.Encode(rf.CurrentTerm)
	enc.Encode(rf.VotedFor)
	enc.Encode(len(rf.Log))
	for i := 0; i < len(rf.Log); i++ {
		enc.Encode(rf.Log[i])
	}

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil { // never crashed, first Make()
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.Log = make([]LogEntry, 0)
		rf.Log = append(rf.Log, LogEntry{nil, -1, 0})
		return
	}
	// restart from a crash
	buf := bytes.NewBuffer(data)
	enc := gob.NewDecoder(buf)
	var sz int
	enc.Decode(&rf.CurrentTerm)
	enc.Decode(&rf.VotedFor)
	enc.Decode(&sz)
	for i := 0; i < sz; i++ {
		var temp LogEntry
		enc.Decode(&temp)
		rf.Log = append(rf.Log, temp)
	}
}

func (rf *Raft) setFields(currentTerm, votedFor, state int) {
	// no mutex lock, need to lock outside the f call
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.state = state
	rf.persist()
}

type RequestVoteArgs struct {
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.printFields("RequestVote()")
	if args.Term > rf.CurrentTerm {
		rf.setFields(args.Term, -1, 0)
	}

	reply.VoteGranted = (args.Term >= rf.CurrentTerm) && rf.upToDate(args) && rf.canVote(args)

	if reply.VoteGranted {
		rf.safeTimerReset()
		rf.setFields(args.Term, args.Id, 0)
	}
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) upToDate(args RequestVoteArgs) bool {
	prevLogIndex := len(rf.Log) - 1
	prevLogTerm := rf.Log[prevLogIndex].Term
	return (args.LastLogTerm == prevLogTerm && args.LastLogIndex >= prevLogIndex) || (args.LastLogTerm > prevLogTerm)
}

func (rf *Raft) canVote(args RequestVoteArgs) bool {
	return (args.Term == rf.CurrentTerm && (rf.VotedFor == args.Id || rf.VotedFor == -1)) || (args.Term > rf.CurrentTerm)
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.printFields("SendRequestVote()")
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Log)
	term, isLead := rf.CurrentTerm, (rf.state == 2)
	if !isLead {
		return -1, -1, false
	}

	entry := LogEntry{command, term, index}
	rf.Log = append(rf.Log, entry)
	rf.persist()

	rf.printFields("Start()")
	go rf.leaderBroadcast(false)
	return index, term, isLead
}

// send out RPCs and wait for the quorum
func (rf *Raft) leaderBroadcast(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader() {
		return
	}
	term, index := rf.CurrentTerm, len(rf.Log)-1

	// broadcast to all followers
	ch := make(chan int, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.AppendEntriesRPC(i, term, ch, heartbeat)
	}

	sum := 1
	waitCh := make(chan bool, 1)
	// wait syncronously and collect responses
	go func() {
		rf.mu.Unlock()
		put := false
		for i := 0; i < len(rf.peers)-1; i++ {
			if <-ch == 0 {
				break
			}
			if sum++; sum > len(rf.peers)/2 {
				put = true
				break
			}
		}
		rf.mu.Lock()
		waitCh <- put
	}()
	if !(<-waitCh) || (rf.CurrentTerm != term) {
		return
	}
	rf.commitIndex = max(rf.commitIndex, index)
	rf.apply()
}

func (rf *Raft) AppendEntriesRPC(serv, term int, ch chan int, heartbeat bool) {

	rf.printFields("Leader Sending His Log()")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.CurrentTerm || !rf.isLeader() {
		return
	}
	defer rf.persist()
	rep := &AppendEntriesReply{}
	var lastLogIndex int
	for {
		lastLogIndex = len(rf.Log) - 1
		var msg AppendEntries

		if rf.nextIndex[serv] <= lastLogIndex && !heartbeat {
			msg = AppendEntries{rf.CurrentTerm, rf.me, rf.nextIndex[serv] - 1, rf.Log[rf.nextIndex[serv]-1].Term, rf.Log[rf.nextIndex[serv]:], rf.commitIndex}
		} else {
			msg = AppendEntries{rf.CurrentTerm, rf.me, lastLogIndex, rf.Log[lastLogIndex].Term, nil, rf.commitIndex}
		}

		// RPC call
		rpcChan := make(chan bool, 1)
		go func() {
			rf.mu.Unlock()
			ok := rf.peers[serv].Call("Raft.AppendEntriesHandler", msg, rep)
			rf.mu.Lock()
			rpcChan <- ok

		}()
		// RPC failure of stale term
		if !(<-rpcChan) || rf.CurrentTerm != msg.Term {
			ch <- 0
			return
		}
		// ok
		if rep.Ok {
			rf.nextIndex[serv] = max(rf.nextIndex[serv], lastLogIndex+1) // someone might have updated between mutex unlock() and lock()
			rf.matchIndex[serv] = rf.nextIndex[serv] - 1
			ch <- 1
			return
		}
		// follower has greater term -> become follower
		if msg.Term < rep.Term {
			rf.toFollower(rep.Term)
			ch <- 0
			return
		}
		// update nextIndex and repeat. Heartbeat = false, because next iter exchanges non-nill logs
		rf.nextIndex[serv] = rep.ReqIndex
		heartbeat = false
	}
}
func (rf *Raft) AppendEntriesHandler(args AppendEntries, rep *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rep.Term = rf.CurrentTerm
	rf.printFields("AppendEntriesHandler()")
	if args.Term < rf.CurrentTerm {
		//fmt.Printf("Rejecting Entries(term) %v: args.Term = %v, rf.Term = %v, reqIndex = %v\n", rf.me, args.Term, rf.CurrentTerm, rep.ReqIndex)
		rep.Ok = false
		return
	}

	rf.handleHeartbeat(args) // update term, timer, and etc
	if !rf.inductOptimized(args, rep) {
		//fmt.Printf("Rejecting entries (induct) reqIndex = %v\n", rep.ReqIndex)
		rep.Ok = false
		return
	}

	rep.Ok = true
	if args.Entries != nil {
		rf.handleCommand(args)
	}
	if args.LeaderCommit > rf.commitIndex {
		//rf.printFields(fmt.Sprintf("Old CI = %v, New CI = %v", rf.commitIndex, args.LeaderCommit))
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	}
	rf.apply() // apply cmds to FSM if needed

}

// AppendEntriesHandler for heartbeats
func (rf *Raft) handleHeartbeat(args AppendEntries) {
	rf.setFields(args.Term, args.LeaderId, 0)
	rf.safeTimerReset()
}

// AppendEntrieshandler for commands

func (rf *Raft) handleCommand(args AppendEntries) {
	rf.printFields("Begin Changing Log()")
	rf.Log = rf.Log[:args.PrevLogIndex+1]
	rf.Log = append(rf.Log, args.Entries...)
	rf.printFields("End Changing Log()")
	rf.persist()
}

func (rf *Raft) inductOptimized(args AppendEntries, rep *AppendEntriesReply) bool {
	// args.index or args.term > max index or max term ---> let the caller do the work
	if args.PrevLogIndex >= len(rf.Log) || (rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		index := min(len(rf.Log)-1, args.PrevLogIndex)
		rep.ReqTerm = rf.Log[index].Term
		rep.ReqIndex = rf.findFirstOfSameTerm(index)
		return false
	}
	return true
}

func (rf *Raft) findFirstOfSameTerm(start int) int {
	term := rf.Log[start].Term
	for i := start - 1; i >= 0; i-- {
		if rf.Log[i].Term != term {
			return i + 1
		}
	}
	return 1 // unattainable
}

func (rf *Raft) leaderRoutine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printFields("LeaderRoutine()")
	go rf.leaderBroadcast(true)
}
func (rf *Raft) followerRoutine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printFields("FollowerRoutine()")
	rf.timer = time.NewTimer(randomDuration()) // renew timer
	for {
		rf.safeTimerReset()
		waitCh := make(chan int, 1)
		selectCh := make(chan int, 1)
		// sync sleep
		go func() {
			rf.mu.Unlock()
			<-rf.timer.C
			rf.mu.Lock()
			waitCh <- 1
		}()
		<-waitCh
		// candidate init
		rf.setFields(rf.CurrentTerm+1, rf.me, 1)
		go rf.candidateRoutine(waitCh)
		go rf.manageElection(selectCh, waitCh)
		// wait for timeout and return if leader now
		<-selectCh
		if rf.isLeader() {
			return
		}
	}
}

func (rf *Raft) manageElection(selectCh, waitCh chan int) {
	rf.mu.Unlock()
	select {
	case <-waitCh:
		break
	case <-time.After(randomDuration()):
		break
	}
	rf.mu.Lock()
	selectCh <- 1
}

func (rf *Raft) candidateRoutine(ch chan int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printFields("CandidateRoutine()")

	msg := RequestVoteArgs{rf.CurrentTerm, rf.me, len(rf.Log) - 1, rf.Log[len(rf.Log)-1].Term}
	voteCnt := 1
	repChan := make(chan *RequestVoteReply, len(rf.peers)-1)

	// send requestVote RPCs
	go rf.candidateBroadcast(msg, repChan)

	// collect responses
	var rep *RequestVoteReply
	for serv := 0; serv < len(rf.peers)-1; serv++ {
		if serv == rf.me {
			continue
		}
		voteCh := make(chan *RequestVoteReply, 1)
		//	sync wait for reply
		go func() {
			rf.mu.Unlock()
			rep := <-repChan
			rf.mu.Lock()
			voteCh <- rep
		}()

		rep = <-voteCh
		// stale term
		if msg.Term != rf.CurrentTerm {
			break
		}
		// ok, vote granted
		if rep.VoteGranted {
			voteCnt++
			// check if majority voted, become leader if true
			if voteCnt > len(rf.peers)/2 {
				rf.leaderInit()
				break
			}
			continue
		}
		// sender has greater term, become follower
		if rep.Term > rf.CurrentTerm {
			rf.toFollower(rep.Term)
			break
		}
	}
	ch <- 1
}

// broadcast requestVote RPCs
func (rf *Raft) candidateBroadcast(msg RequestVoteArgs, repChan chan *RequestVoteReply) {
	// send rpcs
	for serv := 0; serv < len(rf.peers); serv++ {
		if serv == rf.me {
			continue
		}
		go func(i int) {
			rep := &RequestVoteReply{}
			if !rf.sendRequestVote(i, msg, rep) {
				rep.Term, rep.VoteGranted = -1, false
			}
			repChan <- rep
		}(serv)
	}
}

// initialize the newly elected leader fields
func (rf *Raft) leaderInit() {
	rf.setFields(rf.CurrentTerm, rf.me, 2)
	prevLogIndex := len(rf.Log) - 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = prevLogIndex + 1
		if i == rf.me {
			rf.matchIndex[i] = prevLogIndex
		} else {
			rf.matchIndex[i] = 0
		}
	}
}

// leader into follower
func (rf *Raft) toFollower(newTerm int) {
	if newTerm > rf.CurrentTerm { // maybe these are old replies
		rf.setFields(newTerm, -1, 0)
	}
}

func (rf *Raft) isLeader() bool {
	return rf.state == 2
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent sta	// apply
//rf.commitIndex = prevLogIndex
//rf.apply()te, and also initially holds the most
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

	// persistent
	rf.readPersist(persister.ReadRaftState())
	// volatile
	rf.commitIndex, rf.lastApplied = 0, 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// extra
	rf.state = 0
	rf.timer = time.NewTimer(randomDuration())
	rf.applyCh = applyCh

	rf.printFields("Make()")

	go func() {
		for {
			if rf.isLeader() { // me == leader
				rf.leaderRoutine()
				time.Sleep(ElectionTimeout / 2)
			} else {
				rf.followerRoutine()
			}
		}
	}()

	return rf
}

func (rf *Raft) apply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		//	fmt.Printf("S%v, T%v, st%v, la %v, cmd %v\n", rf.me, rf.CurrentTerm, rf.state, rf.lastApplied, rf.Log[rf.lastApplied].Command)
		rf.applyCh <- ApplyMsg{rf.lastApplied, rf.Log[rf.lastApplied].Command, false, nil}
	}
}

func (rf *Raft) safeTimerReset() {
	select {
	case <-rf.timer.C:
		break
	default:
		break
	}
	rf.timer.Reset(randomDuration())
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}

	return a
}

func randomDuration() time.Duration {
	return time.Duration((rand.Float64() + 1.0) * float64(ElectionTimeout))
}

func (rf *Raft) printFields(fname string) {
	/*	var slice []LogEntry
		if len(rf.Log) > 1 {
			slice = rf.Log[len(rf.Log)-2:]
		} else {
			slice = rf.Log[len(rf.Log)-1:]
		}*/
	//fmt.Printf("%v: Serv %v, Term %v, Log %v, CI %v, len = %v\n", fname, rf.me, rf.CurrentTerm, slice, rf.commitIndex, len(rf.Log))
}

func (rf *Raft) Kill() {
}
