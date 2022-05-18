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
	"bytes"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sasha-s/go-deadlock"
)

const (
	HBPeriod = time.Millisecond * 120

	HBTimeoutPeriod      = time.Millisecond * 600 // 心跳超时间隔
	HBTimeoutPeriodDelta = 400                    // 心跳超时间隔增加量[0,Delta]

	// ElectionTimeoutPeriod = HBTimeoutPeriod // 选举等待结果时间
)

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
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state.
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// update upon log coming
	lastLogTerm  int
	lastLogIndex int

	// volatile state on all servers
	commitIndex int64 // use atomic
	lastApplied int

	// volatile state on leaders
	nextIndex  []int // index of the next log entry to send to tha server (initialized to leader last log index+1)
	matchIndex []int // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	receivedHB int32 // have received HB during sleeping
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// State
type State int

const (
	Follower = iota
	Candidate
	Leader
)

// Log Entry
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Need Mu outside.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := PersistentState{
		rf.currentTerm, rf.votedFor, rf.logs,
	}
	if err := e.Encode(state); err != nil {
		panic("Failed to encode raft persistent state: " + err.Error())
	}
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistentState
	if err := d.Decode(&state); err != nil {
		panic("Failed to decode raft persistent state: " + err.Error())
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.logs = state.Logs
	rf.updateLastLog()
}

//
// RequestVote
//

// RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d(%d) requests vote to %d(%d)\n", args.CandidateID, args.Term, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	needPersist := false
	if rf.checkTerm(args.Term) {
		needPersist = true
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver 's log, grant vote
	olderLog := rf.checkOlderLog(args.LastLogTerm, args.LastLogIndex)
	if !olderLog && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		needPersist = true

		// Reset HBTimeout after granting a vote request!
		rf.receiveHB()

		DPrintf("%d voted to %d\n", rf.me, args.CandidateID)
	} else {
		DPrintf("%d did NOT vote to %d. older:%v votedFor:%v\n", rf.me, args.CandidateID, olderLog, rf.votedFor)
	}

	if needPersist {
		rf.persist()
	}
}

// send a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, votes *int64) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.checkTerm(reply.Term) {
			rf.persist()
			return
		}
		if rf.state != Candidate {
			return
		}
		if reply.VoteGranted {
			atomic.AddInt64(votes, 1)
			if int(atomic.LoadInt64(votes))*2 > len(rf.peers) {
				if rf.state != Leader {
					rf.state = Leader
					go rf.becomeLeader()
				}
				return
			}
		}
	}
}

// Attemp to become the leader by stating an election.
func (rf *Raft) startElection() {
	DPrintf("%d starts an election!", rf.me)

	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}
	rf.mu.Unlock()

	var votes int64 = 1 // votes for self
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &votes)
		}
		if rf.getState() != Candidate {
			return
		}
	}
}

//
// Append Entry
//

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler.
// Ensure that args.Entries are sorted by LogEntry.Index.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) > 0 {
		DPrintf("%d(%d) receives appendEntries from %d(%d).\n\tEntries: %v, %v~ %v, %v\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args.Entries[0].Term, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Term, args.Entries[len(args.Entries)-1].Index)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if rf.state != Follower {
		rf.state = Follower
	}
	// rf.votedFor = -1 // no need to reset here
	rf.receiveHB()
	rf.checkTerm(args.Term)
	defer rf.persist()

	// Update logs.

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.lastLogIndex {
		reply.ConflictIndex = rf.lastLogIndex + 1
		return
	}
	if term := rf.logs[args.PrevLogIndex].Term; term != args.PrevLogTerm {
		reply.ConflictTerm = term
		index := args.PrevLogIndex
		for index > 0 && rf.logs[index].Term == term {
			index--
		}
		reply.ConflictIndex = index + 1
		return
	}

	reply.Success = true

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for i, v := range args.Entries {
		// Append any new entries not already in the log
		// Be careful that rf.logs has an invalid log for convenience(logs[0]).
		if v.Index >= len(rf.logs) {
			// don't have
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.updateLastLog()
			break
		} else if v.Term != rf.logs[v.Index].Term {
			// mismatch
			conflictID := v.Index
			rf.logs = append(rf.logs[:conflictID], args.Entries[i:]...)
			rf.updateLastLog()
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > int(atomic.LoadInt64(&rf.commitIndex)) {
		rf.commit(min(args.LeaderCommit, rf.lastLogIndex))
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesTo(server int) {
	// DPrintf("%d runs appendEntriesTo %d\n", rf.me, server)

	rf.mu.Lock()
	// Server may turn into follower while accquiring this lock!!!
	// A follower with lots of incorrect logs and the highest term can take place of the leader!!
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex, entries := rf.getAppendEntriesArgsFor(server)

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: int(atomic.LoadInt64(&rf.commitIndex)),
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		// Lock after sendAppendEntries.
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// sendAppendEntries() may return after a long delay!! (Leader can turn into follower and become leader again)
		if args.Term != rf.currentTerm {
			return
		}

		if reply.Success {
			if len(entries) > 0 {
				// If successful: update nextIndex and matchIndex for follower
				lastEntry := entries[len(entries)-1]

				rf.matchIndex[server] = lastEntry.Index
				rf.nextIndex[server] = lastEntry.Index + 1
				// rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex)
				// rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server] + 1)

				// If there exists an N such that N > commitIndex, a majority of matchIndex[i]>=N, and log[N].term == currentTerm: set commitIndex = N
				temp := make([]int, len(rf.matchIndex))
				copy(temp, rf.matchIndex)
				sort.Ints(temp)
				N := temp[(len(temp)+1)/2]
				DPrintf("%d -> %d {%d} success. N:%d temp:%v\n", rf.me, server, lastEntry.Index, N, temp)

				if N < len(rf.logs) && rf.logs[N].Term == rf.currentTerm {
					if rf.commit(N) {
						// go rf.doHeartBeat()
					}
				}
			}
		} else {
			if rf.checkTerm(reply.Term) {
				rf.persist()
				return
			}
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			// rf.nextIndex[server]-- // too slow
			if reply.ConflictTerm > 0 {
				index := rf.lastLogIndex
				for index > 0 && rf.logs[index].Term != reply.ConflictTerm {
					index--
				}
				if index > 0 {
					rf.nextIndex[server] = index
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
			// rf.matchIndex[server] = rf.nextIndex[server] - 1 // no need?
		}
	}
}

// Run AppendEntries().
func (rf *Raft) doHeartBeat() {
	for i := range rf.peers {
		if rf.getState() != Leader {
			return
		}
		if i != rf.me {
			go rf.appendEntriesTo(i)
		}
	}
}

// Get AppendEntriesArgs for server (PrevLogIndex, Entries).
// Need mu outside.
func (rf *Raft) getAppendEntriesArgsFor(server int) (int, []LogEntry) {
	nextIndex := rf.nextIndex[server]

	var entries []LogEntry

	// There is something to send.
	if rf.lastLogIndex >= nextIndex {
		entries = append(entries, rf.logs[nextIndex:]...)
		// Do NOT use `entries = rf.logs[nextIndex:]` directly!!!
	}
	return nextIndex - 1, entries
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := rf.getState() == Leader

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm

	if isLeader {
		// new log
		index = rf.lastLogIndex + 1
		rf.logs = append(rf.logs, LogEntry{index, rf.currentTerm, command})
		rf.updateLastLog()
		rf.persist()

		// go rf.doHeartBeat()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initialize Raft
	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) // initialize size: 1 (invalid log)
	rf.logs[0] = LogEntry{0, 0, nil}

	rf.lastLogIndex = 0
	rf.lastLogTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// load persist state
	rf.readPersist(persister.ReadRaftState())
	DPrintf("%v %v %v", rf.currentTerm, rf.votedFor, rf.logs)

	// mainloop
	go rf.mainloop()

	return rf
}

// mainloop
func (rf *Raft) mainloop() {
	for !rf.killed() {
		time.Sleep(getRandomPeriod(HBTimeoutPeriod, HBTimeoutPeriodDelta))

		rf.tryElection()
	}
}

func (rf *Raft) tryElection() {
	if rf.getState() == Leader {
		return
	}

	defer rf.resetHB()
	if rf.checkReceiveHB() {
		return
	}

	rf.startElection()
}

// Begin as soon as becoming leader (not in mainLoop).
// Check state outside to avoid becomeLeader() twice!!
func (rf *Raft) becomeLeader() {
	// Server may have already became leader but its state hasn't changed.
	// if rf.getState() == Leader {
	// return
	// }
	// rf.setState(Leader)

	// become leader just now.
	DPrintf("%d becomes leader!!\n", rf.me)

	// initialize
	rf.mu.Lock()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}
	DPrintf("next:%v", rf.nextIndex)
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	for rf.getState() == Leader {
		rf.doHeartBeat()

		time.Sleep(HBPeriod)
	}
}

//
// util
//

// Return rf's state.
// Uses mu.
func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// Set rf's state.
// Uses mu.
func (rf *Raft) setState(state State) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}

// Return rf's currentTerm.
// Uses mu.
func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

// TODO a better implemention?
func getRandomPeriod(base time.Duration, maxDelta int) time.Duration {
	return base + time.Millisecond*time.Duration(rand.Int()%maxDelta)
}

// Return whether to update currentTerm with a higher term and update currentTerm AND state if it's true.
// // Need mu outside if needMu is true.
//// Need lock outside if corresponding arg is false!
//// (needStateMu, needPerStateMu) can NOT be (true, false)!
func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		// DPrintf("%d's term(%d) changed by term(%d)\n", rf.me, rf.currentTerm, term)

		rf.currentTerm = term
		rf.votedFor = -1 // reset here!!
		if rf.state != Follower {
			DPrintf("%d becomes follower from %d", rf.me, rf.state)
			rf.state = Follower
		}
		return true
	}
	return false
}

// Check whether a log is NOT at least up-to-date as current server's log.
func (rf *Raft) checkOlderLog(logTerm, logIndex int) bool {
	if logTerm != rf.lastLogTerm {
		return logTerm < rf.lastLogTerm
	}
	return logIndex < rf.lastLogIndex
}

// Update lastLogIndex and lastLogTerm when logs change.
// Need mu outside!
func (rf *Raft) updateLastLog() {
	rf.lastLogIndex = rf.logs[len(rf.logs)-1].Index
	rf.lastLogTerm = rf.logs[len(rf.logs)-1].Term
}

// Update CommitIndex and commit. Return true if rf.commitIndex < index.
func (rf *Raft) commit(index int) bool {
	if int(atomic.LoadInt64(&rf.commitIndex)) < index {
		DPrintf("%d runs commit(%d)\n", rf.me, index)

		atomic.StoreInt64(&rf.commitIndex, int64(index))

		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
		for rf.lastApplied < index {
			rf.lastApplied++
			log := &rf.logs[rf.lastApplied]
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}
		}
		return true
	}
	return false
}

func (rf *Raft) receiveHB() {
	atomic.StoreInt32(&rf.receivedHB, 1)
}

func (rf *Raft) resetHB() {
	atomic.StoreInt32(&rf.receivedHB, 0)
}

// Check whether server receives heartbeat during sleeping.
func (rf *Raft) checkReceiveHB() bool {
	return atomic.LoadInt32(&rf.receivedHB) > 0
}

// Reset HBTimeoutAt based on time.Now() after receiving a HB.
// func (rf *Raft) resetHBTimeoutAt() {
// 	rf.timeMu.Lock()
// 	rf.HBTimeoutAt = time.Now().Add(getRandomPeriod(HBTimeoutPeriod, HBTimeoutPeriodDelta))
// 	// fmt.Println("resetHBTimeoutAt:", rf.me, rf.HBTimeoutAt)
// 	rf.timeMu.Unlock()
// }

// Check whether HBTimeout happens.
// func (rf *Raft) checkHBTimeout() bool {
// 	rf.timeMu.Lock()
// 	defer rf.timeMu.Unlock()
// 	return time.Now().After(rf.HBTimeoutAt)
// }

// Be sure that len(args) is at least 1.
func min(args ...int) int {
	res := args[0]
	for _, v := range args {
		if res > v {
			res = v
		}
	}
	return res
}

// Be sure that len(args) is at least 1.
func max(args ...int) int {
	res := args[0]
	for _, v := range args {
		if res < v {
			res = v
		}
	}
	return res
}

// func (rf *Raft) getLastLogIndex() int {
// 	return len(rf.logs) - 1
// }

// func (rf *Raft) getLastLogTerm() int {
// 	return rf.logs[len(rf.logs)-1].Term
// }
