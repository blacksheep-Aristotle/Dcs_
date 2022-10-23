//package raft

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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// used in kv raft
	CommandTerm int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	NONE = -1 // none for everything
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
)

type PersistData struct {
	CurrentTerm int
	VotedFor int
	LogIndex int
	Log []Entry
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


	// 2A
	logIndex int // current newest log index, start from 1
	currentTerm int
	currentState int
	currentLeader int
	// every time term increased, votedFor should be set none
	votedFor int

	randLock *sync.Mutex
	heartBeatTimer *time.Timer
	electionTimeoutTimer *time.Timer

	// 2B
	applyCh chan ApplyMsg

	log []Entry
	commitIndex int
	lastApplied int

	// use for leader, reset after each election
	nextIndex []int
	matchIndex []int

	// locked by mu
	replicatorStatus []bool
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.currentState == LEADER
	return term, isLeader
}

func (rf *Raft) getRandomElectionTimeout() time.Duration {
	rf.randLock.Lock()
	defer rf.randLock.Unlock()
	s := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(500)+600
	return time.Duration(s)*time.Millisecond
}

func getStableSendHeartbeatTimeout() time.Duration {
	return 100 * time.Millisecond
}

func logPrint(format string, a ...interface{}) {
	//disablePrint := os.Getenv("lab")
	//if disablePrint!="yes" {
	//	fmt.Println(time.Now().Second(), fmt.Sprintf(format, a...))
	//}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) changeStateWithoutLock(targetState int) {
	if rf.currentState != targetState {
		logPrint("%v: Change state from %v to %v", rf.me, rf.currentState, targetState)
		rf.currentState = targetState
	}
}
//丢弃到lastindex的日志
func (rf *Raft) shrinkLogWithoutLock(remainLog []Entry, lastIndex int, lastTerm int) []Entry {
	newLog := make([]Entry, len(remainLog)+1)
	newLog[0] = Entry{
		Term:  lastTerm,
		Index: lastIndex,
		Data:  nil,
	}
	// copy 数组
	for i:=1; i<len(newLog); i++ {
		newLog[i] = remainLog[i-1]
	}
	logPrint("%v: Shrink new log, lastIndex: %v, lastTerm: %v, length: %v", rf.me, lastIndex, lastTerm, len(newLog)-1)
	return newLog
}

func (rf *Raft) reInitializedLeaderStateWithoutLock() {
	// reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.logIndex+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _:= range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	// 设置自己的matchIndex
	rf.matchIndex[rf.me] = rf.logIndex

	// reset replicator
	for i,_ := range rf.replicatorStatus {
		rf.replicatorStatus[i] = false
	}
}

func (rf *Raft) incTermWithoutLock(targetTerm int) {
	rf.currentTerm = targetTerm
	rf.votedFor = NONE
	rf.persistWithoutLock(nil)
}

func (rf *Raft) closePeerReplicatorWithoutLock(peer int, argsTerm int) {
	if argsTerm == NONE {
		rf.replicatorStatus[peer] = false
	} else if argsTerm == rf.currentTerm {
		rf.replicatorStatus[peer] = false
	}
}

func (rf *Raft) getBeforeLogIndexWithoutLock() int {
	return rf.log[0].Index
}

// 这里要判断是大于，还是大于等于
func (rf *Raft) isLargerHostLogWithoutLock (lastLogIndex int, lastLogTerm int) bool {
	//logPrint("%v: Compare up-to-date log, logIndex: %v, logTerm: %v, ownLogLength: %v, ownLogTerm: %v", rf.me, lastLogIndex, lastLogTerm, rf.logIndex, rf.log[rf.logIndex].Term)
	beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
	if lastLogTerm > rf.log[rf.logIndex-beforeLogIndex].Term {
		return true
	} else if lastLogTerm == rf.log[rf.logIndex-beforeLogIndex].Term {
		return lastLogIndex >= rf.logIndex
	}  else {
		return false
	}
}

func (rf *Raft) checkLeaderCommitIndexWithoutLock() {
	tmpArray := make([]int, len(rf.matchIndex))
	copy(tmpArray, rf.matchIndex)
	sort.Ints(tmpArray)
	min := tmpArray[len(rf.matchIndex)/2]
	logPrint("Leader %v: check commit index, min: %v, nowCommitIndex: %v, all peers matchIndex: %+v", rf.me, min, rf.commitIndex, rf.matchIndex)
	if min <= rf.commitIndex {
		return
	} else {
		//originCommitIndex := rf.commitIndex
		beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
		for i:=rf.commitIndex+1; i<=min; i++ {
			if rf.log[i-beforeLogIndex].Term == rf.currentTerm {
				// only commit own term
				logPrint("Leader %v: Increase commit index from %v to %v", rf.me, rf.commitIndex, i)
				rf.commitIndex = i
			}
		}
		rf.applyCond.Signal()
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		// 临时复制一个，因为 log 后面会被改动
		tmpEntries := make([]Entry, rf.commitIndex-lastApplied)
		logPrint("%v: commitIndex: %v, lastApplied: %v, beforeLogIndex: %v", rf.me, commitIndex, lastApplied, beforeLogIndex)
		copy(tmpEntries, rf.log[lastApplied+1-beforeLogIndex: commitIndex+1-beforeLogIndex])
		rf.mu.Unlock()
		for _, entry := range tmpEntries {
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       entry.Data,
				CommandIndex:  entry.Index,
				// used in kv raft
				CommandTerm: entry.Term,
			}
			rf.applyCh <- applyMsg
			logPrint("%v, Send applyMsg: %+v", rf.me, applyMsg)
		}
		rf.mu.Lock()
		// 因为在放锁的这段时间里面，lastApplied 可能会被 install snapshot 改变
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) startReplication(peer int, heartbeatConflictIndex int) {
	if peer == rf.me {
		// ignore it
		return
	}
	rf.mu.Lock()
	if rf.replicatorStatus[peer] {
		rf.mu.Unlock()
		return
	} else {
		rf.replicatorStatus[peer] = true
		rf.mu.Unlock()
	}

	// 是否一次性发送所有log直到logIndex
	allInBatch := false
	// 如果 conflict 为 none 时，
	conflictIndex := heartbeatConflictIndex
	for rf.killed() == false {
		// execute in loop, until log is synchronized
		rf.mu.Lock()
		if rf.currentState != LEADER {
			//rf.replicatorStatus[peer] = false
			rf.closePeerReplicatorWithoutLock(peer, NONE)
			rf.mu.Unlock()
			return
		}


		// 因为有时候后面的while重试已经自动补全了，所以这里判断下，如果已经 up-to-date 了，就没必要继续了
		if rf.matchIndex[peer] == rf.logIndex {
			// peer's log is up-to-date with leader
			logPrint("Leader %v: %v's log is up-to-date with me, stop replicate", rf.me, peer)
			//rf.replicatorStatus[peer] = false
			rf.closePeerReplicatorWithoutLock(peer, NONE)
			rf.mu.Unlock()
			return
		}

		// 首先判断是否需要安装 snapshot
		if conflictIndex == NONE {
			// 如果没有newLog，nextIndex会溢出，这里判断下
			rf.nextIndex[peer] = min(rf.nextIndex[peer], rf.logIndex)
		} else {
			rf.nextIndex[peer] = conflictIndex
		}

		beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
		if rf.nextIndex[peer] <= beforeLogIndex {
			// need install snapshot
			logPrint("Leader %v: find %v's log %v <= %v lay too much, install snapshot", rf.me, peer, rf.nextIndex[peer], beforeLogIndex)
			snapshotArgs := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log[0].Index,
				LastIncludeTerm:   rf.log[0].Term,
				Data:              rf.readSnapshotWithoutLock(),
			}

			rf.mu.Unlock()
			snapshotReply := &InstallSnapshotReply{}
			if rf.sendInstallSnapshot(peer, snapshotArgs, snapshotReply) {
				rf.mu.Lock()
				if snapshotArgs.Term > rf.currentTerm {
					rf.currentTerm = snapshotReply.Term
					rf.changeStateWithoutLock(FOLLOWER)
					rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
					//rf.replicatorStatus[peer] = false
					rf.closePeerReplicatorWithoutLock(peer, snapshotArgs.Term)
					rf.mu.Unlock()
					return
				}

				if rf.currentState != LEADER || snapshotReply.Term != rf.currentTerm {
					//rf.replicatorStatus[peer] = false
					rf.closePeerReplicatorWithoutLock(peer, snapshotArgs.Term)
					rf.mu.Unlock()
					return
				}

				// install snapshot success
				rf.matchIndex[peer] = max(snapshotArgs.LastIncludedIndex, rf.matchIndex[peer])
				rf.nextIndex[peer] = snapshotArgs.LastIncludedIndex+1
				rf.checkLeaderCommitIndexWithoutLock()
				// 这里 conflict index 设置为none就行了，后面可以靠 nextIndex 继续操作
				logPrint("Leader: %v, Install to %v snapshot success, peer matchIndex: %v, nextIndex: %v", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
				conflictIndex = NONE
				allInBatch = true
				rf.mu.Unlock()
				// 给一段时间让 follower去apply snapshot
				//time.Sleep(time.Second)
				continue
			} else {
				// send failed, retry
				continue
			}
		}

		// 这里才能正常发送 appendEntry
		logPrint("Leader %v: start to replicate to %v, term: %v, nextIndex: %v, beforeIndex: %v", rf.me, peer, rf.currentTerm, rf.nextIndex[peer], beforeLogIndex)
		var sliceEntries []Entry = nil
		if allInBatch {
			sliceEntries = rf.log[rf.nextIndex[peer]-beforeLogIndex: rf.logIndex-beforeLogIndex+1]
		} else {
			sliceEntries = rf.log[rf.nextIndex[peer]-beforeLogIndex: rf.nextIndex[peer]-beforeLogIndex+1]
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[peer]-1,
			PrevLogTerm:  rf.log[rf.nextIndex[peer]-1-beforeLogIndex].Term,
			Entries:      sliceEntries,
			LeaderCommit: rf.commitIndex,
		}

		reply := &AppendEntriesReply{}
		rf.mu.Unlock()
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.changeStateWithoutLock(FOLLOWER)
				rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
				//rf.replicatorStatus[peer] = false
				rf.closePeerReplicatorWithoutLock(peer, args.Term)
				rf.mu.Unlock()
				return
			}

			if rf.currentState != LEADER || args.Term != rf.currentTerm {
				//rf.replicatorStatus[peer] = false
				rf.closePeerReplicatorWithoutLock(peer, args.Term)
				rf.mu.Unlock()
				return
			}
			logPrint("Leader %v: Replicate data to %v, length: %v, get reply: %+v", rf.me, peer, len(args.Entries), reply)
			if reply.Success {
				logPrint("Leader %v: replicate data %+v to %v SUCCESS", rf.me, args, peer)
				rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.checkLeaderCommitIndexWithoutLock()
				allInBatch = true
			} else {
				if reply.ConflictIndex == NONE {
					// outdated reply, ignore
					//rf.replicatorStatus[peer] = false
					rf.closePeerReplicatorWithoutLock(peer, args.Term)
					rf.mu.Unlock()
					return
				}
				// backtrack
				logPrint("Leader %v: replicate data to %v FAILED, start to backtrack, reply: %+v", rf.me, peer, reply)
				// conflictIndex 至少从1开始
				conflictIndex = reply.ConflictIndex
				allInBatch = false
			}
			rf.mu.Unlock()
		} else {
			logPrint("%v: send append entry to %v failed", rf.me, peer)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	logPrint("%v: Start to election, term: %v", rf.me, rf.currentTerm)
	beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
		LastLogIndex: rf.logIndex, LastLogTerm: rf.log[rf.logIndex-beforeLogIndex].Term}
	gotVoted := 1
	rf.votedFor = rf.me
	rf.persistWithoutLock(nil)
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			go func(peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					logPrint("%v, receive requestVote reply from %v: %+v", rf.me, peer, reply)
					if reply.Term > rf.currentTerm {
						logPrint("%v: Receive larger term %v>%v in requestVote reply, convert to follower", rf.me, reply.Term, rf.currentTerm)
						rf.incTermWithoutLock(reply.Term)
						rf.changeStateWithoutLock(FOLLOWER)
						rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
						rf.mu.Unlock()
						return
					}

					if rf.currentState != CANDIDATE || reply.Term != rf.currentTerm {
						// ignore outdated requestVoteReply
						rf.mu.Unlock()
						return
					}

					if reply.VoteGranted {
						gotVoted++
						logPrint("%v: Got votes: %v/%v in term %v", rf.me, gotVoted, len(rf.peers)/2+1, rf.currentTerm)
					}

					if gotVoted > len(rf.peers) / 2 {
						logPrint("%v: Become leader, term: %v", rf.me, rf.currentTerm)
						rf.changeStateWithoutLock(LEADER)
						rf.reInitializedLeaderStateWithoutLock()
						rf.mu.Unlock()
						rf.sendHeartbeats()
						rf.heartBeatTimer.Reset(getStableSendHeartbeatTimeout())
					} else {
						rf.mu.Unlock()
					}
				} else {
					logPrint("%v: send requestVote to %v no reply", rf.me, peer)
				}
			}(peer)
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			go func(peer int) {
				rf.mu.Lock()
				beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
				if rf.nextIndex[peer] <= beforeLogIndex {
					// lay to much, install snapshot first
					if !rf.replicatorStatus[peer] {
						go rf.startReplication(peer, NONE)
					}
					rf.mu.Unlock()
					return
				}
				args :=
					&AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
						PrevLogIndex: rf.nextIndex[peer]-1, PrevLogTerm: rf.log[rf.nextIndex[peer]-1-beforeLogIndex].Term,
						Entries: []Entry{}, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						logPrint("%v: Receive larger term %v>%v in heartbeat reply, convert to follower", rf.me, reply.Term, rf.currentTerm)
						rf.incTermWithoutLock(reply.Term)
						rf.changeStateWithoutLock(FOLLOWER)
						rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
						return
					}

					if rf.currentState == LEADER && reply.Term == rf.currentTerm {
						if !reply.Success {
							if reply.ConflictIndex == NONE {
								// out-dated reply
								return
							}
							if !rf.replicatorStatus[peer] {
								logPrint("Leader %v: Find %v's log in not consistent with me through heartbeat, start to replicate", rf.me, peer)
								go rf.startReplication(peer, reply.ConflictIndex)
							} else {
								logPrint("Leader %v: Find %v's log in not consistent with me through heartbeat, start to replicate, but replicator is already running", rf.me, peer)
							}
						}
					}
				}
			}(peer)
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistWithoutLock(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistData := PersistData{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		LogIndex:    rf.logIndex,
		Log:         rf.log,
	}
	e.Encode(persistData)
	data := w.Bytes()
	if snapshot != nil {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshotWithoutLock() []byte {
	return rf.persister.ReadSnapshot()
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
	var persistData = PersistData{}
	if d.Decode(&persistData) == nil {
		logPrint("%v: Read existed persist data, %+v", rf.me, persistData)
		rf.currentTerm = persistData.CurrentTerm
		rf.votedFor = persistData.VotedFor
		rf.log = persistData.Log
		rf.logIndex = persistData.LogIndex

		if rf.log[0].Index != 0 {
			rf.lastApplied = rf.log[0].Index
			rf.commitIndex = rf.log[0].Index
		}
	}
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		logPrint("%v: Reject outdated snapshot in CondInstallSnapshot, %v <= %v", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.logIndex {
		rf.log =  rf.shrinkLogWithoutLock([]Entry{}, lastIncludedIndex, lastIncludedTerm)
	} else {
		beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
		rf.log = rf.shrinkLogWithoutLock(rf.log[lastIncludedIndex-beforeLogIndex+1:], lastIncludedIndex, lastIncludedTerm)
	}

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.logIndex = max(rf.logIndex, lastIncludedIndex)
	rf.persistWithoutLock(snapshot)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 这里发送过来的一定是已经被 commit 的
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
	if index <= beforeLogIndex {
		logPrint("%v: Already drop outdated data before index: %v", rf.me, index)
		return
	}

	lastIndex := rf.log[index-beforeLogIndex].Index
	lastTerm := rf.log[index-beforeLogIndex].Term
	rf.log = rf.shrinkLogWithoutLock(rf.log[index-beforeLogIndex+1:], lastIndex, lastTerm)
	rf.persistWithoutLock(snapshot)
	logPrint("%v: Created snapshot, lastIndex: %v, lastTerm: %v", rf.me, rf.log[0].Index, rf.log[0].Term)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// 2A
	Term int
	CandidateId int

	// 2B
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	// 2b
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	// not official
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludeTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

type Entry struct {
	Term int
	Index int
	Data interface{}
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	originalState := rf.currentState

	if args.Term >  rf.currentTerm {
		logPrint("%v: Receive larger term %v>%v in requestVote, convert to follower", rf.me, args.Term, rf.currentTerm)
		rf.incTermWithoutLock(args.Term)
		rf.changeStateWithoutLock(FOLLOWER)
		// 暂时还不要 reset，只有投票给它才 reset
		//rf.electionTimeoutTimer.Reset(getRandomElectionTimeout())
		if originalState == LEADER {
			// 用于修复可能无法启动 timer 的 bug
			rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
		}
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == NONE && rf.isLargerHostLogWithoutLock(args.LastLogIndex, args.LastLogTerm) {
		// 只有 follower 才会到这一步
		logPrint("%v: Voted for candidate: %v, term: %v", rf.me, args.CandidateId, rf.currentTerm)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persistWithoutLock(nil)
		rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
	} else {
		logPrint("%v: Already voted for candidate: %v or log is not up-to-date, refuse candidate: %v, term: %v", rf.me, rf.votedFor, args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.incTermWithoutLock(args.Term)
	}
	// 此时 term 一定相等了
	rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
	// 有可能自己是和 leader 一样 term 的candidate, 收到来自同 term 的 leader，强制转换
	rf.changeStateWithoutLock(FOLLOWER)


	rf.currentLeader = args.LeaderId
	reply.Term = rf.currentTerm
	beforeLogIndex := rf.getBeforeLogIndexWithoutLock()

	if args.PrevLogIndex < beforeLogIndex {
		reply.Success = false
		reply.ConflictIndex = NONE
		return
	}

	if args.PrevLogIndex > rf.logIndex || rf.log[args.PrevLogIndex-beforeLogIndex].Term != args.PrevLogTerm {
		logPrint("Follower %v, Received log is not consistent with me, reply false", rf.me)
		reply.Success = false
		// not heartbeat, do something to accelerated log backtracking
		if args.PrevLogIndex > rf.logIndex {
			reply.ConflictIndex = rf.logIndex
			logPrint("Follower %v: Own logIndex %v < args.PrevLogIndex %v, set conflictIndex=%v", rf.me, rf.logIndex, args.PrevLogIndex, reply.ConflictIndex)
		} else {
			conflictTerm := rf.log[args.PrevLogIndex-beforeLogIndex].Term
			for i := args.PrevLogIndex; i>beforeLogIndex; i-- {
				if rf.log[i-beforeLogIndex].Term != conflictTerm {
					reply.ConflictIndex = i+1
					break
				}
			}
			if reply.ConflictIndex == 0 {
				reply.ConflictIndex = beforeLogIndex+1
			}
			logPrint("Follower %v: Own logIndex %v >= args.PrevLogIndex %v, set conflictIndex=%v", rf.me, rf.logIndex, args.PrevLogIndex, reply.ConflictIndex)
		}
	} else {
		reply.Success = true
		if len(args.Entries) > 0 {
			logPrint("Follower %v: Receive log and merge it, term: %v, argsPrevIndex: %v, entry len: %v, command: %+v", rf.me, rf.currentTerm, args.PrevLogIndex, len(args.Entries), args.Entries)

			for i, v := range args.Entries {
				index := args.PrevLogIndex + 1 + i
				if len(rf.log)-1 >= index-beforeLogIndex {
					// overwrite existed log
					rf.log[index-beforeLogIndex] = v
				} else {
					// append new log
					rf.log = append(rf.log, v)
				}
				rf.logIndex=index
			}
			rf.persistWithoutLock(nil)
		} else {
			logPrint("Follower %v: Receive heartbeat from %v", rf.me, args.LeaderId)
		}

		// update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			// 此处有坑，不能用 len(rf.log) 来确定 commitIndex，因为这不能代表你接收的最新
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >  rf.currentTerm {
		logPrint("%v: Receive larger term %v>%v in installSnapshot", rf.me, args.Term, rf.currentTerm)
		rf.incTermWithoutLock(args.Term)
	}
	// 不管怎么样，直接 change state 到 follower
	rf.changeStateWithoutLock(FOLLOWER)
	rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		// outdated data
		logPrint("Follower %v: Receive outdated installSnapshot, %v<=%v", rf.me, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		logPrint("Follower %v: Apply snapshot term: %v, index: %v", rf.me, applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)
		rf.applyCh <- applyMsg
	}()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//tmpCh := make(chan bool)
	//go func() {
	//	tmpCh <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//}()
	//select {
	//case res := <- tmpCh:
	//	close(tmpCh)
	//	return res
	//case <-time.After(time.Millisecond * 2000):
	//	close(tmpCh)
	//	return false
	//}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


// Start
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == LEADER {
		index := rf.logIndex+1
		term := rf.currentTerm
		//logPrint("Leader %v: Receive a command %v, start to send, index: %v, term: %v", rf.me, command, index, term)
		entry := Entry{Term: rf.currentTerm, Index: index, Data: command}
		beforeLogIndex := rf.getBeforeLogIndexWithoutLock()
		if index - beforeLogIndex > len(rf.log) - 1 {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[index-beforeLogIndex] = entry
		}
		rf.logIndex = index
		rf.persistWithoutLock(nil)
		rf.matchIndex[rf.me] = rf.logIndex

		for v := range rf.peers {
			if !rf.replicatorStatus[v] {
				go rf.startReplication(v, NONE)
			}
		}
		return index, term, true
	} else {
		//logPrint2("%v: I'm not leader, refuse command: %v", rf.me, command)
		return -1, -1, false
	}

}

// Kill
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

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.currentState == LEADER {
				rf.mu.Unlock()
				rf.sendHeartbeats()
				rf.heartBeatTimer.Reset(getStableSendHeartbeatTimeout())
			} else {
				rf.mu.Unlock()
			}
		case <-rf.electionTimeoutTimer.C:
			rf.mu.Lock()
			if rf.currentState == FOLLOWER || rf.currentState == CANDIDATE {
				rf.changeStateWithoutLock(CANDIDATE)
				rf.incTermWithoutLock(rf.currentTerm+1)
				rf.mu.Unlock()
				rf.startElection()
				rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

// Make
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

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = NONE
	rf.currentState = NONE
	rf.currentLeader = NONE

	// 2B
	// default contain empty log in 0
	rf.applyCh = applyCh
	rf.log = make([]Entry ,1)
	rf.logIndex = 0
	rf.log[0] = Entry{Index: 0, Term: 0, Data: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.replicatorStatus = make([]bool, len(rf.peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	// 2D
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.randLock = &sync.Mutex{}
	rf.electionTimeoutTimer = time.NewTimer(rf.getRandomElectionTimeout())
	rf.electionTimeoutTimer.Stop()
	rf.heartBeatTimer = time.NewTimer(getStableSendHeartbeatTimeout())
	rf.heartBeatTimer.Stop()

	// start with follower
	rf.changeStateWithoutLock(FOLLOWER)
	rf.electionTimeoutTimer.Reset(rf.getRandomElectionTimeout())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}
