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
	"unicode"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	//集群中的其他节点
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	cluster int     //集群中节点的数量
	statue mstatue  //raft的状态
	leaderid int  //leader的id
	term int //我的任期号
	heartBeat     time.Duration
	electionTime  time.Time
	//以下的元素，每次新任期开始都要清空
	tstatue	termstatue

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type termstatue struct {
	votenum int  //我的票数
	isvote bool  //true=以及投票了

}
//raft的状态
type  mstatue int
const (
	candidate mstatue = iota
	follower
	leader
)
//打印
/*
// example
216: [peer 2 (follower) at Term 0] election timeout
218: [peer 2 (candidate) at Term 1] start a new election
219: [peer 2 (candidate) at Term 1] request vote from peer 0
219: [peer 2 (candidate) at Term 1] request vote from peer 1
222: [peer 0 (follower) at Term 1] vote for peer 2
*/
type  RaftPrint bool
const (
	log RaftPrint=true

)
func MyPrint(rf *Raft, format string, a ...interface{}) {
	if RaftPrint {
		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(rf.allBegin).Milliseconds(), rf.me, rf.statue, rf.term}, a...)
		fmt.Printf(format, a...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term=rf.term
	isleader=false
	if rf.statue==leader{
		isleader=true
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term int //自己的任期号
	candidate int //自己的id
	lastlogindex int//自己最后的日志号
	lastlogterm int//自己最后的日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term int //自己的任期号
	vote bool //是否投票：true=同意
}
//更新任期
func Updateterm(rf *Raft)  {
	rf.tstatue.isvote=false
	rf.tstatue.votenum=0
	//更新时间
	//rf.tstatue.Time
}
func Vote(rf*Raft,reply*RequestVoteReply)  {
	if(rf.tstatue.isvote){
		reply.vote=false
	}else{
		reply.vote=true
		rf.tstatue.isvote=true
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//reply的任期是没有改变的任期吗？version1：是的
	reply.term=rf.term

	switch rf.statue {

		case leader:
			//收到大于自己任期号，说明leader落后了,投票然后转为follower
			if args.term>rf.term{
				rf.statue=follower
				Updateterm(rf)
				//reply.term=rf.term
				rf.term=args.term
				reply.vote=true
				rf.tstatue.isvote=true

			}
		case candidate:
			//如果candidate收到了比他任期号还大的请求，降级为follower
			if(args.term>rf.term){
				rf.statue=follower
				Updateterm(rf)
				//reply.term=rf.term
				rf.term=args.term
				Vote(rf,reply)
			}
		case follower:
			//如果任期号相等,
			if(args.term==rf.term){
				Vote(rf,reply)
			}else if(args.term>rf.term){
				//reply.term=rf.term
				Updateterm(rf)
				rf.term=args.term
				Vote(rf,reply)
			}

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	switch rf.statue {
		case candidate:
			//如果reply的任期比我还高，那么candidate转为follower，停止投票
			if reply.term>rf.term{
				rf.statue=follower
				//重社超时时间
				rf.RandomElection()
				return ok
			}
			rf.tstatue.votenum+=1
			if(rf.tstatue.votenum>=(rf.cluster/2+1)){
				rf.statue=leader
				Updateterm(rf)
				//向所有节点发送keep-alive

			}

	}
	return ok
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

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
//Start（命令）要求Raft启动处理，将命令附加到复制的日志。Start（）应立即返回，而不必等待日志附加完成。
//该服务希望您的实现为每个新提交的日志条目向applyCh通道参数Make（）发送ApplyMsg。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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
//你只应该在以下情况下重置你的选举计时器：a）你从当前的领导者那里得到一个AppendEntries RPC
//（即，如果AppendEntries参数中的任期已经过时，你不应该重启你的计时器）；b）你正在开始一个选举；或者c）你授予另一个对等体一个投票。
func (rf*Raft)RandomElection()  {

}
//开始选举，如果每当选，说明都没发送，等待下次ticker
func(rf*Raft)  Leaderelection() {
	rf.term+=1
	rf.statue=candidate
	var  args=RequestVoteArgs{rf.term,rf.me,0,0}
	var  reply = RequestVoteReply{}
	for i := range rf.peers {
		if i != rf.me {
			//应该要并发处理，否则，发完一个就卡住了！！！
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}
//一条日志
type Entry struct {
	Command	interface{}
	Term	int
	Index	int
	IsEmpty	bool
}
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// 领导人的任期号
	Term			int
	// 领导的Id
	LeaderId		int
	// 最后日志条目的索引值
	PrevLogIndex	int
	// 最后日志条目的任期号
	PrevLogTerm		int
	// 准备存储的日志条目
	Entries 		[]Entry
	// 领导人已经提交的日志的索引值
	LeaderCommit	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {

	// Your data here (2A).
	// 当前任期号，以便于领导人去更新自己的任期号
	Term 		int
	// Follower匹配了PrevLogIndex和PrevLogTerm时为真
	Success		bool

}
//leader维持心跳
func (rf* Raft) Keepalive()  {

}
//ticker会以心跳为周期不断检查状态。如果当前是Leader就会发送心跳包，而心跳包是靠appendEntries()发送空log
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		//defer rf.mu.Unlock()
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.statue == leader {
			//发送心跳包
			rf.Keepalive()
		}
		//如果超时，开始选举
		if time.Now().After(rf.electionTime) {
			MyPrint(rf,"start a new election")
			//设置随机选举超时时间
			rf.RandomElection()
			rf.Leaderelection()
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Unlock()
	}
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

	rf.statue=follower
	//50ms为一心跳
	rf.heartBeat=100 * time.Millisecond
	//设置超时时间，超时时间》心跳
	rf.RandomElection()

	Updateterm(rf)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}