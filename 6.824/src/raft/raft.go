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
	//"debug/elf"
	"fmt"
	"math/rand"
	//"unicode"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//"strings"
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
	heartBeat     time.Duration  //leader的心跳时间
	timeout		  time.Duration  //超时时间，超过就开始选举
	//：a）你从当前的领导者那里得到一个AppendEntries RPC（即，如果AppendEntries参数中的任期已经过时，你不应该重启你的计时器）；
	//b）你正在开始一个选举；或者c）你授予另一个对等体一个投票。
	timeBegin	  time.Time //start time used to debug
	electiontime  time.Time
	//以下的元素，每次新任期开始都要清空
	tstatue	termstatue

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 用来写入通道
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

}
type termstatue struct {
	Votenum int  //我的票数
	Isvote bool  //true=以及投票了

}
//raft的状态
type  mstatue int
const (
	follower mstatue = iota
	candidate
	leader
)


// --------------------------------------------------------RPC参数部分----------------------------------------------------
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //自己的任期号
	Candidate int //自己的id
	Lastlogindex int//自己最后的日志号
	Lastlogterm int//自己最后的日志的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int //自己的任期号
	Vote bool //是否投票：true=同意
}
//一条日志
type Entry struct {
	Command	interface{}
	Term	int
	Index	int
	Isempty	bool
}
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// 领导人的任期号
	Term			int
	// 领导的Id
	Leaderid		int
	// 最后日志条目的索引值
	Previogindex	int
	// 最后日志条目的任期号
	Previogierm		int
	// 准备存储的日志条目
	Entries 		[]Entry
	// 领导人已经提交的日志的索引值
	Ladercommit	int
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//------------------------------------------------------about log
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	//-------------------------------------------------------about term
	rf.Updateterm(0)
	rf.statue=follower
	//-----------------------------------------------------------about time
	//30ms为一心跳
	rf.heartBeat=30 * time.Millisecond
	//初始的超时时间应该是随机的，防止大部分节点同时到candidate
	rf.timeout=time.Duration(150*rand.Intn(200))* time.Millisecond//150-350ms的超时时间
	rf.electiontime=time.Now().Add(rf.timeout)
	//bug1：time.Time类型才能加减，time.Duration不行
	rf.timeBegin=time.Now()

	//--------------------------------------------------------start work
	rf.readPersist(persister.ReadRaftState())
	rf.Log("Start")
	// start ticker goroutine to start elections
	go rf.ticker()
	//开始工作，用来接受log
	go rf.Worker()

	return rf
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

//--------------------------------------------------------ticker() and worker()---------------------------------------------

//ticker会以心跳为周期不断检查状态。如果当前是Leader就会发送心跳包，而心跳包是靠appendEntries()发送空log
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		//defer rf.mu.Unlock()
		//当超时时


		//当超时时
		time.Sleep(rf.heartBeat)
		if rf.killed()==false{
			return
		}
		rf.mu.Lock()
		if rf.statue==leader{
			rf.Log("start keep-alive")
			rf.Keepalive()
			//如果现在的时间超出选举时间，说明超时了，开始选举
		}else if time.Now().After(rf.electiontime){
			rf.Log("start election")
			rf.statue=candidate
			rf.Updateterm(rf.term+1)
			rf.tstatue.Votenum+=1
			rf.tstatue.Isvote=true
			rf.Upelection()
			rf.Leaderelection()
		}
		rf.mu.Unlock()


	}
}

func (rf* Raft) Worker()  {

}

//--------------------------------------------------------选举有关---------------------------------------------
//开始选举，如果每当选，说明都没发送，等待下次ticker
func(rf*Raft)  Leaderelection() {
	rf.statue=candidate
	var  args=RequestVoteArgs{rf.term,rf.me,0,0}

	for i := range rf.peers {
		if i != rf.me {
			var  reply = RequestVoteReply{}
			//应该要并发处理，否则，发完一个就卡住了！！！
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}
////：a）你从当前的领导者那里得到一个AppendEntries RPC（即，如果AppendEntries参数中的任期已经过时，你不应该重启你的计时器）；
//	//b）你正在开始一个选举；或者c）你授予另一个对等体一个投票。
func(rf*Raft) Upelection() {
	t:=time.Duration(150*rand.Intn(200))* time.Millisecond
	rf.electiontime=time.Now().Add(t)
}
//更新任期
func(rf *Raft) Updateterm(t int)  {
	rf.term=t
	rf.tstatue.Isvote=false
	rf.tstatue.Votenum=0
	//更新时间
	//rf.tstatue.Time
}
func Vote(rf*Raft,reply*RequestVoteReply)  {
	if rf.tstatue.Isvote{
		reply.Vote=false
		//三种情况之一：投出票时重置
	}else{
		rf.Log("vote to ")
		reply.Vote=true
		rf.tstatue.Isvote=true
		rf.Upelection()
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Log("recieve a vote which come from %v",args.Candidate)
	//如果arg的term大于我的term，reply=args.term，如果arg的term小于我的term，不改直接发
	reply.Term=rf.term
	switch rf.statue {

	case leader:
		//收到大于自己任期号，说明leader落后了,投票然后转为follower
		if args.Term>rf.term{
			rf.statue=follower
			rf.Updateterm(args.Term)
			reply.Term=rf.term

			Vote(rf,reply)
		}else {
			reply.Vote=false
		}
	case candidate:
		//如果candidate收到了比他任期号还大的请求，降级为follower
		if args.Term>rf.term{
			rf.statue=follower
			rf.Updateterm(args.Term)
			reply.Term=rf.term
			Vote(rf,reply)
		}else {
			reply.Vote=false
		}
	case follower:
		//如果任期号相等,是不投票的！
		if args.Term>rf.term{  //说明进入下一次任期了
			//reply.term=rf.term
			rf.Updateterm(args.Term)
			reply.Term=rf.term
			Vote(rf,reply)
		}else {
			reply.Vote=false
		}

	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.Log("Send a quest vote to %v",server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok{
		if rf.killed()==true{
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.statue {
	//对于leader而言，如果reply的term》leader，说明leader已经过期了
	case leader:
		if reply.Term>rf.term{
			rf.Log("receive a higher request %v",reply.Term)
			rf.statue=follower
			return ok
		}
	case candidate:
		//如果reply的任期比我还高，那么candidate转为follower，停止投票
		if reply.Term>rf.term{
			rf.statue=follower
			//如果收到比我任期还大的，不用重设超时时间
			return ok
		}
		rf.tstatue.Votenum+=1
		if rf.tstatue.Votenum>=(rf.cluster/2+1){
			rf.Log("Become leader!!!!")
			rf.statue=leader
			rf.Updateterm(rf.term+1)

			rf.Keepalive()
			//向所有节点发送keep-alive
		}

	}
	return ok
}

//--------------------------------------------------------keep-alive---------------------------------------------

//leader维持心跳
func (rf* Raft) Keepalive()  {
	args:=AppendEntriesArgs{Term: rf.term,Leaderid:rf.me}

	for i:=0;i< len(rf.peers);i++{
		if i!=rf.me{
			reply:=AppendEntriesReply{}
			go rf.sendAppendEntry(i,&args,&reply)
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestApp", args, reply)
	for !ok{
		if rf.killed() {
			return false
		}
		ok=rf.peers[server].Call("Raft.RequestApp", args, reply)
	}
	//对于leader而言，如果reply的term》leader，说明leader已经过期了

	if reply.Term>rf.term{
		rf.statue=follower
		return ok
	}

	return reply.Success
}

//对append的回应：注意：不要将心跳和日志append分开处理！！！！
func (rf *Raft) RequestApp(args *AppendEntriesArgs, reply *AppendEntriesReply)  {

	rf.Log("receive a keep-alive from leader %v which term %v",args.Leaderid,args.Term)
	if args.Term>rf.term{
		rf.Updateterm(args.Term)
	}
	if args.Term>=rf.term{
		rf.statue=follower
		//收到心跳包
		rf.Upelection()

		reply.Success=true
		reply.Term=rf.term
	}else{
		//如果收到term比我低的心跳包，要重置时间吗？应该不用吧。。。
		reply.Term=rf.term
		reply.Success=false
	}

}
//--------------------------------------------------------持久化部分---------------------------------------------
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




//Start（命令）要求Raft启动处理，将命令附加到复制的日志。Start（）应立即返回，而不必等待日志附加完成。
//该服务希望您的实现为每个新提交的日志条目向applyCh通道参数Make（）发送ApplyMsg。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.Log("I AM DEAD")

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

//运行时间 peer id号 （状态：0-follower 1-candidate 2-leader） 任期
func(rf *Raft) Log(format string,a ...interface{}) {
	RaftPrint:=true
	if RaftPrint {
		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(rf.timeBegin).Milliseconds(), rf.me, rf.statue, rf.term}, a...)
		fmt.Printf(format, a...)
	}
}
