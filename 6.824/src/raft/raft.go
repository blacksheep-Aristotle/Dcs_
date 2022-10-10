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
	"debug/elf"
	"flag"

	//"debug/elf"
	"fmt"
	"go/ast"
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


	Commitindex  int//已知被提交的最高日志条目的索引（初始化为0，单调增加）
	LastApplied  int //已提交的最高日志索引（初始化为0，单调增加）
	NextIndex[] int //对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为领导者的最后一个日志索引+1）
	matchIndex[] int //对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调增加）
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	Logs []Entry

	heartBeat     time.Duration  //leader的心跳时间
	timeout		  time.Duration  //超时时间，超过就开始选举
	timeBegin	  time.Time //start time used to debug
	electiontime  time.Time
	tstatue	termstatue //以下的元素，每次新任期开始都要清空

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//------------------------------------------------------about log
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.Commitindex=0  //提交的日志下标
	rf.LastApplied=0  //提交了的日志下标
	rf.NextIndex=make([]int,len(peers))  //各个节点要发送的下一个日志下标---》当前发送的日志下标=next-1
	rf.matchIndex=make([]int,len(peers)) //各个节点以提交的最大下标
	rf.Logs=make([]Entry,0)
	//-------------------------------------------------------about term
	rf.Updateterm(0)
	rf.statue=follower
	//-----------------------------------------------------------about time
	//30ms为一心跳
	rf.heartBeat=30 * time.Millisecond
	//初始的超时时间应该是随机的，防止大部分节同时到candidate
	rf.timeout=time.Duration(150+rand.Intn(200))* time.Millisecond//150-350ms的超时时间
	//rf.Log("timeout %v",rf.timeout)
	rf.electiontime=time.Now().Add(rf.timeout)
	//bug1：time.Time类型才能加减，time.Duration不行
	rf.timeBegin=time.Now()

	//--------------------------------------------------------start work
	rf.readPersist(persister.ReadRaftState())
	//rf.Log("Start")
	// start ticker goroutine to start elections
	go rf.ticker()
	//开始工作，用来接受log
	go rf.Worker()

	return rf
}
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
}
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// 领导人的任期号
	Term			int
	// 领导的Id
	Leaderid		int
	//--------这两个是用来一致性检查，只有都和follower一致，才会被接受，否则
	// 前一个日志号
	Previogindex	int
	// 前一个日志的任期号
	Previogierm		int
	// 准备存储的日志条目（做心跳使用时，内容为空；为了提高效率可一次性发送多个）
	Entries 		[]Entry
	// 领导人已经提交的最高日志值 comm=min(Ladercommit,my last log)
	Ladercommit	int
}

type AppendEntriesReply struct {

	// Your data here (2A).
	// 当前任期号，以便于领导人去更新自己的任期号
	Term 		int
	// Follower匹配了PrevLogIndex和PrevLogTerm时为真
	Success		bool
	// 更新请求节点的nextIndex【i】,是发送未提交的最大值还是全部日志的最大值？？
	Upnextindex	int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

		time.Sleep(30 * time.Millisecond)

		rf.mu.Lock()

		if rf.statue==leader{
			//rf.Log("start keep-alive")
			rf.Keepalive()  //keep-alive如果有新加的条目，就一次性发送
			//如果现在的时间超出选举时间，说明超时了，开始选举
		}else if time.Now().After(rf.electiontime) {
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
	var  args=RequestVoteArgs{rf.term,rf.me,len(rf.Logs),0}

	if args.Lastlogindex>0{
		args.Lastlogterm=rf.Logs[len(rf.Logs)-1].Term
	}

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
	t:=time.Duration(150+rand.Intn(200))* time.Millisecond
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
func Vote(rf*Raft,args *RequestVoteArgs,reply*RequestVoteReply)  {
	if rf.tstatue.Isvote{
		reply.Vote=false
		//三种情况之一：投出票时重置
	}else{
		rf.Log("vote to %v",args.Candidate)
		reply.Vote=true
		rf.tstatue.Isvote=true
		rf.Upelection()
	}
}
//
// example RequestVote RPC handler.
//
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//如果arg的term大于我的term，reply=args.term，如果arg的term小于我的term，不改直接发
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Log("recieve a vote which come from %v",args.Candidate)
	reply.Term=rf.term

	if args.Term>rf.term{
		rf.statue=follower
		rf.Updateterm(args.Term)
	}
	if rf.statue==follower{
		if args.Term>=rf.term {
			//如果是初始状态,不论是谁都投票
			if len(rf.Logs)==0 {
				Vote(rf,args,reply)
				return
			}
			//否则要进行一致性检验
			if args.Lastlogterm<rf.Logs[len(rf.Logs)-1].Term{
				rf.Log("Vote false on yizhixing come from %v his loglastterm %v",args.Candidate,args.Lastlogterm)
				reply.Vote=false
				reply.Term=rf.term
				return
			}else if args.Lastlogterm==rf.Logs[len(rf.Logs)-1].Term {
				if args.Lastlogindex<len(rf.Logs){
					rf.Log("Vote false on yizhixing come from %v his Lastlogindex %v",args.Candidate,args.Lastlogindex)
					reply.Vote=false
					reply.Term=rf.term
					return
				}
			}else {
				rf.Log("Vote pass yizhixing")
				reply.Term=rf.term
				Vote(rf,args,reply)

			}
		}
	}



}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok{
		if rf.killed()==true||rf.statue!=candidate{
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
			rf.Updateterm(reply.Term)
			return reply.Vote
		}
	case candidate:
		//如果reply的任期比我还高，那么candidate转为follower，停止投票
		if reply.Term>rf.term{

			rf.Log("receive a higher request %v",reply.Term)
			rf.statue=follower
			//如果收到比我任期还大的，不用重设超时时间
			rf.Updateterm(reply.Term)
			return reply.Vote
		}
		if reply.Vote {

			rf.tstatue.Votenum+=1
			if rf.tstatue.Votenum>=(rf.cluster/2+1){

				rf.tstatue.Votenum=0
				rf.Log("Become leader!!!!")
				rf.statue=leader
				rf.Keepalive()
				//向所有节点发送keep-alive
				return reply.Vote
			}
		}


	}
	return reply.Vote
}

//--------------------------------------------------------keep-alive---------------------------------------------

//leader维持心跳
func (rf* Raft) Keepalive()  {
	num:=1
	for i:=0;i< len(rf.peers);i++{
		if i!=rf.me{
			args:=AppendEntriesArgs{
				Term: rf.term,
				Leaderid:rf.me,
				Previogierm: 0,
				Previogindex: 0,
				Entries: nil,
				Ladercommit: rf.Commitindex,
			}
			reply:=AppendEntriesReply{}
			go rf.sendAppendEntry(i,&args,&reply,&num)
		}
	}
}
//如果follower没有响应，不断重发。如果follower拒绝，根据reply.Upnextindex恢复follower缺少的日志
//第一条有效日志：term=0
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply,num* int) bool {

	//将节点缺失的日志一次性全发出去！！！而不是等到服务端一次次回滚
	//如果nextIndex[i]长度不等于rf.logs,代表与leader的log entries不一致，需要附带过去
	args.Entries=rf.Logs[rf.NextIndex[server]-1:]

	//说明不是第一次
	if rf.NextIndex[server]>0{

		args.Previogindex=rf.NextIndex[server]-1
	}
	if args.Previogindex>0{

		args.Previogierm=rf.Logs[args.Previogindex-1].Term
	}
	ok := rf.peers[server].Call("Raft.RequestApp", args, reply)
	for !ok{
		if rf.killed()||rf.statue!=leader {
			return false
		}
		ok=rf.peers[server].Call("Raft.RequestApp", args, reply)
	}
	//对于leader而言，如果reply的term》leader，说明leader已经过期了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term>rf.term{
		rf.statue=follower
		rf.Updateterm(reply.Term)
		return reply.Success
	}
	if rf.statue!=leader{
		return  false
	}
	if reply.Success{
		rf.Log("peer accept %v",server)
		rf.NextIndex[server]=len(args.Entries)  //可能会一次提交多条日志
		*num+=1
		if *num>=len(rf.peers)/2+1{
			*num=0  //保证不会提交两次！！
			if len(rf.Logs)==0{
				return true
			}
			for rf.LastApplied<len(rf.Logs) {
				rf.LastApplied++
				apply:=ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.LastApplied,
					Command: rf.Logs[rf.LastApplied-1].Command,
				}
				rf.applyCh<-apply
				rf.Commitindex=rf.LastApplied
			}
			rf.Log("Commited %v",rf.Commitindex)
		}
	}else {
		rf.Log("peer refused %v,which want %v",server,reply.Upnextindex)
		rf.NextIndex[server]=reply.Upnextindex
	}

	return reply.Success
}

//对append的回应：注意：不要将心跳和日志append分开处理！！！！
//
func (rf *Raft) RequestApp(args *AppendEntriesArgs, reply *AppendEntriesReply)  {

	//rf.Log("receive a keep-alive from leader %v which term %v",args.Leaderid,args.Term)
	Min := func(x int, y int) int {
		if x<y{
			return  x
		}else {
			return y
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>rf.term{
		rf.Updateterm(args.Term)
		rf.statue=follower
	}
	if rf.statue==leader{
		reply.Success=false
		reply.Term=rf.term
		return
	}
	if args.Term>=rf.term{
		//收到心跳包
		rf.Upelection()
		//如果节点此时是空日志,判断是否是第一条日志，如果不是返回
		if  len(rf.Logs)==0{
			if args.Previogindex!=0||args.Previogierm!=0{  //说明缺失了第一条日志
				rf.Log("miss the first")
				reply.Success=false
				reply.Upnextindex=0
				reply.Term=rf.term
				return
			}else {
				rf.Log("accept first")
				// 如果存在日志包那么进行追加
				if args.Entries != nil {
					rf.Logs = rf.Logs[:args.Previogindex]
					rf.Logs = append(rf.Logs, args.Entries...)

				}
				reply.Success=true
				reply.Term=rf.term

				rf.Commitindex=Min(len(rf.Logs),args.Ladercommit)
				for rf.LastApplied<=rf.Commitindex {
					rf.LastApplied++
					apply:=ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.LastApplied,
						Command: rf.Logs[rf.LastApplied-1].Command,
					}
					rf.applyCh<-apply
				}
				rf.Log("accept the log index %v, peer have commit %v",len(rf.Logs),rf.Commitindex)
				reply.Success=true
				return
			}
		}
		//如果通过一致性检验
		if args.Previogindex>0&& args.Previogindex<len(rf.Logs)&&rf.Logs[args.Previogindex-1].Term!=args.Previogierm{
			rf.Log("accept yizhixing ")
			// 如果存在日志包那么进行追加
			if args.Entries != nil {
				rf.Logs = rf.Logs[:args.Previogindex]
				rf.Logs = append(rf.Logs, args.Entries...)

			}

			rf.Commitindex=Min(len(rf.Logs),args.Ladercommit)
			for rf.LastApplied<rf.Commitindex {

				rf.LastApplied++
				apply:=ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.LastApplied,
					Command: rf.Logs[rf.LastApplied-1].Command,
				}
				rf.applyCh<-apply
			}
			rf.Log("accept the log index %v, peer have commit %v",len(rf.Logs),rf.Commitindex)
			reply.Success=true
		} else {

			reply.Upnextindex=rf.LastApplied+1
			reply.Success=false
			rf.Log("log append false, I want %v",reply.Upnextindex)
		}
	}else{
		//如果收到term比我低的心跳包，要重置时间吗？应该不用吧。。。
		rf.Log("accept a lower keep-alive! from %v",args.Leaderid)
		reply.Success=false
	}
	reply.Term=rf.term
}
//--------------------------------------------------------Log部分---------------------------------------------
//Start（命令）要求Raft启动处理，将命令附加到复制的日志。Start（）应立即返回，而不必等待日志附加完成。
//该服务希望您的实现为每个新提交的日志条目向applyCh通道参数Make（）发送ApplyMsg。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() == true||rf.statue != leader {
		return -1, -1, false
	} else {
		index := len(rf.Logs) + 1
		term := rf.term
		rf.Logs = append(rf.Logs, Entry{Term: term, Command: command})
		rf.persist()

		rf.Log("a new log to leader %v",index)
		return index, term, true
	}

	// Your code here (2B).
	//return index, term, isLeader
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






func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.Log("I AM DEAD")
	rf.mu.Unlock()
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
const RaftPrint = true
//运行时间 peer id号 （状态：0-follower 1-candidate 2-leader） 任期
func(rf *Raft) Log(format string,a ...interface{}) {
	if RaftPrint {
		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(rf.timeBegin).Milliseconds(), rf.me, rf.statue, rf.term}, a...)
		fmt.Printf(format, a...)
	}
}