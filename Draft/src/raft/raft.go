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
	"6.824/labgob"

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


type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
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
	NextIndex[] int //对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为领导者的最后一个日志索引+1），用于性能
	matchIndex[] int //对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调增加）,用于安全
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	Logs []Entry

	lastIncludeIndex int  //快照包含的最后下标
	lastIncludeTerm  int  //快照包含的最后任期

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

	rf.mu.Lock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//------------------------------------------------------about log
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.Commitindex=0  //提交的日志下标
	rf.LastApplied=0  //提交了的日志下标
	rf.lastIncludeIndex=0
	rf.lastIncludeTerm=0
	rf.NextIndex=make([]int,len(peers))  //各个节点要发送的下一个日志下标---》当前发送的日志下标=next-1
	rf.matchIndex=make([]int,len(peers)) // 对于每一个server，已经复制给该server的最后日志条目下标
	rf.Logs=make([]Entry,1)
	rf.Logs[0].Term=0
	//-------------------------------------------------------about term
	rf.Updateterm(0)
	rf.statue=follower
	//-----------------------------------------------------------about time
	//30ms为一心跳
	rf.heartBeat=40 * time.Millisecond
	//初始的超时时间应该是随机的，防止大部分节同时到candidate
	rf.timeout=time.Duration(150+rand.Intn(200))* time.Millisecond//150-350ms的超时时间
	//rf.Log("timeout %v",rf.timeout)
	rf.electiontime=time.Now().Add(rf.timeout)
	//bug1：time.Time类型才能加减，time.Duration不行
	rf.timeBegin=time.Now()

	//--------------------------------------------------------start work
	//回复crash前的日志,忘记加锁，bug+1

	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	//rf.Log("Start")
	// start ticker goroutine to start elections
	go rf.ticker()
	//开始工作，用来将提交的log写进状态机
	go rf.Applier()

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

func (rf*Raft) Getleader() int  {
	if rf.killed() {
		return -1
	}
	return rf.leaderid
}
//--------------------------------------------------------ticker() and worker()---------------------------------------------

//ticker会以心跳为周期不断检查状态。如果当前是Leader就会发送心跳包，而心跳包是靠appendEntries()发送空log
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		time.Sleep(40 * time.Millisecond)

		rf.mu.Lock()

		if rf.statue==leader{
			//rf.Log("start keep-alive")
			rf.Keepalive()  //keep-alive如果有新加的条目，就一次性发送
			//如果现在的时间超出选举时间，说明超时了，开始选举
		}else if time.Now().After(rf.electiontime) {

			rf.statue=candidate
			rf.Updateterm(rf.term+1)
			rf.tstatue.Votenum+=1
			rf.tstatue.Isvote=true
			rf.Upelection()

			rf.Log("start election")

			rf.Leaderelection()
		}

		rf.mu.Unlock()


	}
}

func (rf* Raft) Applier()  {

	for rf.killed()==false {

		time.Sleep(35*time.Millisecond)

		rf.mu.Lock()

		if len(rf.Logs)==0||rf.LastApplied>=rf.Commitindex  {
			rf.mu.Unlock()
			continue
		}

		appendlog:=make([]ApplyMsg,0)

		for rf.LastApplied < rf.Commitindex {
				rf.LastApplied += 1
				appendlog = append(appendlog, ApplyMsg{
					CommandValid:  true,
					SnapshotValid: false,
					CommandIndex:  rf.LastApplied,
					Command:       rf.restoreLog(rf.LastApplied).Command,
				})
		}

		rf.persist()

		rf.mu.Unlock()

		for _,applog := range appendlog{
			rf.applyCh<-applog
		}
	}

}

//--------------------------------------------------------选举有关---------------------------------------------
//开始选举，如果每当选，说明都没发送，等待下次ticker
func(rf*Raft)  Leaderelection() {
	rf.statue=candidate
	var  args=RequestVoteArgs{rf.term,rf.me,rf.getLastIndex(),rf.getLastTerm()}

	for i := range rf.peers {
		if i != rf.me {
			var  reply = RequestVoteReply{}
			//应该要并发处理，否则，发完一个就卡住了！！！
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

//no-op 当上leader后立即提交，而不用等到下次再来,并快速定位各个节点的状态机情况
func (rf* Raft) quaickcommit()  {
	rf.Start(Entry{})
	rf.Keepalive()
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
		rf.Log("a higher vote,Become follower")
	}
	if rf.statue==follower{
		if args.Term>=rf.term {
			//否则要进行一致性检验
			if args.Lastlogterm<rf.getLastTerm(){
				rf.Log("Vote false on LastTerm who come from %v his loglastterm %v",args.Candidate,args.Lastlogterm)
				reply.Vote=false
				reply.Term=rf.term
				return
			}else if args.Lastlogterm==rf.getLastTerm(){
				if args.Lastlogindex<rf.getLastIndex(){
					rf.Log("Vote false on LastIndex who come from %v his Lastlogindex %v",args.Candidate,args.Lastlogindex)
					reply.Vote=false
					reply.Term=rf.term
					return
				}else {
					rf.Log("Vote pass yizhixing")
					reply.Term=rf.term
					Vote(rf,args,reply)
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
	if ok==false||rf.killed() {
		return false
	}


	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.statue {
	//对于leader而言，如果reply的term》leader，说明leader已经过期了
	case leader:
		if reply.Term>rf.term{
			rf.Log("receive a higher reply %v , become follower",reply.Term)
			rf.statue=follower
			rf.Updateterm(reply.Term)
			return reply.Vote
		}
	case candidate:
		//如果reply的任期比我还高，那么candidate转为follower，停止投票
		if reply.Term>rf.term{

			rf.Log("receive a higher reply %v , become follower",reply.Term)
			rf.statue=follower
			//如果收到比我任期还大的，不用重设超时时间
			rf.Updateterm(reply.Term)
			return reply.Vote
		}
		if reply.Vote {

			rf.tstatue.Votenum+=1
			if rf.tstatue.Votenum>=(len(rf.peers)/2+1){

				rf.tstatue.Votenum=0   //防止多次唤醒

				rf.statue=leader

				ll:=rf.getLastIndex()
				for i := range rf.peers  {
					rf.NextIndex[i]=ll+1
				}
				rf.matchIndex[rf.me]=ll

				rf.Log("Become leader!!!! leader lastlog index %v , leader commit %v",rf.getLastIndex(),rf.Commitindex)
				//新leader的next下标应该是最大值，否则从0开始发送，网络负担太大
				//rf.NextIndex=make([]int,len(rf.peers))
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
	var num=1
	for i:=0;i< len(rf.peers);i++{
		if i!=rf.me{

			reply:=AppendEntriesReply{}

			args:=AppendEntriesArgs{
				Term: rf.term,
				Leaderid:rf.me,
				Previogierm: 0,
				Previogindex: 0,
				Entries: nil,
				Ladercommit: rf.Commitindex,
			}

			args.Previogindex,args.Previogierm=rf.getPrevLogInfo(i)

			//如果要发送的日志在快照里，发送快照
			if rf.lastIncludeIndex>=rf.NextIndex[i] {

				//go 	rf.leaderSendSnapShot(i)
				continue
			}else if rf.getLastIndex()>=rf.NextIndex[i]{    //如果要发送的日志不在快照且在范围内
				tmp:=rf.restoreindex(rf.NextIndex[i])
				entries := make([]Entry, 0)
				entries = append(entries,rf.Logs[tmp:]...)
				rf.Log("send log entries size %v",len(entries))
			}
			//将节点缺失的日志一次性全发出去！！！而不是等到服务端一次次回滚
			//如果nextIndex[i]长度不等于rf.logs,代表与leader的log entries不一致，需要附带过去
			go func(i int) {
				ok := rf.peers[server].Call("Raft.RequestApp", args, reply)
				if ok==false||rf.killed() {
					return
				}

				//对于leader而言，如果reply的term》leader，说明leader已经过期了
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term>rf.term{
					rf.statue=follower
					rf.Updateterm(reply.Term)
					rf.Log("Higher append reply , become follower")
					return
				}
				if rf.statue!=leader{
					return
				}
				if reply.Success{  //如果时reply success，那么所有节点的日志都应该是一样的，因为leader会将冲突到最新的节点一次性发送
					//这里有bug，如果是keeepalive，那么args.Entries=0,所以应该是+而不是=
					rf.matchIndex[i]=args.Previogindex+len(args.Entries)  //可能会一次提交多条日志
					rf.NextIndex[i] = rf.matchIndex[i] + 1
					rf.Log("peer %v accept append which waht %v",i,rf.NextIndex[i],args.Previogindex)
					num++
					if num >= len(rf.peers)/2+1{
						num=0  //保证不会提交两次！！
						if len(rf.Logs)==0{
							return
						}
						if  rf.matchIndex[i]>rf.matchIndex[rf.me]{

							rf.matchIndex[rf.me]=rf.matchIndex[i]
							upidx:=rf.matchIndex[rf.me]

							if rf.Logs[upidx].Term==rf.term {
								rf.Commitindex=upidx
								rf.Log("Leader Commited %v",rf.Commitindex)
							}else {
								rf.Log("Leader Commited failde beacuse log.term %v are not now term %v",rf.Logs[upidx].Term,rf.term)
							}
						}
						//leader只能提交当前任期的日志！！
						//对于leader而言，只要通过半数就可提交了

					}
				}else {

					rf.NextIndex[i]=reply.Upnextindex
					rf.Log("peer %v refused ,which want %v",server,reply.Upnextindex)
				}
			}(i)

			//go rf.sendAppendEntry(i,&args,&reply,&num)
		}
	}
}
//如果follower没有响应，不断重发。如果follower拒绝，根据reply.Upnextindex恢复follower缺少的日志
//第一条有效日志：term=0
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply,num* int) bool {


	ok := rf.peers[server].Call("Raft.RequestApp", args, reply)
	if ok==false||rf.killed() {
		return false
	}

	//对于leader而言，如果reply的term》leader，说明leader已经过期了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term>rf.term{
		rf.statue=follower
		rf.Updateterm(reply.Term)
		rf.Log("Higher append reply , become follower")
		return reply.Success
	}
	if rf.statue!=leader{
		return  false
	}
	if reply.Success{  //如果时reply success，那么所有节点的日志都应该是一样的，因为leader会将冲突到最新的节点一次性发送
		//这里有bug，如果是keeepalive，那么args.Entries=0,所以应该是+而不是=
		rf.matchIndex[server]=args.Previogindex+len(args.Entries)  //可能会一次提交多条日志
		rf.NextIndex[server] = rf.matchIndex[server] + 1
		rf.Log("peer %v accept append which waht %v Pre %v",server,rf.NextIndex[server],args.Previogindex)
		*num++
		if *num >= len(rf.peers)/2+1{
			*num=0  //保证不会提交两次！！
			if len(rf.Logs)==0{
				return true
			}
			if  rf.matchIndex[server]>rf.matchIndex[rf.me]{

				rf.matchIndex[rf.me]=rf.matchIndex[server]
				upidx:=rf.matchIndex[rf.me]

				if rf.Logs[upidx].Term==rf.term {
					rf.Commitindex=upidx
					rf.Log("Leader Commited %v",rf.Commitindex)
				}else {
					rf.Log("Leader Commited failde beacuse log.term %v are not now term %v",rf.Logs[upidx].Term,rf.term)
				}
			}
			//leader只能提交当前任期的日志！！
			//对于leader而言，只要通过半数就可提交了

		}
	}else {

		rf.NextIndex[server]=reply.Upnextindex
		rf.Log("peer %v refused ,which want %v",server,reply.Upnextindex)
	}

	return reply.Success
}

//对append的回应：注意：不要将心跳和日志append分开处理！！！！
//
func (rf *Raft) RequestApp(args *AppendEntriesArgs, reply *AppendEntriesReply)  {

	//rf.Log("receive a keep-alive from leader %v which term %v",args.Leaderid,args.Term)


	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term>rf.term{
		rf.Updateterm(args.Term)
		rf.statue=follower
		rf.Log("Higher append , become follower")
	}
	if rf.statue==leader{
		reply.Success=false
		reply.Term=rf.term
		return
	}
	if args.Term>=rf.term{
		//收到心跳包
		rf.Upelection()

		if (args.Previogindex>=rf.lastIncludeIndex&& args.Previogindex<=rf.getLastIndex()&&rf.restoreLogTerm(args.Previogindex)==args.Previogierm){

			// 如果存在日志包那么进行追加
			if args.Entries != nil {

				/*firstIndex := rf.lastIncludeIndex
				//找到第一个冲突日志
				tmpidx :=args.Previogindex+1
				for index,e := range args.Entries{
					if rf.restoreLogTerm(tmpidx)!=e.Term {
						rf.Logs = append(rf.Logs[:args.Previogindex+1-rf.lastIncludeIndex], args.Entries...)
					}
				}
				for index, entry := range args.Entries {
					if tmpidx-firstIndex >= len(rf.Logs) || rf.Logs[entry.Index-firstIndex].Term != entry.Term {
						rf.Logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...))
						break
					}
				}*/
				//+1是因为切片末尾-1
				rf.Logs = append(rf.Logs[:args.Previogindex+1-rf.lastIncludeIndex], args.Entries...)
			}

			rf.Commitindex=Min(rf.getLastIndex(),args.Ladercommit)

			rf.Log("accept the log now lastlog index %v, peer have commit %v",rf.getLastIndex(),rf.Commitindex)
			reply.Success=true
		} else {

			conflicidx:=0
			if args.Previogindex>rf.getLastIndex(){
				conflicidx=rf.getLastIndex()+1
			}else if args.Previogindex<rf.lastIncludeIndex {
				conflicidx=rf.lastIncludeIndex+1
			} else{
				tempTerm := rf.restoreLogTerm(args.Previogindex)
				//我在这任期出问题了，返回这个任期内的第一个索引
				for index := args.Previogindex; index > rf.lastIncludeIndex; index-- {
					if rf.restoreLogTerm(index) != tempTerm {
						conflicidx= index+1
						break
					}
				}
			}

			reply.Upnextindex=Max(conflicidx,rf.lastIncludeIndex+1)  //返回的应该是commited了
			reply.Success=false
			rf.Log("log append false, I want %v but accept a log after Previogindex %v",reply.Upnextindex,args.Previogindex)
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

		term := rf.term
		rf.Logs = append(rf.Logs, Entry{Term: term, Command: command})
		index:=rf.getLastIndex()
		//rf.persist()
		rf.Log("a new log to leader , now leader lastlog index %v",index)
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
	rf.persister.SaveRaftState(rf.persistdate())
}
func (rf *Raft) persistdate() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.Logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.Commitindex)
	e.Encode(rf.LastApplied)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	return data
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []Entry
	var lastidx int
	var lastterm int
	var comidx int
	var appidx int

	//var lastIncludeIndex int
	//var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastidx) != nil ||
		d.Decode(&lastterm) != nil ||
		d.Decode(&comidx) != nil ||
		d.Decode(&appidx) != nil{
		rf.Log("Decode error")
	} else {
		rf.term = currentTerm
		//rf.tstatue.Votenum=votenum
		rf.Logs = logs
		rf.lastIncludeIndex=lastidx
		rf.lastIncludeTerm=lastterm
		rf.Commitindex=comidx
		rf.LastApplied=appidx
		//rf.lastIncludeIndex = lastIncludeIndex
		//rf.lastIncludeTerm = lastIncludeTerm
	}

}



//--------------------------------------------------------快照---------------------------------------------

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
//由领导者调用，向跟随者发送快照的分块。领导者总是按顺序发送分块。
func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.term,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)

	if ok == true {
		rf.mu.Lock()
		if rf.statue != leader || rf.term != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.term {
			rf.statue = follower
			rf.Updateterm(reply.Term)
			//rf.persist()
			rf.Upelection()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.NextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
//您需要实现本文中讨论的InstallSnapshot RPC，
//它允许Raft领导者告诉滞后的Raft对等方用快照替换其状态。您可能需要仔细考虑InstallSnapshot应该如何与图2中的状态和规则交互。

//当追随者的Raft代码收到InstallSnapshot RPC时，它可以使用applyCh将快照发送到ApplyMsg中的服务。
//ApplyMsg结构定义已经包含了您需要的字段（以及测试人员需要的字段）。请注意，这些快照只会提升服务的状态，而不会导致服务向后移动。

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.term > args.Term {
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}

	rf.Updateterm(args.Term)

	reply.Term = args.Term

	//rf.persist()
	rf.Upelection()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}


	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.Logs = tempLog
	if index > rf.Commitindex {
		rf.Commitindex = index
	}
	if index > rf.LastApplied {
		rf.lastIncludeIndex = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistdate(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <-msg

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	//fmt.Println("[Snapshot] commintIndex", rf.commitIndex)
	if rf.lastIncludeIndex >= index || index > rf.Commitindex {
		return
	}
	// 更新快照日志
	sLogs := make([]Entry, 0)
	sLogs = append(sLogs, Entry{})
	for i := index + 1; i <= len(rf.Logs); i++ {
		//sLogs = append(sLogs, rf.restoreLog(i))
	}

	//fmt.Printf("[Snapshot-Rf(%v)]rf.commitIndex:%v,index:%v\n", rf.me, rf.commitIndex, index)
	// 更新快照下标/任期
	if index == len(rf.Logs)+1 {
		rf.lastIncludeTerm = rf.Logs[len(rf.Logs)-1].Term
	} else {
		//rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.Logs = sLogs

	// apply了快照就应该重置commitIndex、lastApplied
	if index > rf.Commitindex {
		rf.Commitindex = index
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
	}

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistdate(), snapshot)

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

