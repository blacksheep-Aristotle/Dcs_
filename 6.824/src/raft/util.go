package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const RaftPrint = true
//运行时间 peer id号 （状态：0-follower 1-candidate 2-leader） 任期
func(rf *Raft) Log(format string,a ...interface{}) {

	if RaftPrint {
		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{time.Now().Sub(rf.timeBegin).Milliseconds(), rf.me, rf.statue, rf.term}, a...)
		fmt.Printf(format, a...)
	}
}

func Min(x int, y int) int {
	if x<y{
		return  x
	}else {
		return y
	}
}
func Max(x int,y int) int {
	if x>y {
		return x
	}else {
		return y
	}
}
func (rf* Raft) restoreindex(curIndex int) int {
	return curIndex-rf.lastIncludeIndex
}
// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) Entry {
	return rf.Logs[curIndex-rf.lastIncludeIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex==rf.lastIncludeIndex {
		return rf.lastIncludeTerm
	}
	//fmt.Printf("[GET] curIndex:%v,rf.lastIncludeIndex:%v\n", curIndex, rf.lastIncludeIndex)
	return rf.Logs[curIndex-rf.lastIncludeIndex].Term
}

// 获取最后的快照日志下标(代表已存储）
func (rf *Raft) getLastIndex() int {
	return len(rf.Logs) -1 + rf.lastIncludeIndex
}

// 获取最后的任期(快照版本
func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，否则最直接len == 0
	if len(rf.Logs)-1 == 0 {
		return rf.lastIncludeTerm
	} else {
		return rf.Logs[len(rf.Logs)-1].Term
	}
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := Max(rf.NextIndex[server] - 1,0)
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex >= lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

////：a）你从当前的领导者那里得到一个AppendEntries RPC（即，如果AppendEntries参数中的任期已经过时，你不应该重启你的计时器）；
//	//b）你正在开始一个选举；或者c）你授予另一个对等体一个投票。
func(rf*Raft) Upelection() {
	t:=time.Duration(180+rand.Intn(200))* time.Millisecond
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