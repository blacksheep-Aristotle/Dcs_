package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"go/ast"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//日志格式
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Commid    int
	ClientId int64
	Index    int   // raft服务层传来的Index
	OpType   comtype  //操作类型
}

type comtype string
const (
	New = "New"
	Add = "Add"
)
type KVServer struct {

	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	applyCh chan raft.ApplyMsg
	lastindex int
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvPersist map[string]string
	seqMap map[int64]int


	Db  KVStateMachine

	lastClientOperation map[int64]ClientOperation   //client 最后的操作,commid,value
	notifyChans map[int]chan ChanResult				//唤醒协程，value
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.Isrepeat(args.ClerkId,args.CommId) {
		reply.Statue=true
		reply.Value,reply.Err = kv.Db.Get(args.Key)
		return
	}

	_,flag := kv.rf.GetState()

	if !flag {
		reply.Err=ErrWrongLeader
		reply.Statue=false
		reply.LeaderId=kv.rf.Getleader()
		return
	}
	op := Op{OpType: "Get", Key: args.Key,Commid: args.CommId, ClientId: args.ClerkId}
	lastindex,_,_:=kv.rf.Start(op)

	timer := time.NewTicker(100 * time.Millisecond)
	notice := kv.createNotifyChan(lastindex)

	select {
		case <-notice:

		case <-timer.C:
			reply.Err=ErrTimeout
			reply.Statue=false
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.Isrepeat(args.ClerkId,args.CommId) {
		reply.Statue=true
		reply.Value,reply.Err = kv.Db.Get(args.Key)
		return
	}

	_,flag := kv.rf.GetState()

	if !flag {
		reply.Err=ErrWrongLeader
		reply.Statue=false
		reply.LeaderId=kv.rf.Getleader()
		return
	}
	op := Op{OpType: "Get", Key: args.Key,Commid: args.CommId, ClientId: args.ClerkId}
	lastindex,_,_:=kv.rf.Start(op)

	timer := time.NewTicker(100 * time.Millisecond)
	notice := kv.createNotifyChan(lastindex)

	select {
	case res:=<-notice:
		reply.Err=res.Err
		reply.Value=res.value
		reply.Statue=true
	case <-timer.C:
		reply.Err=ErrTimeout
		reply.Statue=false
	}
}
//创造一个通知协程，当applier写入了对应index的下标，
func (kv *KVServer)	createNotifyChan(index int) chan ChanResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := make(chan ChanResult)
	kv.notifyChans[index] = res
	return res
}

func (kv *KVServer) GetNotifyChan(index int) chan  ChanResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res,exit:=kv.notifyChans[index]
	if !exit {
		res=kv.createNotifyChan(index)
	}
	return res
}
func (kv*KVServer) Upcommd(Clerk int64,operation ClientOperation)  {

	if !kv.Isrepeat(Clerk,operation.SequenceNum) {
		kv.lastClientOperation[Clerk]=operation
	}

}
//type ApplyMsg struct {
//	CommandValid bool
//	Command      interface{}
//	CommandIndex int
//
//	// For 2D:
//	SnapshotValid bool
//	Snapshot      []byte
//	SnapshotTerm  int
//	SnapshotIndex int
//}

func (kv* KVServer) Applier()  {
	for kv.killed()==false {
		select {
			case msg :=<-kv.applyCh:
				//如果有效
				if msg.CommandValid {

					if msg.CommandIndex<=kv.lastindex {

						continue
					}

					var result ChanResult
					var err Err
					kv.lastindex=msg.CommandIndex
					command:=msg.Command.(Op)
					//如果已经提交到状态机里了
					if !kv.Isrepeat(command.ClientId,command.Commid) {
						switch command.OpType {
							case New:
								err=kv.Db.Put(command.Key,command.Value)
							case Add:
								err=kv.Db.Append(command.Key,command.Value)
						}
					}
					result.value=command.Value
					result.Err=err
					//更新最后状态
					kv.Upcommd(command.ClientId,ClientOperation{
						command.Commid,
						result,
					})

					kv.GetNotifyChan(msg.CommandIndex)<-result

					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
						snapshot := kv.PersistSnapShot()
						kv.rf.Snapshot(msg.CommandIndex, snapshot)
					}

				}else if msg.SnapshotValid {

					if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
						kv.DecodeSnapShot(msg.Snapshot)
						kv.lastindex = msg.SnapshotIndex
					}

				}
		}
	}
}

//------------------------------------------------------持久化部分--------------------------------------------------------

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)

	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}


// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.me, message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command := message.Command.(Command)

				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), message, kv.lastOperations[command.ClientId], command.ClientId)
					response = kv.lastOperations[command.ClientId].LastResponse
				} else {
					response = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

//这个操作是否重复
func (kv *KVServer) Isrepeat(Clinet int64,commid int)  bool{

	if value, ok := kv.lastClientOperation[Clinet]; ok {
		if value.SequenceNum <= commid {
			return true
		}
	}
	return false
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}

