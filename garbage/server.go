
package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)


//type Op struct {
//	// Your definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister
	db *KvDb
	lastClientOperation map[int64]ClientOperation
	notifyChans map[int]chan ChanResult

	lastApplied int
}

type ClientOperation struct {
	SequenceNum int
	Value string
	Err Err
}

type ChanResult struct {
	Value string
	Err Err
}

type PersistData struct {
	Db *KvDb
	LastClientOperation map[int64]ClientOperation
}

func (kv *KVServer) HandleCommand(args *ClientRequestArgs, reply *ClientRequestReply) {
	kv.mu.Lock()

	if kv.isDuplicateRequestWithoutLock(args.ClientId, args.SequenceNum) {
		reply.Status = true
		clientOperation := kv.lastClientOperation[args.ClientId]
		reply.Response = clientOperation.Response
		reply.Err = clientOperation.Err
		logPrint("%v: %v-%v is duplicate request, return directly", kv.me, args.ClientId, args.SequenceNum)
		kv.mu.Unlock()
		return
	}

	if kv.isOutdatedRequestWithoutLock(args.ClientId, args.SequenceNum) {
		reply.Status = false
		reply.Response = ""
		reply.Err = ErrNone
		logPrint("%v: %v-%v is outdated data, return Err", kv.me, args.ClientId, args.SequenceNum)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(*args)

	if !isLeader {
		reply.Status = false
		reply.Response = ""
		reply.Err = ErrWrongLeader
		return
	}
	notify := kv.createNotifyChan(index)
	logPrint("%v: create notify chan %v for clientId %v, sequenceNum %v, wait for result", kv.me, index, args.ClientId, args.SequenceNum)
	select {
	case res := <- notify:
		reply.Status = true
		reply.Response = res.response
		reply.Err = res.Err
		logPrint("%v: receive from notify chan %v, success. ClientId %v, SequenceNum %v", kv.me, index, args.ClientId, args.SequenceNum)

	case <-time.After(ExecuteTimeout):
		reply.Status = false
		reply.Response = ""
		reply.Err = ErrTimeout
		logPrint("%v: receive from notify chan %v timeout. ClientId %v, SequenceNum %v", kv.me, index, args.ClientId, args.SequenceNum)
	}
	go func() {
		// release notify chan
		kv.mu.Lock()
		close(kv.notifyChans[index])
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}
//创建通知管道
func (kv *KVServer)	createNotifyChan(index int) chan ChanResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := make(chan ChanResult)
	kv.notifyChans[index] = res
	return res
}

//向通知管道写结果
func (kv *KVServer) saveNotifyChanWithoutLock(index int, res ChanResult) {
	if value, ok := kv.notifyChans[index]; ok {
		value <- res
	}
}
//是否是最后一个日志
func (kv *KVServer) isDuplicateRequestWithoutLock(clientId int64, sequenceNum int) bool {
	if value, ok := kv.lastClientOperation[clientId]; ok {
		if value.SequenceNum == sequenceNum {
			return true
		}
	}
	return false
}
//是否超界
func (kv *KVServer) isOutdatedRequestWithoutLock(clientId int64, sequenceNum int) bool {
	if value, ok := kv.lastClientOperation[clientId]; ok {
		if value.SequenceNum > sequenceNum {
			return true
		}
	}
	return false
}
//保存client最后结果
func (kv *KVServer) saveClientRequestReplyWithoutLock(clientId int64, sequenceNum int, response string, err Err) {
	kv.lastClientOperation[clientId] = ClientOperation{
		SequenceNum: sequenceNum,
		Response:    response,
		Err:         err,
	}
}
//是否需要压缩
func (kv *KVServer) needSnapshotWithoutLock() bool {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		return true
	} else {
		return false
	}
}
//通知rfat压缩
func (kv *KVServer) takeSnapshotWithoutLock(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistData := &PersistData{
		Db:                  kv.db,
		LastClientOperation: kv.lastClientOperation,
	}
	e.Encode(persistData)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) restoreFromSnapshotWithoutLock(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	persistData := &PersistData{}
	if d.Decode(&persistData) == nil {
		logPrint("%v: Read existed persist data, %+v", kv.me, persistData)
		kv.db = persistData.Db
		kv.lastClientOperation = persistData.LastClientOperation
	}

}

// Kill
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

//从状态机读数据
func (kv *KVServer) executeDbCommandWithoutLock(command Command) (string, Err) {
	if command.Op == OpRead {
		return kv.db.get(command.Key)
	} else if command.Op == OpPut {
		err := kv.db.put(command.Key, command.Value)
		return "", err
	} else if command.Op == OpAppend {
		err := kv.db.append(command.Key, command.Value)
		return "", err
	} else {
		return "", ErrNone
	}
}
//将commit的日志添加到状态机中
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <- kv.applyCh:
			kv.mu.Lock()
			if applyMsg.CommandValid {
				chanResult := ChanResult{}
				if applyMsg.CommandIndex > kv.lastApplied {
					args := applyMsg.Command.(ClientRequestArgs)
					// check is dup request
					if kv.isDuplicateRequestWithoutLock(args.ClientId, args.SequenceNum) {
						clientOperation := kv.lastClientOperation[args.ClientId]
						chanResult.response = clientOperation.Response
						chanResult.Err = clientOperation.Err
						logPrint("%v: %v-%v is duplicate request in apply msg, return directly", kv.me, args.ClientId, args.SequenceNum)
					} else {
						//将日志写入状态机
						response, err := kv.executeDbCommandWithoutLock(args.Command)
						kv.lastApplied = applyMsg.CommandIndex
						chanResult.response = response
						chanResult.Err = err
						logPrint("KV %v: Execute db command %+v, index: %v", kv.me, args.Command, applyMsg.CommandIndex)
						//将结果写入最后一个
						kv.saveClientRequestReplyWithoutLock(args.ClientId, args.SequenceNum, response, err)
					}

					if kv.needSnapshotWithoutLock() {
						kv.takeSnapshotWithoutLock(applyMsg.CommandIndex)
					}
				}

				// you need to check term
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == applyMsg.CommandTerm {
					kv.saveNotifyChanWithoutLock(applyMsg.CommandIndex, chanResult)
				}


			} else if applyMsg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.restoreFromSnapshotWithoutLock(applyMsg.Snapshot)
					kv.lastApplied = applyMsg.SnapshotIndex
				}
			}
			kv.mu.Unlock()
		case <-time.After(10*time.Second):
			// use for check kv.killed
		}
	}
}

// StartKVServer
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
	labgob.Register(ClientRequestArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.lastApplied = 0
	kv.db = MakeDb()
	kv.notifyChans = make(map[int]chan ChanResult)
	kv.lastClientOperation = make(map[int64]ClientOperation)


	kv.restoreFromSnapshotWithoutLock(kv.persister.ReadSnapshot())
	go kv.applier()
	return kv
}
