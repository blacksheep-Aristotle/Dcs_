package kvraft
package shardctrler


import (
"6.824/raft"
"math"
"sort"
"sync/atomic"
"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32 // set by Kill()
	lastClientOperation map[int64]ClientOperation
	notifyChans map[int]chan ChanResult
	lastApplied int

	configs []Config // indexed by config num
}

type ClientOperation struct {
	SequenceNum int
	Config Config
	Err Err
}

type ChanResult struct {
	Config Config
	Err Err
}


func (sc *ShardCtrler) HandleControllerCommand(args *ControllerArgs, reply *ControllerReply) {
	sc.mu.Lock()
	if sc.isDuplicateRequestWithoutLock(args.ClientId, args.SequenceNum) {
		clientOperation := sc.lastClientOperation[args.ClientId]
		reply.Config = clientOperation.Config
		reply.Err = clientOperation.Err
		sc.mu.Unlock()
		return
	}

	if sc.isOutdatedRequestWithoutLock(args.ClientId, args.SequenceNum) {
		reply.Err = ErrNone
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(*args)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notify := sc.createNotifyChan(index)
	select {
	case res := <- notify:
		reply.Err = res.Err
		reply.Config = res.Config
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		close(sc.notifyChans[index])
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) createNotifyChan(index int) chan ChanResult {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	res := make(chan ChanResult)
	sc.notifyChans[index] = res
	return res
}

func (sc *ShardCtrler) saveNotifyChanWithoutLock(index int, res ChanResult) {
	if value, ok := sc.notifyChans[index]; ok {
		value <- res
	}
}

func (sc *ShardCtrler) isDuplicateRequestWithoutLock(clientId int64, sequenceNum int) bool {
	if value, ok := sc.lastClientOperation[clientId]; ok {
		if value.SequenceNum == sequenceNum {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) isOutdatedRequestWithoutLock(clientId int64, sequenceNum int) bool {
	if value, ok := sc.lastClientOperation[clientId]; ok {
		if value.SequenceNum > sequenceNum {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		select {
		case applyMsg := <- sc.applyCh:
			sc.mu.Lock()
			chanResult := ChanResult{}
			if applyMsg.CommandValid && applyMsg.CommandIndex > sc.lastApplied {
				args := applyMsg.Command.(ControllerArgs)
				if sc.isDuplicateRequestWithoutLock(args.ClientId, args.SequenceNum) {
					clientOperation := sc.lastClientOperation[args.ClientId]
					chanResult.Config = clientOperation.Config
					chanResult.Err = clientOperation.Err
				} else {
					config := sc.executeControllerCommandWithoutLock(args)
					sc.lastApplied = applyMsg.CommandIndex
					chanResult.Err = OK
					chanResult.Config = config
					sc.saveClientRequestReplyWithoutLock(args.ClientId, args.SequenceNum, config, OK)
				}
			}

			if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == applyMsg.CommandTerm {
				sc.saveNotifyChanWithoutLock(applyMsg.CommandIndex, chanResult)
			}

			sc.mu.Unlock()
		case <-time.After(10*time.Second):

		}
	}
}

func (sc *ShardCtrler) executeControllerCommandWithoutLock(args ControllerArgs) Config {
	switch args.ControllerType {
	case ControllerTypeMove:
		// use for tester
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num+1,
			Shards: lastConfig.Shards,
			Groups: deepCopyMap(lastConfig.Groups),
		}
		newConfig.Shards[args.Shard] = args.GID
		sc.configs = append(sc.configs, newConfig)
	case ControllerTypeLeave:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num+1,
			Shards: lastConfig.Shards,
			Groups: deepCopyMap(lastConfig.Groups),
		}
		for _, gid := range args.GIDs {
			delete(newConfig.Groups, gid)
		}
		balanceShards(sc.me, &newConfig)
		sc.configs = append(sc.configs, newConfig)
	case ControllerTypeJoin:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num+1,
			Shards: lastConfig.Shards,
			Groups: deepCopyMap(lastConfig.Groups),
		}
		// add groups
		for gid, servers := range args.Servers {
			newConfig.Groups[gid] = servers
		}
		// balance shards
		balanceShards(sc.me, &newConfig)
		sc.configs = append(sc.configs, newConfig)
	case ControllerTypeQuery:
		if args.Num == -1 || args.Num >= len(sc.configs)-1 {
			return sc.configs[len(sc.configs)-1]
		} else {
			return sc.configs[args.Num]
		}
	}

	return Config{}
}

func deepCopyMap(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func balanceShards(me int, config *Config)  {
	logPrint("%v: Balance config before %+v", me, config)
	if len(config.Groups) == 0 {
		// reset
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// only 1 gid
		for k, _ := range config.Groups {
			for i:=0; i<NShards; i++ {
				config.Shards[i] = k
			}
		}
	} else {
		gidShardMap := createGidShardMapFromConfig(*config)

		for {
			minGid, min, maxGid, max := findMinMaxGid(gidShardMap)
			if math.Abs(float64(max - min)) <= 1 {
				break
			}
			// start to balance
			source := gidShardMap[maxGid]
			// must sort
			sort.Ints(source)
			moveShard := source[len(source)-1]
			gidShardMap[maxGid] = source[:len(source)-1]
			target := gidShardMap[minGid]
			gidShardMap[minGid] = append(target, moveShard)
		}

		for gid, shards := range gidShardMap {
			for _, shard := range shards {
				config.Shards[shard] = gid
			}
		}
	}
	logPrint("%v: Balance config after %+v", me, config)
}

func createGidShardMapFromConfig(config Config) map[int][]int {
	gidShardMap := make(map[int][]int)
	for k, _ := range config.Groups {
		gidShardMap[k] = []int{}
	}
	for k, v := range config.Shards {
		origin, ok := gidShardMap[v]
		if ok {
			gidShardMap[v] = append(origin, k)
		} else {
			minGid, _, _, _ := findMinMaxGid(gidShardMap)
			gidShardMap[minGid] = append(gidShardMap[minGid], k)
		}
	}
	return gidShardMap
}

func findMinMaxGid(gidShardCountMap map[int][]int) (int, int, int, int) {
	i := 0
	min := 0
	minGid := 0
	max := 0
	maxGid := 0
	for gid, shard := range gidShardCountMap {
		if i==0 {
			min = len(shard)
			minGid = gid
			max = len(shard)
			maxGid = gid
		} else {
			if len(shard) > max {
				max = len(shard)
				maxGid = gid
			} else if len(shard) == max && gid > maxGid {
				maxGid = gid
			} else if len(shard) < min {
				min = len(shard)
				minGid = gid
			} else if len(shard) == min && gid < minGid {
				minGid = gid
			}
		}
		i++
	}
	return minGid, min, maxGid, max
}


func (sc *ShardCtrler) saveClientRequestReplyWithoutLock(clientId int64, sequenceNum int, config Config, err Err) {
	sc.lastClientOperation[clientId] = ClientOperation{
		SequenceNum: sequenceNum,
		Config:    config,
		Err:         err,
	}
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z==1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(ControllerArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.notifyChans = make(map[int]chan ChanResult)
	sc.lastClientOperation = make(map[int64]ClientOperation)

	go sc.applier()

	return sc
}
