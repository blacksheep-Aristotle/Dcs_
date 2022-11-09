package kvraft

import (
	"fmt"
	"sync"
	"time"
)


type ClientOperation struct {
	SequenceNum int
	Result ChanResult
}


type ChanResult struct {
	value string
	Comid int
	Clerkid int64
	Err Err
}

type Err string
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)


// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    op // "Put" or "Append"
	ClerkId int64
	CommId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type  op int
const (
	Put op = iota
	Append
)

type PutAppendReply struct {
	Err   Err
	Statue bool
	Value string
	LeaderId int
}

type GetArgs struct {
	Key string
	CommId int
	ClerkId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Statue bool
	Value string
	LeaderId int
}



type Snowflake struct {
	sync.Mutex     // 锁
	timestamp    int64 // 时间戳 ，毫秒
	workerid     int64  // 工作节点
	datacenterid int64 // 数据中心机房id
	sequence     int64 // 序列号
}

const (
	epoch             = int64(1577808000000)                           // 设置起始时间(时间戳/毫秒)：2020-01-01 00:00:00，有效期69年
	timestampBits     = uint(41)                                       // 时间戳占用位数
	datacenteridBits  = uint(2)                                        // 数据中心id所占位数
	workeridBits      = uint(7)                                        // 机器id所占位数
	sequenceBits      = uint(12)                                       // 序列所占的位数
	timestampMax      = int64(-1 ^ (-1 << timestampBits))              // 时间戳最大值
	datacenteridMax   = int64(-1 ^ (-1 << datacenteridBits))           // 支持的最大数据中心id数量
	workeridMax       = int64(-1 ^ (-1 << workeridBits))               // 支持的最大机器id数量
	sequenceMask      = int64(-1 ^ (-1 << sequenceBits))               // 支持的最大序列id数量
	workeridShift     = sequenceBits                                   // 机器id左移位数
	datacenteridShift = sequenceBits + workeridBits                    // 数据中心id左移位数
	timestampShift    = sequenceBits + workeridBits + datacenteridBits // 时间戳左移位数
)

func (s *Snowflake) NextVal() int64 {
	s.Lock()
	now := time.Now().UnixNano() / 1000000 // 转毫秒
	if s.timestamp == now {
		// 当同一时间戳（精度：毫秒）下多次生成id会增加序列号
		s.sequence = (s.sequence + 1) & sequenceMask
		if s.sequence == 0 {
			// 如果当前序列超出12bit长度，则需要等待下一毫秒
			// 下一毫秒将使用sequence:0
			for now <= s.timestamp {
				now = time.Now().UnixNano() / 1000000
			}
		}
	} else {
		// 不同时间戳（精度：毫秒）下直接使用序列号：0
		s.sequence = 0
	}
	t := now - epoch
	if t > timestampMax {
		s.Unlock()
		return 0
	}
	s.timestamp = now
	r := int64((t)<<timestampShift | (s.datacenterid << datacenteridShift) | (s.workerid << workeridShift) | (s.sequence))
	s.Unlock()
	return r
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}


const Kvraft = true
//运行时间 peer id号 （状态：0-follower 1-candidate 2-leader） 任期
func(kv *KVServer) Log(format string,a ...interface{}) {

	if Kvraft {
		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{kv.me}, a...)
		fmt.Printf(format, a...)
	}
}