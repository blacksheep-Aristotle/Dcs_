package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"math"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	ClerkId int64
	CommandId int
	LeaderId int
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClerkId=nrand()
	ck.CommandId=0
	ck.LeaderId=math.Rand(len(servers))
	// Your code here.
	return ck
}
//查询分片所在的分组配置
func (ck *Clerk) Query(num int) Config {
	ck.CommandId+=1

	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Log.ClerkId=ck.ClerkId
	args.Log.CommandId=ck.CommandId
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}else if reply.WrongLeader {
			ck.LeaderId=reply.LeaderId
		}

		//加睡眠是为了防止节点crash
		time.Sleep(100 * time.Millisecond)
	}
}
//新的配置应该尽可能均匀地将碎片分配到整个组中，并且应该尽可能少地移动碎片以实现这一目标。如果GID不是当前配置的一部分，
//则shardctrler应允许重复使用GID（即应允许GID加入，然后离开，然后再次加入）。
//为某个分组添加节点，或添加一个分组
func (ck *Clerk) Join(servers map[int][]string) {
	ck.CommandId+=1
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Log.ClerkId=ck.ClerkId
	args.Log.CommandId=ck.CommandId
	for {
		// try each known server.
	Retry:
		var reply JoinReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}else if  reply.WrongLeader {
			ck.LeaderId=reply.LeaderId
			goto Retry
		}

		time.Sleep(100 * time.Millisecond)
	}
}
//删除分组
func (ck *Clerk) Leave(gids []int) {
	ck.CommandId+=1
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Log.ClerkId=ck.ClerkId
	args.Log.CommandId=ck.CommandId
	for {
		// try each known server.
	Retry:
		var reply LeaveReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}else if  reply.WrongLeader {
			ck.LeaderId=reply.LeaderId
			goto Retry
		}
		time.Sleep(100 * time.Millisecond)
	}
}
//某个分片迁徙到另一个分组
func (ck *Clerk) Move(shard int, gid int) {
	ck.CommandId+=1
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Log.ClerkId=ck.ClerkId
	args.Log.CommandId=ck.CommandId
	for {
		// try each known server.
	Retry:
		var reply MoveReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}else if  reply.WrongLeader {
			ck.LeaderId=reply.LeaderId
			goto Retry
		}
		time.Sleep(100 * time.Millisecond)
	}
}
