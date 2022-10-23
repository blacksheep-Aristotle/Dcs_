package kvraft

import (
	"6.824/labrpc"
	"go/ast"
	"go/constant"
	"reflect"
	"strings"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	ClerkId int
	CommentId int
	LeaderId int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}



//参数：服务器列表
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClerkId=snow()
	ck.CommentId=0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args:=GetArgs{
		key,
		ck.CommentId,
		ck.ClerkId,
	}
	reply:=GetReply{}
	ck.Getcommit(ck.LeaderId,&args,&reply)
	if reply.Err==OK {
		return  reply.Value
	}else if reply.Err==ErrWrongLeader{
		ck.LeaderId=reply.LeaderId
		return ck.Get(key)
	}else if reply.Err==ErrNoKey{
		return ""
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args:=PutAppendArgs{
		key,
		value,
		op,
		ck.ClerkId,
		ck.CommentId,
	}
	reply:=PutAppendReply{}
	ck.Addcommit(ck.LeaderId, &args, &reply)
	if reply.Err==OK {
		return
	}else if reply.Err==ErrWrongLeader{
		ck.LeaderId=reply.LeaderId
		ck.PutAppend(key,value,op)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Addcommit(server int,args *PutAppendArgs,reply *PutAppendReply)  {
	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
	for ok==false{
		ok = ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
	}
}

func (ck* Clerk) Getcommit(server int,args *GetArgs,reply *GetReply) {
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
	for ok==false{
		ok = ck.servers[server].Call("KVServer.Get", &args, &reply)
	}
}