package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	id      int64
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader    int
	lastCommandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	commandId := ck.lastCommandId + 1
	args := GetArgs{
		Key:       key,
		CkId:      ck.id,
		CommandId: commandId,
	}
	serverId := ck.lastLeader
	value := ""
	for ; ; serverId = (serverId + 1) % len(ck.servers) {
		var reply GetReply
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			Debug(dTrace, "client[%d] failed to send Get request [%v].ok [%v], reply [%v]", ck.id, args, ok, reply)
			continue
		} else {
			Debug(dTrace, "client[%d] send Get request [%v] successfully. reply is [%v]", ck.id, args, reply)
			ck.lastLeader = serverId
			ck.lastCommandId = commandId
			value = reply.Value
			break
		}
	}
	// You will have to modify this function.
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	Debug(dTrace, "[CK] %d try to PutAppend key %s value %s op %s from server", ck.id, key, value, op)
	commandId := ck.lastCommandId + 1
	args := PutAppendArgs{
		CommandId: commandId,
		Key:       key,
		CkId:      ck.id,
		Value:     value,
		OP:        op,
	}

	serverId := ck.lastLeader
	for ; ; serverId = (serverId + 1) % len(ck.servers) {
		var reply PutAppendReply
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			Debug(dTrace, "client[%d] failed to send PutAppend request [%v].ok=[%v], reply=[%v]", ck.id, args, ok, reply)
			continue
		} else {
			Debug(dTrace, "client[%d] send PutAppend request [%v] successfully", ck.id, args)
			ck.lastLeader = serverId
			ck.lastCommandId = commandId
			break
		}
	}
	// You will have to modify this function.

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
