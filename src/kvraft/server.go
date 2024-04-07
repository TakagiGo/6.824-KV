package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const DEBUG = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}

type CommandType string

const (
	PutMethod    = "Put"
	AppendMethod = "Append"
	GetMethod    = "Get"
)

type Op struct {
	ClientId    int64
	CommandId   int
	CommandType CommandType
	Key         string
	Value       string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type CommandContext struct {
	Command int
	Reply   ApplyNotifyMsg
}
type KVServer struct {
	id           int64
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	kvDataBase   map[string]string
	dead         int32 // set by Kill()
	clientReply  map[int64]CommandContext
	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	replyChMap   map[int]chan ApplyNotifyMsg
	// Your definitions here.
}

type ApplyNotifyMsg struct {
	Term  int
	Value string
	Err   Err
}

func (kv *KVServer) listenCommit() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.SnapshotValid {
				kv.processCommand(msg)
			} else {
				kv.ApplySnapshot(msg)
			}
		}
	}
}

func (kv *KVServer) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotIndex < kv.lastApplied {
		return
	}
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastApplied = msg.SnapshotIndex
		kv.readSnapshot(msg.Snapshot)
	}
}

func (kv *KVServer) needSnapshot() bool {

	if kv.maxraftstate == -1 {
		return false
	}
	var proportion float32
	proportion = float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

func (kv *KVServer) processCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)
	if commandContext, ok := kv.clientReply[op.ClientId]; ok {
		if op.CommandId <= commandContext.Command {
			return
		}
	}
	var reply ApplyNotifyMsg
	if op.CommandType == PutMethod {
		kv.kvDataBase[op.Key] = op.Value
		// Debug(dTrace, "server[%d] finsh PutMethod key=%s value=%s", kv.id, op.Key, kv.kvDataBase[op.Key])
	} else if op.CommandType == AppendMethod {
		kv.kvDataBase[op.Key] += op.Value
		Debug(dTrace, "server[%d] finsh AppendMethod key=%s value=%s commandId=%d", kv.id, op.Key, kv.kvDataBase[op.Key], kv.clientReply[op.ClientId].Command)
	} else {
		reply.Value = kv.kvDataBase[op.Key]
		Debug(dTrace, "server[%d] finsh GetMethod key=%s value=%s commandId=%d", kv.id, op.Key, kv.kvDataBase[op.Key], kv.clientReply[op.ClientId].Command)
	}
	commandContext := CommandContext{
		Reply:   reply,
		Command: op.CommandId,
	}
	kv.clientReply[op.ClientId] = commandContext
	if replyCh, ok := kv.replyChMap[msg.CommandIndex]; ok {
		reply.Term = msg.CommandTerm
		reply.Err = OK
		replyCh <- reply
	}
	if kv.needSnapshot() {
		kv.KVinstallSnapShot(msg.CommandIndex)
	}
}

func (kv *KVServer) startSnapshot(index int) {
	DPrintf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

// 生成server的状态的snapshot
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.clientReply)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.kvDataBase)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}

func (kv *KVServer) KVinstallSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码currentTerm
	err := e.Encode(kv.clientReply)
	if err != nil {
		DPrintf("id[%d].lastApplied %d: encode currentTerm error: %v\n", kv.id, kv.lastApplied, err)
	}
	err = e.Encode(kv.kvDataBase)
	if err != nil {
		DPrintf("id[%d].kvDataBase %v: encode currentTerm error: %v\n", kv.id, kv.kvDataBase, err)
	}
	data := w.Bytes()
	go kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readSnapshot(data []byte) {
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clientReply map[int64]CommandContext
	var kvDataBase map[string]string
	if d.Decode(&clientReply) != nil || d.Decode(&kvDataBase) != nil {
		DPrintf("id[%d]: decode error\n", kv.id)
	} else {
		kv.clientReply = clientReply
		kv.kvDataBase = kvDataBase
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if commandContext, ok := kv.clientReply[args.CkId]; ok {
		if args.CommandId <= commandContext.Command {
			msg := commandContext.Reply
			reply.Value = msg.Value
			reply.Err = msg.Err
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ClientId:    args.CkId,
		CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case msg := <-replyCh:
		// Debug(dTrace, "server[%d] GetArgs [%v] sucess", kv.id, args)
		if term == msg.Term {
			kv.lastApplied = index
			reply.Value = msg.Value
			reply.Err = msg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.CloseChan(index)
}

// Your code here.

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		Debug(dTrace, "server[%d] failed to close %d replyCh", kv.id, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if commandContext, ok := kv.clientReply[args.CkId]; ok {
		if args.CommandId <= commandContext.Command {
			msg := commandContext.Reply
			reply.Err = msg.Err
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{
		ClientId:  args.CkId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Value:     args.Value,
	}
	if args.OP == "Put" {
		op.CommandType = PutMethod
	} else {
		op.CommandType = AppendMethod
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dTrace, "server[%d] begin PutAppend [%v]", kv.id, args)
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case msg := <-replyCh:
		// Debug(dTrace, "server[%d] PutAppend [%v] sucess", kv.id, args)
		if term == msg.Term {
			kv.lastApplied = index
			reply.Err = msg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.CloseChan(index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	kv := new(KVServer)
	kv.me = me
	kv.id = nrand()
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.kvDataBase = make(map[string]string)
	kv.clientReply = make(map[int64]CommandContext)
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.listenCommit()
	// You may need initialization code here.

	return kv
}
