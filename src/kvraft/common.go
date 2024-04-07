package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	CkId      int64
	CommandId int
	OP        string // "Put" or "Append"
	Key       string
	Value     string

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	CkId      int64
	CommandId int
	Key       string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
