package kvsrv

// Put or Append

type MSGTYPE int

const (
	MODIFY MSGTYPE = iota + 1
	NOTIFY
)

type PutAppendArgs struct {
	Key   string
	Value string

	Sequence int64
	MsgType  MSGTYPE
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
