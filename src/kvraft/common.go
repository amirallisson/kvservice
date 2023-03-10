package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	ClientID      int64
	TransactionID int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key           string
	ClientID      int64
	TransactionID int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
