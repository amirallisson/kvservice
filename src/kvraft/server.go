package raftkv

import (
	"encoding/gob"
	"log"
	"src/labrpc"
	"src/raft"
	"sync"
	"time"
)

const Debug = 0
const RetryTimeout = 1000 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key           string
	Value         string
	Op            string
	ClientID      int64
	TransactionID int64
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	// helper fields
	data          map[string]string            // the kv storage of committed entries
	notifyCh      map[int](chan raft.ApplyMsg) // notifyCh[index] -> for blocked method to know if submitted entry committed
	lastTransacID map[int64]int64              // lastTransacID[msg.transactionId], store the last succesfull transaction
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// check if duplicate
	kv.mu.Lock()
	if lastID, ok := kv.lastTransacID[args.ClientID]; ok && lastID >= args.TransactionID {
		val, ok := kv.data[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = val
			reply.Err = OK
		}
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	submitOp := Op{args.Key, "", "", args.ClientID, args.TransactionID}
	index, _, isLead := kv.rf.Start(submitOp)

	if !isLead {
		reply.WrongLeader = true
		return
	}

	// create a channel notification
	kv.mu.Lock()
	_, ok := kv.notifyCh[index]
	if !ok {
		kv.notifyCh[index] = make(chan raft.ApplyMsg, 1)
	}
	kv.mu.Unlock()

	// wait until 1) goroutine takes the submitted entry from applyCh 2) timeout
	select {
	case <-time.After(RetryTimeout):
		reply.WrongLeader = true
	case msg := <-kv.notifyCh[index]:
		if reply.WrongLeader = (msg.Command.(Op) != submitOp); !reply.WrongLeader {
			var foundOk bool
			kv.mu.Lock()
			defer kv.mu.Unlock()
			reply.Value, foundOk = kv.data[args.Key]
			if !foundOk {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() { reply.Err = OK }()

	// check if duplicate
	kv.mu.Lock()
	if lastID, ok := kv.lastTransacID[args.ClientID]; ok && lastID >= args.TransactionID {
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	submitOp := Op{args.Key, args.Value, args.Op, args.ClientID, args.TransactionID}
	index, _, isLead := kv.rf.Start(submitOp)

	if !isLead {
		reply.WrongLeader = true
		return
	}

	// create channels for kill and notification
	kv.mu.Lock()
	_, ok := kv.notifyCh[index]
	if !ok {
		kv.notifyCh[index] = make(chan raft.ApplyMsg, 1)
	}
	kv.mu.Unlock()

	select {
	case <-time.After(RetryTimeout):
		reply.WrongLeader = true
	case msg := <-kv.notifyCh[index]:
		reply.WrongLeader = (msg.Command.(Op) != submitOp)
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)
	kv.notifyCh = make(map[int](chan raft.ApplyMsg))
	kv.lastTransacID = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		// get commits from ApplyCh and submit them to the FSM
		for {
			applied := <-kv.applyCh

			kv.mu.Lock()
			cmd := applied.Command.(Op)

			// maybe duplicate?
			if lastID, ok := kv.lastTransacID[cmd.ClientID]; ok && lastID >= cmd.TransactionID {
				kv.mu.Unlock()
				continue
			}

			// put or append -> modify the kv storage
			if cmd.Op == "Put" {
				kv.data[cmd.Key] = cmd.Value
			} else if cmd.Op == "Append" {
				val, ok := kv.data[cmd.Key]
				if !ok {
					val = cmd.Value
				} else {
					val += cmd.Value
				}
				kv.data[cmd.Key] = val
			}

			// update the last succesfull transaction for this client
			kv.lastTransacID[cmd.ClientID] = cmd.TransactionID

			// notify the RPC handler
			_, ok := kv.notifyCh[applied.Index]
			if !ok {
				kv.notifyCh[applied.Index] = make(chan raft.ApplyMsg, 1)
			}
			kv.notifyCh[applied.Index] <- applied
			kv.mu.Unlock()
		}
	}()

	return kv
}
