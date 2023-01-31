package raftkv

import (
	"crypto/rand"
	"math/big"
	"src/labrpc"
	"sync"
	"time"
)

type Clerk struct {
	mu            sync.Mutex
	servers       []*labrpc.ClientEnd
	clientID      int64
	nextTransacID int64
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
	ck.clientID = nrand()
	ck.nextTransacID = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	// create a message and increment the transactionID for the next msg
	ck.mu.Lock()
	msg := &GetArgs{key, ck.clientID, ck.nextTransacID}
	ck.nextTransacID++
	defer ck.mu.Unlock()

	// retry until commit
	for ; ; time.Sleep(10 * time.Millisecond) {
		for i := 0; i < len(ck.servers); i++ {
			rep := &GetReply{}
			if ck.servers[i].Call("RaftKV.Get", msg, rep) && !rep.WrongLeader {
				if rep.Err == OK {
					return rep.Value
				}
				return ""
			}
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// create a message and increment the transactionID for the next msg
	ck.mu.Lock()
	msg := &PutAppendArgs{key, value, op, ck.clientID, ck.nextTransacID}
	ck.nextTransacID++
	defer ck.mu.Unlock()

	// retry until commit
	for ; ; time.Sleep(10 * time.Millisecond) {
		for i := 0; i < len(ck.servers); i++ {
			rep := &PutAppendReply{}
			if ck.servers[i].Call("RaftKV.PutAppend", msg, rep) && !rep.WrongLeader {
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
