package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}

	IsHeartBeat bool
	IsEmpty     bool
}

const (
	FOLLOWER  = "f"
	LEADER    = "l"
	CANDIDATE = "c"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	state       string
	applyCh     *chan ApplyMsg
	loseHBCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if strings.Compare(rf.state, LEADER) == 0 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currntTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currntTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		rf.logPrintf("read persist error!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currntTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.logPrintfWithLock("read persist success!")
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

func (rf *Raft) logPrintf(format string, vars ...interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Peer %d(%v,%v): "+format, rf.me, rf.currentTerm, rf.state, vars)
}

func (rf *Raft) logPrintfWithLock(format string, vars ...interface{}) {
	log.Printf("Peer %d(%v,%v): "+format, rf.me, rf.currentTerm, rf.state, vars)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true

	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	n := len(rf.log)
	if rf.log[n-1].Term > args.LastLogTerm ||
		(rf.log[n-1].Term == args.LastLogTerm && n > args.LastLogIndex) {
		rf.currentTerm = args.Term
		if rf.state == LEADER {
			rf.BecomeFollowerWithLock()
		}
		rf.persist()
		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = args.Term
	rf.BecomeFollowerWithLock()
	if rf.votedFor != -1 {
		reply.VoteGranted = false
	} else {
		rf.votedFor = args.CandidateId
		rf.logPrintfWithLock("voted for %v", args.CandidateId)
	}
	rf.persist()
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logPrintfWithLock("receive append entry from %v, try to append......", args.LeaderId)

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.CommitIndex = rf.commitIndex
		rf.logPrintfWithLock("your term need a update, dear %v (%v)", args.LeaderId, args.Term)
		return
	} else {
		rf.currentTerm = args.Term
		if rf.state != FOLLOWER {
			rf.BecomeFollowerWithLock()
			rf.votedFor = args.LeaderId
		}
		rf.persist()
	}

	if len(args.Entries) == 0 {
		rf.logPrintfWithLock("receive heart from %v ,append success.", args.LeaderId)
		if rf.commitIndex < args.LeaderCommit {
			if !(args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
				rf.commitIndex = Min(args.PrevLogIndex, args.LeaderCommit)
			}
		}
		rf.loseHBCount = 0
		reply.Success = true
	} else {
		if args.PrevLogIndex+len(args.Entries) <= rf.commitIndex {
			rf.logPrintfWithLock("already have")
			reply.Success = true
		} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.logPrintfWithLock("term don't match")
			reply.Success = false
			reply.Term = args.Term
			reply.CommitIndex = rf.commitIndex
			return
		} else {
			rf.logPrintfWithLock("%v (%v) request append,is Entry,match success", args.LeaderId, args.Term)
			rf.log = rf.log[0 : args.PrevLogIndex+1] //截断
			for _, l := range args.Entries {
				rf.log = append(rf.log, l)
			}

			reply.Success = true

			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = Min(args.LeaderCommit, Max(args.PrevLogIndex, rf.commitIndex)) // why ??
			}
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
	reply.CommitIndex = rf.commitIndex

	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	term = rf.currentTerm
	index = len(rf.log) - rf.GetEmptyLogCountWithLock() + 1
	l := LogEntry{index, term, command, false, false}
	rf.log = append(rf.log, l)
	rf.persist()

	rf.logPrintfWithLock("Leader Start log index:%d,term:%d", index, term)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Timer() {
	rf.logPrintf("timer begin to work..")
	for {
		time.Sleep(100 * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if strings.Compare(rf.state, FOLLOWER) == 0 {
			//rf.logPrintfWithLock("     %v", rf.loseHBCount)
			rf.loseHBCount += 1
			if rf.loseHBCount == 3 {
				rf.loseHBCount = 0 //计数器置零
				rf.state = CANDIDATE
				go rf.BecomeCandidate()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) BecomeCandidate() {
	rf.logPrintf(" become a candidate! ")
	for {
		t := rand.Intn(100)
		time.Sleep(time.Millisecond * time.Duration(t))
		//rf.logPrintf("sleep %v", t)
		if rf.killed() {
			return
		}

		rf.mu.Lock()

		if rf.state == FOLLOWER || rf.state == LEADER {
			rf.mu.Unlock()
			return
		}

		rf.currentTerm += 1
		//rf.loseHBCount = 0
		rf.votedFor = rf.me
		rf.persist()

		args := RequestVoteArgs{}

		//初始化voteRPC args
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		//args.LastLogIndex = rf.log[len(rf.log)-1].Index
		args.LastLogIndex = len(rf.log) // 之前一直跑不通的原因
		args.LastLogTerm = rf.log[len(rf.log)-1].Term

		rf.mu.Unlock()

		w := sync.WaitGroup{}
		var count int32 = 1

		for i, _ := range rf.peers {
			reply := RequestVoteReply{0, false} //复用同一个会报错
			if i == rf.me {
				continue
			}
			w.Add(1)
			go func(i int) {
				ok := rf.sendRequestVote(i, &args, &reply)
				w.Done()
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					atomic.AddInt32(&count, 1)
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.BecomeFollowerWithLock()
					}
				}
			}(i)
		}
		isEnd := make(chan bool)
		go func() {
			w.Wait()
			isEnd <- true
		}()

		select {
		case <-time.After(100 * time.Millisecond):
			rf.logPrintf("vote rpc time out! ")

		case <-isEnd:
			rf.logPrintf("vote rpc finished! ")
		}
		rf.logPrintf("count: %v", int(count))

		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		if int(count) > len(rf.peers)/2 {
			rf.BecomeLeaderWithLock()
			rf.mu.Unlock()
			return
		}

		rf.BecomeFollowerWithLock()
		rf.persist()
		rf.mu.Unlock()
		return

	}

}

func (rf *Raft) BecomeFollowerWithLock() {
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.loseHBCount = 0
	rf.logPrintfWithLock("become a follower")
}

func (rf *Raft) BecomeLeaderWithLock() {
	rf.state = LEADER
	rf.votedFor = -1
	rf.loseHBCount = 0
	rf.logPrintfWithLock("become a leader")

	for i := 0; i < len(rf.matchIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	l := LogEntry{0, rf.currentTerm, -1, false, true}
	rf.log = append(rf.log, l)

	rf.persist()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.sendAppendEntriesFunc(i)
		}(i)
	}
}

func (rf *Raft) HeartBeat() {
	rf.logPrintf("HeartBeat begin to work..")
	for {
		//t := 100
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			l := make([]LogEntry, 0)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				args := &AppendEntryArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, l, rf.commitIndex}
				reply := &AppendEntryReply{0, false, 0}
				curr := i
				go func() {
					rf.sendAppendEntries(curr, args, reply)
				}()
			}
		}
		rf.mu.Unlock()
		if rf.killed() {
			return
		}
	}
}

func (rf *Raft) sendAppendEntriesFunc(peer int) {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.killed() {
			return
		}
		for {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.logPrintfWithLock("already not a leader")
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[peer] >= len(rf.log) {
				rf.logPrintfWithLock("to %v synchronize process finished", peer)
				rf.mu.Unlock()
				break
			}

			//截取要同步的数据
			//data := make([]LogEntry, len(rf.log)-rf.nextIndex[peer])
			data := make([]LogEntry, len(rf.log[rf.nextIndex[peer]:len(rf.log)]))
			copy(data, rf.log[rf.nextIndex[peer]:len(rf.log)])

			args := &AppendEntryArgs{rf.currentTerm, rf.me, rf.nextIndex[peer] - 1, rf.log[rf.nextIndex[peer]-1].Term, data, rf.commitIndex}
			reply := &AppendEntryReply{0, false, 0}

			rf.mu.Unlock()

			w := sync.WaitGroup{}
			w.Add(1)
			ok := false

			go func() {
				ok = rf.sendAppendEntries(peer, args, reply)
				w.Done()
			}()

			ch := make(chan bool)
			go func() {
				w.Wait()
				ch <- false
			}()

			select {
			case <-time.After(100 * time.Millisecond):
				rf.logPrintf("SYN append entry time out!")

			case <-ch:
				rf.logPrintf("SYN append entry send finished!")
			}

			if ok {
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[peer] += len(data)
					rf.matchIndex[peer] = Max(rf.nextIndex[peer]-1, rf.matchIndex[peer])
					rf.logPrintfWithLock("SYN append entry send success, nextIndex: %v, matchIndex :%v", rf.nextIndex[peer], rf.matchIndex[peer])
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if reply.Term <= rf.currentTerm { //term比我小但是添加失败，是因为没有match，调整nextIndex
						rf.logPrintfWithLock("SYN append entry send %v fail, match failed, change to %v", rf.nextIndex[peer], reply.CommitIndex+1)
						rf.nextIndex[peer] = reply.CommitIndex + 1 //从commit的下一个log开始发
						rf.mu.Unlock()
					} else {
						//waiting for a new leader
						rf.mu.Unlock()
						rf.logPrintf("need a new leader")
						return
					}
				}
			} else {
				rf.logPrintf("syn ok is false")
				break
			}

		}
	}
}

func (rf *Raft) GetEmptyLogCountWithLock() int {
	count := 0
	for _, l := range rf.log {
		if l.IsEmpty {
			count++
		}
	}
	return count
}

func (rf *Raft) BackWork() {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		mi := make([]int, len(rf.matchIndex))
		copy(mi, rf.matchIndex)
		mi[rf.me] = len(rf.log) - 1
		sort.Ints(mi)
		if rf.state == LEADER {
			midIndex := len(mi) / 2
			if rf.log[mi[midIndex]].Term == rf.currentTerm { //不负责为之前任期的leader留下的过半复制log专门进行提交，只能提交自己任期内的log
				//提交自己任期log时能够自动把之前的都提交了
				//paper Figure 8
				rf.commitIndex = Max(mi[midIndex], rf.commitIndex)
			}

		}
		rf.persist()
		for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {
			log.Printf("%d apply", rf.lastApplied)
			if rf.log[rf.lastApplied].IsEmpty { //非用户指令，不用apply
				continue
			} else {
				//log.Printf("Peer %d(%d) :apply %d(%d)",rf.me,rf.term,rf.log[rf.lastApplied].LogIndex,rf.log[rf.lastApplied].Command.(int))
				m := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.log[rf.lastApplied].Index}
				*rf.applyCh <- m
			}
			//apply
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) LogThread() {
	for {
		time.Sleep(100 * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		s := ""
		s += strconv.Itoa(rf.commitIndex)
		s += "["
		for _, l := range rf.log {
			s += strconv.Itoa(l.Term)

			s += "("
			switch l.Command.(type) {
			case int:
				s += strconv.Itoa(l.Command.(int))
			case string:
				s += l.Command.(string)
			default:
				log.Printf("log type error")
				log.Fatal()
			}

			s += ")"

			s += ","
		}
		s += "]"
		log.Print(rf.me, ":", s)
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func log_init() {
	file := "./" + "message" + ".txt"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[qSkipTool]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
	return
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	l := LogEntry{0, 0, -1, false, true}
	rf.log = append(rf.log, l)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	rf.applyCh = &applyCh
	rf.state = FOLLOWER
	rf.loseHBCount = 0

	log_init()

	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logPrintfWithLock("begin")
	go rf.Timer()
	go rf.HeartBeat()
	go rf.BackWork()
	go rf.LogThread()

	return rf
}
