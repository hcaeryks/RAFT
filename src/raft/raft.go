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

import "time"
import "sync"
import "sd/labrpc"
import "math/rand"
import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	command interface{}
	TERM int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	state int // 0: líder , 1: candidato , 2: seguidor

	term int
	votedFor int
	logs []Log

	conheceLider bool
	vivo bool

	commitIndex int
	lastApplied int

	cond      *sync.Cond
	apply_num int
}

func (rf *Raft) iniciaEleicao() {
	rf.state = 1
	rf.term += 1

	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			rf.votedFor = i
			continue
		}

		server := i

		go func() {
			var reply RequestVoteReply
			var args RequestVoteArgs

			rf.mu.Lock()

			args.CandidateId = rf.me
			args.Term = rf.term
			args.LastLogIndex = len(rf.logs) - 1
			args.Server = rf.me

			if args.LastLogIndex >= 0 {
				args.LastLogTerm = rf.logs[args.LastLogIndex].TERM
			} else {
				args.LastLogTerm = -1
			}

			rf.mu.Unlock()

			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()

			if ok {
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.state = 2
					rf.votedFor = -1

				} else if reply.VoteGranted == true {
					votes += 1
					if votes*2 > len(rf.peers) {
						ok := rf.CheckStateAndTransfer(0)
						if ok {
							go rf.liderHeartBeat()
							return
						}
					}
				}
			}

			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) liderHeartBeat() {
	primeira := true

	for {
		// Verifica se é a primeira iteração para otimizar os lockss
		if primeira == false {
			rf.mu.Lock()
		}

		if rf.vivo == false || rf.state != 0 {
			rf.mu.Unlock()
			return
		}

		getReply := 1

		for i := 0; i < len(rf.peers); i++ {
			server := i

			if i != rf.me {
				var reply AppendReply
				var args AppendArgs

				args.Server = rf.me
				args.Term = rf.term
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = -100

				if rf.commitIndex >= 0 {
					args.PrevLogTerm = rf.logs[rf.commitIndex].TERM
				}

				go func() {
					ok := rf.sendAppendEntry(server, &args, &reply)

					if ok {
						getReply += 1

						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.state = 2
							rf.votedFor = -1
						}
					}
				}()
			}
		}

		rf.mu.Unlock()
		primeira = false
		time.Sleep(222 * time.Millisecond)
	}
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.term
	isleader = rf.state == 0
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
	
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int
	var votedFor int
	logs := new([]Log)

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(logs) != nil {

	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.logs = *logs
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
	Server       int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.state = 2
	}

	if args.Term < rf.term {
		reply.VoteGranted = false
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}

	if rf.state == 0 || rf.state == 1 {
		reply.VoteGranted = false
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}

	atualizado := rf.atualizado(args.LastLogIndex, args.LastLogTerm)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && atualizado == false{
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.term

	rf.mu.Unlock()
	return
}

func (rf *Raft) atualizado(index, term int) bool {
	maxIndex := len(rf.logs) - 1

	var current_term int
	
	if maxIndex >= 0 {
		current_term = rf.logs[maxIndex].TERM
	} else {
		current_term = -1
	}

	if current_term > term {
		return true
	}

	if current_term == term {
		return index < maxIndex
	}

	return false
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

func (rf *Raft) sendAppendEntry(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) CheckStateAndTransfer(state int) bool {

	if state == 2 || (state == 1 && rf.state == 2) || (state == 0 && rf.state == 1) {
		rf.state = state
		return true
	}

	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an iniciaEleicao.
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
	defer rf.Unlock()

	if rf.state != 0 {
		isLeader = false
		return index, term, isLeader
	}

	term = rf.term
	rf.logs = append(rf.logs, Log{command, rf.term})
	rf.persist()
	index = len(rf.logs)
	peers_num := len(rf.peers)
	success_count := 1

	for i := 0; i < peers_num; i++ {
		server := i
		if i != rf.me {
			var args AppendArgs
			var reply AppendReply
			args.Server = rf.me
			args.Term = rf.term
			args.LeaderId = rf.me
			startIndex := max(0, index-5)
			go func() {
				retry := 2
				for {
					rf.mu.Lock()
					if rf.vivo == false || rf.term != args.Term || rf.state != 0 {
						rf.mu.Unlock()
						break
					}
					var entries []Log
					for i := startIndex; i < index; i += 1 {
						entries = append(entries, rf.logs[i])
					}
					args.Entries = entries
					args.PrevLogIndex = startIndex - 1
					args.LeaderCommit = rf.commitIndex
					if args.PrevLogIndex >= 0 {
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].TERM
					}

					rf.mu.Unlock()
					ok := rf.sendAppendEntry(server, &args, &reply)

					rf.mu.Lock()
					if ok {
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.state = 2
							rf.votedFor = -1

							rf.mu.Unlock()

							break
						} else {
							if reply.Success {
								success_count += 1

								if success_count > peers_num/2 {
									if rf.commitIndex < index-1 {
										rf.commitIndex = max(rf.commitIndex, index-1)
										rf.cond.Signal()
									}
								}
								rf.mu.Unlock()
								break
							} else {

								startIndex = max(0, index-5*retry)
								if startIndex != 0 {
									retry *= 2
								}
							}
						}
					} else {
						retry += 1
					}
					rf.mu.Unlock()
				}
			}()
		}
	}

	return index - 1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

type AppendArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
	Server       int
}

type AppendReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.Unlock()

	if rf.term > args.Term {
		reply.Success = false
		reply.Term = rf.term
		return
	}
	if rf.term < args.Term {
		rf.votedFor = -1
	}

	rf.conheceLider = true
	rf.term = args.Term
	rf.state = 2

	if args.PrevLogIndex == -100 {
		reply.Success = true
		reply.Term = rf.term
		if args.LeaderCommit == -1 || len(rf.logs) <= args.LeaderCommit || rf.logs[args.LeaderCommit].TERM != args.PrevLogTerm {
			return
		}

		if rf.commitIndex < min(args.LeaderCommit, len(rf.logs)-1) {
			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
			rf.cond.Signal()
		}
		return
	}
	if args.PrevLogIndex < len(rf.logs) &&
		(args.PrevLogIndex == -1 || rf.logs[args.PrevLogIndex].TERM == args.PrevLogTerm) {

		targetIndex := args.PrevLogIndex + len(args.Entries)

		if !(targetIndex < len(rf.logs) && rf.logs[targetIndex].TERM == args.Term) {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			for j := 0; j < len(args.Entries); j++ {
				rf.logs = append(rf.logs, args.Entries[j])
			}
			rf.persist()
			if rf.commitIndex < min(args.LeaderCommit, len(rf.logs)-1) {
				rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
				rf.cond.Signal()
			}
		}
		reply.Success = true
		reply.Term = rf.term
		return
	}
	reply.Success = false
	reply.Term = rf.term
}

var r *rand.Rand

func (rf *Raft) applyEntry(applyCh chan ApplyMsg) {
	for {
		rf.cond.L.Lock()
		rf.cond.Wait()
		rf.cond.L.Unlock()
		rf.apply_num += 1
		for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {

			rf.mu.Lock()
			var command interface{}
			if rf.lastApplied < len(rf.logs) {
				command = rf.logs[rf.lastApplied].command
			} else {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			applyCh <- ApplyMsg{rf.lastApplied, command, false, nil}
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 2
	rf.votedFor = -1
	rf.term = 0
	rf.conheceLider = false
	rf.vivo = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.cond = sync.NewCond(&sync.Mutex{})
	rf.commitIndex = -1
	rf.lastApplied = 0
	rf.apply_num = 0
	go rf.applyEntry(applyCh)

	if r == nil {
		r = rand.New(rand.NewSource(88))
	}

	go func() {
		for {
			dura := 600 + r.Int()%600
			time.Sleep(time.Duration(dura) * time.Millisecond)

			rf.mu.Lock()
			if rf.vivo == false {
				rf.mu.Unlock()
				return
			}

			if rf.conheceLider == false && rf.state != 0 {
				rf.iniciaEleicao()
				rf.mu.Unlock()
				continue
			}

			rf.conheceLider = false

			rf.mu.Unlock()
		}
	}()

	return rf
}

// Funções auxiliares:

func (rf *Raft) GetIndexTerm(index int) int {
	return rf.logs[index].TERM
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
