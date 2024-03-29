package raftgo

import (
	"log"
	"math/rand"
	"sort"
	"time"
)

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)

type Config struct {
	RPCTimeout        time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatInterval time.Duration
}

type Raft struct {
	LeaderID         *int
	ID               int
	CurrentTerm      int
	VotedFor         *int
	Status           RaftStatus
	heartbeatTimeout *ResettableTimeout
	shutdownChannel  chan bool
	heartChannel     chan bool
	Logs             *Logs
	Peers            map[int]Peer
	Config           Config
	CommitEmitter    map[int][]chan bool
}

func NewRaft(id int, logs *Logs, peers map[int]Peer, config Config) *Raft {
	shutdownChannel := make(chan bool)
	heartChannel := make(chan bool)
	r := &Raft{
		ID:               id,
		CurrentTerm:      0,
		Status:           Follower,
		Config:           config,
		shutdownChannel:  shutdownChannel,
		heartChannel:     heartChannel,
		CommitEmitter:    make(map[int][]chan bool),
		Logs:             logs,
		Peers:            peers,
		heartbeatTimeout: NewResettableTimeout(config.HeartbeatTimeout, heartChannel),
	}
	go r.run()
	r.heartbeatTimeout.Start()

	return r
}

func (r *Raft) SetCurrentTerm(term int) {
	r.VotedFor = nil
	r.CurrentTerm = term
}

func (r *Raft) HandleAppendEntries(aea AppendEntriesArgs) AppendEntriesReply {
	if aea.Term < r.CurrentTerm {
		return AppendEntriesReply{Term: r.CurrentTerm, Success: false}
	}

	if aea.Term > r.CurrentTerm {
		r.SetCurrentTerm(aea.Term)
		r.Status = Follower
	}

	if !r.Logs.AppendEntries(aea.PrevLogIndex, aea.PrevLogTerm, aea.Entries, aea.LeaderCommit) {
		return AppendEntriesReply{Term: r.CurrentTerm, Success: false}
	}
	if r.Status != Leader {
		r.LeaderID = &aea.LeaderID
	}
	if r.Status == Follower {
		r.heartbeatTimeout.Reset()
	}
	return AppendEntriesReply{Term: r.CurrentTerm, Success: true}
}

func (r *Raft) HandleRequestVote(rv RequestVoteArgs) RequestVoteReply {
	if rv.Term < r.CurrentTerm {
		return RequestVoteReply{Term: r.CurrentTerm, VoteGranted: false, Online: true}
	}

	if rv.Term > r.CurrentTerm {
		r.SetCurrentTerm(rv.Term)
		r.Status = Follower
	}

	if r.VotedFor == nil || *r.VotedFor == rv.CandidateID {
		if r.Logs.IsUpToDate(rv.LastLogIndex, rv.LastLogTerm) {
			r.Status = Follower
			r.VotedFor = &rv.CandidateID
			return RequestVoteReply{Term: r.CurrentTerm, VoteGranted: true, Online: true}
		}
	}
	return RequestVoteReply{Term: r.CurrentTerm, VoteGranted: false, Online: true}
}

func (r *Raft) HandleAppend(command string) chan bool {
	logIndex := r.Logs.Append(command, r.CurrentTerm)
	handleChannel := make(chan bool)
	if _, ok := r.CommitEmitter[logIndex]; !ok {
		r.CommitEmitter[logIndex] = make([]chan bool, 0)
	}
	r.CommitEmitter[logIndex] = append(r.CommitEmitter[logIndex], handleChannel)
	return handleChannel
}

func (r *Raft) IsLeader() bool {
	return r.ID == *r.LeaderID
}
func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownChannel:
			return
		default:
			r.LeaderID = nil
			switch r.Status {
			case Follower:
				r.runFollower()
			case Candidate:
				r.runCandidate()
			case Leader:
				r.runLeader()
			}
		}
	}
}

func (r *Raft) runFollower() {
	r.heartbeatTimeout.Reset()
	log.Printf("entering follower state. id: %d term: %d\n", r.ID, r.CurrentTerm)
	for r.Status == Follower {
		select {
		case <-r.heartChannel:
			r.Status = Candidate
			return
		case <-r.shutdownChannel:
			return
		}
	}
}

func (r *Raft) runCandidate() {
	log.Printf("entering candidate state. id: %d term: %d\n", r.ID, r.CurrentTerm)
	for r.Status == Candidate {
		select {
		case votes := <-r.electSelf():
			grantedVotes := 1
			votesNeeded := r.quorumSize()
			for _, vote := range votes {
				if vote.Term > r.CurrentTerm {
					log.Printf("[runCandidate] newer term discovered, fallback to follower\n")
					r.Status = Follower
					r.SetCurrentTerm(vote.Term)
					return
				}
				if vote.VoteGranted {
					grantedVotes++
				}
				if grantedVotes >= votesNeeded {
					log.Printf("election won. tally: %d\n", grantedVotes)
					r.Status = Leader
					return
				}
			}
			time.Sleep(time.Duration(r.Config.HeartbeatInterval))
		case <-r.shutdownChannel:
			return
		}
	}
}

func (r *Raft) runLeader() {
	log.Printf("entering leader state. leader: %d term: %d\n", r.ID, r.CurrentTerm)
	r.CommitEmitter = make(map[int][]chan bool)
	r.LeaderID = &r.ID

	nextIndex := make(map[int]int)
	matchIndex := make(map[int]int)

	for _, id := range r.peerIds() {
		nextIndex[id] = r.Logs.LastIndex() + 1
		matchIndex[id] = 0
	}

	for r.Status == Leader {
		select {
		case <-time.After(time.Duration(r.Config.HeartbeatInterval)):
			replies := r.leaderSendHeartbeat(nextIndex)
			for _, reply := range replies {
				if reply.AReply.Term > r.CurrentTerm {
					log.Printf("[runLeader] newer term discovered, fallback to follower\n")
					r.Status = Follower
					r.SetCurrentTerm(reply.AReply.Term)
					return
				}
				if reply.AReply.Success {
					nextIndex[reply.PeerID] += reply.AppendLen
					matchIndex[reply.PeerID] = nextIndex[reply.PeerID] - 1
				} else {
					nextIndex[reply.PeerID]--
				}
			}
			mi := r.majorityIndex(matchIndex)
			oldCommitIndex := r.Logs.GetCommitIndex()
			if mi > r.Logs.GetCommitIndex() {
				r.Logs.SetCommitIndex(mi)
			}

			for i := oldCommitIndex + 1; i <= r.Logs.GetCommitIndex(); i++ {
				emitters, ok := r.CommitEmitter[i]
				if ok {
					for len(emitters) > 0 {
						emitters[0] <- true
						emitters = emitters[1:]
					}
				}
			}
		case <-r.shutdownChannel:
			return
		}
	}
}

// leaderSendHeartbeat Reply
type Reply struct {
	AReply    AppendEntriesReply
	PeerID    int
	AppendLen int
}

func (r *Raft) leaderSendHeartbeat(nextIndex map[int]int) []Reply {
	appendEntriesArgs := make(map[int]AppendEntriesArgs)
	for _, peerID := range r.peerIds() {
		logIndex, logTerm, entries := r.Logs.BatchEntries(nextIndex[peerID], -1)
		appendEntriesArgs[peerID] = AppendEntriesArgs{
			Term:         r.CurrentTerm,
			LeaderID:     r.ID,
			LeaderCommit: r.Logs.GetCommitIndex(),
			PrevLogIndex: logIndex,
			PrevLogTerm:  logTerm,
			Entries:      entries,
		}
	}
	var replies []Reply
	for peerID, args := range appendEntriesArgs {
		reply, err := r.Peers[peerID].AppendEntries(args, time.Duration(r.Config.RPCTimeout))

		if err == nil {
			replies = append(replies, Reply{
				AReply:    reply,
				PeerID:    peerID,
				AppendLen: len(args.Entries),
			})
		}
	}

	return replies
}

func (r *Raft) electSelf() <-chan []RequestVoteReply {
	r.SetCurrentTerm(r.CurrentTerm + 1)
	r.VotedFor = &r.ID
	last := r.Logs.Last()
	var votes []RequestVoteReply
	var res = make(chan []RequestVoteReply, 1)
	for _, peer := range r.Peers {
		vote, _ := peer.RequestVote(RequestVoteArgs{
			Term:         r.CurrentTerm,
			CandidateID:  r.ID,
			LastLogIndex: last.LogIndex,
			LastLogTerm:  last.LogTerm,
		}, time.Duration(r.Config.RPCTimeout))

		if vote.Online {
			votes = append(votes, vote)
		}
	}
	res <- votes
	return res
}

func (r *Raft) majorityIndex(matchIndex map[int]int) int {
	var arr []int
	for _, v := range matchIndex {
		arr = append(arr, v)
	}
	sort.Ints(arr)
	return arr[len(arr)/2]
}

func (r *Raft) peerIds() []int {
	var ids []int
	for id := range r.Peers {
		ids = append(ids, id)
	}
	return ids
}

func (r *Raft) quorumSize() int {
	return len(r.Peers)/2 + 1
}

type ResettableTimeout struct {
	Delay    time.Duration
	Timer    *time.Timer
	Callback chan<- bool
}

func NewResettableTimeout(delay time.Duration, callback chan bool) *ResettableTimeout {
	return &ResettableTimeout{
		Delay:    delay,
		Callback: callback,
	}
}

func (rt *ResettableTimeout) Stop() {
	if rt.Timer != nil {
		rt.Timer.Stop()
	}
}

func (rt *ResettableTimeout) Start() {
	rt.Timer = time.AfterFunc(rt.Delay+time.Duration(rand.Intn(int(rt.Delay))), func() {
		rt.Callback <- true
	})
}

func (rt *ResettableTimeout) Reset() {
	rt.Timer.Reset(rt.Delay + time.Duration(rand.Intn(int(rt.Delay))))
}
