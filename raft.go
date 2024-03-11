package raftgo

import (
	"math/rand"
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
	currentTerm      int
	votedFor         *int
	status           RaftStatus
	heartbeatTimeout *ResettableTimeout
	shutdownChannel  chan bool
	heartChannel     chan bool
	logs             *Logs
	peers            map[int]Peer
	config           Config
	commitEmitter    map[int][]func()
}

func NewRaft(id int, logs *Logs, peers map[int]Peer, config Config) *Raft {
	shutdownChannel := make(chan bool)
	heartChannel := make(chan bool)
	r := &Raft{
		ID:               id,
		currentTerm:      0,
		status:           Follower,
		config:           config,
		shutdownChannel:  shutdownChannel,
		heartChannel:     heartChannel,
		commitEmitter:    make(map[int][]func()),
		logs:             logs,
		peers:            peers,
		heartbeatTimeout: NewResettableTimeout(config.HeartbeatTimeout, heartChannel),
	}

	go r.run()
	r.heartbeatTimeout.Reset()

	return r
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
	rt.Stop()
	rt.Start()
}
