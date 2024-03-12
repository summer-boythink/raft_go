package raftgo

import (
	"encoding/base64"
	"sync"
)

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  string
}

type Logs struct {
	store        Storage
	stateMachine StateMachine
	commitIndex  int
	lastApplied  int
	mu           sync.RWMutex
}

func NewLogs(store Storage, stateMachine StateMachine) *Logs {
	return &Logs{
		store:        store,
		stateMachine: stateMachine,
	}
}

func (l *Logs) GetCommitIndex() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.commitIndex
}

func (l *Logs) SetCommitIndex(index int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commitIndex = index
	for l.commitIndex > l.lastApplied {
		l.lastApplied++
		command, _ := base64.StdEncoding.DecodeString(l.Entry(l.lastApplied).Command)
		l.stateMachine.Apply(command)
	}
}

func (l *Logs) Entry(logIndex int) *LogEntry {
	return l.store.Entry(logIndex)
}

func (l *Logs) LastIndex() int {
	return l.store.LastIndex()
}

func (l *Logs) Last() *LogEntry {
	return l.store.Entry(l.store.LastIndex())
}

func (l *Logs) Append(command string, term int) int {
	logIndex := l.LastIndex() + 1
	l.store.TruncateAndAppend([]LogEntry{{LogIndex: logIndex, LogTerm: term, Command: command}})
	return logIndex
}

func (l *Logs) AppendEntries(prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) bool {
	entry := l.Entry(prevLogIndex)
	if entry == nil || entry.LogTerm != prevLogTerm {
		return false
	}

	if len(entries) > 0 {
		entries = entries[l.findConflict(entries)-entries[0].LogIndex:]
		l.store.TruncateAndAppend(entries)
	}

	if leaderCommit > l.GetCommitIndex() {
		l.SetCommitIndex(min(leaderCommit, prevLogIndex+len(entries)))
	}
	return true
}

func (l *Logs) BatchEntries(startLogIndex int, maxLen int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	if maxLen == -1 {
		maxLen = 100
	}
	entries = l.store.BatchEntries(startLogIndex-1, maxLen+1)
	prevLog := entries[0]
	entries = entries[1:]
	return prevLog.LogIndex, prevLog.LogTerm, entries
}

func (l *Logs) IsUpToDate(logIndex int, logTerm int) bool {
	last := l.Last()
	return logTerm > last.LogTerm || (last.LogTerm == logTerm && logIndex >= last.LogIndex)
}

func (l *Logs) findConflict(entries []LogEntry) int {
	for _, e := range entries {
		entry := l.Entry(e.LogIndex)
		if entry == nil || entry.LogTerm != e.LogTerm {
			return e.LogIndex
		}
	}
	return entries[0].LogIndex
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
