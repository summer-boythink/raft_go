package raftgo

import (
	"sync"
)

type Storage interface {
	LastIndex() int
	Entry(index int) *LogEntry
	BatchEntries(startIndex int, maxLen int) []LogEntry
	TruncateAndAppend(entries []LogEntry)
}

type MemStorage struct {
	entries []LogEntry
	mu      sync.RWMutex
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		entries: []LogEntry{{LogIndex: 0, LogTerm: 0, Command: ""}},
	}
}

func (m *MemStorage) BatchEntries(startIndex int, maxLen int) []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	endIndex := startIndex + maxLen
	if endIndex > len(m.entries) {
		endIndex = len(m.entries)
	}
	return m.entries[startIndex:endIndex]
}

func (m *MemStorage) Entry(index int) *LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if index < len(m.entries) {
		return &m.entries[index]
	}
	return nil
}

func (m *MemStorage) LastIndex() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries[len(m.entries)-1].LogIndex
}

func (m *MemStorage) TruncateAndAppend(entries []LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(entries) == 0 {
		return
	}
	for _, e := range entries {
		m.entries[e.LogIndex] = e
	}
	lastId := entries[len(entries)-1].LogIndex
	m.entries = m.entries[:lastId+1]
}
