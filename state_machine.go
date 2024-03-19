package raftgo

import (
	"encoding/json"
	"log"
	"sync"
)

type Command struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type StateMachine interface {
	Apply(command []byte)
}

type MemStateMachine struct {
	state map[string]string
	mu    sync.RWMutex
}

func NewMemStateMachine() *MemStateMachine {
	return &MemStateMachine{
		state: make(map[string]string),
	}
}

func (m *MemStateMachine) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.state[key]
	if ok {
		return value
	}
	return ""
}

func (m *MemStateMachine) Apply(command []byte) {
	var cmd Command
	err := json.Unmarshal(command, &cmd)
	log.Println(string(command))
	if err != nil {
		log.Fatalf("Error unmarshalling command: %v", err)
	}
	log.Printf("%+v", cmd)
	switch cmd.Type {
	case "set":
		m.mu.Lock()
		m.state[cmd.Key] = cmd.Value
		m.mu.Unlock()
	case "rm":
		m.mu.Lock()
		delete(m.state, cmd.Key)
		m.mu.Unlock()
	}
}
