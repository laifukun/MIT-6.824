package raft

import "sync"

type StateBroadcaster struct {
	mu           sync.Mutex
	subscribers  []chan bool
	stateChanged chan bool
}

func NewStateBroadcaster() *StateBroadcaster {
	sb := StateBroadcaster{}
	sb.subscribers = make([]chan bool, 0)
	return &sb
}
func (sb *StateBroadcaster) Subscribe() chan bool {
	ch := make(chan bool)

	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.subscribers = append(sb.subscribers, ch)

	return ch
}

func (sb *StateBroadcaster) NotifyAll() {

	sb.mu.Lock()
	defer sb.mu.Unlock()
	for _, ch := range sb.subscribers {
		ch <- true
	}
}

func (sb *StateBroadcaster) Close() {
	for _, ch := range sb.subscribers {
		close(ch)
	}
}
