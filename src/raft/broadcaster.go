package raft

import (
	//"fmt"
	"sync"
)

type StateBroadcaster struct {
	mu           sync.Mutex
	subscribers  []chan bool
	//stateChanged chan bool
}

func NewStateBroadcaster() *StateBroadcaster {
	sb := StateBroadcaster{}
	sb.subscribers = make([]chan bool, 0)
	return &sb
}
func (sb *StateBroadcaster) Subscribe() chan bool {	

	sb.mu.Lock()
	defer sb.mu.Unlock()
	ch := make(chan bool,1)
	sb.subscribers = append(sb.subscribers, ch)

	return ch
}

func (sb *StateBroadcaster) NotifyAll(dead int32) {
	if dead == 1 {
		return
	}
	sb.mu.Lock()
	defer sb.mu.Unlock()
	for _, ch := range sb.subscribers {
		//fmt.Printf("Element queud: %d",len(ch))
		if len(ch) > 0 {
			continue
		}
		ch <- true
	}
}

func (sb *StateBroadcaster) Close() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	for _, ch := range sb.subscribers {
		ch <- false
	}
	//sb.subscribers = make([]chan bool, 0)
}
