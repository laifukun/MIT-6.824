package raft

/*
import (
	"fmt"
)
*/
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type Log struct {
	entries    []Entry
	lastSnapshotIndex int
	lastSnapshotTerm  int
}

func (log *Log) isEmpty() bool {
	return len(log.entries) == 0
}
func (log *Log) logIndexToPos(index int) int {
	return index - log.lastSnapshotIndex - 1
}
func (log *Log) getStartIndex() int {
	//if len(log.entries) > 0 {
	//	return log.entries[0].Index
	//}
	return log.lastSnapshotIndex+1
}
func (log *Log) getLastIndex() int {
	//if len(log.entries) > 0 {
	//	return log.entries[len(log.entries)-1].Index
	//}
	return log.lastSnapshotIndex + len(log.entries)
}
func (log *Log) nextIndex() int {
	return log.getLastIndex() + 1
}

func (log *Log) getLastLogTerm() int {
	if len(log.entries) > 0 {
		return log.entries[len(log.entries)-1].Term
	}
	return log.lastSnapshotTerm
}
func (log *Log) getLogTerm(id int) int {

	if id <= log.lastSnapshotIndex {
		return log.lastSnapshotTerm
	}
	if id > log.getLastIndex() {
		return log.getLastLogTerm()
	}
	//fmt.Printf("ID : %d", id)
	return log.entries[log.logIndexToPos(id)].Term
}

func (log *Log) replicateEntries(entry []Entry) {
	if entry == nil {
		return
	}
	log.entries = append(log.entries, entry...)
}

func (log *Log) appendEntry(entry Entry) {
	log.entries = append(log.entries, entry)
}

func (log *Log) getEntry(index int) Entry {
	if index <= log.lastSnapshotIndex || index > log.getLastIndex() {
		return Entry{}
	}
	return log.entries[log.logIndexToPos(index)]
	
}
func (log *Log) getEntries(startId int, endId int) []Entry {

	if startId > endId || log.isEmpty() {
		return nil
	}

	if startId >= log.getStartIndex(){
		sId := log.logIndexToPos(startId)

		if endId > log.getLastIndex() {
			endId = log.getLastIndex()
		}
		eId := log.logIndexToPos(endId)
		entries := make([]Entry, len(log.entries[sId : eId+1]))
		copy(entries,log.entries[sId : eId+1])
		//entries := log.entries[sId : eId+1]
		return entries
	}
	return nil
}

func (log *Log) removeEntriesAfterIndex(id int) {
	
	// remove entries after index id - keep entries of index id
	if id > log.getLastIndex() {
		return
	}
	if id >= log.getStartIndex() {
		entries := make([]Entry, log.logIndexToPos(id)+1)
		copy(entries,log.entries[0 : log.logIndexToPos(id)+1] )
		log.entries = entries
	}
	if id < log.getStartIndex() {
		log.entries = nil
	}
}

func (log *Log) takeEntriesUntilIndex(id int) []Entry {

	if id <= log.getLastIndex() {
		entries := log.entries[0 : log.logIndexToPos(id)+1]
		log.entries = log.entries[log.logIndexToPos(id)+1 : len(log.entries)]
		return entries
	}
	return nil
}

func (log *Log) discardEntriesBefore(index int, term int) {
	//discard entries before index id - including index id
	if index >= log.getLastIndex() || log.getLogTerm(index) != term {
		log.entries = nil
		return
	}
	if  index < log.getStartIndex() {
		return
	}
	idx := log.logIndexToPos(index+1)
	leftEntries := make([]Entry, len(log.entries) - idx)
	copy(leftEntries,log.entries[idx:])
	log.entries = leftEntries
	//log.startIndex = index
}

func (log *Log) snapshot(lastSnapshotIndex int, lastSnapshotTerm int) {
	
	log.discardEntriesBefore(lastSnapshotIndex, lastSnapshotTerm)
	log.setSnapshotParameter(lastSnapshotIndex, lastSnapshotTerm)
}

func (log *Log) setSnapshotParameter(lastSnapshotIndex int, lastSnapshotTerm int) {

	log.lastSnapshotIndex = lastSnapshotIndex
	log.lastSnapshotTerm = lastSnapshotTerm
}