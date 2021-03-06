package kvraft

import (
)

type KVPair struct {
	Key   string
	Value string
}

type Datastore struct {
	KeyValue map[string]string
}


func (db *Datastore) Init() {
	db.KeyValue = make(map[string]string)
}

func (db *Datastore) Get(key string) (string, Err) {

	value, ok := db.KeyValue[key]
	if ok {
		return value, OK
	}
	return value, ErrNoKey
}

func (db *Datastore) Put(key string, value string) {
	if db.KeyValue == nil {
		db.Init()
	}
	db.KeyValue[key] = value

}

func (db *Datastore) Append(key string, value string) {
	//_, ok := db.KeyValue[key]
	//if !ok {
	//	db.Put(key,value)
	//	return
	//} 
	db.KeyValue[key] += value
}

func (db *Datastore) remove(key string) string {
	val, _ := db.KeyValue[key]
	delete(db.KeyValue, key)
	return val
}

/*
func (db *Datastore) GetSnapshot(lastIndex int, lastTerm int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(lastIndex)
	e.Encode(lastTerm)
	e.Encode(db.KeyValue)
	data := w.Bytes()

	return data
}

func (db *Datastore) PutSnapshot(data []byte) (int, int) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return -1, -1
	}
	//DPrintf("Server %d read from storage......", kv.me)
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIndex int
	var lastTerm int
	if d.Decode(&lastIndex) != nil || d.Decode(&lastTerm) != nil {
		DPrintf("Error reading state")
	}
	
		else {
			kv.lastRaftIndex = lastIndex
			kv.lastRaftTerm = lastTerm
		}
	
	var tempkv map[string]string
	d.Decode(&tempkv)
	db.KeyValue = tempkv

	return lastIndex, lastTerm

}
*/
