package shardkv

/*
Two level map for store shard key value data; First key is shard converted to string, second level is key;
It is used to store both shard key value and shard request record
*/

type Shardstore struct {
	ShardData map[string]map[string]string // shard to keyvalue
}

func (db *Shardstore) Init() {
	db.ShardData = make(map[string]map[string]string)
}

func (db *Shardstore) Get(shard string, key string) (string, Err) {
	//shardStr := strconv.Itoa(shard)
	kv, ok := db.ShardData[shard]
	if !ok {
		return "", ErrWrongGroup
	}
	value, ok2 := kv[key]
	if !ok2 {
		return "", ErrNoKey
	}
	return value, OK

}

func (db *Shardstore) Put(shard string, key string, value string) {
	//shardStr := strconv.Itoa(shard)
	_, ok := db.ShardData[shard]
	if !ok {
		db.createShard(shard)
	}

	db.ShardData[shard][key] = value
}

func (db *Shardstore) Append(shard string, key string, value string) {

	_, ok := db.ShardData[shard]
	if !ok {
		db.createShard(shard)
	}

	db.ShardData[shard][key] += value
}

func (db *Shardstore) createShard(shard string) {
	db.ShardData[shard] = make(map[string]string)
}

func (db *Shardstore) RemoveShard(shard string) map[string]string {
	kv, _ := db.ShardData[shard]
	delete(db.ShardData, shard)
	return kv
}

func (db *Shardstore) PutShard(shard string, kv map[string]string) {
	db.ShardData[shard] = kv
}

func (db *Shardstore) GetShard(shard string) (map[string]string, bool) {
	kv, ok := db.ShardData[shard]
	return kv, ok
}
