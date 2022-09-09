package dict

import "sync"

// SyncDict 并发安全的字典 最底层的结构 其上的结构是 16 个 DB
type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// 下面是对底层 Map 的封装
// 这个地方的 value 可能是 不同的类型，所以是空接口

func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	// Load 是 Map 中的 Get
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(key, value interface{}) bool {
		length++
		// 会一直持续到 最后
		return true
	})
	return length
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent 没有的时候插入
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		return 0
	}
	dict.m.Store(key, val)
	return 1
}

func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1
	}
	return 0
}

func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		// 因为我们实现的是 string 	Map 底层的是空接口
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, dict.Len())
	for i := 0; i < limit; i++ {
		// 随机的 从 Range 取出一个
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

// RandomDistinctKeys 不能重复
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

func (dict *SyncDict) Clear() {
	// 可以直接换一个新的 旧的让系统去做垃圾回收就行了
	*dict = *MakeSyncDict()
}
