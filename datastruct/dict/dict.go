package dict

// 这里是用 Map 因为用的是接口 之后想用其他方式实现更换实现即可

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool) // Get 返回 值 和 是否存在
	Len() int
	Put(key string, val interface{}) (result int)         // 回复存进去几个
	PutIfAbsent(key string, val interface{}) (result int) // 是否存在
	PutIfExists(key string, val interface{}) (result int) // 跟上面相反
	Remove(key string) (result int)                       // 在字典中删除
	ForEach(consumer Consumer)                            // 传入一个方法
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string // 不重复的建
	Clear()                                // refactor 重构集体改动
}
