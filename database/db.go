package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

// DB 每一个 Redis 的分数据库
type DB struct {
	index int
	data  dict.Dict
}

// ExecFunc 所有的指令实现
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine 二维切片
type CmdLine = [][]byte

func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
	}
	return db
}

// Exec 每一个分数据库指令的实现
func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command" + cmdName)
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.exector
	// set k v 不需要第一个 set
	return fun(db, cmdLine[1:])
}

// validateArity 校验指令的参数是否符合要求
// set k v arity = 3
// Exists k1 k2 k3 k4 arity = -2 加个符号 数值为最小值 表示是否变长
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	// 大于 说明定长
	if arity >= 0 {
		return argNum == arity
	}
	// -3 <= -2
	return argNum >= -arity
}

// GetEntity 在 DB 层面的封装 虽然之前在 Dict 封装过
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	// raw 是原始类型 空接口
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity int 指 put 多少个
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	// Put 形参为空接口 entity 实参自动旋换为 空接口
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return
}

func (db *DB) Flush() {
	db.data.Clear()
}
