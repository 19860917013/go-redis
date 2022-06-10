package database

import "strings"

// cmdTable 记录系统所有指令 每一个指令对应一个 command 结构体
// 在这用 map 因为之后是只读的 只需要开启的时候初始化一下
var cmdTable = make(map[string]*command)

// 每一个指令 Get Put 都是一个 command
type command struct {
	exector ExecFunc
	arity   int // 参数的数量
}

// RegisterCommand 注册方法
func RegisterCommand(name string, exector ExecFunc, arity int) {
	// 转换为小写 统一
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		exector: exector,
		arity:   arity,
	}
}
