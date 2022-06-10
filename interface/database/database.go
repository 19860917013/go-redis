package database

import (
	"go-redis/interface/resp"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// Database is the interface for redis style storage engine
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	AfterClientClose(c resp.Connection)
	Close()
}

// DataEntity 指代 Redis 的各种数据类型 string set list
// 先实现基础的 string 其他功能预留 以后便于实现其他功能
type DataEntity struct {
	Data interface{}
}
