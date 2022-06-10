package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

// init 相当于特殊关键字
// 包在启动的时候就会调用
func init() {
	RegisterCommand("ping", Ping, 1)
}
