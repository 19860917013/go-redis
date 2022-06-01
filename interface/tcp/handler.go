package tcp

import (
	"context"
	"net"
)

/*
handler 处理器
redis的核心业务引擎
为了处理一个逻辑
让 tcp 这一层只处理 tcp 连接
*/

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
