package tcp

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// 一个客户端

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait // 等待客户端此次的工作做完才能关闭 工具包中的 Wait 有超时功能
}

// 实现系统 Closer 接口  系统会来关闭 实现接口就得实现 error 返回 不想返回错误就返回 nil

func (e *EchoClient) Close() error {
	// 等待 10s 然后关闭 Conn
	// 没超时的情况下就是一个正常的 Wait 操作 等待下面的 Add 和 Done
	e.Waiting.WaitWithTimeout(10 * time.Second)
	// 改成 _ 就不会报错 因为关闭了错了就错了
	_ = e.Conn.Close()
	return nil
}

/*
echo 回复 回响
EchoHandler 最简单的回发业务 测试
相当于简单的服务端
*/

type EchoHandler struct {
	activeConn sync.Map       // 多少个连接
	closing    atomic.Boolean // 如果正在关闭 就不再接收连接了 考虑到并发 用原子的 bool
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

// ctrl + i 可以快捷实现

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 关闭的过程中 来连接 直接关闭即可
	if handler.closing.Get() {
		_ = conn.Close()
	}

	// conn 传进来的是 客户端结构体中的 一个 conn
	client := &EchoClient{
		Conn: conn,
	}
	// 当作 hashset 用 普通的 map 直接 [] 赋值就行
	handler.activeConn.Store(client, struct{}{})

	// 接下来收发连接就行 不断的去服务这个 client
	reader := bufio.NewReader(conn) // 返回一个 IO 流  作为一个缓存区  因为网络可能是时断时续的
	for true {
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 客户端退出了
			if err == io.EOF {
				logger.Info("Connection close")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}

		client.Waiting.Add(1)
		// msg 转化为字节流
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	// 改为 true 则不在接收
	handler.closing.Set(true)
	// 对 Map 的删除 返回 true 就操作下一个 k
	handler.activeConn.Range(func(key, value interface{}) bool {
		// Map 的 k 是空接口 所以要转换
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		return true
	})
	return nil
}
