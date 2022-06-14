package connection

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection redis-cli 协议层对连接的描述
// 扩展一下的话连接 Connection 可以加密码等字段
type Connection struct {
	conn net.Conn

	// 关闭服务前 进行处理
	waitingReply wait.Wait

	// 操作一个客户上一把锁 并发问题
	mu sync.Mutex

	// selected db
	selectedDB int
}

// RemoteAddr 看一下客户端的地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close 等待之后关闭
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	// 在同一时刻 只能有一个协程对客户端写数据
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
