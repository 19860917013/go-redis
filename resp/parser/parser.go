package parser

import (
	"go-redis/interface/resp"
	"io"
)

// Redis 解析器 用户发送的 \r\n 等如何识别去执行

type Payload struct {
	Data resp.Reply // Reply 客户端发送 和 服务端回复用的是相同的数据结构
	Err  error
}

// readState 解析器的状态
type readState struct {
	readingMultiLine  bool     // 正在解析单行数据还是多行数据
	expectedArgsCount int      // 应该有几个参数
	msgType           byte     // 用户信息类型
	args              [][]byte // 用户传过来的数据
	bulkLen           int64    //字节组的长度
}

// finished 解析是否完成
func (s *readState) finished() bool {
	// len(s.args) 解析出来的长度
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream 对外接口 让解析并发进行 TCP 调用 ParseStream
func ParseStream(reader io.Reader) <-chan *Payload {
	// 通过管道交付 不用卡在这
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// io.Reader 读取客户端的字节流
func parse0(reader io.Reader, ch chan<- *Payload) {

}
