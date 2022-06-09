package reply

// 做通用回复

import (
	"bytes"
	"go-redis/interface/resp"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1")
	CRLF               = "\r\n"
)

// BulkReply 不符合 协议的 转换为符合协议的
type BulkReply struct {
	Arg []byte
}

// ToBytes 许多这种地方要用引用类型 *BulkReply
func (b *BulkReply) ToBytes() []byte {
	if len(b.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// MultiBulkReply 数组返回 MultiBulk 多个字符串 => 二维的
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer // buffer 方便做拼装 效率更好一些
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

// StatusReply 回复状态 +OK\r\n
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IntReply 通用数字回复
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	//  FormatInt 第二个参数为 几进制
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

// ErrorReply 将系统的 Error 接口和 ToBytes 缝合在一起
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply 标准异常回复
type StandardErrReply struct {
	Status string
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply 判断回复是否为正常或异常
func IsErrorReply(reply resp.Reply) bool {
	// 输入 Reply 接口 判断转成字节后第一个成员是不是 “-”
	return reply.ToBytes()[0] == '-'
}
