package resp

// 代表了客户端的回复

type Reply interface {
	ToBytes() []byte // 因为 TCP 协议来回是写字节流的
}
