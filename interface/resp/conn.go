package resp

// 代表了协议层一个客户端的连接

type Connection interface {
	Write([]byte) error // 给客户端回应
	GetDBIndex() int    // 客户端连接的是哪个库
	SelectDB(int)       // 切换库
}
