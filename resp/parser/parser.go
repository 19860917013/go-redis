package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
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
	bulkLen           int64    //字节组的长度  预设 读取的长度
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
	// 每一个用户一个解析器
	go parse0(reader, ch)
	return ch
}

// io.Reader 读取客户端的字节流 解析器
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		// 处理下 recover 中的 error
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	// 只要连接之后 会处于这个死循环中 断开之后才会退出
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr { // IO 错误 塞管道关闭退出
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 最大的判断是从 是否开启多行模式 作为分支
		if !state.readingMultiLine {
			// receive new response
			if msg[0] == '*' {
				// parseMultiBulkHeader 中会将状态置为多行模式
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					// 是给 Redis 底层返回一个信息 这里不是给 用户返回
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk reply
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // 重置位一个新的状态进行服务
					continue
				}
			} else {
				// single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// 多行模式使用 readBody
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				// 定义一个 Reply 如果解析完成塞进管道
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine 从 IO 中取出一行 以 \n 结尾
// bool 是否为 IO 错误
// error 错误本身
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	/*
	*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
	*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
	 */

	if state.bulkLen == 0 { // read normal line
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// parseMultiBulkHeader  多个字符串的头  *3 把数组解析出来 并且相应的改变状态
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	// msg[1:len(msg)-2] 这样处理 300 这种多位数字
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// *3 说明后面 set k v  3个结构
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseBulkHeader 单个字符串的头  $n \r\n  "  " \r\n  PING
// 初始化解析时调用的方法 如果开始是 * 后来是 $ 就不走这个方法
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseSingleLineReply 客户端发送 +OK -ERR 等
// 比较简单一行解析完返回 Reply 即可
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	// TrimSuffix 把后缀干掉
	str := strings.TrimSuffix(string(msg), "\r\n")
	// Reply 同时适用于服务端客户端
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	default:
		// parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}
	return result, nil
}

// readBody 开头被上面几种方法解决完 该方法解决后面的主体部分
// 可能出现情况   * 干掉后剩下 $ 	或者剩下 PING (不是 $ 的情况)
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { //  $0\r\n
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
