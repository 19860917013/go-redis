package database

import (
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

// StandaloneDatabase 成员由 DB 组成
type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler // 加个参数名 不加参数名就成组合了
}

// NewStandaloneDatabase 初始化
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb)
		if err != nil {
			// 因为这是在启动的过程中报 panic
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			// 引用第二遍时 发生逃逸到了 堆上
			// 闭包问题 引用外部的变量会变
			sdb := db // sdb 在引用第二次时 可能名字相同 但是地址已经不同了
			sdb.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return mdb
}

// Exec 把用户的指令转交给 分 DB
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	// 特殊指令 select 1,2
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// 一般的指令
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	// 这个地方可以用来断点测试
	return selectedDB.Exec(c, cmdLine)
}

func (mdb *StandaloneDatabase) Close() {

}

func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
}

// execSelect 用户切换 DB 时执行的指令
// select 1、2
func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
