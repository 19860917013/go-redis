package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

/*
Config
启动 tcpserver 的一些配置
*/

type Config struct {
	Address string // 监听地址
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	// Notify 系统传的信号告知 sigChan
	// 可以查下 系统挂起 杀掉 一般就是这几个信号
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	// listener 相当于 listen 状态的 socket
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	// listener 监听新连接
	logger.Info("start listen")
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	// 如果程序退出 用户关闭窗口或者 kill 走不到 下面逻辑 需要 chan 感知退出
	go func() {
		// 如果 chan 为空 则一直读不出阻塞 有信号的话往下执行
		<-closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		// 可以将返回值 error 扔掉
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	// 保证 连接失败 break 之前所有的连接退出
	var waitDone sync.WaitGroup
	for true {
		// 接收新连接出问题 就从死循环跳出去
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted link")
		// 一个协程一个连接
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
			//	waitDone.Done() 如果 handler 中 panic 则无法执行
		}()
	}
	// break 退出 for 时等待
	waitDone.Wait()
}
