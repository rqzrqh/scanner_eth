package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"scanner_eth/mocknode"
	"sync"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
)

var cmdMockNode = &cli.Command{
	Name:  "mocknode",
	Usage: "Start mock Ethereum JSON-RPC nodes backed by scanner database",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "conf",
			Usage: "mock node config file",
			Value: "mocknode.conf",
		},
	},
	Action: runMockNode,
}

var cmdMockNodeChild = &cli.Command{
	Name:   "mocknode-child",
	Usage:  "Start one mock Ethereum JSON-RPC node",
	Hidden: true,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "conf",
			Usage: "mock node config file",
			Value: "mocknode.conf",
		},
		&cli.IntFlag{
			Name:  "node-id",
			Usage: "mock node id",
		},
		&cli.StringFlag{
			Name:  "addr",
			Usage: "mock node listen address",
		},
	},
	Action: runMockNodeChild,
}

func runMockNode(cctx *cli.Context) error {
	confPath := cctx.String("conf")
	conf, err := mocknode.LoadConfig(confPath)
	if err != nil {
		return err
	}
	ctx, stopSignals := signal.NotifyContext(cctx.Context, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	ctx, stop := context.WithCancel(ctx)
	defer stopSignals()
	defer stop()

	nodeCount := conf.NodeCount()
	processes := make([]*managedMockNode, 0, nodeCount)
	childExit := make(chan error, nodeCount)
	for nodeID := 0; nodeID < nodeCount; nodeID++ {
		addr := mocknode.NodeAddr(conf, nodeID)
		proc, err := startMockNodeChild(ctx, confPath, nodeID, addr, childExit)
		if err != nil {
			stopManagedMockNodes(processes)
			return err
		}
		processes = append(processes, proc)
		fmt.Printf("mocknode parent started child. node_id:%d pid:%d url:http://%s\n", nodeID, proc.cmd.Process.Pid, addr)
	}

	select {
	case <-ctx.Done():
		stopManagedMockNodes(processes)
		return nil
	case err := <-childExit:
		stopManagedMockNodes(processes)
		return err
	}
}

func runMockNodeChild(cctx *cli.Context) error {
	return mocknode.RunNode(cctx.String("conf"), cctx.Int("node-id"), cctx.String("addr"))
}

type managedMockNode struct {
	nodeID int
	cmd    *exec.Cmd
	done   chan error
}

func startMockNodeChild(ctx context.Context, confPath string, nodeID int, addr string, childExit chan<- error) (*managedMockNode, error) {
	cmd := exec.Command(os.Args[0], "mocknode-child", "--conf", confPath, "--node-id", fmt.Sprint(nodeID), "--addr", addr)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	proc := &managedMockNode{
		nodeID: nodeID,
		cmd:    cmd,
		done:   make(chan error, 1),
	}
	var logWG sync.WaitGroup
	logWG.Add(2)
	go prefixChildOutput(nodeID, "stdout", stdout, &logWG)
	go prefixChildOutput(nodeID, "stderr", stderr, &logWG)
	go func() {
		err := cmd.Wait()
		logWG.Wait()
		proc.done <- err
		if err != nil && ctx.Err() == nil {
			childExit <- fmt.Errorf("mocknode child exited. node_id:%d err:%w", nodeID, err)
			return
		}
		if ctx.Err() == nil {
			childExit <- fmt.Errorf("mocknode child exited. node_id:%d", nodeID)
		}
	}()
	return proc, nil
}

func prefixChildOutput(nodeID int, stream string, r io.Reader, wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Printf("[mocknode:%d %s] %s\n", nodeID, stream, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("[mocknode:%d %s] read failed: %v\n", nodeID, stream, err)
	}
}

func stopManagedMockNodes(processes []*managedMockNode) {
	for _, proc := range processes {
		if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
			continue
		}
		_ = proc.cmd.Process.Signal(syscall.SIGTERM)
	}

	deadline := time.After(5 * time.Second)
	for _, proc := range processes {
		if proc == nil {
			continue
		}
		select {
		case <-proc.done:
		case <-deadline:
			for _, leftover := range processes {
				if leftover != nil && leftover.cmd != nil && leftover.cmd.Process != nil {
					_ = leftover.cmd.Process.Kill()
				}
			}
			return
		}
	}
}
