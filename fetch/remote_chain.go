package fetch

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type RemoteChain struct {
	mtx    *sync.RWMutex
	height uint64
	hash   string
}

func NewRemoteChain() *RemoteChain {
	return &RemoteChain{
		mtx: &sync.RWMutex{},
	}
}

func (c *RemoteChain) Update(height uint64, hash string) {
	logrus.Debugf("update remote chain. height:%v hash:%v", height, hash)

	c.mtx.Lock()
	c.height = height
	c.hash = hash
	c.mtx.Unlock()
}

func (c *RemoteChain) GetChainInfo() (uint64, string) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.height, c.hash
}
