package fetch

import (
	"github.com/sirupsen/logrus"
)

type RemoteChain struct {
	height uint64
	hash   string
}

func NewRemoteChain() *RemoteChain {
	return &RemoteChain{}
}

func (c *RemoteChain) Update(height uint64, hash string) {
	logrus.Debugf("update remote chain. height:%v hash:%v", height, hash)
	c.height = height
	c.hash = hash
}

func (c *RemoteChain) GetChainInfo() (uint64, string) {
	return c.height, c.hash
}
