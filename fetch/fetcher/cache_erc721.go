package fetcher

import (
	"scanner_eth/data"
	"scanner_eth/model"
	"sync"
	"time"

	"gorm.io/gorm"
)

const cacheErc721TTL = 1 * time.Hour

var (
	erc721ContractCacheOnce sync.Once
	erc721ContractCacheInst *erc721ContractCache
)

func initErc721Cache() {
	erc721ContractCacheOnce.Do(func() {
		erc721ContractCacheInst = newErc721ContractCache()
	})
}

// InitErc721Cache initializes the ERC721 contract cache for block fetching runtime.
func InitErc721Cache() {
	initErc721Cache()
}

type erc721ContractCache struct {
	mu    sync.RWMutex
	items map[string]*erc721ContractCacheItem
}

type erc721ContractCacheItem struct {
	v        *data.ContractErc721
	expireAt time.Time
}

func newErc721ContractCache() *erc721ContractCache {
	return &erc721ContractCache{items: make(map[string]*erc721ContractCacheItem)}
}

func (c *erc721ContractCache) Get(contractAddr string, db *gorm.DB) (*data.ContractErc721, bool) {
	c.mu.RLock()
	item, ok := c.items[contractAddr]
	c.mu.RUnlock()
	if ok && item != nil && time.Now().Before(item.expireAt) {
		return item.v, true
	}
	if ok && item != nil {
		c.mu.Lock()
		delete(c.items, contractAddr)
		c.mu.Unlock()
	}

	if db != nil {
		var m model.ContractErc721
		if db.Where("contract_addr = ?", contractAddr).First(&m).Error == nil {
			v := &data.ContractErc721{
				ContractAddr: m.ContractAddr,
				Name:         m.Name,
				Symbol:       m.Symbol,
			}
			c.Set(contractAddr, v)
			return v, true
		}
	}
	return nil, false
}

func (c *erc721ContractCache) Set(contractAddr string, v *data.ContractErc721) {
	if v == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[contractAddr] = &erc721ContractCacheItem{v: v, expireAt: time.Now().Add(cacheErc721TTL)}
}
