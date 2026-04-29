package fetcher

import (
	"scanner_eth/data"
	"scanner_eth/model"
	"sync"
	"time"

	"gorm.io/gorm"
)

const cacheErc20TTL = 1 * time.Hour

var (
	tokenCacheOnce sync.Once
	tokenCacheInst *tokenCache
)

func initErc20Cache() {
	tokenCacheOnce.Do(func() {
		tokenCacheInst = newTokenCache()
	})
}

// InitErc20Cache initializes the ERC20 cache for block fetching runtime.
func InitErc20Cache() {
	initErc20Cache()
}

type tokenCache struct {
	mu    sync.RWMutex
	items map[string]*tokenCacheItem
}

type tokenCacheItem struct {
	v        *data.ContractErc20
	expireAt time.Time
}

func newTokenCache() *tokenCache {
	return &tokenCache{items: make(map[string]*tokenCacheItem)}
}

func (c *tokenCache) Get(contractAddr string, db *gorm.DB) (*data.ContractErc20, bool) {
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
		var m model.ContractErc20
		if db.Where("contract_addr = ?", contractAddr).First(&m).Error == nil {
			v := &data.ContractErc20{
				ContractAddr: m.ContractAddr,
				Name:         m.Name,
				Symbol:       m.Symbol,
				Decimals:     m.Decimals,
				TotalSupply:  m.TotalSupply,
			}
			c.Set(contractAddr, v)
			return v, true
		}
	}
	return nil, false
}

func (c *tokenCache) Set(contractAddr string, v *data.ContractErc20) {
	if v == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[contractAddr] = &tokenCacheItem{v: v, expireAt: time.Now().Add(cacheErc20TTL)}
}
