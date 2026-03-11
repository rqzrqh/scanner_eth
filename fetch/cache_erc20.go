package fetch

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

// InitErc20Cache 初始化 ERC20 缓存，由 NewFetchManager 调用一次
func InitErc20Cache() {
	initErc20Cache()
}

// tokenCache ContractErc20 缓存，key 为合约地址。未命中时在缓存内查 DB。
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

// Get 先查内存缓存，未命中则在缓存内查 DB；命中则回填缓存并返回。返回 (value, true) 表示命中， (nil, false) 表示需链上拉取。
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

	// 缓存未命中：在缓存内执行 DB 读取
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
