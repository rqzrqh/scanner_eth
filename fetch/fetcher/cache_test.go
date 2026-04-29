package fetcher

import (
	"fmt"
	"testing"
	"time"

	"scanner_eth/data"
	"scanner_eth/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newErc20CacheTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.ContractErc20{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func newErc721CacheTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.ContractErc721{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func TestTokenCacheGetSetAndDBFallback(t *testing.T) {
	cache := newTokenCache()
	if got, ok := cache.Get("0xnotfound", nil); ok || got != nil {
		t.Fatalf("expected miss for empty cache")
	}

	v := &data.ContractErc20{ContractAddr: "0x1", Name: "n", Symbol: "s", Decimals: 18, TotalSupply: "1"}
	cache.Set("0x1", v)
	if got, ok := cache.Get("0x1", nil); !ok || got == nil || got.ContractAddr != "0x1" {
		t.Fatalf("expected memory cache hit, got=%v ok=%v", got, ok)
	}

	cache.Set("0xnil", nil)
	if _, exists := cache.items["0xnil"]; exists {
		t.Fatal("nil set should not create cache item")
	}

	cache.items["0x1"].expireAt = time.Now().Add(-time.Second)
	if got, ok := cache.Get("0x1", nil); ok || got != nil {
		t.Fatalf("expected expired item miss, got=%v ok=%v", got, ok)
	}
	if _, exists := cache.items["0x1"]; exists {
		t.Fatal("expired item should be removed from cache")
	}

	db := newErc20CacheTestDB(t)
	if err := db.Create(&model.ContractErc20{ContractAddr: "0xdb20", Name: "Token20", Symbol: "T20", Decimals: 18, TotalSupply: "1000"}).Error; err != nil {
		t.Fatalf("insert contract_erc20 failed: %v", err)
	}
	fromDB, ok := cache.Get("0xdb20", db)
	if !ok || fromDB == nil || fromDB.Symbol != "T20" {
		t.Fatalf("expected db fallback hit, got=%v ok=%v", fromDB, ok)
	}
	if _, ok := cache.Get("0xdb20", nil); !ok {
		t.Fatal("db fallback result should be refilled into memory cache")
	}
}

func TestErc721CacheGetSetAndDBFallback(t *testing.T) {
	cache := newErc721ContractCache()
	if got, ok := cache.Get("0xnotfound", nil); ok || got != nil {
		t.Fatalf("expected miss for empty cache")
	}

	v := &data.ContractErc721{ContractAddr: "0x2", Name: "nft", Symbol: "n"}
	cache.Set("0x2", v)
	if got, ok := cache.Get("0x2", nil); !ok || got == nil || got.ContractAddr != "0x2" {
		t.Fatalf("expected memory cache hit, got=%v ok=%v", got, ok)
	}

	cache.Set("0xnil", nil)
	if _, exists := cache.items["0xnil"]; exists {
		t.Fatal("nil set should not create cache item")
	}

	cache.items["0x2"].expireAt = time.Now().Add(-time.Second)
	if got, ok := cache.Get("0x2", nil); ok || got != nil {
		t.Fatalf("expected expired item miss, got=%v ok=%v", got, ok)
	}
	if _, exists := cache.items["0x2"]; exists {
		t.Fatal("expired item should be removed from cache")
	}

	db := newErc721CacheTestDB(t)
	if err := db.Create(&model.ContractErc721{ContractAddr: "0xdb721", Name: "NFT", Symbol: "N"}).Error; err != nil {
		t.Fatalf("insert contract_erc721 failed: %v", err)
	}
	fromDB, ok := cache.Get("0xdb721", db)
	if !ok || fromDB == nil || fromDB.Symbol != "N" {
		t.Fatalf("expected db fallback hit, got=%v ok=%v", fromDB, ok)
	}
	if _, ok := cache.Get("0xdb721", nil); !ok {
		t.Fatal("db fallback result should be refilled into memory cache")
	}
}
