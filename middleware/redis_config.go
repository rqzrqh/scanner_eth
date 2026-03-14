package middleware

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

	// 高级配置（可选）
	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`
	DialTimeout  int `yaml:"dial_timeout"`  // 秒
	ReadTimeout  int `yaml:"read_timeout"`  // 秒
	WriteTimeout int `yaml:"write_timeout"` // 秒
}

// NewRedisClient 从 RedisConfig 创建 Redis 客户端
func NewRedisClient(cfg RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Second,
	})
}
