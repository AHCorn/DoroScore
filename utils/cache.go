package utils

import (
	"gohbase/config"
	"gohbase/utils/cache"
)

// Cache 全局缓存实例
var Cache *cache.MemoryCache

// InitCache 初始化缓存系统
func InitCache(cfg *config.Config) {
	Cache = cache.NewMemoryCache(
		cfg.GetCacheDefaultExpiration(),
		cfg.GetCacheCleanupInterval(),
	)
}
