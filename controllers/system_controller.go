package controllers

import (
	"gohbase/utils"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
)

// SystemController 系统控制器
type SystemController struct{}

// NewSystemController 创建系统控制器
func NewSystemController() *SystemController {
	return &SystemController{}
}

// GetSystemLogs 获取系统日志
func (sc *SystemController) GetSystemLogs(c *gin.Context) {
	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "系统日志功能待实现",
		"logs":    []string{},
	})
}

// GetCacheStats 获取缓存统计
func (sc *SystemController) GetCacheStats(c *gin.Context) {
	stats := utils.Cache.Stats()

	utils.SuccessData(c, gin.H{
		"status": "success",
		"stats":  stats,
	})
}

// BuildSearchIndex 构建搜索索引
func (sc *SystemController) BuildSearchIndex(c *gin.Context) {
	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "搜索索引构建功能待实现",
	})
}

// GetSearchIndexStats 获取搜索索引统计
func (sc *SystemController) GetSearchIndexStats(c *gin.Context) {
	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "搜索索引统计功能待实现",
		"stats":   gin.H{},
	})
}

// GetHBasePerformanceStats 获取HBase性能统计
func (sc *SystemController) GetHBasePerformanceStats(c *gin.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := gin.H{
		"status": "success",
		"data": gin.H{
			"memory": gin.H{
				"allocated_mb":       bToMb(m.Alloc),
				"total_allocated_mb": bToMb(m.TotalAlloc),
				"system_mb":          bToMb(m.Sys),
				"gc_count":           m.NumGC,
				"heap_objects":       m.HeapObjects,
				"heap_inuse_mb":      bToMb(m.HeapInuse),
				"heap_released_mb":   bToMb(m.HeapReleased),
			},
			"goroutines": runtime.NumGoroutine(),
			"timestamp":  time.Now().Format("2006-01-02 15:04:05"),
		},
		"recommendations": sc.getPerformanceRecommendations(&m),
	}

	utils.SuccessData(c, stats)
}

// GetHBaseDiagnostics 获取HBase诊断信息
func (sc *SystemController) GetHBaseDiagnostics(c *gin.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	diagnostics := gin.H{
		"status": "success",
		"diagnostics": gin.H{
			"memory_pressure":   sc.checkMemoryPressure(&m),
			"gc_pressure":       sc.checkGCPressure(&m),
			"goroutine_leak":    sc.checkGoroutineLeak(),
			"connection_health": "需要实现HBase连接健康检查",
		},
		"suggestions": sc.getOptimizationSuggestions(&m),
		"timestamp":   time.Now().Format("2006-01-02 15:04:05"),
	}

	utils.SuccessData(c, diagnostics)
}

// ForceGC 强制垃圾回收
func (sc *SystemController) ForceGC(c *gin.Context) {
	var beforeGC, afterGC runtime.MemStats
	runtime.ReadMemStats(&beforeGC)

	runtime.GC()
	runtime.GC() // 执行两次确保彻底清理

	runtime.ReadMemStats(&afterGC)

	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "垃圾回收已执行",
		"before": gin.H{
			"allocated_mb":  bToMb(beforeGC.Alloc),
			"heap_inuse_mb": bToMb(beforeGC.HeapInuse),
		},
		"after": gin.H{
			"allocated_mb":  bToMb(afterGC.Alloc),
			"heap_inuse_mb": bToMb(afterGC.HeapInuse),
		},
		"freed_mb":  bToMb(beforeGC.Alloc - afterGC.Alloc),
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	})
}

// 辅助函数：字节转MB
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// 检查内存压力
func (sc *SystemController) checkMemoryPressure(m *runtime.MemStats) string {
	allocatedMB := bToMb(m.Alloc)
	heapInuseMB := bToMb(m.HeapInuse)

	if allocatedMB > 500 {
		return "高内存使用 (>500MB)"
	} else if allocatedMB > 200 {
		return "中等内存使用 (200-500MB)"
	} else if heapInuseMB > allocatedMB*2 {
		return "内存碎片化严重"
	}
	return "正常"
}

// 检查GC压力
func (sc *SystemController) checkGCPressure(m *runtime.MemStats) string {
	if m.NumGC > 1000 {
		return "GC频繁 (>1000次)"
	} else if m.NumGC > 500 {
		return "GC较频繁 (500-1000次)"
	}
	return "正常"
}

// 检查协程泄漏
func (sc *SystemController) checkGoroutineLeak() string {
	goroutines := runtime.NumGoroutine()
	if goroutines > 1000 {
		return "可能存在协程泄漏 (>1000个)"
	} else if goroutines > 100 {
		return "协程数量较多 (100-1000个)"
	}
	return "正常"
}

// 获取性能建议
func (sc *SystemController) getPerformanceRecommendations(m *runtime.MemStats) []string {
	var recommendations []string

	allocatedMB := bToMb(m.Alloc)
	heapInuseMB := bToMb(m.HeapInuse)
	goroutines := runtime.NumGoroutine()

	if allocatedMB > 300 {
		recommendations = append(recommendations, "内存使用过高，建议优化数据结构或增加GC频率")
	}

	if heapInuseMB > allocatedMB*2 {
		recommendations = append(recommendations, "内存碎片化严重，建议调整对象分配策略")
	}

	if m.NumGC > 500 {
		recommendations = append(recommendations, "GC频繁，建议减少小对象分配")
	}

	if goroutines > 100 {
		recommendations = append(recommendations, "协程数量较多，检查是否存在协程泄漏")
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "系统运行状态良好")
	}

	return recommendations
}

// 获取优化建议
func (sc *SystemController) getOptimizationSuggestions(m *runtime.MemStats) []string {
	var suggestions []string

	suggestions = append(suggestions, "使用批量写入减少HBase连接开销")
	suggestions = append(suggestions, "实现连接池管理HBase客户端")
	suggestions = append(suggestions, "使用对象池减少内存分配")
	suggestions = append(suggestions, "定期执行垃圾回收释放内存")
	suggestions = append(suggestions, "监控协程数量防止泄漏")
	suggestions = append(suggestions, "使用原子操作减少锁竞争")

	return suggestions
}
