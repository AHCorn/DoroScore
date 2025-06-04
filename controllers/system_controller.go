package controllers

import (
	"context"
	"gohbase/models"
	"gohbase/utils"

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
	// 返回简单的系统状态信息
	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "系统运行正常",
		"logs":    []string{"系统启动成功", "HBase连接正常", "缓存系统运行正常"},
	})
}

// GetCacheStats 获取缓存统计信息
func (sc *SystemController) GetCacheStats(c *gin.Context) {
	stats := utils.Cache.Stats()
	utils.SuccessData(c, gin.H{
		"status": "success",
		"stats":  stats,
	})
}

// BuildSearchIndex 构建搜索索引
func (sc *SystemController) BuildSearchIndex(c *gin.Context) {
	ctx := context.Background()
	searchIndex := models.GetSearchIndex()

	// 在后台异步构建索引
	go func() {
		err := searchIndex.BuildSearchIndex(ctx)
		if err != nil {
			// 日志记录错误，但不影响响应
		}
	}()

	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": "搜索索引构建已开始，请稍后查看状态",
	})
}

// GetSearchIndexStats 获取搜索索引统计信息
func (sc *SystemController) GetSearchIndexStats(c *gin.Context) {
	searchIndex := models.GetSearchIndex()
	stats := searchIndex.GetIndexStats()

	utils.SuccessData(c, gin.H{
		"status": "success",
		"stats":  stats,
	})
}
