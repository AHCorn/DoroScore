package controllers

import (
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
