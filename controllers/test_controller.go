package controllers

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"gohbase/services"
	"gohbase/utils"

	"github.com/gin-gonic/gin"
	"github.com/tsuna/gohbase/hrpc"
)

// TestController 测试控制器
type TestController struct {
	isRunning     bool
	stopChan      chan bool
	mu            sync.RWMutex
	logs          []string
	movieStats    map[string]int
	totalInserted int
	startTime     time.Time
}

// NewTestController 创建测试控制器
func NewTestController() *TestController {
	return &TestController{
		isRunning:  false,
		stopChan:   make(chan bool),
		logs:       make([]string, 0),
		movieStats: make(map[string]int),
	}
}

// StartRandomRatings 开始随机写入评分数据
func (tc *TestController) StartRandomRatings(c *gin.Context) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.isRunning {
		utils.BadRequest(c, "随机写入已在运行中")
		return
	}

	// 重置状态
	tc.isRunning = true
	tc.stopChan = make(chan bool)
	tc.logs = make([]string, 0)
	tc.movieStats = make(map[string]int)
	tc.totalInserted = 0
	tc.startTime = time.Now()

	// 启动后台写入任务
	go tc.runRandomRatingsTask()

	tc.addLog("🚀 随机评分写入任务已启动")

	utils.SuccessData(c, gin.H{
		"status":      "success",
		"message":     "随机评分写入任务已启动",
		"startTime":   tc.startTime.Format("2006-01-02 15:04:05"),
		"maxDuration": "5分钟",
	})
}

// StopRandomRatings 停止随机写入评分数据
func (tc *TestController) StopRandomRatings(c *gin.Context) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if !tc.isRunning {
		utils.BadRequest(c, "随机写入未在运行")
		return
	}

	// 停止任务
	tc.isRunning = false
	close(tc.stopChan)

	duration := time.Since(tc.startTime)
	tc.addLog(fmt.Sprintf("⏹️ 随机评分写入任务已停止，运行时长: %v", duration))

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"message":       "随机评分写入任务已停止",
		"duration":      duration.String(),
		"totalInserted": tc.totalInserted,
	})
}

// GetRandomRatingsStatus 获取随机写入状态
func (tc *TestController) GetRandomRatingsStatus(c *gin.Context) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	var duration time.Duration
	if tc.isRunning {
		duration = time.Since(tc.startTime)
	}

	// 找出写入最多的电影
	var topMovie string
	var maxCount int
	for movieID, count := range tc.movieStats {
		if count > maxCount {
			maxCount = count
			topMovie = movieID
		}
	}

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"isRunning":     tc.isRunning,
		"startTime":     tc.startTime.Format("2006-01-02 15:04:05"),
		"duration":      duration.String(),
		"totalInserted": tc.totalInserted,
		"topMovie": gin.H{
			"movieId": topMovie,
			"count":   maxCount,
		},
		"movieCount": len(tc.movieStats),
	})
}

// GetRandomRatingsLogs 获取随机写入日志
func (tc *TestController) GetRandomRatingsLogs(c *gin.Context) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// 获取最近的日志条数，默认50条
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200 // 最大200条
	}

	// 获取最近的日志
	logs := tc.logs
	if len(logs) > limit {
		logs = logs[len(logs)-limit:]
	}

	// 找出写入最多的电影TOP 10
	type movieStat struct {
		MovieID string `json:"movieId"`
		Count   int    `json:"count"`
	}

	var topMovies []movieStat
	for movieID, count := range tc.movieStats {
		topMovies = append(topMovies, movieStat{
			MovieID: movieID,
			Count:   count,
		})
	}

	// 按写入数量排序
	for i := 0; i < len(topMovies)-1; i++ {
		for j := i + 1; j < len(topMovies); j++ {
			if topMovies[i].Count < topMovies[j].Count {
				topMovies[i], topMovies[j] = topMovies[j], topMovies[i]
			}
		}
	}

	// 只取前10个
	if len(topMovies) > 10 {
		topMovies = topMovies[:10]
	}

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"isRunning":     tc.isRunning,
		"totalInserted": tc.totalInserted,
		"logs":          logs,
		"topMovies":     topMovies,
		"movieCount":    len(tc.movieStats),
	})
}

// runRandomRatingsTask 运行随机评分写入任务
func (tc *TestController) runRandomRatingsTask() {
	ctx := context.Background()

	// 获取HBase客户端
	client := utils.GetClient().(interface {
		Put(request *hrpc.Mutate) (*hrpc.Result, error)
	})

	// 设置5分钟超时
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(100 * time.Millisecond) // 每100ms写入一次
	defer ticker.Stop()

	tc.addLog("📝 开始随机写入评分数据...")

	for {
		select {
		case <-tc.stopChan:
			tc.addLog("🛑 收到停止信号，任务结束")
			return
		case <-timeout:
			tc.mu.Lock()
			tc.isRunning = false
			tc.mu.Unlock()
			tc.addLog("⏰ 达到5分钟时间限制，任务自动结束")
			return
		case <-ticker.C:
			// 执行一次随机写入
			tc.performRandomWrite(ctx, client)
		}
	}
}

// performRandomWrite 执行一次随机写入
func (tc *TestController) performRandomWrite(ctx context.Context, client interface {
	Put(request *hrpc.Mutate) (*hrpc.Result, error)
}) {
	// 随机选择电影ID (1-50)
	movieID := rand.Intn(50) + 1
	movieIDStr := strconv.Itoa(movieID)

	// 生成随机用户ID (10000-99999)
	userID := rand.Intn(90000) + 10000
	userIDStr := strconv.Itoa(userID)

	// 生成随机评分 (0.5-5.0, 步长0.5)
	ratingFloat := (float64(rand.Intn(10)) + 1) * 0.5

	// 使用通用评分写入函数
	err := services.GlobalRatingTracker.WriteRatingToHBase(ctx, movieIDStr, userIDStr, ratingFloat, "test")
	if err != nil {
		tc.addLog(fmt.Sprintf("❌ 写入失败 (电影%s, 用户%s): %v", movieIDStr, userIDStr, err))
		return
	}

	// 更新统计
	tc.mu.Lock()
	tc.totalInserted++
	tc.movieStats[movieIDStr]++
	tc.mu.Unlock()

	// 每10次写入记录一次日志
	if tc.totalInserted%10 == 0 {
		tc.addLog(fmt.Sprintf("✅ 已写入 %d 条评分数据，最新: 电影%s 用户%s 评分%.1f",
			tc.totalInserted, movieIDStr, userIDStr, ratingFloat))
	}
}

// addLog 添加日志
func (tc *TestController) addLog(message string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	timestamp := time.Now().Format("15:04:05")
	logEntry := fmt.Sprintf("[%s] %s", timestamp, message)
	tc.logs = append(tc.logs, logEntry)

	// 保持最多1000条日志
	if len(tc.logs) > 1000 {
		tc.logs = tc.logs[len(tc.logs)-1000:]
	}
}

// GenerateRandomRatingsForMovie 为指定电影生成随机评分
func (tc *TestController) GenerateRandomRatingsForMovie(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		utils.BadRequest(c, "电影ID不能为空")
		return
	}

	ctx := context.Background()

	// 获取要生成的评分数量，默认10个
	countStr := c.DefaultQuery("count", "10")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		count = 10
	}
	if count > 100 {
		count = 100 // 限制最大数量
	}

	var inserted int
	var errors []string

	// 生成指定数量的随机评分
	for i := 0; i < count; i++ {
		// 生成随机用户ID (10000-99999)
		userID := rand.Intn(90000) + 10000
		userIDStr := strconv.Itoa(userID)

		// 生成随机评分 (0.5-5.0, 步长0.5)
		ratingFloat := (float64(rand.Intn(10)) + 1) * 0.5

		// 使用通用评分写入函数
		err := services.GlobalRatingTracker.WriteRatingToHBase(ctx, movieID, userIDStr, ratingFloat, "api")
		if err != nil {
			errors = append(errors, fmt.Sprintf("写入失败 (用户%s): %v", userIDStr, err))
			continue
		}

		inserted++
	}

	// 构建响应
	response := gin.H{
		"status":  "success",
		"message": fmt.Sprintf("为电影 %s 生成随机评分完成", movieID),
		"data": gin.H{
			"movieId":     movieID,
			"requested":   count,
			"inserted":    inserted,
			"successRate": fmt.Sprintf("%.1f%%", float64(inserted)/float64(count)*100),
		},
	}

	if len(errors) > 0 {
		response["errors"] = errors
		response["errorCount"] = len(errors)
	}

	utils.SuccessData(c, response)
}

// ClearMovieRatings 清除指定电影的所有评分数据
func (tc *TestController) ClearMovieRatings(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		utils.BadRequest(c, "电影ID不能为空")
		return
	}

	ctx := context.Background()

	// 获取HBase客户端
	client := utils.GetClient().(interface {
		Delete(request *hrpc.Mutate) (*hrpc.Result, error)
	})

	// 构建行键: "{movieId}_ratings"
	rowKey := fmt.Sprintf("%s_ratings", movieID)

	// 创建Delete请求
	deleteRequest, err := hrpc.NewDelStr(ctx, "movies", rowKey, nil)
	if err != nil {
		utils.InternalError(c, "创建删除请求失败", err)
		return
	}

	// 执行Delete操作
	_, err = client.Delete(deleteRequest)
	if err != nil {
		utils.InternalError(c, "删除评分数据失败", err)
		return
	}

	utils.SuccessData(c, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("电影 %s 的评分数据已清除", movieID),
		"movieId": movieID,
	})
}
