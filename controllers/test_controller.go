package controllers

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gohbase/services"
	"gohbase/utils"

	"github.com/gin-gonic/gin"
	"github.com/tsuna/gohbase/hrpc"
)

// TestController 测试控制器 - 优化版本
type TestController struct {
	isRunning     int32 // 使用原子操作
	stopChan      chan bool
	mu            sync.RWMutex
	logs          []string
	movieStats    map[string]int64 // 使用int64支持原子操作
	totalInserted int64            // 使用原子操作
	startTime     time.Time

	// 新增：批量写入相关
	batchSize   int
	batchBuffer []BatchWriteItem
	batchMu     sync.Mutex
	lastFlush   time.Time

	// 新增：性能监控
	writeLatency []time.Duration
	errorCount   int64

	// 新增：详细写入记录
	recentWrites []WriteRecord
	writesMu     sync.RWMutex
}

// BatchWriteItem 批量写入项
type BatchWriteItem struct {
	MovieID string
	UserID  string
	Rating  float64
	Source  string
}

// WriteRecord 写入记录
type WriteRecord struct {
	MovieID   string    `json:"movieId"`
	UserID    string    `json:"userId"`
	Rating    float64   `json:"rating"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"timestamp"`
}

// NewTestController 创建测试控制器
func NewTestController() *TestController {
	return &TestController{
		isRunning:    0,
		stopChan:     make(chan bool),
		logs:         make([]string, 0, 1000), // 预分配容量
		movieStats:   make(map[string]int64),
		batchSize:    50, // 批量大小
		batchBuffer:  make([]BatchWriteItem, 0, 50),
		writeLatency: make([]time.Duration, 0, 100),
		recentWrites: make([]WriteRecord, 0, 500), // 保存最近500条写入记录
	}
}

// StartRandomRatings 开始随机写入评分数据 - 优化版本
func (tc *TestController) StartRandomRatings(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&tc.isRunning, 0, 1) {
		utils.BadRequest(c, "随机写入已在运行中")
		return
	}

	// 重置状态
	tc.mu.Lock()
	tc.stopChan = make(chan bool)
	tc.logs = tc.logs[:0] // 重用切片，避免重新分配
	tc.movieStats = make(map[string]int64)
	atomic.StoreInt64(&tc.totalInserted, 0)
	atomic.StoreInt64(&tc.errorCount, 0)
	tc.startTime = time.Now()
	tc.lastFlush = time.Now()
	tc.batchBuffer = tc.batchBuffer[:0]
	tc.writeLatency = tc.writeLatency[:0]
	tc.mu.Unlock()

	// 启动后台写入任务
	go tc.runOptimizedRandomRatingsTask()

	tc.addLog("🚀 优化版随机评分写入任务已启动 (批量模式)")

	utils.SuccessData(c, gin.H{
		"status":      "success",
		"message":     "优化版随机评分写入任务已启动",
		"startTime":   tc.startTime.Format("2006-01-02 15:04:05"),
		"maxDuration": "5分钟",
		"batchSize":   tc.batchSize,
		"mode":        "optimized_batch",
	})
}

// StopRandomRatings 停止随机写入评分数据
func (tc *TestController) StopRandomRatings(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&tc.isRunning, 1, 0) {
		utils.BadRequest(c, "随机写入未在运行")
		return
	}

	// 停止任务
	close(tc.stopChan)

	// 刷新剩余的批量数据
	tc.flushBatch()

	duration := time.Since(tc.startTime)
	totalInserted := atomic.LoadInt64(&tc.totalInserted)
	errorCount := atomic.LoadInt64(&tc.errorCount)

	tc.addLog(fmt.Sprintf("⏹️ 随机评分写入任务已停止，运行时长: %v, 成功: %d, 错误: %d",
		duration, totalInserted, errorCount))

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"message":       "随机评分写入任务已停止",
		"duration":      duration.String(),
		"totalInserted": totalInserted,
		"errorCount":    errorCount,
		"successRate":   fmt.Sprintf("%.2f%%", float64(totalInserted)/float64(totalInserted+errorCount)*100),
	})
}

// GetRandomRatingsStatus 获取随机写入状态 - 优化版本
func (tc *TestController) GetRandomRatingsStatus(c *gin.Context) {
	tc.mu.RLock()
	tc.writesMu.RLock()

	isRunning := atomic.LoadInt32(&tc.isRunning) == 1
	totalInserted := atomic.LoadInt64(&tc.totalInserted)
	errorCount := atomic.LoadInt64(&tc.errorCount)

	var duration time.Duration
	if isRunning {
		duration = time.Since(tc.startTime)
	}

	// 找出写入最多的电影
	var topMovie string
	var maxCount int64
	for movieID, count := range tc.movieStats {
		if count > maxCount {
			maxCount = count
			topMovie = movieID
		}
	}

	// 计算平均延迟
	var avgLatency time.Duration
	if len(tc.writeLatency) > 0 {
		var total time.Duration
		for _, lat := range tc.writeLatency {
			total += lat
		}
		avgLatency = total / time.Duration(len(tc.writeLatency))
	}

	// 计算评分统计
	ratingStats := tc.calculateRatingStats()

	tc.writesMu.RUnlock()
	tc.mu.RUnlock()

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"isRunning":     isRunning,
		"startTime":     tc.startTime.Format("2006-01-02 15:04:05"),
		"duration":      duration.String(),
		"totalInserted": totalInserted,
		"errorCount":    errorCount,
		"successRate":   fmt.Sprintf("%.2f%%", float64(totalInserted)/float64(totalInserted+errorCount)*100),
		"avgLatency":    avgLatency.String(),
		"topMovie": gin.H{
			"movieId": topMovie,
			"count":   maxCount,
		},
		"movieCount":   len(tc.movieStats),
		"batchSize":    tc.batchSize,
		"mode":         "optimized_batch",
		"ratingStats":  ratingStats,
		"writeRecords": len(tc.recentWrites),
	})
}

// GetRandomRatingsLogs 获取随机写入日志
func (tc *TestController) GetRandomRatingsLogs(c *gin.Context) {
	tc.mu.RLock()
	tc.writesMu.RLock()
	defer tc.mu.RUnlock()
	defer tc.writesMu.RUnlock()

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

	// 获取最近的写入记录
	recentWrites := tc.recentWrites
	if len(recentWrites) > limit {
		recentWrites = recentWrites[len(recentWrites)-limit:]
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
			Count:   int(count),
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

	// 计算评分统计
	ratingStats := tc.calculateRatingStats()

	utils.SuccessData(c, gin.H{
		"status":        "success",
		"isRunning":     atomic.LoadInt32(&tc.isRunning) == 1,
		"totalInserted": atomic.LoadInt64(&tc.totalInserted),
		"logs":          logs,
		"recentWrites":  recentWrites,
		"topMovies":     topMovies,
		"movieCount":    len(tc.movieStats),
		"ratingStats":   ratingStats,
	})
}

// calculateRatingStats 计算评分统计信息
func (tc *TestController) calculateRatingStats() map[string]interface{} {
	if len(tc.recentWrites) == 0 {
		return map[string]interface{}{
			"avgRating":          0.0,
			"minRating":          0.0,
			"maxRating":          0.0,
			"ratingRange":        "0.5-5.0",
			"userIdRange":        "10000-99999",
			"movieIdRange":       "1-50",
			"totalUsers":         0,
			"ratingDistribution": map[string]int{},
		}
	}

	var totalRating float64
	minRating := 5.0
	maxRating := 0.5
	userSet := make(map[string]bool)
	ratingDistribution := make(map[string]int)

	for _, record := range tc.recentWrites {
		totalRating += record.Rating
		if record.Rating < minRating {
			minRating = record.Rating
		}
		if record.Rating > maxRating {
			maxRating = record.Rating
		}
		userSet[record.UserID] = true

		// 评分分布统计
		ratingKey := fmt.Sprintf("%.1f", record.Rating)
		ratingDistribution[ratingKey]++
	}

	avgRating := totalRating / float64(len(tc.recentWrites))

	return map[string]interface{}{
		"avgRating":          avgRating,
		"minRating":          minRating,
		"maxRating":          maxRating,
		"ratingRange":        "0.5-5.0",
		"userIdRange":        "10000-99999",
		"movieIdRange":       "1-50",
		"totalUsers":         len(userSet),
		"ratingDistribution": ratingDistribution,
	}
}

// runOptimizedRandomRatingsTask 运行优化的随机评分写入任务
func (tc *TestController) runOptimizedRandomRatingsTask() {
	// 设置5分钟超时
	timeout := time.After(5 * time.Minute)

	// 降低写入频率，改为每500ms生成一批数据
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// 批量刷新定时器，每2秒强制刷新一次
	flushTicker := time.NewTicker(2 * time.Second)
	defer flushTicker.Stop()

	tc.addLog("📝 开始优化版随机写入评分数据...")

	for {
		select {
		case <-tc.stopChan:
			tc.addLog("🛑 收到停止信号，任务结束")
			return
		case <-timeout:
			atomic.StoreInt32(&tc.isRunning, 0)
			tc.addLog("⏰ 达到5分钟时间限制，任务自动结束")
			return
		case <-ticker.C:
			// 生成一批随机数据
			tc.generateBatchData()
		case <-flushTicker.C:
			// 定期刷新批量数据
			tc.flushBatch()
		}
	}
}

// generateBatchData 生成批量数据
func (tc *TestController) generateBatchData() {
	tc.batchMu.Lock()
	defer tc.batchMu.Unlock()

	// 生成5-10个随机评分数据
	batchCount := rand.Intn(6) + 5

	for i := 0; i < batchCount; i++ {
		// 随机选择电影ID (1-50)
		movieID := rand.Intn(50) + 1
		movieIDStr := strconv.Itoa(movieID)

		// 生成随机用户ID (10000-99999)
		userID := rand.Intn(90000) + 10000
		userIDStr := strconv.Itoa(userID)

		// 生成随机评分 (0.5-5.0, 步长0.5)
		ratingFloat := (float64(rand.Intn(10)) + 1) * 0.5

		tc.batchBuffer = append(tc.batchBuffer, BatchWriteItem{
			MovieID: movieIDStr,
			UserID:  userIDStr,
			Rating:  ratingFloat,
			Source:  "test_batch",
		})
	}

	// 如果批量缓冲区满了，立即刷新
	if len(tc.batchBuffer) >= tc.batchSize {
		tc.flushBatchUnsafe()
	}
}

// flushBatch 刷新批量数据（带锁）
func (tc *TestController) flushBatch() {
	tc.batchMu.Lock()
	defer tc.batchMu.Unlock()
	tc.flushBatchUnsafe()
}

// flushBatchUnsafe 刷新批量数据（不带锁）
func (tc *TestController) flushBatchUnsafe() {
	if len(tc.batchBuffer) == 0 {
		return
	}

	startTime := time.Now()
	ctx := context.Background()

	// 批量写入到HBase
	successCount, errorCount := tc.batchWriteToHBase(ctx, tc.batchBuffer)

	// 更新统计信息
	atomic.AddInt64(&tc.totalInserted, int64(successCount))
	atomic.AddInt64(&tc.errorCount, int64(errorCount))

	// 计算本批次的统计信息（在清空缓冲区之前）
	var avgRating float64
	userCount := make(map[string]bool)
	batchSize := len(tc.batchBuffer)

	for _, item := range tc.batchBuffer {
		avgRating += item.Rating
		userCount[item.UserID] = true
	}

	if batchSize > 0 {
		avgRating /= float64(batchSize)
	}

	// 更新电影统计和写入记录
	tc.mu.Lock()
	tc.writesMu.Lock()

	timestamp := time.Now()
	for _, item := range tc.batchBuffer {
		tc.movieStats[item.MovieID]++

		// 记录详细写入信息
		writeRecord := WriteRecord{
			MovieID:   item.MovieID,
			UserID:    item.UserID,
			Rating:    item.Rating,
			Source:    item.Source,
			Timestamp: timestamp,
		}
		tc.recentWrites = append(tc.recentWrites, writeRecord)
	}

	// 保持最近500条写入记录
	if len(tc.recentWrites) > 500 {
		tc.recentWrites = tc.recentWrites[len(tc.recentWrites)-500:]
	}

	// 记录延迟
	latency := time.Since(startTime)
	if len(tc.writeLatency) >= 100 {
		tc.writeLatency = tc.writeLatency[1:] // 保持最近100次的延迟记录
	}
	tc.writeLatency = append(tc.writeLatency, latency)

	tc.writesMu.Unlock()
	tc.mu.Unlock()

	// 清空缓冲区
	tc.batchBuffer = tc.batchBuffer[:0]
	tc.lastFlush = time.Now()

	// 记录详细日志
	if successCount > 0 {
		tc.addLog(fmt.Sprintf("✅ 批量写入完成: 成功 %d 条, 失败 %d 条, 耗时 %v | 平均评分: %.1f, 用户数: %d",
			successCount, errorCount, latency, avgRating, len(userCount)))
	}
}

// batchWriteToHBase 批量写入到HBase
func (tc *TestController) batchWriteToHBase(ctx context.Context, items []BatchWriteItem) (int, int) {
	if len(items) == 0 {
		return 0, 0
	}

	var successCount, errorCount int

	// 按电影ID分组，减少HBase行锁竞争
	movieGroups := make(map[string][]BatchWriteItem)
	for _, item := range items {
		movieGroups[item.MovieID] = append(movieGroups[item.MovieID], item)
	}

	// 并发写入不同电影的数据
	var wg sync.WaitGroup
	var mu sync.Mutex

	for movieID, movieItems := range movieGroups {
		wg.Add(1)
		go func(mID string, mItems []BatchWriteItem) {
			defer wg.Done()

			success, errors := tc.writeMovieRatingsBatch(ctx, mID, mItems)

			mu.Lock()
			successCount += success
			errorCount += errors
			mu.Unlock()
		}(movieID, movieItems)
	}

	wg.Wait()
	return successCount, errorCount
}

// writeMovieRatingsBatch 批量写入单个电影的评分数据
func (tc *TestController) writeMovieRatingsBatch(ctx context.Context, movieID string, items []BatchWriteItem) (int, int) {
	// 构建批量Put请求
	values := make(map[string][]byte)
	timestamp := time.Now().Unix()

	for _, item := range items {
		ratingValue := fmt.Sprintf("%.1f:%s:%d", item.Rating, item.UserID, timestamp)
		values[item.UserID] = []byte(ratingValue)
	}

	// 构建行键
	rowKey := fmt.Sprintf("%s_ratings", movieID)

	// 创建Put请求
	putRequest, err := hrpc.NewPutStr(ctx, "movies", rowKey, map[string]map[string][]byte{
		"ratings": values,
	})

	if err != nil {
		return 0, len(items)
	}

	// 获取HBase客户端并执行
	client := utils.GetClient().(interface {
		Put(request *hrpc.Mutate) (*hrpc.Result, error)
	})

	_, err = client.Put(putRequest)
	if err != nil {
		return 0, len(items)
	}

	// 记录到追踪服务
	for _, item := range items {
		services.GlobalRatingTracker.RecordRatingWrite(item.MovieID, item.UserID, item.Rating, item.Source)
	}

	return len(items), 0
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
