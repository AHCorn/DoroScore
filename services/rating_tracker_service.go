package services

import (
	"context"
	"fmt"
	"gohbase/models"
	"gohbase/utils"
	"sort"
	"sync"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

// RatingWriteRecord 评分写入记录
type RatingWriteRecord struct {
	MovieID   string    `json:"movieId"`
	UserID    string    `json:"userId"`
	Rating    float64   `json:"rating"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"` // "test", "api", "import" 等
}

// MovieHotness 电影热度信息
type MovieHotness struct {
	MovieID      string    `json:"movieId"`
	Title        string    `json:"title"`
	WriteCount   int       `json:"writeCount"`
	LastWrite    time.Time `json:"lastWrite"`
	AvgRating    float64   `json:"avgRating"`
	HotnessScore float64   `json:"hotnessScore"` // 综合热度分数
	// 新增字段用于10%阈值检查
	LastRatingCount    int `json:"lastRatingCount"`    // 上次重新计算时的评分总数
	NewWritesSinceCalc int `json:"newWritesSinceCalc"` // 自上次计算后的新增写入数
}

// RatingTrackerService 评分追踪服务
type RatingTrackerService struct {
	mu           sync.RWMutex
	writeRecords []RatingWriteRecord
	movieStats   map[string]*MovieHotness
	maxRecords   int
}

// NewRatingTrackerService 创建评分追踪服务
func NewRatingTrackerService() *RatingTrackerService {
	return &RatingTrackerService{
		writeRecords: make([]RatingWriteRecord, 0),
		movieStats:   make(map[string]*MovieHotness),
		maxRecords:   10000, // 最多保存10000条记录
	}
}

// RecordRatingWrite 记录评分写入（通用函数）
func (rts *RatingTrackerService) RecordRatingWrite(movieID, userID string, rating float64, source string) {
	rts.mu.Lock()
	defer rts.mu.Unlock()

	now := time.Now()

	// 创建写入记录
	record := RatingWriteRecord{
		MovieID:   movieID,
		UserID:    userID,
		Rating:    rating,
		Timestamp: now,
		Source:    source,
	}

	// 添加到记录列表
	rts.writeRecords = append(rts.writeRecords, record)

	// 保持记录数量限制
	if len(rts.writeRecords) > rts.maxRecords {
		rts.writeRecords = rts.writeRecords[len(rts.writeRecords)-rts.maxRecords:]
	}

	// 更新电影统计
	if hotness, exists := rts.movieStats[movieID]; exists {
		hotness.WriteCount++
		hotness.LastWrite = now
		hotness.NewWritesSinceCalc++ // 增加新写入计数
		// 更新平均评分（简单移动平均）
		hotness.AvgRating = (hotness.AvgRating + rating) / 2
	} else {
		// 初始化电影统计，获取当前评分总数
		ctx := context.Background()
		currentRatingCount := rts.getCurrentRatingCount(ctx, movieID)
		
		rts.movieStats[movieID] = &MovieHotness{
			MovieID:            movieID,
			WriteCount:         1,
			LastWrite:          now,
			AvgRating:          rating,
			LastRatingCount:    currentRatingCount,
			NewWritesSinceCalc: 1,
		}
	}

	// 检查是否需要重新计算评分（10%阈值）
	rts.checkAndRecalculateRating(movieID)

	// 重新计算热度分数
	rts.calculateHotnessScore(movieID)
}

// getCurrentRatingCount 获取当前电影的评分总数
func (rts *RatingTrackerService) getCurrentRatingCount(ctx context.Context, movieID string) int {
	stats, err := utils.GetMovieStats(ctx, movieID)
	if err != nil {
		return 0
	}
	
	if ratingCount, ok := stats["ratingCount"].(int); ok {
		return ratingCount
	}
	return 0
}

// checkAndRecalculateRating 检查并重新计算评分（10%阈值逻辑）
func (rts *RatingTrackerService) checkAndRecalculateRating(movieID string) {
	hotness := rts.movieStats[movieID]
	if hotness == nil {
		return
	}

	// 计算10%阈值
	threshold := int(float64(hotness.LastRatingCount) * 0.1)
	if threshold < 1 {
		threshold = 1 // 至少1个新评分才触发重新计算
	}

	// 检查是否达到阈值
	if hotness.NewWritesSinceCalc >= threshold {
		fmt.Printf("🔄 电影 %s 新增评分数 %d 达到阈值 %d (总评分数的10%%)，开始重新计算评分...\n", 
			movieID, hotness.NewWritesSinceCalc, threshold)
		
		// 异步重新计算评分
		go rts.recalculateMovieRating(movieID)
	}
}

// recalculateMovieRating 重新计算电影评分
func (rts *RatingTrackerService) recalculateMovieRating(movieID string) {
	ctx := context.Background()
	
	// 重新计算并存储评分
	avgRating, ratingCount, err := models.CalculateAndStoreMovieAvgRating(ctx, movieID)
	if err != nil {
		fmt.Printf("❌ 重新计算电影 %s 评分失败: %v\n", movieID, err)
		return
	}
	
	// 更新统计信息
	rts.mu.Lock()
	defer rts.mu.Unlock()
	
	if hotness, exists := rts.movieStats[movieID]; exists {
		hotness.LastRatingCount = ratingCount
		hotness.NewWritesSinceCalc = 0 // 重置新增计数
		hotness.AvgRating = avgRating
	}
	
	fmt.Printf("✅ 电影 %s 评分重新计算完成: 平均评分=%.2f, 总评分数=%d\n", 
		movieID, avgRating, ratingCount)
}

// calculateHotnessScore 计算热度分数
func (rts *RatingTrackerService) calculateHotnessScore(movieID string) {
	hotness := rts.movieStats[movieID]
	if hotness == nil {
		return
	}

	now := time.Now()

	// 时间衰减因子（最近的写入权重更高）
	timeDiff := now.Sub(hotness.LastWrite).Hours()
	timeDecay := 1.0 / (1.0 + timeDiff/24.0) // 24小时衰减

	// 写入频率分数
	writeScore := float64(hotness.WriteCount)

	// 评分质量分数
	ratingScore := hotness.AvgRating / 5.0

	// 综合热度分数
	hotness.HotnessScore = writeScore * timeDecay * (0.7 + 0.3*ratingScore)
}

// GetHotMovies 获取热门电影列表
func (rts *RatingTrackerService) GetHotMovies(limit int) ([]*MovieHotness, error) {
	rts.mu.RLock()
	defer rts.mu.RUnlock()

	// 重新计算所有电影的热度分数
	for movieID := range rts.movieStats {
		rts.calculateHotnessScore(movieID)
	}

	// 转换为切片并排序
	var hotMovies []*MovieHotness
	for _, hotness := range rts.movieStats {
		// 获取电影标题
		if hotness.Title == "" {
			if title, err := rts.getMovieTitle(hotness.MovieID); err == nil {
				hotness.Title = title
			} else {
				hotness.Title = fmt.Sprintf("电影 %s", hotness.MovieID)
			}
		}
		hotMovies = append(hotMovies, hotness)
	}

	// 按热度分数排序
	sort.Slice(hotMovies, func(i, j int) bool {
		return hotMovies[i].HotnessScore > hotMovies[j].HotnessScore
	})

	// 限制返回数量
	if limit > 0 && len(hotMovies) > limit {
		hotMovies = hotMovies[:limit]
	}

	return hotMovies, nil
}

// GetMovieHotness 获取指定电影的热度信息
func (rts *RatingTrackerService) GetMovieHotness(movieID string) (*MovieHotness, error) {
	rts.mu.RLock()
	defer rts.mu.RUnlock()

	if hotness, exists := rts.movieStats[movieID]; exists {
		// 重新计算热度分数
		rts.calculateHotnessScore(movieID)

		// 获取电影标题
		if hotness.Title == "" {
			if title, err := rts.getMovieTitle(movieID); err == nil {
				hotness.Title = title
			} else {
				hotness.Title = fmt.Sprintf("电影 %s", movieID)
			}
		}

		return hotness, nil
	}

	return nil, fmt.Errorf("电影 %s 没有热度数据", movieID)
}

// GetRecentWrites 获取最近的写入记录
func (rts *RatingTrackerService) GetRecentWrites(limit int) []RatingWriteRecord {
	rts.mu.RLock()
	defer rts.mu.RUnlock()

	records := rts.writeRecords
	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}

	// 返回副本，避免并发问题
	result := make([]RatingWriteRecord, len(records))
	copy(result, records)

	return result
}

// GetWriteStats 获取写入统计信息
func (rts *RatingTrackerService) GetWriteStats() map[string]interface{} {
	rts.mu.RLock()
	defer rts.mu.RUnlock()

	now := time.Now()

	// 统计最近1小时、24小时的写入
	var lastHour, lastDay int
	for _, record := range rts.writeRecords {
		if now.Sub(record.Timestamp).Hours() <= 1 {
			lastHour++
		}
		if now.Sub(record.Timestamp).Hours() <= 24 {
			lastDay++
		}
	}

	// 按来源统计
	sourceStats := make(map[string]int)
	for _, record := range rts.writeRecords {
		sourceStats[record.Source]++
	}

	return map[string]interface{}{
		"totalWrites": len(rts.writeRecords),
		"totalMovies": len(rts.movieStats),
		"lastHour":    lastHour,
		"lastDay":     lastDay,
		"sourceStats": sourceStats,
	}
}

// getMovieTitle 获取电影标题
func (rts *RatingTrackerService) getMovieTitle(movieID string) (string, error) {
	ctx := context.Background()

	// 从HBase获取电影信息
	data, err := utils.GetMovie(ctx, movieID)
	if err != nil {
		return "", err
	}

	if data == nil {
		return "", fmt.Errorf("电影不存在")
	}

	// 解析电影数据
	movieData := utils.ParseMovieData(movieID, data)
	if title, ok := movieData["title"].(string); ok {
		return title, nil
	}

	return "", fmt.Errorf("无法获取电影标题")
}

// WriteRatingToHBase 写入评分到HBase并记录追踪信息（通用函数）
func (rts *RatingTrackerService) WriteRatingToHBase(ctx context.Context, movieID, userID string, rating float64, source string) error {
	// 获取HBase客户端
	client := utils.GetClient().(interface {
		Put(request *hrpc.Mutate) (*hrpc.Result, error)
	})

	// 生成时间戳
	timestamp := time.Now().Unix()

	// 构建评分数据值: "{rating}:{userId}:{timestamp}"
	ratingValue := fmt.Sprintf("%.1f:%s:%d", rating, userID, timestamp)

	// 构建行键: "{movieId}_ratings"
	rowKey := fmt.Sprintf("%s_ratings", movieID)

	// 创建Put请求
	putRequest, err := hrpc.NewPutStr(ctx, "movies", rowKey, map[string]map[string][]byte{
		"ratings": {
			userID: []byte(ratingValue),
		},
	})

	if err != nil {
		return fmt.Errorf("创建Put请求失败: %v", err)
	}

	// 执行Put操作
	_, err = client.Put(putRequest)
	if err != nil {
		return fmt.Errorf("写入HBase失败: %v", err)
	}

	// 记录追踪信息
	rts.RecordRatingWrite(movieID, userID, rating, source)

	return nil
}

// GetMovieRatingThresholdStatus 获取电影评分阈值状态
func (rts *RatingTrackerService) GetMovieRatingThresholdStatus(movieID string) map[string]interface{} {
	rts.mu.RLock()
	defer rts.mu.RUnlock()

	if hotness, exists := rts.movieStats[movieID]; exists {
		threshold := int(float64(hotness.LastRatingCount) * 0.1)
		if threshold < 1 {
			threshold = 1
		}

		return map[string]interface{}{
			"movieId":              movieID,
			"lastRatingCount":      hotness.LastRatingCount,
			"newWritesSinceCalc":   hotness.NewWritesSinceCalc,
			"threshold":            threshold,
			"thresholdPercentage": "10%",
			"needsRecalculation":   hotness.NewWritesSinceCalc >= threshold,
			"progress":             fmt.Sprintf("%d/%d", hotness.NewWritesSinceCalc, threshold),
		}
	}

	return map[string]interface{}{
		"movieId": movieID,
		"error":   "电影没有评分追踪数据",
	}
}

// 全局实例
var GlobalRatingTracker = NewRatingTrackerService()
