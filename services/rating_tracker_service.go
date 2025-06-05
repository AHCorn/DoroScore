package services

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"gohbase/utils"

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
		// 更新平均评分（简单移动平均）
		hotness.AvgRating = (hotness.AvgRating + rating) / 2
	} else {
		rts.movieStats[movieID] = &MovieHotness{
			MovieID:    movieID,
			WriteCount: 1,
			LastWrite:  now,
			AvgRating:  rating,
		}
	}

	// 重新计算热度分数
	rts.calculateHotnessScore(movieID)
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

// 全局实例
var GlobalRatingTracker = NewRatingTrackerService()
