package controllers

import (
	"gohbase/services"
	"gohbase/utils"
	"strconv"

	"github.com/gin-gonic/gin"
)

// HotnessController 热度控制器
type HotnessController struct{}

// NewHotnessController 创建热度控制器
func NewHotnessController() *HotnessController {
	return &HotnessController{}
}

// GetHotMovies 获取热门电影列表
func (hc *HotnessController) GetHotMovies(c *gin.Context) {
	// 获取限制数量，默认20部
	limitStr := c.DefaultQuery("limit", "20")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100 // 最大100部
	}

	// 获取热门电影
	hotMovies, err := services.GlobalRatingTracker.GetHotMovies(limit)
	if err != nil {
		utils.InternalError(c, "获取热门电影失败", err)
		return
	}

	utils.SuccessData(c, gin.H{
		"status": "success",
		"data": gin.H{
			"hotMovies": hotMovies,
			"count":     len(hotMovies),
			"limit":     limit,
		},
		"message": "获取热门电影成功",
	})
}

// GetMovieHotness 获取指定电影的热度信息
func (hc *HotnessController) GetMovieHotness(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		utils.BadRequest(c, "电影ID不能为空")
		return
	}

	// 获取电影热度信息
	hotness, err := services.GlobalRatingTracker.GetMovieHotness(movieID)
	if err != nil {
		utils.NotFound(c, err.Error())
		return
	}

	utils.SuccessData(c, gin.H{
		"status":  "success",
		"data":    hotness,
		"message": "获取电影热度信息成功",
	})
}

// GetWriteStats 获取写入统计信息
func (hc *HotnessController) GetWriteStats(c *gin.Context) {
	stats := services.GlobalRatingTracker.GetWriteStats()

	utils.SuccessData(c, gin.H{
		"status":  "success",
		"data":    stats,
		"message": "获取写入统计信息成功",
	})
}

// GetRecentWrites 获取最近的写入记录
func (hc *HotnessController) GetRecentWrites(c *gin.Context) {
	// 获取限制数量，默认50条
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500 // 最大500条
	}

	// 获取最近写入记录
	records := services.GlobalRatingTracker.GetRecentWrites(limit)

	utils.SuccessData(c, gin.H{
		"status": "success",
		"data": gin.H{
			"records": records,
			"count":   len(records),
			"limit":   limit,
		},
		"message": "获取最近写入记录成功",
	})
}

// GetHotnessRanking 获取热度排行榜
func (hc *HotnessController) GetHotnessRanking(c *gin.Context) {
	// 获取排行榜类型，默认为综合热度
	rankType := c.DefaultQuery("type", "hotness")

	// 获取限制数量，默认50部
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	// 获取热门电影
	hotMovies, err := services.GlobalRatingTracker.GetHotMovies(limit)
	if err != nil {
		utils.InternalError(c, "获取热度排行榜失败", err)
		return
	}

	// 根据类型重新排序
	switch rankType {
	case "writeCount":
		// 按写入次数排序
		for i := 0; i < len(hotMovies)-1; i++ {
			for j := i + 1; j < len(hotMovies); j++ {
				if hotMovies[i].WriteCount < hotMovies[j].WriteCount {
					hotMovies[i], hotMovies[j] = hotMovies[j], hotMovies[i]
				}
			}
		}
	case "avgRating":
		// 按平均评分排序
		for i := 0; i < len(hotMovies)-1; i++ {
			for j := i + 1; j < len(hotMovies); j++ {
				if hotMovies[i].AvgRating < hotMovies[j].AvgRating {
					hotMovies[i], hotMovies[j] = hotMovies[j], hotMovies[i]
				}
			}
		}
	case "recent":
		// 按最近写入时间排序
		for i := 0; i < len(hotMovies)-1; i++ {
			for j := i + 1; j < len(hotMovies); j++ {
				if hotMovies[i].LastWrite.Before(hotMovies[j].LastWrite) {
					hotMovies[i], hotMovies[j] = hotMovies[j], hotMovies[i]
				}
			}
		}
	default:
		// 默认按热度分数排序（已经排序好了）
	}

	utils.SuccessData(c, gin.H{
		"status": "success",
		"data": gin.H{
			"ranking": hotMovies,
			"count":   len(hotMovies),
			"type":    rankType,
			"limit":   limit,
		},
		"message": "获取热度排行榜成功",
	})
}

// GetHotnessTrends 获取热度趋势（简化版）
func (hc *HotnessController) GetHotnessTrends(c *gin.Context) {
	// 获取最近的写入记录
	records := services.GlobalRatingTracker.GetRecentWrites(1000)

	// 按小时统计写入趋势
	hourlyStats := make(map[int]int)
	movieHourlyStats := make(map[string]map[int]int)

	for _, record := range records {
		hour := record.Timestamp.Hour()
		hourlyStats[hour]++

		if movieHourlyStats[record.MovieID] == nil {
			movieHourlyStats[record.MovieID] = make(map[int]int)
		}
		movieHourlyStats[record.MovieID][hour]++
	}

	// 找出最活跃的时间段
	var peakHour int
	var maxWrites int
	for hour, count := range hourlyStats {
		if count > maxWrites {
			maxWrites = count
			peakHour = hour
		}
	}

	utils.SuccessData(c, gin.H{
		"status": "success",
		"data": gin.H{
			"hourlyStats":    hourlyStats,
			"peakHour":       peakHour,
			"peakHourWrites": maxWrites,
			"totalRecords":   len(records),
		},
		"message": "获取热度趋势成功",
	})
}
