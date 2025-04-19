package utils

import (
	"context"
	"gohbase/config"
	"gohbase/utils/hbase"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var hbaseClient gohbase.Client

// InitHBase 初始化HBase客户端
func InitHBase(conf *config.HBaseConfig) error {
	return hbase.InitHBase(conf)
}

// GetClient 获取HBase客户端
func GetClient() gohbase.Client {
	// 直接调用 hbase 包中的 GetClient 函数
	return hbase.GetClient()
}

// GetMovie 根据ID获取电影信息
func GetMovie(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	return hbase.GetMovie(ctx, movieID)
}

// GetMovieWithFamilies 根据ID和指定的列族获取电影信息
func GetMovieWithFamilies(ctx context.Context, movieID string, families []string) (map[string]map[string][]byte, error) {
	return hbase.GetMovieWithFamilies(ctx, movieID, families)
}

// GetMoviesMultiple 根据多个ID获取电影信息
func GetMoviesMultiple(ctx context.Context, movieIDs []string) (map[string]map[string]map[string][]byte, error) {
	return hbase.GetMoviesMultiple(ctx, movieIDs)
}

// ParseMovieData 从HBase结果解析电影数据
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	return hbase.ParseMovieData(movieID, data)
}

// ScanMovies 扫描电影列表（带缓存）
func ScanMovies(ctx context.Context, startRow, endRow string, limit int64) ([]*hrpc.Result, error) {
	return hbase.ScanMovies(ctx, startRow, endRow, limit)
}

// ScanMoviesWithFamilies 带特定列族的电影列表扫描
func ScanMoviesWithFamilies(ctx context.Context, startRow, endRow string, families []string, limit int64) ([]*hrpc.Result, error) {
	return hbase.ScanMoviesWithFamilies(ctx, startRow, endRow, families, limit)
}

// ScanMoviesByGenre 按类型扫描电影
func ScanMoviesByGenre(ctx context.Context, genre string, limit int64) ([]*hrpc.Result, error) {
	return hbase.ScanMoviesByGenre(ctx, genre, limit)
}

// ScanMoviesByTag 按标签扫描电影
func ScanMoviesByTag(ctx context.Context, tag string, limit int64) ([]*hrpc.Result, error) {
	return hbase.ScanMoviesByTag(ctx, tag, limit)
}

// ScanMoviesWithPagination 扫描电影列表并支持分页
func ScanMoviesWithPagination(ctx context.Context, page, pageSize int) ([]*hrpc.Result, int, error) {
	return hbase.ScanMoviesWithPagination(ctx, page, pageSize)
}

// GetMovieRatingStats 获取电影评分统计信息
func GetMovieRatingStats(ctx context.Context, movieID string) (map[string]float64, error) {
	return hbase.GetMovieRatingStats(ctx, movieID)
}

// GetMoviesByRatingRange 获取特定评分范围内的电影
func GetMoviesByRatingRange(ctx context.Context, minRating, maxRating float64, limit int64) ([]string, error) {
	// 由于HBase不支持直接的数值范围查询，我们需要扫描所有电影并在应用层过滤
	// 注意：这种方法在数据量大时效率较低，实际应用中应考虑建立二级索引或使用其他辅助表

	// 创建扫描请求，只获取评分列族
	scan, err := hrpc.NewScanStr(ctx, "moviedata",
		hrpc.Families(map[string][]string{"rating": nil}))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)

	// 存储满足条件的电影ID
	var matchedMovieIDs []string

	// 扫描所有电影
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		if len(res.Cells) == 0 {
			continue
		}

		// 获取电影ID
		movieID := string(res.Cells[0].Row)

		// 解析评分数据
		var sumRating float64
		var count int

		// 遍历所有单元格
		for _, cell := range res.Cells {
			qualifier := string(cell.Qualifier)

			// 只处理评分列，不处理时间戳列
			if strings.HasPrefix(qualifier, "rating:") {
				rating, err := strconv.ParseFloat(string(cell.Value), 64)
				if err == nil {
					sumRating += rating
					count++
				}
			}
		}

		// 计算平均评分
		var avgRating float64
		if count > 0 {
			avgRating = sumRating / float64(count)

			// 检查评分是否在范围内
			if avgRating >= minRating && avgRating <= maxRating {
				matchedMovieIDs = append(matchedMovieIDs, movieID)

				// 如果达到限制数量，则停止扫描
				if int64(len(matchedMovieIDs)) >= limit {
					break
				}
			}
		}
	}

	return matchedMovieIDs, nil
}

// GetMovieWithAllData 获取电影的所有数据，包括基本信息、链接、评分和标签
func GetMovieWithAllData(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieWithAllData(ctx, movieID)
}

// EnableCompression 为表启用压缩功能
func EnableCompression(compression string) error {
	return hbase.EnableCompression(compression)
}

// GetMovieRatings 获取电影的所有评分
func GetMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieRatings(ctx, movieID)
}

// GetMovieTags 获取电影的所有标签
func GetMovieTags(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	return hbase.GetMovieTags(ctx, movieID)
}

// GetUserRating 获取特定用户对电影的评分
func GetUserRating(ctx context.Context, movieID string, userID string) (float64, int64, error) {
	return hbase.GetUserRating(ctx, movieID, userID)
}

// GetTotalMoviesCount 获取电影总数
func GetTotalMoviesCount(ctx context.Context) (int, error) {
	// 使用缓存优化性能
	cacheKey := "total_movies_count"
	if cachedCount, found := Cache.Get(cacheKey); found {
		return cachedCount.(int), nil
	}

	// 构建扫描请求，只获取行键以提高效率
	scanRequest, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return 0, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	count := 0

	// 计算总行数
	for {
		_, err := scanner.Next()
		if err != nil {
			break
		}
		count++
	}

	// 将结果存入缓存（24小时有效）
	Cache.Set(cacheKey, count)

	return count, nil
}

// GetMoviesRatingsBatch 批量获取多部电影的评分信息
func GetMoviesRatingsBatch(ctx context.Context, movieIDs []string) (map[string]map[string]interface{}, error) {
	return hbase.GetMoviesRatingsBatch(ctx, movieIDs)
}
