package utils

import (
	"context"
	"gohbase/config"
	"gohbase/utils/hbase"

	"github.com/tsuna/gohbase/hrpc"
)

// InitHBase 初始化HBase客户端
func InitHBase(conf *config.HBaseConfig) error {
	return hbase.InitHBase(conf)
}

// GetMovie 根据ID获取电影信息
func GetMovie(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	return hbase.GetMovie(ctx, movieID)
}

// ParseMovieData 从HBase结果解析电影数据
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	return hbase.ParseMovieData(movieID, data)
}

// ScanMovies 扫描电影列表（带缓存）
func ScanMovies(ctx context.Context, startRow, endRow string, limit int64) ([]*hrpc.Result, error) {
	return hbase.ScanMovies(ctx, startRow, endRow, limit)
}

// ScanMoviesWithPagination 扫描电影列表并支持分页
func ScanMoviesWithPagination(ctx context.Context, page, pageSize int) ([]*hrpc.Result, int, error) {
	return hbase.ScanMoviesWithPagination(ctx, page, pageSize)
}

// GetMovieWithAllData 获取电影的所有数据
func GetMovieWithAllData(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieWithAllData(ctx, movieID)
}

// GetMovieRatings 获取电影的所有评分
func GetMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieRatings(ctx, movieID)
}

// GetTotalMoviesCount 获取电影总数（带缓存）
func GetTotalMoviesCount(ctx context.Context) (int, error) {
	cacheKey := "total_movies_count"
	if cachedCount, found := Cache.Get(cacheKey); found {
		return cachedCount.(int), nil
	}

	// 使用 ScanMoviesWithPagination 获取总数
	_, totalCount, err := hbase.ScanMoviesWithPagination(ctx, 1, 1)
	if err != nil {
		return 0, err
	}

	Cache.Set(cacheKey, totalCount)
	return totalCount, nil
}

// GetClient 获取HBase客户端
func GetClient() interface{} {
	return hbase.GetClient()
}

// GetMoviesRatingsBatch 批量获取多部电影的评分信息
func GetMoviesRatingsBatch(ctx context.Context, movieIDs []string) (map[string]map[string]interface{}, error) {
	return hbase.GetMoviesRatingsBatch(ctx, movieIDs)
}

// GetMovieLinks 获取电影外部链接（通用函数）
func GetMovieLinks(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieLinksWithUrls(ctx, movieID)
}

// GetMovieTags 获取电影标签（通用函数）
func GetMovieTags(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieTagsWithDetails(ctx, movieID)
}

// GetMovieStats 获取电影统计信息
func GetMovieStats(ctx context.Context, movieID string) (map[string]interface{}, error) {
	return hbase.GetMovieStats(ctx, movieID)
}
