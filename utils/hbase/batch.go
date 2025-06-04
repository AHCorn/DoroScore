package hbase

import (
	"context"
	"fmt"
)

// GetMoviesMultiple 根据多个ID获取电影信息
func GetMoviesMultiple(ctx context.Context, movieIDs []string) (map[string]map[string]map[string][]byte, error) {
	results := make(map[string]map[string]map[string][]byte)

	// 使用goroutine并发获取多部电影信息
	type result struct {
		id   string
		data map[string]map[string][]byte
		err  error
	}

	resultChan := make(chan result, len(movieIDs))

	for _, id := range movieIDs {
		go func(movieID string) {
			data, err := GetMovie(ctx, movieID)
			resultChan <- result{id: movieID, data: data, err: err}
		}(id)
	}

	// 收集结果
	for range movieIDs {
		res := <-resultChan
		if res.err == nil && res.data != nil {
			results[res.id] = res.data
		}
	}

	return results, nil
}

// GetMovieRatingStats 获取电影评分统计（使用新的数据库结构）
func GetMovieRatingStats(ctx context.Context, movieID string) (map[string]float64, error) {
	// 先尝试从stats行获取预计算的统计信息
	statsData, err := GetMovieStats(ctx, movieID)
	if err == nil && len(statsData) > 0 {
		result := make(map[string]float64)

		if avgRating, ok := statsData["avgRating"].(float64); ok {
			result["avgRating"] = avgRating
		}
		if ratingCount, ok := statsData["ratingCount"].(int); ok {
			result["count"] = float64(ratingCount)
		}

		// 如果有预计算的统计信息，直接返回
		if len(result) > 0 {
			// 设置默认的min和max值
			result["minRating"] = 0.5 // MovieLens最小评分
			result["maxRating"] = 5.0 // MovieLens最大评分
			return result, nil
		}
	}

	// 如果没有预计算的统计信息，从ratings行实时计算
	ratingsData, err := GetMovieRatings(ctx, movieID)
	if err != nil {
		return nil, err
	}

	result := make(map[string]float64)

	if avgRating, ok := ratingsData["avgRating"].(float64); ok {
		result["avgRating"] = avgRating
	}
	if minRating, ok := ratingsData["minRating"].(float64); ok {
		result["minRating"] = minRating
	}
	if maxRating, ok := ratingsData["maxRating"].(float64); ok {
		result["maxRating"] = maxRating
	}
	if count, ok := ratingsData["count"].(int); ok {
		result["count"] = float64(count)
	}

	return result, nil
}

// GetMoviesRatingsBatch 批量获取多部电影的评分信息
func GetMoviesRatingsBatch(ctx context.Context, movieIDs []string) (map[string]map[string]interface{}, error) {
	results := make(map[string]map[string]interface{})

	// 使用goroutine并发获取多部电影评分
	type result struct {
		id   string
		data map[string]interface{}
		err  error
	}

	resultChan := make(chan result, len(movieIDs))

	for _, id := range movieIDs {
		go func(movieID string) {
			// 先尝试从stats获取预计算的评分
			statsData, err := GetMovieStats(ctx, movieID)
			if err == nil && len(statsData) > 0 {
				if avgRating, ok := statsData["avgRating"].(float64); ok {
					data := map[string]interface{}{
						"avgRating": avgRating,
					}
					if ratingCount, ok := statsData["ratingCount"].(int); ok {
						data["count"] = ratingCount
					}
					resultChan <- result{id: movieID, data: data, err: nil}
					return
				}
			}

			// 如果没有预计算的统计信息，从ratings行获取
			data, err := GetMovieRatings(ctx, movieID)
			resultChan <- result{id: movieID, data: data, err: err}
		}(id)
	}

	// 收集结果
	for range movieIDs {
		res := <-resultChan
		if res.err == nil && res.data != nil {
			results[res.id] = res.data
		} else {
			// 为出错的电影提供默认值
			results[res.id] = map[string]interface{}{
				"avgRating": 0.0,
				"count":     0,
			}
		}
	}

	return results, nil
}

// GetMoviesWithAllDataBatch 批量获取多部电影的完整信息
func GetMoviesWithAllDataBatch(ctx context.Context, movieIDs []string) (map[string]map[string]interface{}, error) {
	results := make(map[string]map[string]interface{})

	// 使用goroutine并发获取多部电影的完整信息
	type result struct {
		id   string
		data map[string]interface{}
		err  error
	}

	resultChan := make(chan result, len(movieIDs))

	for _, id := range movieIDs {
		go func(movieID string) {
			data, err := GetMovieWithAllData(ctx, movieID)
			resultChan <- result{id: movieID, data: data, err: err}
		}(id)
	}

	// 收集结果
	for range movieIDs {
		res := <-resultChan
		if res.err == nil && res.data != nil {
			results[res.id] = res.data
		}
	}

	return results, nil
}

// 解析浮点数，出错时返回默认值
func parseFloat(s string, defaultValue float64) float64 {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return defaultValue
	}
	return v
}
