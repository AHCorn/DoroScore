package hbase

import (
	"context"
	"fmt"

	"github.com/tsuna/gohbase/hrpc"
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

// GetMovieRatingStats 获取电影评分统计
func GetMovieRatingStats(ctx context.Context, movieID string) (map[string]float64, error) {
	// 使用行键前缀扫描来获取指定电影的所有评分
	startRow := movieID + "_" // 起始行: movieId_
	endRow := movieID + "`"   // 结束行: 确保扫描所有以movieId_开头的行

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, endRow,
		hrpc.Families(map[string][]string{"rating": {"rating"}}))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scanRequest)

	// 统计变量
	var count, sum, min, max float64
	count = 0
	min = 5 // 初始化为最高分
	max = 0

	// 扫描所有结果
	for {
		result, err := scanner.Next()
		if err != nil {
			break // 扫描结束或发生错误
		}

		if len(result.Cells) == 0 {
			continue
		}

		// 处理每个结果
		for _, cell := range result.Cells {
			if string(cell.Family) == "rating" && string(cell.Qualifier) == "rating" {
				rating := parseFloat(string(cell.Value), 0)
				if rating > 0 {
					sum += rating
					count++

					if rating < min {
						min = rating
					}
					if rating > max {
						max = rating
					}
				}
			}
		}
	}

	// 计算平均分
	avgRating := 0.0
	if count > 0 {
		avgRating = sum / count
	}

	// 如果没有评分，设置最小最大值为0
	if count == 0 {
		min = 0
	}

	return map[string]float64{
		"avgRating": avgRating,
		"minRating": min,
		"maxRating": max,
		"count":     count,
	}, nil
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
