package hbase

import (
	"context"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// GetMovie 根据ID获取电影信息
func GetMovie(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	get, err := hrpc.NewGetStr(ctx, "moviedata", movieID)
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到电影
	if result.Cells == nil || len(result.Cells) == 0 {
		return nil, nil
	}

	// 手动构建结果映射
	resultMap := make(map[string]map[string][]byte)

	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	return resultMap, nil
}

// GetMovieWithFamilies 根据ID和指定的列族获取电影信息
func GetMovieWithFamilies(ctx context.Context, movieID string, families []string) (map[string]map[string][]byte, error) {
	// 构建列族映射
	familiesMap := make(map[string][]string)
	for _, family := range families {
		familiesMap[family] = nil
	}

	// 创建Get请求并指定列族
	get, err := hrpc.NewGetStr(ctx, "moviedata", movieID, hrpc.Families(familiesMap))
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到电影
	if result.Cells == nil || len(result.Cells) == 0 {
		return nil, nil
	}

	// 手动构建结果映射
	resultMap := make(map[string]map[string][]byte)

	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	return resultMap, nil
}

// GetMovieWithAllData 获取电影的所有信息
func GetMovieWithAllData(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 从HBase获取电影数据
	data, err := GetMovie(ctx, movieID)
	if err != nil {
		return nil, err
	}

	// 如果电影不存在
	if data == nil {
		return nil, nil
	}

	// 解析电影数据
	movieData := ParseMovieData(movieID, data)
	return movieData, nil
}

// GetMovieRatings 获取电影评分
func GetMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 创建scan请求，使用行键前缀匹配查找指定movieID的所有评分
	// 由于现在使用的是复合键movieId_userId，需要使用前缀扫描
	startRow := movieID + "_" // 起始行：movieId_
	endRow := movieID + "`"   // 结束行：使用比"_"大的字符，确保扫描所有以movieId_开头的行

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, endRow,
		hrpc.Families(map[string][]string{"rating": nil}))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scanRequest)

	// 用于存储评分数据的数组
	var ratings []float64
	ratingsData := make([]map[string]interface{}, 0)

	// 扫描所有结果
	for {
		result, err := scanner.Next()
		if err != nil {
			break // 扫描结束或发生错误
		}

		// 跳过不匹配的结果
		if len(result.Cells) == 0 {
			continue
		}

		// 解析复合键中的userId
		rowID := string(result.Cells[0].Row)
		parts := strings.Split(rowID, "_")
		if len(parts) != 2 {
			continue // 跳过不符合格式的行键
		}

		userId := parts[1]

		// 处理每个结果
		for _, cell := range result.Cells {
			// 过滤出rating列
			qualifier := string(cell.Qualifier)
			if string(cell.Family) == "rating" && qualifier == "rating" {
				// 获取评分值
				ratingValue, err := strconv.ParseFloat(string(cell.Value), 64)
				if err == nil {
					// 将评分添加到数组中
					ratings = append(ratings, ratingValue)

					// 构建评分数据，包含userId
					ratingInfo := map[string]interface{}{
						"userId": userId,
						"rating": ratingValue,
					}

					// 添加时间戳，需要确保正确处理指针类型
					if cell.Timestamp != nil {
						ratingInfo["timestamp"] = int64(*cell.Timestamp)
					}

					ratingsData = append(ratingsData, ratingInfo)
				}
			}
		}
	}

	// 如果没有找到评分
	if len(ratings) == 0 {
		return map[string]interface{}{
			"ratings":   []map[string]interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		}, nil
	}

	// 计算统计信息
	var sum, min, max float64
	count := len(ratings)
	min = ratings[0]
	max = ratings[0]
	sum = ratings[0]

	// 计算统计信息
	for i := 1; i < count; i++ {
		rating := ratings[i]
		sum += rating

		if rating < min {
			min = rating
		}
		if rating > max {
			max = rating
		}
	}

	// 计算平均评分
	avg := sum / float64(count)

	// 返回评分数据和统计信息
	return map[string]interface{}{
		"ratings":   ratingsData,
		"count":     count,
		"avgRating": avg,
		"minRating": min,
		"maxRating": max,
	}, nil
}

// GetMovieTags 获取电影标签
func GetMovieTags(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	// 使用行键前缀扫描来获取指定电影的所有标签
	startRow := movieID + "_" // 起始行: movieId_
	endRow := movieID + "`"   // 结束行: 确保扫描所有以movieId_开头的行

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, endRow,
		hrpc.Families(map[string][]string{"tag": nil}))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scanRequest)

	// 用于存储标签数据
	resultMap := make(map[string]map[string][]byte)
	resultMap["tag"] = make(map[string][]byte)

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
			if string(cell.Family) == "tag" {
				qualifier := string(cell.Qualifier)
				if qualifier == "tag" {
					// 提取用户ID
					rowID := string(cell.Row)
					parts := strings.Split(rowID, "_")
					if len(parts) == 2 {
						userId := parts[1]
						// 使用user_tag:userId形式作为键
						key := "user_tag:" + userId
						resultMap["tag"][key] = cell.Value
					}
				}
			}
		}
	}

	return resultMap, nil
}
