package hbase

import (
	"context"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// GetMovie 根据ID获取电影的基本信息
func GetMovie(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	// 根据新的数据库结构，获取电影的info数据
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_info")
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

// GetMovieWithAllData 获取电影的所有信息（包括评分、标签等）
func GetMovieWithAllData(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 使用scan获取电影的所有相关数据
	startRow := movieID + "_"
	endRow := movieID + "`"

	scan, err := hrpc.NewScanRangeStr(ctx, "movies", startRow, endRow)
	if err != nil {
		return nil, err
	}

	scanner := hbaseClient.Scan(scan)

	// 收集所有数据
	allData := make(map[string]map[string][]byte)

	for {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		if len(result.Cells) == 0 {
			continue
		}

		for _, cell := range result.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if _, ok := allData[family]; !ok {
				allData[family] = make(map[string][]byte)
			}

			allData[family][qualifier] = cell.Value
		}
	}

	if len(allData) == 0 {
		return nil, nil
	}

	// 解析电影数据
	movieData := ParseMovieData(movieID, allData)
	return movieData, nil
}

// GetMovieRatings 获取电影评分（使用新的宽列格式）
func GetMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的ratings行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_ratings")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到评分数据
	if result.Cells == nil || len(result.Cells) == 0 {
		return map[string]interface{}{
			"ratings":   []map[string]interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		}, nil
	}

	// 解析宽列格式的评分数据
	var ratings []float64
	ratingsData := make([]map[string]interface{}, 0)

	for _, cell := range result.Cells {
		if string(cell.Family) == "ratings" {
			userID := string(cell.Qualifier)
			// 解析评分数据格式: "{rating}:{userId}:{timestamp}"
			ratingStr := string(cell.Value)
			parts := strings.Split(ratingStr, ":")

			if len(parts) >= 3 {
				if ratingValue, err := strconv.ParseFloat(parts[0], 64); err == nil {
					ratings = append(ratings, ratingValue)

					ratingInfo := map[string]interface{}{
						"userId": userID,
						"rating": ratingValue,
					}

					// 添加时间戳
					if timestamp, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
						ratingInfo["timestamp"] = timestamp
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

// GetMovieTags 获取电影标签（使用新的宽列格式）
func GetMovieTags(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	// 获取电影的tags行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_tags")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到标签数据
	if result.Cells == nil || len(result.Cells) == 0 {
		return map[string]map[string][]byte{
			"tag": make(map[string][]byte),
		}, nil
	}

	// 构建结果映射
	resultMap := make(map[string]map[string][]byte)
	resultMap["tag"] = make(map[string][]byte)

	for _, cell := range result.Cells {
		if string(cell.Family) == "tags" {
			userID := string(cell.Qualifier)
			// 解析标签数据格式: "{tag}:{userId}:{timestamp}"
			tagData := string(cell.Value)

			// 使用user_tag:userId形式作为键
			key := "user_tag:" + userID
			resultMap["tag"][key] = []byte(tagData)
		}
	}

	return resultMap, nil
}

// GetMovieStats 获取电影统计信息
func GetMovieStats(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的stats行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_stats")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})

	if result.Cells != nil {
		for _, cell := range result.Cells {
			if string(cell.Family) == "info" {
				qualifier := string(cell.Qualifier)
				value := string(cell.Value)

				switch qualifier {
				case "avg_rating":
					if avgRating, err := strconv.ParseFloat(value, 64); err == nil {
						stats["avgRating"] = avgRating
					}
				case "rating_count":
					if count, err := strconv.Atoi(value); err == nil {
						stats["ratingCount"] = count
					}
				case "updated_time":
					if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
						stats["updatedTime"] = timestamp
					}
				}
			}
		}
	}

	return stats, nil
}

// GetMovieLinks 获取电影外部链接
func GetMovieLinks(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的links行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_links")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	links := make(map[string]interface{})

	if result.Cells != nil {
		for _, cell := range result.Cells {
			if string(cell.Family) == "info" {
				qualifier := string(cell.Qualifier)
				value := string(cell.Value)

				switch qualifier {
				case "imdbId":
					links["imdbId"] = value
				case "tmdbId":
					links["tmdbId"] = value
				}
			}
		}
	}

	return links, nil
}

// GetMovieGenome 获取电影基因分数（使用新的宽列格式）
func GetMovieGenome(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的genome行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_genome")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	genome := make(map[string]interface{})

	if result.Cells != nil {
		for _, cell := range result.Cells {
			if string(cell.Family) == "genome" {
				tagID := string(cell.Qualifier)
				if relevance, err := strconv.ParseFloat(string(cell.Value), 64); err == nil {
					genome[tagID] = relevance
				}
			}
		}
	}

	return genome, nil
}
