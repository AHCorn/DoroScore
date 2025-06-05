package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

// GetRandomMovies 获取随机电影（带缓存）- 适配新的数据库结构
func GetRandomMovies(count int) ([]Movie, error) {
	ctx := context.Background()

	// 获取总电影数
	totalMovies, err := GetTotalMoviesCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取电影总数失败: %w", err)
	}

	// 构建缓存键 - 使用当前时间的小时数作为缓存键，这样每小时刷新一次随机结果
	currentHour := time.Now().Hour()
	cacheKey := fmt.Sprintf("random_movies:%d:%d", count, currentHour)

	// 检查缓存中是否有随机电影数据
	if cachedMovies, found := utils.Cache.Get(cacheKey); found {
		return cachedMovies.([]Movie), nil
	}

	// 生成随机ID列表
	randomIDs := generateRandomIDs(totalMovies, count)
	movies := []Movie{}

	// 获取每个随机ID的电影信息
	for _, id := range randomIDs {
		movieID := fmt.Sprintf("%d", id)

		// 使用新的数据库结构获取电影信息 - 同时读取stats数据
		data, err := utils.GetMovie(ctx, movieID)
		if err != nil {
			continue
		}

		if data == nil {
			continue
		}

		// 尝试读取stats数据
		resultMap := data
		statsGet, err := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_stats", movieID))
		if err == nil {
			client := utils.GetClient().(interface {
				Get(request *hrpc.Get) (*hrpc.Result, error)
			})

			if statsResult, err := client.Get(statsGet); err == nil && len(statsResult.Cells) > 0 {
				// 将stats数据合并到resultMap中
				if resultMap["info"] == nil {
					resultMap["info"] = make(map[string][]byte)
				}
				for _, cell := range statsResult.Cells {
					family := string(cell.Family)
					qualifier := string(cell.Qualifier)

					if family == "info" {
						resultMap["info"][qualifier] = cell.Value
					}
				}
			}
		}

		movieData := utils.ParseMovieData(movieID, resultMap)

		movie := Movie{
			MovieID: movieID,
		}

		if title, ok := movieData["title"].(string); ok {
			movie.Title = title
			// 尝试从标题中提取年份
			if matches := strings.Split(title, " ("); len(matches) > 1 {
				yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
				if year, err := strconv.Atoi(yearStr); err == nil {
					movie.Year = year
				}
			}
		}

		if genres, ok := movieData["genres"].([]string); ok {
			movie.Genres = genres
		}

		if avgRating, ok := movieData["avgRating"].(float64); ok {
			movie.AvgRating = avgRating
		} else {
			// 如果没有评分数据，尝试计算并存储
			avgRating, ratingCount, err := CalculateAndStoreMovieAvgRating(ctx, movieID)
			if err == nil && avgRating > 0.0 {
				movie.AvgRating = avgRating
				fmt.Printf("✅ 随机电影: 成功计算并存储电影 %s 的平均评分: %.2f (基于 %d 个评分)\n",
					movieID, avgRating, ratingCount)
			}
		}

		// 添加标签
		if tags, ok := movieData["uniqueTags"].([]string); ok {
			movie.Tags = tags
		}

		movies = append(movies, movie)
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, movies)

	return movies, nil
}

// generateRandomIDs 生成不重复的随机ID列表
func generateRandomIDs(max, count int) []int {
	if count > max {
		count = max
	}

	// 使用map确保唯一性
	idMap := make(map[int]bool)
	for len(idMap) < count {
		id := rng.Intn(max) + 1 // 从1开始
		idMap[id] = true
	}

	// 转换为切片
	ids := make([]int, 0, len(idMap))
	for id := range idMap {
		ids = append(ids, id)
	}

	return ids
}
