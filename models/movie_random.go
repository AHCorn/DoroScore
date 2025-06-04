package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"
	"time"
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

		// 使用新的数据库结构获取电影信息
		data, err := utils.GetMovie(ctx, movieID)
		if err != nil {
			continue
		}

		if data == nil {
			continue
		}

		movieData := utils.ParseMovieData(movieID, data)

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
