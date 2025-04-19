package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// SearchMovies 搜索电影（带个缓存🚀）
func SearchMovies(query string, page, perPage int) (*MovieList, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("search:%s:%d:%d", query, page, perPage)

	// 检查缓存
	if cachedResults, found := utils.Cache.Get(cacheKey); found {
		return cachedResults.(*MovieList), nil
	}

	ctx := context.Background()

	// 创建全表扫描
	scan, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return nil, err
	}

	scanner := utils.GetClient().Scan(scan)
	matchedMovies := []Movie{}
	matchedMovieIDs := []string{} // 用于收集匹配的电影ID，后续批量获取评分

	// 将查询转为小写以进行不区分大小写的匹配
	queryLower := strings.ToLower(query)

	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾
		}

		// 获取行键（movieId）
		var movieID string
		for _, cell := range res.Cells {
			movieID = string(cell.Row)
			break
		}

		if movieID == "" {
			continue
		}

		// 手动构建结果映射
		resultMap := make(map[string]map[string][]byte)
		for _, cell := range res.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if _, ok := resultMap[family]; !ok {
				resultMap[family] = make(map[string][]byte)
			}

			resultMap[family][qualifier] = cell.Value
		}

		movieData := utils.ParseMovieData(movieID, resultMap)

		// 检查标题是否匹配
		if title, ok := movieData["title"].(string); ok {
			if strings.Contains(strings.ToLower(title), queryLower) {
				movie := Movie{
					MovieID: movieID,
					Title:   title,
				}

				// 尝试从标题中提取年份
				if matches := strings.Split(title, " ("); len(matches) > 1 {
					yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
					if year, err := strconv.Atoi(yearStr); err == nil {
						movie.Year = year
					}
				}

				if genres, ok := movieData["genres"].([]string); ok {
					movie.Genres = genres
				}

				// 不再在这里获取评分，仅使用默认值或保存在movieData中的值
				if avgRating, ok := movieData["avgRating"].(float64); ok {
					movie.AvgRating = avgRating
				} else {
					movie.AvgRating = 0.0
				}

				// 添加标签
				if tags, ok := movieData["uniqueTags"].([]string); ok {
					movie.Tags = tags
				}

				matchedMovies = append(matchedMovies, movie)
				matchedMovieIDs = append(matchedMovieIDs, movieID)
				continue
			}
		}

		// 检查类型是否匹配
		if genres, ok := movieData["genres"].([]string); ok {
			for _, genre := range genres {
				if strings.Contains(strings.ToLower(genre), queryLower) {
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

					movie.Genres = genres

					// 不再在这里获取评分，仅使用默认值或保存在movieData中的值
					if avgRating, ok := movieData["avgRating"].(float64); ok {
						movie.AvgRating = avgRating
					} else {
						movie.AvgRating = 0.0
					}

					// 添加标签
					if tags, ok := movieData["uniqueTags"].([]string); ok {
						movie.Tags = tags
					}

					matchedMovies = append(matchedMovies, movie)
					matchedMovieIDs = append(matchedMovieIDs, movieID)
					break
				}
			}
		}
	}

	// 如果有匹配结果，批量获取评分数据
	if len(matchedMovieIDs) > 0 {
		ratingsMap, err := utils.GetMoviesRatingsBatch(ctx, matchedMovieIDs)
		if err == nil {
			// 使用批量获取的评分数据更新电影评分
			for i := range matchedMovies {
				movieID := matchedMovies[i].MovieID
				if rating, ok := ratingsMap[movieID]; ok {
					if avgRating, ok := rating["avgRating"].(float64); ok {
						matchedMovies[i].AvgRating = avgRating
					}
				}
			}
		}
	}

	// 计算分页
	totalMatches := len(matchedMovies)
	totalPages := (totalMatches + perPage - 1) / perPage

	startIdx := (page - 1) * perPage
	endIdx := startIdx + perPage
	if endIdx > totalMatches {
		endIdx = totalMatches
	}

	// 如果没有匹配项
	if startIdx >= totalMatches {
		result := &MovieList{
			Movies:      []Movie{},
			TotalMovies: totalMatches,
			Page:        page,
			PerPage:     perPage,
			TotalPages:  totalPages,
		}

		// 缓存搜索结果
		utils.Cache.Set(cacheKey, result)

		return result, nil
	}

	// 构建结果
	result := &MovieList{
		Movies:      matchedMovies[startIdx:endIdx],
		TotalMovies: totalMatches,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}

	// 缓存搜索结果
	utils.Cache.Set(cacheKey, result)

	return result, nil
}
