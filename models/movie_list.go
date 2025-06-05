package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// GetTotalMoviesCount 获取电影总数
func GetTotalMoviesCount(ctx context.Context) (int, error) {
	// 使用缓存优化性能
	cacheKey := "total_movies_count"
	if cachedCount, found := utils.Cache.Get(cacheKey); found {
		return cachedCount.(int), nil
	}

	// 使用 ScanMoviesWithPagination，它会返回总数，而不直接使用客户端
	_, totalCount, err := utils.ScanMoviesWithPagination(ctx, 1, 1)
	if err != nil {
		return 0, err // 出错时返回0和错误，而不是硬编码值
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, totalCount)

	return totalCount, nil
}

// GetMoviesList 获取电影列表（适配新的数据库结构）
func GetMoviesList(page, perPage int) (*MovieList, error) {
	ctx := context.Background()

	// 获取总电影数
	totalMovies, err := GetTotalMoviesCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取电影总数失败: %w", err)
	}

	// 使用分页扫描获取电影列表
	results, actualTotal, err := utils.ScanMoviesWithPagination(ctx, page, perPage)
	if err != nil {
		return nil, err
	}

	// 更新总数（如果实际扫描得到的总数不同）
	if actualTotal != totalMovies {
		totalMovies = actualTotal
		utils.Cache.Set("total_movies_count", totalMovies)
	}

	// 解析电影列表 - 改进为同时读取stats数据
	movies := []Movie{}

	for _, result := range results {
		// 获取行键
		if len(result.Cells) == 0 {
			continue
		}

		rowKey := string(result.Cells[0].Row)

		// 确保是_info行，提取电影ID
		if !strings.HasSuffix(rowKey, "_info") {
			continue
		}

		movieID := strings.TrimSuffix(rowKey, "_info")

		// 手动构建结果映射
		resultMap := make(map[string]map[string][]byte)
		infoFamily := make(map[string][]byte)

		// 处理_info行数据
		for _, cell := range result.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if family == "info" {
				infoFamily[qualifier] = cell.Value
			}
		}

		// 尝试读取stats数据
		statsGet, err := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_stats", movieID))
		if err == nil {
			client := utils.GetClient().(interface {
				Get(request *hrpc.Get) (*hrpc.Result, error)
			})

			if statsResult, err := client.Get(statsGet); err == nil && len(statsResult.Cells) > 0 {
				// 将stats数据合并到infoFamily中
				for _, cell := range statsResult.Cells {
					family := string(cell.Family)
					qualifier := string(cell.Qualifier)

					if family == "info" {
						infoFamily[qualifier] = cell.Value
					}
				}
			}
		}

		resultMap["info"] = infoFamily
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
				fmt.Printf("✅ 电影列表: 成功计算并存储电影 %s 的平均评分: %.2f (基于 %d 个评分)\n",
					movieID, avgRating, ratingCount)
			}
		}

		// 添加链接数据
		if links, ok := movieData["links"].(map[string]interface{}); ok {
			linkObj := Links{}

			if imdbId, ok := links["imdbId"].(string); ok {
				linkObj.ImdbID = imdbId
			}
			if imdbUrl, ok := links["imdbUrl"].(string); ok {
				linkObj.ImdbURL = imdbUrl
			}
			if tmdbId, ok := links["tmdbId"].(string); ok {
				linkObj.TmdbID = tmdbId
			}
			if tmdbUrl, ok := links["tmdbUrl"].(string); ok {
				linkObj.TmdbURL = tmdbUrl
			}

			movie.Links = linkObj
		}

		// 添加标签数据
		if uniqueTags, ok := movieData["uniqueTags"].([]string); ok {
			movie.Tags = uniqueTags
		}

		movies = append(movies, movie)
	}

	// 构建响应
	totalPages := (totalMovies + perPage - 1) / perPage // 计算总页数

	return &MovieList{
		Movies:      movies,
		TotalMovies: totalMovies,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}, nil
}
