package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"
)

// GetMovieByID 根据ID获取电影（带缓存）
func GetMovieByID(movieID string) (*MovieDetail, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("movie_detail:%s", movieID)

	// 检查缓存
	if cachedData, found := utils.Cache.Get(cacheKey); found {
		return cachedData.(*MovieDetail), nil
	}

	ctx := context.Background()

	// 从HBase获取电影数据
	data, err := utils.GetMovie(ctx, movieID)
	if err != nil {
		return nil, err
	}

	// 如果电影不存在
	if data == nil {
		return nil, nil
	}

	// 解析电影数据
	movieData := utils.ParseMovieData(movieID, data)

	// 构建电影详情响应
	detail := &MovieDetail{}

	// 设置基本信息
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

	// 使用 utils.GetMovieRatings 获取评分数据，与 /api/ratings/movie/:id 保持一致
	ratingData, err := utils.GetMovieRatings(ctx, movieID)
	if err == nil && ratingData != nil {
		if avgRating, ok := ratingData["avgRating"].(float64); ok {
			movie.AvgRating = avgRating
		}
	} else {
		movie.AvgRating = 0.0
	}

	// 设置链接（使用通用函数）
	if linksData, err := utils.GetMovieLinks(ctx, movieID); err == nil {
		linkObj := Links{}

		if imdbId, ok := linksData["imdbId"].(string); ok {
			linkObj.ImdbID = imdbId
		}
		if imdbUrl, ok := linksData["imdbUrl"].(string); ok {
			linkObj.ImdbURL = imdbUrl
		}
		if tmdbId, ok := linksData["tmdbId"].(string); ok {
			linkObj.TmdbID = tmdbId
		}
		if tmdbUrl, ok := linksData["tmdbUrl"].(string); ok {
			linkObj.TmdbURL = tmdbUrl
		}

		movie.Links = linkObj
	}

	// 设置标签（使用通用函数）
	var tagCount int
	if tagsData, err := utils.GetMovieTags(ctx, movieID); err == nil {
		if uniqueTags, ok := tagsData["uniqueTags"].([]string); ok {
			movie.Tags = uniqueTags
		}
		if taggedUsers, ok := tagsData["taggedUsers"].([]map[string]string); ok {
			detail.TaggedUsers = taggedUsers
		}
		if count, ok := tagsData["tagCount"].(int); ok {
			tagCount = count
		}
	}

	detail.Movie = movie

	// 获取评分计数，但不填充评分数组
	var ratingCount int
	if ratingData != nil {
		if count, ok := ratingData["count"].(int); ok {
			ratingCount = count
		}
	}

	// 构建统计数据
	detail.Stats = map[string]float64{
		"ratingCount": float64(ratingCount),
		"tagCount":    float64(tagCount),
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, detail)

	return detail, nil
}
