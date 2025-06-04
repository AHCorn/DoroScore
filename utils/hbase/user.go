package hbase

import (
	"context"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// GetUserRating 获取用户对电影的评分（适配新的数据库结构）
func GetUserRating(ctx context.Context, movieID string, userID string) (float64, int64, error) {
	// 根据新的数据库结构，评分存储在{movieId}_ratings行的宽列中
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_ratings")
	if err != nil {
		return 0, 0, err
	}

	// 执行查询
	result, err := hbaseClient.Get(get)
	if err != nil {
		return 0, 0, err
	}

	// 如果没有找到数据
	if result.Cells == nil || len(result.Cells) == 0 {
		return 0, 0, nil
	}

	// 查找特定用户的评分
	for _, cell := range result.Cells {
		if string(cell.Family) == "ratings" && string(cell.Qualifier) == userID {
			// 解析评分数据格式: "{rating}:{userId}:{timestamp}"
			ratingStr := string(cell.Value)
			parts := strings.Split(ratingStr, ":")

			if len(parts) >= 3 {
				if rating, err := strconv.ParseFloat(parts[0], 64); err == nil {
					if timestamp, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
						return rating, timestamp, nil
					}
					return rating, 0, nil
				}
			}
		}
	}

	return 0, 0, nil
}

// GetUserMovieRatings 获取用户的所有电影评分（使用users表）
func GetUserMovieRatings(ctx context.Context, userID string) (map[string]interface{}, error) {
	// 根据新的数据库结构，从users表获取用户的所有评分
	get, err := hrpc.NewGetStr(ctx, "users", userID)
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	if result.Cells == nil || len(result.Cells) == 0 {
		return map[string]interface{}{
			"ratings": []map[string]interface{}{},
			"count":   0,
		}, nil
	}

	var ratings []map[string]interface{}

	for _, cell := range result.Cells {
		if string(cell.Family) == "movies" {
			movieID := string(cell.Qualifier)
			// 解析评分数据格式: "{rating}:{movieId}:{timestamp}"
			ratingStr := string(cell.Value)
			parts := strings.Split(ratingStr, ":")

			if len(parts) >= 3 {
				if rating, err := strconv.ParseFloat(parts[0], 64); err == nil {
					ratingInfo := map[string]interface{}{
						"movieId": movieID,
						"rating":  rating,
					}

					if timestamp, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
						ratingInfo["timestamp"] = timestamp
					}

					ratings = append(ratings, ratingInfo)
				}
			}
		}
	}

	return map[string]interface{}{
		"ratings": ratings,
		"count":   len(ratings),
	}, nil
}

// GetUserTags 获取用户的标签（使用users表）
func GetUserTags(ctx context.Context, userID string) ([]string, error) {
	// 根据新的数据库结构，从users表获取用户的所有标签
	get, err := hrpc.NewGetStr(ctx, "users", userID)
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	if result.Cells == nil || len(result.Cells) == 0 {
		return []string{}, nil
	}

	var tags []string
	tagSet := make(map[string]bool)

	for _, cell := range result.Cells {
		if string(cell.Family) == "tags" {
			// 解析标签数据格式: "{tag}:{movieId}:{timestamp}"
			tagStr := string(cell.Value)
			parts := strings.Split(tagStr, ":")

			if len(parts) >= 1 {
				tag := parts[0]
				if !tagSet[tag] {
					tags = append(tags, tag)
					tagSet[tag] = true
				}
			}
		}
	}

	return tags, nil
}

// GetUserFavoriteGenres 获取用户最喜欢的电影类型
func GetUserFavoriteGenres(ctx context.Context, userID string) (map[string]int, error) {
	// 获取用户的所有评分
	userRatings, err := GetUserMovieRatings(ctx, userID)
	if err != nil {
		return map[string]int{}, err
	}

	genreCount := make(map[string]int)

	if ratings, ok := userRatings["ratings"].([]map[string]interface{}); ok {
		for _, rating := range ratings {
			if movieID, ok := rating["movieId"].(string); ok {
				// 获取电影信息以获取类型
				movieData, err := GetMovie(ctx, movieID)
				if err == nil && movieData != nil {
					if infoData, ok := movieData["info"]; ok {
						if genresBytes, ok := infoData["genres"]; ok {
							genresStr := string(genresBytes)
							genres := strings.Split(genresStr, "|")
							for _, genre := range genres {
								genre = strings.TrimSpace(genre)
								if genre != "" {
									genreCount[genre]++
								}
							}
						}
					}
				}
			}
		}
	}

	return genreCount, nil
}

// GetRecommendedMoviesForUser 获取推荐给用户的电影
func GetRecommendedMoviesForUser(ctx context.Context, userID string) ([]string, error) {
	// 基于用户喜好的类型推荐电影
	favoriteGenres, err := GetUserFavoriteGenres(ctx, userID)
	if err != nil {
		return []string{}, err
	}

	// 找到用户最喜欢的类型
	var topGenre string
	maxCount := 0
	for genre, count := range favoriteGenres {
		if count > maxCount {
			maxCount = count
			topGenre = genre
		}
	}

	if topGenre == "" {
		return []string{}, nil
	}

	// 根据最喜欢的类型搜索电影
	results, err := ScanMoviesByGenre(ctx, topGenre, 10)
	if err != nil {
		return []string{}, err
	}

	var movieIDs []string
	for _, result := range results {
		if len(result.Cells) > 0 {
			rowKey := string(result.Cells[0].Row)
			if strings.HasSuffix(rowKey, "_info") {
				movieID := strings.TrimSuffix(rowKey, "_info")
				movieIDs = append(movieIDs, movieID)
			}
		}
	}

	return movieIDs, nil
}
