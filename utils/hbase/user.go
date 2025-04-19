package hbase

import (
	"context"
	"strconv"

	"github.com/tsuna/gohbase/hrpc"
)

// GetUserRating 获取用户对电影的评分
func GetUserRating(ctx context.Context, movieID string, userID string) (float64, int64, error) {
	// 构造复合行键
	rowKey := movieID + "_" + userID

	// 使用复合键直接获取特定用户对特定电影的评分
	get, err := hrpc.NewGetStr(ctx, "moviedata", rowKey,
		hrpc.Families(map[string][]string{"rating": nil}))
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

	var rating float64
	var timestamp int64

	// 从结果中提取评分和时间戳
	for _, cell := range result.Cells {
		if string(cell.Family) == "rating" {
			qualifier := string(cell.Qualifier)
			if qualifier == "rating" {
				rating, _ = strconv.ParseFloat(string(cell.Value), 64)
				if cell.Timestamp != nil {
					timestamp = int64(*cell.Timestamp)
				}
			} else if qualifier == "timestamp" {
				ts, err := strconv.ParseInt(string(cell.Value), 10, 64)
				if err == nil {
					// 如果timestamp列存在单独的值，则使用它
					timestamp = ts
				}
			}
		}
	}

	return rating, timestamp, nil
}

// GetUserFavoriteGenres 获取用户最喜欢的电影类型
func GetUserFavoriteGenres(ctx context.Context, userID string) (map[string]int, error) {
	// 这里需要扫描用户的所有评分
	// 为简化实现，我们暂时返回空结果
	return map[string]int{}, nil
}

// GetUserTags 获取用户的标签
func GetUserTags(ctx context.Context, userID string) ([]string, error) {
	// 为简化实现，暂时返回空结果
	return []string{}, nil
}

// GetRecommendedMoviesForUser 获取推荐给用户的电影
func GetRecommendedMoviesForUser(ctx context.Context, userID string) ([]string, error) {
	// 为简化实现，暂时返回空结果
	return []string{}, nil
}
