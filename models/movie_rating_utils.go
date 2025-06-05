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

// CalculateAndStoreMovieAvgRating 计算并存储电影平均评分（通用函数）
func CalculateAndStoreMovieAvgRating(ctx context.Context, movieID string) (float64, int, error) {
	// 获取电影的所有评分数据
	ratingsGet, err := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_ratings", movieID))
	if err != nil {
		return 0.0, 0, err
	}

	ratingsResult, err := utils.GetClient().(interface {
		Get(request *hrpc.Get) (*hrpc.Result, error)
	}).Get(ratingsGet)
	if err != nil || len(ratingsResult.Cells) == 0 {
		return 0.0, 0, fmt.Errorf("没有评分数据")
	}

	// 解析评分数据并计算平均值
	var ratings []float64
	for _, cell := range ratingsResult.Cells {
		if string(cell.Family) == "ratings" {
			// 解析评分数据格式: "{rating}:{userId}:{timestamp}"
			ratingStr := string(cell.Value)
			parts := strings.Split(ratingStr, ":")

			if len(parts) >= 1 {
				if rating, parseErr := strconv.ParseFloat(parts[0], 64); parseErr == nil {
					ratings = append(ratings, rating)
				}
			}
		}
	}

	if len(ratings) == 0 {
		return 0.0, 0, fmt.Errorf("没有有效的评分数据")
	}

	// 计算平均评分
	var sum float64
	for _, rating := range ratings {
		sum += rating
	}
	avgRating := sum / float64(len(ratings))
	ratingCount := len(ratings)

	// 存储到stats行
	err = StoreMovieAvgRatingToStats(ctx, movieID, avgRating, ratingCount)
	if err != nil {
		return avgRating, ratingCount, err
	}

	return avgRating, ratingCount, nil
}

// StoreMovieAvgRatingToStats 存储电影平均评分到stats行（通用函数）
func StoreMovieAvgRatingToStats(ctx context.Context, movieID string, avgRating float64, ratingCount int) error {
	// 创建Put请求到stats行
	rowKey := fmt.Sprintf("%s_stats", movieID)

	// 准备数据
	avgRatingStr := fmt.Sprintf("%.6f", avgRating)
	ratingCountStr := fmt.Sprintf("%d", ratingCount)
	currentTime := time.Now().Unix()
	updatedTimeStr := fmt.Sprintf("%d", currentTime)

	// 创建values映射
	values := map[string]map[string][]byte{
		"info": {
			"avg_rating":   []byte(avgRatingStr),
			"rating_count": []byte(ratingCountStr),
			"updated_time": []byte(updatedTimeStr),
		},
	}

	put, err := hrpc.NewPutStr(ctx, "movies", rowKey, values)
	if err != nil {
		return err
	}

	// 执行写入
	_, err = utils.GetClient().(interface {
		Put(request *hrpc.Mutate) (*hrpc.Result, error)
	}).Put(put)

	if err != nil {
		return fmt.Errorf("写入stats失败: %v", err)
	}

	return nil
}
