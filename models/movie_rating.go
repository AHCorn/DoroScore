package models

import (
	"context"
	"gohbase/utils"
)

// GetMovieRatings 获取电影评分
func GetMovieRatings(movieID string) (map[string]interface{}, error) {
	ctx := context.Background()
	return utils.GetMovieRatings(ctx, movieID)
}
