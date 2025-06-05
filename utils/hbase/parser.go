package hbase

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// ParseMovieData 从HBase结果解析电影数据（适配新的数据库结构）
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	result := map[string]interface{}{
		"movieId": movieID,
	}

	// 处理基本信息（info列族）
	if infoData, ok := data["info"]; ok {
		if title, ok := infoData["title"]; ok {
			result["title"] = string(title)
		}
		if genres, ok := infoData["genres"]; ok {
			result["genres"] = strings.Split(string(genres), "|")
		}

		// 处理统计信息
		if avgRating, ok := infoData["avg_rating"]; ok {
			if rating, err := strconv.ParseFloat(string(avgRating), 64); err == nil {
				result["avgRating"] = rating
			}
		}
		if ratingCount, ok := infoData["rating_count"]; ok {
			if count, err := strconv.Atoi(string(ratingCount)); err == nil {
				result["ratingCount"] = count
			}
		}
		if updatedTime, ok := infoData["updated_time"]; ok {
			if timestamp, err := strconv.ParseInt(string(updatedTime), 10, 64); err == nil {
				result["updatedTime"] = timestamp
			}
		}
	}

	// 处理评分数据（ratings列族 - 宽列格式）
	if ratingsData, ok := data["ratings"]; ok {
		var ratings []map[string]interface{}
		var ratingValues []float64

		for userID, ratingBytes := range ratingsData {
			// 解析评分数据格式: "{rating}:{userId}:{timestamp}"
			ratingStr := string(ratingBytes)
			parts := strings.Split(ratingStr, ":")

			if len(parts) >= 3 {
				if rating, err := strconv.ParseFloat(parts[0], 64); err == nil {
					ratingInfo := map[string]interface{}{
						"userId": userID,
						"rating": rating,
					}

					// 添加时间戳
					if timestamp, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
						ratingInfo["timestamp"] = timestamp
					}

					ratings = append(ratings, ratingInfo)
					ratingValues = append(ratingValues, rating)
				}
			}
		}

		result["ratings"] = ratings

		// 计算平均评分（如果没有预计算的值）
		if _, hasAvgRating := result["avgRating"]; !hasAvgRating && len(ratingValues) > 0 {
			var sum float64
			for _, rating := range ratingValues {
				sum += rating
			}
			result["avgRating"] = sum / float64(len(ratingValues))
		}
	}

	// 处理标签数据（tags列族 - 宽列格式）
	if tagsData, ok := data["tags"]; ok {
		var uniqueTags []string
		tagSet := make(map[string]bool)

		for _, tagBytes := range tagsData {
			// 解析标签数据格式: "{tag}:{userId}:{timestamp}"
			tagStr := string(tagBytes)
			parts := strings.Split(tagStr, ":")

			if len(parts) >= 1 {
				tag := parts[0]
				if !tagSet[tag] {
					uniqueTags = append(uniqueTags, tag)
					tagSet[tag] = true
				}
			}
		}

		result["uniqueTags"] = uniqueTags
	}

	// 处理基因分数数据（genome列族 - 宽列格式）
	if genomeData, ok := data["genome"]; ok {
		genome := make(map[string]float64)

		for tagID, relevanceBytes := range genomeData {
			if relevance, err := strconv.ParseFloat(string(relevanceBytes), 64); err == nil {
				genome[tagID] = relevance
			}
		}

		result["genome"] = genome
	}

	// 设置默认值
	if result["avgRating"] == nil {
		result["avgRating"] = 0.0
	}
	if result["ratings"] == nil {
		result["ratings"] = []interface{}{}
	}
	if result["uniqueTags"] == nil {
		result["uniqueTags"] = []string{}
	}
	if result["genome"] == nil {
		result["genome"] = map[string]float64{}
	}

	return result
}

// GetMovieLinksWithUrls 获取电影外部链接并生成完整URL（通用函数）
func GetMovieLinksWithUrls(ctx context.Context, movieID string) (map[string]interface{}, error) {
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
					// 生成IMDB URL
					links["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", value)
				case "tmdbId":
					links["tmdbId"] = value
					// 生成TMDB URL
					links["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", value)
				}
			}
		}
	}

	// 如果没有找到任何链接数据，返回空的links对象
	if len(links) == 0 {
		links = map[string]interface{}{
			"imdbId":  "",
			"imdbUrl": "",
			"tmdbId":  "",
			"tmdbUrl": "",
		}
	}

	return links, nil
}

// GetMovieTagsWithDetails 获取电影标签并返回详细信息（通用函数）
func GetMovieTagsWithDetails(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的tags行
	get, err := hrpc.NewGetStr(ctx, "movies", movieID+"_tags")
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]interface{})
	var uniqueTags []string
	var taggedUsers []map[string]string
	tagSet := make(map[string]bool)

	if result.Cells != nil {
		for _, cell := range result.Cells {
			if string(cell.Family) == "info" {
				userID := string(cell.Qualifier)
				// 解析标签数据格式: "{tag}:{userId}:{timestamp}"
				tagStr := string(cell.Value)
				parts := strings.Split(tagStr, ":")

				if len(parts) >= 3 {
					tag := parts[0]
					timestamp := parts[2]

					// 添加到唯一标签列表
					if !tagSet[tag] {
						uniqueTags = append(uniqueTags, tag)
						tagSet[tag] = true
					}

					// 添加到标签用户列表
					taggedUsers = append(taggedUsers, map[string]string{
						"userId":    userID,
						"tag":       tag,
						"timestamp": timestamp,
					})
				}
			}
		}
	}

	// 构建返回结果
	tags["uniqueTags"] = uniqueTags
	tags["taggedUsers"] = taggedUsers
	tags["tagCount"] = len(uniqueTags)
	tags["userTagCount"] = len(taggedUsers)

	return tags, nil
}
