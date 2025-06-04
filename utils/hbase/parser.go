package hbase

import (
	"fmt"
	"strconv"
	"strings"
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

		// 处理外部链接
		if imdbId, ok := infoData["imdbId"]; ok {
			if result["links"] == nil {
				result["links"] = map[string]interface{}{}
			}
			imdbIdStr := string(imdbId)
			result["links"].(map[string]interface{})["imdbId"] = imdbIdStr
			result["links"].(map[string]interface{})["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", imdbIdStr)
		}
		if tmdbId, ok := infoData["tmdbId"]; ok {
			if result["links"] == nil {
				result["links"] = map[string]interface{}{}
			}
			tmdbIdStr := string(tmdbId)
			result["links"].(map[string]interface{})["tmdbId"] = tmdbIdStr
			result["links"].(map[string]interface{})["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", tmdbIdStr)
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
	if result["links"] == nil {
		result["links"] = map[string]interface{}{}
	}
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
