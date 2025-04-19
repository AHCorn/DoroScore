package hbase

import (
	"fmt"
	"strings"
)

// ParseMovieData 从HBase结果解析电影数据
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	result := map[string]interface{}{
		"movieId": movieID,
	}

	// 处理基本信息
	if movieData, ok := data["movie"]; ok {
		if title, ok := movieData["title"]; ok {
			result["title"] = string(title)
		}
		if genres, ok := movieData["genres"]; ok {
			result["genres"] = strings.Split(string(genres), "|")
		}
	}

	// 处理链接信息
	if linkData, ok := data["link"]; ok {
		links := map[string]interface{}{}

		if imdbId, ok := linkData["imdbId"]; ok {
			imdbIdStr := string(imdbId)
			links["imdbId"] = imdbIdStr
			links["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", imdbIdStr)
		}

		if tmdbId, ok := linkData["tmdbId"]; ok {
			tmdbIdStr := string(tmdbId)
			links["tmdbId"] = tmdbIdStr
			links["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", tmdbIdStr)
		}

		result["links"] = links
	} else {
		// 添加一个空的链接对象以避免前端错误
		result["links"] = map[string]interface{}{}
	}

	// 处理评分 - 新的数据结构已在GetMovieRatings函数中处理，此处移除旧的处理逻辑
	// 仅设置默认值，实际评分数据由GetMovieRatings函数提供
	result["avgRating"] = 0.0
	result["ratings"] = []interface{}{}

	// 处理标签 - 新的数据结构已在GetMovieTags函数中处理，此处移除旧的处理逻辑
	// 仅提供空数组，实际标签数据由GetMovieTags函数提供
	result["uniqueTags"] = []string{}

	return result
}
