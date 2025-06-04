package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// SearchMovies 搜索电影
func SearchMovies(query string, page, perPage int) (*MovieList, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("search:%s:%d:%d", query, page, perPage)

	// 检查缓存
	if cachedResults, found := utils.Cache.Get(cacheKey); found {
		return cachedResults.(*MovieList), nil
	}

	ctx := context.Background()

	// 优先使用索引搜索（如果索引已建立）
	searchIndex := GetSearchIndex()
	if searchIndex.IsIndexReady() {
		result, err := searchIndex.SearchMoviesWithIndex(ctx, query, page, perPage)
		if err == nil {
			// 缓存搜索结果
			utils.Cache.Set(cacheKey, result)
			return result, nil
		}
		// 如果索引搜索失败，继续使用原有方法
		fmt.Printf("索引搜索失败，使用原有方法: %v\n", err)
	}

	// Fallback: 智能搜索策略
	var matchedMovies []Movie
	var err error

	// 1. 检查是否为电影ID搜索
	if movieID, parseErr := strconv.Atoi(query); parseErr == nil {
		matchedMovies, err = searchByMovieID(ctx, movieID)
	} else {
		// 2. 文本搜索：限制扫描范围
		matchedMovies, err = searchByTextOptimized(ctx, query)
	}

	if err != nil {
		return nil, err
	}

	// 批量获取评分数据（如果需要）
	if len(matchedMovies) > 0 {
		err = enrichMoviesWithRatings(ctx, matchedMovies)
		if err != nil {
			// 评分获取失败不影响搜索结果，只记录错误
			fmt.Printf("Warning: Failed to get ratings: %v\n", err)
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

	// 构建结果
	var resultMovies []Movie
	if startIdx < totalMatches {
		resultMovies = matchedMovies[startIdx:endIdx]
	} else {
		resultMovies = []Movie{}
	}

	result := &MovieList{
		Movies:      resultMovies,
		TotalMovies: totalMatches,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}

	// 缓存搜索结果
	utils.Cache.Set(cacheKey, result)

	return result, nil
}

// searchByMovieID 根据电影ID精确搜索（使用前缀扫描）
func searchByMovieID(ctx context.Context, movieID int) ([]Movie, error) {
	movieIDStr := strconv.Itoa(movieID)

	// 使用前缀扫描获取该电影的所有信息
	// 行键格式: {movieId}_info, {movieId}_stats 等
	startRow := fmt.Sprintf("%s_", movieIDStr)
	endRow := fmt.Sprintf("%s`", movieIDStr) // ` 是 _ 的下一个ASCII字符

	scan, err := hrpc.NewScanRangeStr(ctx, "movies", startRow, endRow)
	if err != nil {
		return nil, err
	}

	scanner := utils.GetClient().(interface {
		Scan(request *hrpc.Scan) hrpc.Scanner
	}).Scan(scan)

	// 收集该电影的所有数据
	movieData := make(map[string]map[string][]byte)

	for {
		res, err := scanner.Next()
		if err != nil {
			break
		}

		if len(res.Cells) == 0 {
			continue
		}

		rowKey := string(res.Cells[0].Row)

		// 解析行键类型
		parts := strings.Split(rowKey, "_")
		if len(parts) < 2 {
			continue
		}

		rowType := parts[1]

		// 构建结果映射
		if _, ok := movieData[rowType]; !ok {
			movieData[rowType] = make(map[string][]byte)
		}

		for _, cell := range res.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)
			key := fmt.Sprintf("%s:%s", family, qualifier)
			movieData[rowType][key] = cell.Value
		}
	}

	// 如果找到info数据，构建Movie对象
	if _, exists := movieData["info"]; exists {
		parsedData := utils.ParseMovieData(movieIDStr, movieData)
		movie := buildMovieFromData(movieIDStr, parsedData, movieData)
		return []Movie{movie}, nil
	}

	return []Movie{}, nil
}

// searchByTextOptimized （只扫描_info行）
func searchByTextOptimized(ctx context.Context, query string) ([]Movie, error) {
	// 使用简化但高效的方案：限制扫描结果数量并快速匹配
	scan, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return nil, err
	}

	scanner := utils.GetClient().(interface {
		Scan(request *hrpc.Scan) hrpc.Scanner
	}).Scan(scan)

	queryLower := strings.ToLower(query)
	var matchedMovies []Movie

	// 限制处理的行数，避免无限扫描
	maxRowsToProcess := 10000
	processedInfoRows := 0

	for processedInfoRows < maxRowsToProcess {
		res, err := scanner.Next()
		if err != nil {
			break
		}

		if len(res.Cells) == 0 {
			continue
		}

		rowKey := string(res.Cells[0].Row)

		// 只处理_info行
		if !strings.HasSuffix(rowKey, "_info") {
			continue
		}

		processedInfoRows++

		// 提取电影ID
		movieID := strings.TrimSuffix(rowKey, "_info")

		// 快速匹配并构建电影对象
		movie := quickMatchAndBuild(movieID, res.Cells, queryLower)
		if movie != nil {
			matchedMovies = append(matchedMovies, *movie)
		}

		// 如果已经找到足够的结果，可以提前退出
		if len(matchedMovies) >= 1000 { // 限制最大返回结果
			break
		}
	}

	return matchedMovies, nil
}

// quickMatchAndBuild 快速匹配并构建电影对象
func quickMatchAndBuild(movieID string, cells []*hrpc.Cell, query string) *Movie {
	// 快速提取标题和类型进行匹配
	var title string
	var genres []string

	for _, cell := range cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if family == "info" {
			switch qualifier {
			case "title":
				title = string(cell.Value)
			case "genres":
				if len(cell.Value) > 0 {
					genres = strings.Split(string(cell.Value), "|")
				}
			}
		}
	}

	// 快速匹配检查
	titleMatch := title != "" && strings.Contains(strings.ToLower(title), query)
	genreMatch := false

	if !titleMatch && len(genres) > 0 {
		for _, genre := range genres {
			if strings.Contains(strings.ToLower(genre), query) {
				genreMatch = true
				break
			}
		}
	}

	// 如果不匹配，直接返回nil
	if !titleMatch && !genreMatch {
		return nil
	}

	// 匹配成功，构建完整的电影对象
	resultMap := make(map[string]map[string][]byte)
	infoFamily := make(map[string][]byte)

	for _, cell := range cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if family == "info" {
			infoFamily[qualifier] = cell.Value
		}
	}

	resultMap["info"] = infoFamily
	movieData := utils.ParseMovieData(movieID, resultMap)

	return buildMovieFromParsedData(movieID, movieData)
}

// buildMovieFromParsedData 从解析的数据构建Movie对象
func buildMovieFromParsedData(movieID string, movieData map[string]interface{}) *Movie {
	movie := &Movie{
		MovieID: movieID,
	}

	if title, ok := movieData["title"].(string); ok {
		movie.Title = title

		// 从标题提取年份
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
	}

	if tags, ok := movieData["uniqueTags"].([]string); ok {
		movie.Tags = tags
	}

	return movie
}

// buildMovieFromData 从完整数据构建Movie对象（用于ID搜索）
func buildMovieFromData(movieID string, parsedData map[string]interface{}, allData map[string]map[string][]byte) Movie {
	movie := Movie{
		MovieID: movieID,
	}

	if title, ok := parsedData["title"].(string); ok {
		movie.Title = title

		// 从标题提取年份
		if matches := strings.Split(title, " ("); len(matches) > 1 {
			yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
			if year, err := strconv.Atoi(yearStr); err == nil {
				movie.Year = year
			}
		}
	}

	if genres, ok := parsedData["genres"].([]string); ok {
		movie.Genres = genres
	}

	// 优先使用stats中的预计算评分
	if statsData, hasStats := allData["stats"]; hasStats {
		if avgRatingBytes, ok := statsData["info:avg_rating"]; ok {
			if avgRating, err := strconv.ParseFloat(string(avgRatingBytes), 64); err == nil {
				movie.AvgRating = avgRating
			}
		}
	} else if avgRating, ok := parsedData["avgRating"].(float64); ok {
		movie.AvgRating = avgRating
	}

	if tags, ok := parsedData["uniqueTags"].([]string); ok {
		movie.Tags = tags
	}

	return movie
}

// enrichMoviesWithRatings 批量为电影补充评分信息
func enrichMoviesWithRatings(ctx context.Context, movies []Movie) error {
	movieIDs := make([]string, len(movies))
	for i, movie := range movies {
		movieIDs[i] = movie.MovieID
	}

	ratingsMap, err := utils.GetMoviesRatingsBatch(ctx, movieIDs)
	if err != nil {
		return err
	}

	// 更新评分信息
	for i := range movies {
		movieID := movies[i].MovieID
		if rating, ok := ratingsMap[movieID]; ok {
			if avgRating, ok := rating["avgRating"].(float64); ok && movies[i].AvgRating == 0.0 {
				movies[i].AvgRating = avgRating
			}
		}
	}

	return nil
}
