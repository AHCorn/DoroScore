package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

// SearchIndex 内存搜索索引
type SearchIndex struct {
	titleIndex  map[string][]string // 标题词 -> 电影ID列表
	genreIndex  map[string][]string // 类型 -> 电影ID列表
	lastUpdated time.Time
	mu          sync.RWMutex
}

var globalSearchIndex *SearchIndex
var indexOnce sync.Once

// GetSearchIndex 获取全局搜索索引实例
func GetSearchIndex() *SearchIndex {
	indexOnce.Do(func() {
		globalSearchIndex = &SearchIndex{
			titleIndex: make(map[string][]string),
			genreIndex: make(map[string][]string),
		}
	})
	return globalSearchIndex
}

// BuildSearchIndex 构建搜索索引
func (si *SearchIndex) BuildSearchIndex(ctx context.Context) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	fmt.Println("开始构建搜索索引...")
	start := time.Now()

	// 清空现有索引
	si.titleIndex = make(map[string][]string)
	si.genreIndex = make(map[string][]string)

	// 扫描所有电影_info行
	scan, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return err
	}

	scanner := utils.GetClient().(interface {
		Scan(request *hrpc.Scan) hrpc.Scanner
	}).Scan(scan)

	indexedCount := 0
	for {
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

		movieID := strings.TrimSuffix(rowKey, "_info")

		// 提取标题和类型信息
		var title string
		var genres []string

		for _, cell := range res.Cells {
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

		// 建立标题索引
		if title != "" {
			words := tokenizeTitle(title)
			for _, word := range words {
				word = strings.ToLower(word)
				if len(word) >= 2 { // 只索引长度>=2的词
					si.titleIndex[word] = appendUnique(si.titleIndex[word], movieID)
				}
			}
		}

		// 建立类型索引
		for _, genre := range genres {
			genre = strings.ToLower(strings.TrimSpace(genre))
			if genre != "" {
				si.genreIndex[genre] = appendUnique(si.genreIndex[genre], movieID)
			}
		}

		indexedCount++
		if indexedCount%1000 == 0 {
			fmt.Printf("已索引 %d 部电影...\n", indexedCount)
		}
	}

	si.lastUpdated = time.Now()
	duration := time.Since(start)

	fmt.Printf("搜索索引构建完成！索引了 %d 部电影，耗时: %v\n", indexedCount, duration)
	fmt.Printf("标题索引词数: %d, 类型索引数: %d\n", len(si.titleIndex), len(si.genreIndex))

	return nil
}

// SearchMoviesWithIndex 使用索引进行快速搜索
func (si *SearchIndex) SearchMoviesWithIndex(ctx context.Context, query string, page, perPage int) (*MovieList, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if len(si.titleIndex) == 0 {
		// 索引未建立，返回错误提示需要构建索引
		return nil, fmt.Errorf("搜索索引未建立，请先调用BuildSearchIndex")
	}

	queryLower := strings.ToLower(strings.TrimSpace(query))
	var matchedMovieIDs []string

	// 1. 尝试精确匹配类型
	if genreMovies, exists := si.genreIndex[queryLower]; exists {
		matchedMovieIDs = append(matchedMovieIDs, genreMovies...)
	}

	// 2. 标题搜索
	titleMatches := si.searchInTitleIndex(queryLower)
	matchedMovieIDs = append(matchedMovieIDs, titleMatches...)

	// 3. 去重
	matchedMovieIDs = removeDuplicates(matchedMovieIDs)

	// 4. 排序（可选）
	sort.Strings(matchedMovieIDs)

	if len(matchedMovieIDs) == 0 {
		return &MovieList{
			Movies:      []Movie{},
			TotalMovies: 0,
			Page:        page,
			PerPage:     perPage,
			TotalPages:  0,
		}, nil
	}

	// 5. 分页
	totalMatches := len(matchedMovieIDs)
	totalPages := (totalMatches + perPage - 1) / perPage
	startIdx := (page - 1) * perPage
	endIdx := startIdx + perPage
	if endIdx > totalMatches {
		endIdx = totalMatches
	}

	var pageMovieIDs []string
	if startIdx < totalMatches {
		pageMovieIDs = matchedMovieIDs[startIdx:endIdx]
	}

	// 6. 批量获取电影详情
	movies, err := si.getMovieDetailsBatch(ctx, pageMovieIDs)
	if err != nil {
		return nil, err
	}

	return &MovieList{
		Movies:      movies,
		TotalMovies: totalMatches,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}, nil
}

// searchInTitleIndex 在标题索引中搜索
func (si *SearchIndex) searchInTitleIndex(query string) []string {
	var results []string

	// 1. 完整匹配
	if movieIDs, exists := si.titleIndex[query]; exists {
		results = append(results, movieIDs...)
	}

	// 2. 前缀匹配
	for word, movieIDs := range si.titleIndex {
		if strings.HasPrefix(word, query) && word != query {
			results = append(results, movieIDs...)
		}
	}

	// 3. 包含匹配
	for word, movieIDs := range si.titleIndex {
		if strings.Contains(word, query) && !strings.HasPrefix(word, query) && word != query {
			results = append(results, movieIDs...)
		}
	}

	return results
}

// getMovieDetailsBatch 批量获取电影详情
func (si *SearchIndex) getMovieDetailsBatch(ctx context.Context, movieIDs []string) ([]Movie, error) {
	var movies []Movie

	for _, movieID := range movieIDs {
		// 获取电影基本信息和统计信息
		infoGet, err := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_info", movieID))
		if err != nil {
			continue
		}

		statsGet, err := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_stats", movieID))
		if err != nil {
			continue
		}

		client := utils.GetClient().(interface {
			Get(request *hrpc.Get) (*hrpc.Result, error)
		})

		// 获取基本信息
		infoResult, err := client.Get(infoGet)
		if err != nil || len(infoResult.Cells) == 0 {
			continue
		}

		// 获取统计信息（可能不存在）
		statsResult, _ := client.Get(statsGet)

		// 构建结果映射
		resultMap := make(map[string]map[string][]byte)
		infoFamily := make(map[string][]byte)

		// 处理基本信息
		for _, cell := range infoResult.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if family == "info" {
				infoFamily[qualifier] = cell.Value
			}
		}

		// 处理统计信息（如果存在）
		if statsResult != nil && len(statsResult.Cells) > 0 {
			for _, cell := range statsResult.Cells {
				family := string(cell.Family)
				qualifier := string(cell.Qualifier)

				if family == "info" {
					// 将stats行的info数据合并到infoFamily中
					infoFamily[qualifier] = cell.Value
				}
			}
		}

		resultMap["info"] = infoFamily
		movieData := utils.ParseMovieData(movieID, resultMap)

		movie := buildMovieFromParsedData(movieID, movieData)
		if movie != nil {
			// 如果仍然没有平均评分，则计算并存储
			if movie.AvgRating == 0.0 {
				avgRating, ratingCount, err := CalculateAndStoreMovieAvgRating(ctx, movieID)
				if err == nil && avgRating > 0.0 {
					movie.AvgRating = avgRating
					fmt.Printf("✅ 成功计算并存储电影 %s 的平均评分: %.2f (基于 %d 个评分)\n",
						movieID, avgRating, ratingCount)
				}
			}
			movies = append(movies, *movie)
		}
	}

	return movies, nil
}

// 辅助函数

// tokenizeTitle 分词标题
func tokenizeTitle(title string) []string {
	// 简单的分词：按空格、括号、连字符分割
	title = strings.ToLower(title)
	separators := []string{" ", "(", ")", "-", ":", ",", ".", "'", "\""}

	words := []string{title} // 从完整标题开始
	for _, sep := range separators {
		var newWords []string
		for _, word := range words {
			parts := strings.Split(word, sep)
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if len(part) > 0 {
					newWords = append(newWords, part)
				}
			}
		}
		words = newWords
	}

	return words
}

// appendUnique 添加唯一元素
func appendUnique(slice []string, item string) []string {
	for _, existing := range slice {
		if existing == item {
			return slice
		}
	}
	return append(slice, item)
}

// removeDuplicates 去重
func removeDuplicates(slice []string) []string {
	keys := make(map[string]bool)
	var result []string

	for _, item := range slice {
		if !keys[item] {
			keys[item] = true
			result = append(result, item)
		}
	}

	return result
}

// IsIndexReady 检查索引是否已准备好
func (si *SearchIndex) IsIndexReady() bool {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return len(si.titleIndex) > 0
}

// GetIndexStats 获取索引统计信息
func (si *SearchIndex) GetIndexStats() map[string]interface{} {
	si.mu.RLock()
	defer si.mu.RUnlock()

	return map[string]interface{}{
		"titleIndexSize": len(si.titleIndex),
		"genreIndexSize": len(si.genreIndex),
		"lastUpdated":    si.lastUpdated,
		"isReady":        len(si.titleIndex) > 0,
	}
}
