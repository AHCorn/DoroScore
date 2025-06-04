package hbase

import (
	"context"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// ScanMovies 扫描电影（使用新的数据库结构）
func ScanMovies(ctx context.Context, startRow, endRow string, limit int64) ([]*hrpc.Result, error) {
	// 构建Scan对象，扫描movies表
	scanRequest, err := hrpc.NewScanRangeStr(ctx, "movies", startRow, endRow)
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果，只获取_info行（电影基本信息）
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_info行（电影基本信息）
		if strings.HasSuffix(rowKey, "_info") {
			results = append(results, result)
			count++
		}
	}

	return results, nil
}

// ScanMoviesWithFamilies 使用指定列族扫描电影
func ScanMoviesWithFamilies(ctx context.Context, startRow, endRow string, families []string, limit int64) ([]*hrpc.Result, error) {
	// 构建列族映射
	familiesMap := make(map[string][]string)
	for _, family := range families {
		familiesMap[family] = nil
	}

	// 构建Scan对象，并指定列族
	scanRequest, err := hrpc.NewScanRangeStr(ctx, "movies", startRow, endRow, hrpc.Families(familiesMap))
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果，只获取_info行
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_info行（电影基本信息）
		if strings.HasSuffix(rowKey, "_info") {
			results = append(results, result)
			count++
		}
	}

	return results, nil
}

// ScanMoviesByGenre 根据电影类型扫描电影
func ScanMoviesByGenre(ctx context.Context, genre string, limit int64) ([]*hrpc.Result, error) {
	// 扫描所有_info行，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果并筛选包含指定类型的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_info行（电影基本信息）
		if !strings.HasSuffix(rowKey, "_info") {
			continue
		}

		// 检查这个结果是否包含指定的类型
		for _, cell := range result.Cells {
			if string(cell.Family) == "info" && string(cell.Qualifier) == "genres" {
				genreValue := string(cell.Value)
				if strings.Contains(strings.ToLower(genreValue), strings.ToLower(genre)) {
					results = append(results, result)
					count++
					break
				}
			}
		}
	}

	return results, nil
}

// ScanMoviesByTag 根据标签扫描电影
func ScanMoviesByTag(ctx context.Context, tag string, limit int64) ([]*hrpc.Result, error) {
	// 扫描所有_tags行，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)
	processedMovies := make(map[string]bool) // 避免重复处理同一部电影

	// 收集结果并筛选包含指定标签的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_tags行
		if !strings.HasSuffix(rowKey, "_tags") {
			continue
		}

		// 提取电影ID
		movieID := strings.TrimSuffix(rowKey, "_tags")
		if processedMovies[movieID] {
			continue
		}

		// 检查是否有匹配的标签
		foundTag := false
		for _, cell := range result.Cells {
			if string(cell.Family) == "tags" {
				// 解析标签数据格式: "{tag}:{userId}:{timestamp}"
				tagData := string(cell.Value)
				parts := strings.Split(tagData, ":")
				if len(parts) >= 1 {
					tagValue := parts[0]
					if strings.Contains(strings.ToLower(tagValue), strings.ToLower(tag)) {
						foundTag = true
						break
					}
				}
			}
		}

		if foundTag {
			// 获取对应的电影基本信息
			movieInfoResult, err := GetMovie(ctx, movieID)
			if err == nil && movieInfoResult != nil {
				// 构建一个包含电影基本信息的Result
				infoResult := &hrpc.Result{}
				for family, qualifiers := range movieInfoResult {
					for qualifier, value := range qualifiers {
						cell := &hrpc.Cell{
							Row:       []byte(movieID + "_info"),
							Family:    []byte(family),
							Qualifier: []byte(qualifier),
							Value:     value,
						}
						infoResult.Cells = append(infoResult.Cells, cell)
					}
				}
				results = append(results, infoResult)
				count++
				processedMovies[movieID] = true
			}
		}
	}

	return results, nil
}

// ScanMoviesWithPagination 带分页的电影扫描
func ScanMoviesWithPagination(ctx context.Context, page, pageSize int) ([]*hrpc.Result, int, error) {
	// 构建扫描请求，只扫描_info行
	scanRequest, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return nil, 0, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var allResults []*hrpc.Result

	// 收集所有_info行结果
	for {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_info行（电影基本信息）
		if strings.HasSuffix(rowKey, "_info") {
			allResults = append(allResults, result)
		}
	}

	// 计算总行数
	totalRows := len(allResults)

	// 计算分页
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalRows {
		endIndex = totalRows
	}

	// 如果起始索引超出范围
	if startIndex >= totalRows {
		return []*hrpc.Result{}, totalRows, nil
	}

	// 返回分页结果
	return allResults[startIndex:endIndex], totalRows, nil
}

// SearchMovies 搜索电影
func SearchMovies(ctx context.Context, query string, limit int64) ([]*hrpc.Result, error) {
	query = strings.ToLower(query)

	// 扫描所有_info行，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果并筛选匹配查询的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 确保至少有一个单元格
		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键
		rowKey := string(result.Cells[0].Row)

		// 只处理_info行（电影基本信息）
		if !strings.HasSuffix(rowKey, "_info") {
			continue
		}

		// 检查标题和类型是否匹配查询
		isMatch := false
		for _, cell := range result.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)
			value := string(cell.Value)

			if family == "info" && (qualifier == "title" || qualifier == "genres") {
				if strings.Contains(strings.ToLower(value), query) {
					isMatch = true
					break
				}
			}
		}

		if isMatch {
			results = append(results, result)
			count++
		}
	}

	return results, nil
}

// GetMovieIDsFromInfoRows 从_info行中提取电影ID列表
func GetMovieIDsFromInfoRows(results []*hrpc.Result) []string {
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
	return movieIDs
}
