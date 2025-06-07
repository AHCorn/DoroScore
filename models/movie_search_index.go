package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// SearchIndex 搜索索引管理器。
type SearchIndex struct {
	mu sync.RWMutex
}

// MovieIdWithTitle 用于存储电影ID和标题的简单结构体。
type MovieIdWithTitle struct {
	ID    string
	Title string
}

var globalSearchIndex *SearchIndex
var indexOnce sync.Once

// GetSearchIndex 返回全局搜索索引实例。
func GetSearchIndex() *SearchIndex {
	indexOnce.Do(func() {
		globalSearchIndex = &SearchIndex{}
	})
	return globalSearchIndex
}

// BuildSearchIndex 扫描HBase并构建持久化的SQLite索引。
func (si *SearchIndex) BuildSearchIndex(ctx context.Context) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	logrus.Info("开始构建SQLite搜索索引...")
	start := time.Now()

	if err := utils.ResetDatabase(); err != nil {
		logrus.Warnf("无法重置数据库，但仍将继续: %v", err)
	}
	db, err := utils.InitDB()
	if err != nil {
		return fmt.Errorf("初始化SQLite数据库失败: %w", err)
	}

	scan, err := hrpc.NewScanStr(ctx, "movies")
	if err != nil {
		return fmt.Errorf("创建HBase扫描失败: %w", err)
	}
	scanner := utils.GetClient().(gohbase.Client).Scan(scan)

	// 使用事务进行批量插入以提高性能
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO movie_index (movie_id, title) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

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
		if !strings.HasSuffix(rowKey, "_info") {
			continue
		}

		movieID := strings.TrimSuffix(rowKey, "_info")
		var title string
		for _, cell := range res.Cells {
			qualifier := string(cell.Qualifier)
			if string(cell.Family) == "info" && qualifier == "title" {
				title = string(cell.Value)
			}
		}

		if title != "" {
			if _, err := stmt.Exec(movieID, title); err != nil {
				return err
			}
			indexedCount++
			if indexedCount%1000 == 0 {
				logrus.Infof("已索引 %d 部电影到SQLite...", indexedCount)
			}
		}
	}

	// 提交数据
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	// 在数据插入后创建FTS表并重建索引
	// UNINDEXED告诉FTS5不要为movie_id创建全文索引，以节省空间和提高效率
	if _, err := db.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS movie_fts USING fts5(movie_id UNINDEXED, title, content='movie_index', content_rowid='id')`); err != nil {
		return fmt.Errorf("创建FTS表失败: %w", err)
	}
	if _, err := db.Exec(`INSERT INTO movie_fts(movie_fts) VALUES('rebuild')`); err != nil {
		return fmt.Errorf("重建FTS索引失败: %w", err)
	}

	duration := time.Since(start)
	logrus.Infof("SQLite搜索索引构建成功！共索引 %d 部电影，耗时 %v", indexedCount, duration)
	return nil
}

// SearchMoviesWithIndex 使用SQLite索引进行快速搜索。
func (si *SearchIndex) SearchMoviesWithIndex(ctx context.Context, query string, page, perPage int) (*MovieList, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if !si.IsIndexReady() {
		return nil, fmt.Errorf("搜索索引未就绪")
	}

	db, err := utils.GetDB()
	if err != nil {
		return nil, err
	}

	// 构造FTS5查询语句
	sanitizedQuery := `"` + strings.ReplaceAll(query, `"`, `""`) + `*"`

	// 查询FTS表 - 修改为同时获取标题
	rows, err := db.QueryContext(ctx, "SELECT mi.movie_id, mi.title FROM movie_index mi JOIN movie_fts ft ON mi.id = ft.rowid WHERE ft.title MATCH ? ORDER BY ft.rank", sanitizedQuery)
	if err != nil {
		return nil, fmt.Errorf("在SQLite FTS索引中搜索失败: %w", err)
	}
	defer rows.Close()

	var matchedMovies []MovieIdWithTitle
	for rows.Next() {
		var movie MovieIdWithTitle
		if err := rows.Scan(&movie.ID, &movie.Title); err != nil {
			return nil, err
		}
		matchedMovies = append(matchedMovies, movie)
	}

	if len(matchedMovies) == 0 {
		return &MovieList{Movies: []Movie{}, TotalMovies: 0, Page: page, PerPage: perPage, TotalPages: 0}, nil
	}

	totalMatches := len(matchedMovies)
	totalPages := (totalMatches + perPage - 1) / perPage
	startIdx := (page - 1) * perPage
	endIdx := startIdx + perPage
	if endIdx > totalMatches {
		endIdx = totalMatches
	}

	var pageMovies []MovieIdWithTitle
	if startIdx < totalMatches {
		pageMovies = matchedMovies[startIdx:endIdx]
	}

	// 传递电影ID和标题给批量获取函数
	movies, err := si.getMovieDetailsBatchWithTitles(ctx, pageMovies)
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

// IsIndexReady 检查SQLite索引是否可用。
func (si *SearchIndex) IsIndexReady() bool {
	if _, err := os.Stat("./movie_index.db"); os.IsNotExist(err) {
		return false
	}

	db, err := utils.GetDB()
	if err != nil {
		logrus.Warnf("无法检查索引就绪状态，获取数据库失败: %v", err)
		return false
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM movie_index").Scan(&count)
	if err != nil {
		logrus.Warnf("无法查询索引计数: %v", err)
		return false
	}

	return count > 0
}

// getMovieDetailsBatchWithTitles 批量获取电影详情，使用SQLite中的标题。
func (si *SearchIndex) getMovieDetailsBatchWithTitles(ctx context.Context, moviesWithTitles []MovieIdWithTitle) ([]Movie, error) {
	var movies []Movie
	var getReqs []*hrpc.Get

	// 映射，用于快速查找每个ID对应的标题
	titleMap := make(map[string]string)

	for _, movie := range moviesWithTitles {
		// 存储ID->标题的映射
		titleMap[movie.ID] = movie.Title

		// 只获取电影的stats（评分、统计信息）
		statsGet, _ := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_stats", movie.ID))
		// 以及其他可能需要的数据，如links等
		linksGet, _ := hrpc.NewGetStr(ctx, "movies", fmt.Sprintf("%s_links", movie.ID))

		getReqs = append(getReqs, statsGet, linksGet)
	}

	// TODO: 可使用goroutine并发获取以提升性能
	movieDataMap := make(map[string]map[string]map[string][]byte)
	client := utils.GetClient().(gohbase.Client)

	for _, req := range getReqs {
		res, err := client.Get(req)
		if err != nil || res == nil || len(res.Cells) == 0 {
			continue
		}

		rowKey := string(res.Cells[0].Row)
		parts := strings.Split(rowKey, "_")
		movieID := parts[0]
		rowType := parts[1]

		if _, ok := movieDataMap[movieID]; !ok {
			movieDataMap[movieID] = make(map[string]map[string][]byte)
		}
		if _, ok := movieDataMap[movieID][rowType]; !ok {
			movieDataMap[movieID][rowType] = make(map[string][]byte)
		}

		for _, cell := range res.Cells {
			key := fmt.Sprintf("%s:%s", string(cell.Family), string(cell.Qualifier))
			movieDataMap[movieID][rowType][key] = cell.Value
		}
	}

	// 为每个找到的movieID构建完整的Movie对象
	for _, movieWithTitle := range moviesWithTitles {
		movieID := movieWithTitle.ID

		// 获取从SQLite中读取的标题
		title := movieWithTitle.Title

		// 创建基本的Movie对象
		movie := Movie{
			MovieID: movieID,
			Title:   title, // 直接设置标题
		}

		// 如果有HBase数据，填充其他详情
		if data, ok := movieDataMap[movieID]; ok {
			// 从HBase解析数据
			parsedData := utils.ParseMovieData(movieID, data)

			// 使用buildMovieFromData填充其他字段
			fullMovie := buildMovieFromData(movieID, parsedData, data)

			// 复制所有非空字段，但保留我们已经设置的标题
			if fullMovie.Genres != nil {
				movie.Genres = fullMovie.Genres
			}
			if fullMovie.AvgRating != 0 {
				movie.AvgRating = fullMovie.AvgRating
			}
			if fullMovie.Links.ImdbID != "" {
				movie.Links = fullMovie.Links
			}
			if fullMovie.Tags != nil {
				movie.Tags = fullMovie.Tags
			}
		}

		// 如果平均分为0，尝试计算它
		if movie.AvgRating == 0.0 {
			avgRating, ratingCount, err := CalculateAndStoreMovieAvgRating(ctx, movieID)
			if err == nil && avgRating > 0.0 {
				movie.AvgRating = avgRating
				logrus.Infof("✅ 成功计算并存储电影 %s 的平均评分: %.2f (基于 %d 个评分)",
					movieID, avgRating, ratingCount)
			}
		}

		movies = append(movies, movie)
	}

	return movies, nil
}

func indexOf(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1
}
