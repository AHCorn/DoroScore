package controllers

import (
	"gohbase/services"
	"gohbase/utils"
	"strconv"

	"github.com/gin-gonic/gin"
)

// MovieController 电影控制器
type MovieController struct {
	movieService services.MovieService
}

// NewMovieController 创建电影控制器
func NewMovieController() *MovieController {
	return &MovieController{
		movieService: services.NewMovieService(),
	}
}

// GetMovies 获取电影列表
func (mc *MovieController) GetMovies(c *gin.Context) {
	page := getIntParam(c, "page", 1)
	perPage := getIntParam(c, "per_page", 12)

	// 限制每页最大数量
	if perPage > 50 {
		perPage = 50
	}

	movies, err := mc.movieService.GetMoviesList(page, perPage)
	if err != nil {
		utils.InternalError(c, "获取电影列表失败", err)
		return
	}

	utils.SuccessData(c, movies)
}

// GetMovie 获取电影详情
func (mc *MovieController) GetMovie(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		utils.BadRequest(c, "电影ID不能为空")
		return
	}

	movie, err := mc.movieService.GetMovieByID(movieID)
	if err != nil {
		utils.InternalError(c, "获取电影详情失败", err)
		return
	}

	if movie == nil {
		utils.NotFound(c, "电影不存在")
		return
	}

	utils.SuccessData(c, movie)
}

// GetRandomMovies 获取随机电影
func (mc *MovieController) GetRandomMovies(c *gin.Context) {
	count := getIntParam(c, "count", 10)

	movies, err := mc.movieService.GetRandomMovies(count)
	if err != nil {
		utils.InternalError(c, "获取随机电影失败", err)
		return
	}

	utils.SuccessData(c, movies)
}

// RandomMoviesPost POST方式获取随机电影
func (mc *MovieController) RandomMoviesPost(c *gin.Context) {
	mc.GetRandomMovies(c)
}

// SearchMovies 搜索电影
func (mc *MovieController) SearchMovies(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		utils.BadRequest(c, "搜索关键词不能为空")
		return
	}

	page := getIntParam(c, "page", 1)
	perPage := getIntParam(c, "per_page", 12)

	result, err := mc.movieService.SearchMovies(query, page, perPage)
	if err != nil {
		utils.InternalError(c, "搜索电影失败", err)
		return
	}

	utils.SuccessData(c, result)
}

// GetMovieRatings 获取电影评分
func (mc *MovieController) GetMovieRatings(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		utils.BadRequest(c, "电影ID不能为空")
		return
	}

	ratings, err := mc.movieService.GetMovieRatings(movieID)
	if err != nil {
		utils.InternalError(c, "获取电影评分失败", err)
		return
	}

	if ratings == nil {
		utils.SuccessData(c, gin.H{
			"status":    "success",
			"ratings":   []interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		})
		return
	}

	utils.SuccessData(c, gin.H{
		"status":    "success",
		"ratings":   ratings["ratings"],
		"count":     ratings["count"],
		"avgRating": ratings["avgRating"],
		"minRating": ratings["minRating"],
		"maxRating": ratings["maxRating"],
	})
}

// getIntParam 获取整数参数
func getIntParam(c *gin.Context, key string, defaultValue int) int {
	valueStr := c.DefaultQuery(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil || value < 1 {
		return defaultValue
	}

	return value
}
