package routes

import (
	"gohbase/controllers"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// SetupRouter 设置路由
func SetupRouter() *gin.Engine {
	// 创建默认路由
	router := gin.Default()

	// 静态文件服务
	router.Static("/static", "./static")
	router.GET("/test-dashboard", func(c *gin.Context) {
		c.File("./static/test-dashboard.html")
	})
	router.GET("/hotness-dashboard", func(c *gin.Context) {
		c.File("./static/hotness-dashboard.html")
	})

	// 添加CORS中间件
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Cache-Check", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "X-Cache-Hit"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// 创建API路由组
	api := router.Group("/api")

	// 创建控制器实例
	movieController := controllers.NewMovieController()
	systemController := controllers.NewSystemController()
	testController := controllers.NewTestController()
	hotnessController := controllers.NewHotnessController()

	// 电影相关路由
	movies := api.Group("/movies")
	{
		movies.GET("", movieController.GetMovies)
		movies.GET("/:id", movieController.GetMovie)
		movies.GET("/random", movieController.GetRandomMovies)
		movies.POST("/random", movieController.RandomMoviesPost)
		movies.GET("/search", movieController.SearchMovies)
	}

	// 评分相关路由
	ratings := api.Group("/ratings")
	{
		ratings.GET("/movie/:id", movieController.GetMovieRatings)
	}

	// 系统相关路由
	system := api.Group("/system")
	{
		system.GET("/logs", systemController.GetSystemLogs)
		system.GET("/cache", systemController.GetCacheStats)
		system.POST("/search-index/build", systemController.BuildSearchIndex)
		system.GET("/search-index/stats", systemController.GetSearchIndexStats)

		// 性能监控和诊断
		system.GET("/performance", systemController.GetHBasePerformanceStats)
		system.GET("/diagnostics", systemController.GetHBaseDiagnostics)
		system.POST("/gc", systemController.ForceGC)
	}

	// 测试相关路由
	test := api.Group("/test")
	{
		// 随机写入控制
		test.POST("/ratings/start", testController.StartRandomRatings)
		test.POST("/ratings/stop", testController.StopRandomRatings)
		test.GET("/ratings/status", testController.GetRandomRatingsStatus)
		test.GET("/ratings/logs", testController.GetRandomRatingsLogs)

		// 单次操作
		test.POST("/ratings/movie/:id", testController.GenerateRandomRatingsForMovie)
		test.DELETE("/ratings/movie/:id", testController.ClearMovieRatings)
	}

	// 热度相关路由
	hotness := api.Group("/hotness")
	{
		hotness.GET("/movies", hotnessController.GetHotMovies)
		hotness.GET("/movie/:id", hotnessController.GetMovieHotness)
		hotness.GET("/movie/:id/threshold", hotnessController.GetMovieRatingThreshold)
		hotness.GET("/stats", hotnessController.GetWriteStats)
		hotness.GET("/writes", hotnessController.GetRecentWrites)
		hotness.GET("/ranking", hotnessController.GetHotnessRanking)
		hotness.GET("/trends", hotnessController.GetHotnessTrends)
	}

	return router
}
