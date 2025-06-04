package services

import (
	"gohbase/models"
)

// MovieService 电影服务接口
type MovieService interface {
	GetMoviesList(page, perPage int) (*models.MovieList, error)
	GetMovieByID(movieID string) (*models.MovieDetail, error)
	GetRandomMovies(count int) ([]models.Movie, error)
	SearchMovies(query string, page, perPage int) (*models.MovieList, error)
	GetMovieRatings(movieID string) (map[string]interface{}, error)
}

// movieService 电影服务实现
type movieService struct{}

// NewMovieService 创建电影服务实例
func NewMovieService() MovieService {
	return &movieService{}
}

// GetMoviesList 获取电影列表
func (s *movieService) GetMoviesList(page, perPage int) (*models.MovieList, error) {
	return models.GetMoviesList(page, perPage)
}

// GetMovieByID 获取电影详情
func (s *movieService) GetMovieByID(movieID string) (*models.MovieDetail, error) {
	return models.GetMovieByID(movieID)
}

// GetRandomMovies 获取随机电影
func (s *movieService) GetRandomMovies(count int) ([]models.Movie, error) {
	return models.GetRandomMovies(count)
}

// SearchMovies 搜索电影
func (s *movieService) SearchMovies(query string, page, perPage int) (*models.MovieList, error) {
	return models.SearchMovies(query, page, perPage)
}

// GetMovieRatings 获取电影评分
func (s *movieService) GetMovieRatings(movieID string) (map[string]interface{}, error) {
	return models.GetMovieRatings(movieID)
}
