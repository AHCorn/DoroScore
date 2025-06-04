package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 应用配置
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	HBase   HBaseConfig   `yaml:"hbase"`
	Cache   CacheConfig   `yaml:"cache"`
	Logging LoggingConfig `yaml:"logging"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port string `yaml:"port"`
}

// HBaseConfig HBase数据库配置
type HBaseConfig struct {
	Host       string `yaml:"host"`
	ZkQuorum   string `yaml:"zk_quorum"`
	ZkPort     string `yaml:"zk_port"`
	MasterPort string `yaml:"master_port"`
	ThriftPort string `yaml:"thrift_port"`
}

// CacheConfig 缓存配置
type CacheConfig struct {
	CleanupInterval   string `yaml:"cleanup_interval"`
	DefaultExpiration string `yaml:"default_expiration"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level     string `yaml:"level"`
	Format    string `yaml:"format"`
	Timestamp bool   `yaml:"timestamp"`
}

var globalConfig *Config

// GetConfig 获取配置（单例模式）
func GetConfig() *Config {
	if globalConfig == nil {
		globalConfig = loadConfig()
	}
	return globalConfig
}

// loadConfig 加载配置
func loadConfig() *Config {
	config := &Config{}

	// 尝试从配置文件加载
	if err := loadFromFile(config, "config.yaml"); err != nil {
		// 如果文件不存在或解析失败，使用默认配置
		config = getDefaultConfig()
	}

	// 环境变量覆盖
	overrideFromEnv(config)

	return config
}

// loadFromFile 从文件加载配置
func loadFromFile(config *Config, filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, config)
}

// overrideFromEnv 环境变量覆盖配置
func overrideFromEnv(config *Config) {
	if port := os.Getenv("SERVER_PORT"); port != "" {
		config.Server.Port = port
	}
	if host := os.Getenv("HBASE_HOST"); host != "" {
		config.HBase.Host = host
	}
	if zkQuorum := os.Getenv("HBASE_ZK_QUORUM"); zkQuorum != "" {
		config.HBase.ZkQuorum = zkQuorum
	}
	if zkPort := os.Getenv("HBASE_ZK_PORT"); zkPort != "" {
		config.HBase.ZkPort = zkPort
	}
}

// getDefaultConfig 获取默认配置
func getDefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: "5000",
		},
		HBase: HBaseConfig{
			Host:       "192.168.2.154",
			ZkQuorum:   "192.168.2.154",
			ZkPort:     "2181",
			MasterPort: "16000",
			ThriftPort: "9090",
		},
		Cache: CacheConfig{
			CleanupInterval:   "5m",
			DefaultExpiration: "10m",
		},
		Logging: LoggingConfig{
			Level:     "info",
			Format:    "text",
			Timestamp: true,
		},
	}
}

// GetCacheCleanupInterval 获取缓存清理间隔
func (c *Config) GetCacheCleanupInterval() time.Duration {
	if dur, err := time.ParseDuration(c.Cache.CleanupInterval); err == nil {
		return dur
	}
	return 5 * time.Minute
}

// GetCacheDefaultExpiration 获取缓存默认过期时间
func (c *Config) GetCacheDefaultExpiration() time.Duration {
	if dur, err := time.ParseDuration(c.Cache.DefaultExpiration); err == nil {
		return dur
	}
	return 10 * time.Minute
}
