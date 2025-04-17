package config

// Config 应用配置
type Config struct {
	HBase  HBaseConfig
	Server ServerConfig
}

// HBaseConfig HBase数据库配置
type HBaseConfig struct {
	Host       string
	ZkQuorum   string
	ZkPort     string
	MasterPort string
	ThriftPort string
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port string
}

// GetConfig 获取配置
func GetConfig() *Config {
	return &Config{
		HBase: HBaseConfig{
			Host:       "192.168.2.154",
			ZkQuorum:   "192.168.2.154",
			ZkPort:     "2181",
			MasterPort: "16000",
			ThriftPort: "9090",
		},
		Server: ServerConfig{
			Port: "5000",
		},
	}
}
