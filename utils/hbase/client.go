package hbase

import (
	"context"
	"fmt"
	"gohbase/config"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var hbaseClient gohbase.Client

// InitHBase 初始化HBase客户端
func InitHBase(conf *config.HBaseConfig) error {
	// 构建ZooKeeper连接字符串
	zkQuorum := fmt.Sprintf("%s:%s", conf.ZkQuorum, conf.ZkPort)

	// 创建HBase客户端
	hbaseClient = gohbase.NewClient(zkQuorum)

	// 测试连接是否成功
	ctx := context.Background()
	// 尝试获取一条记录来测试连接，使用新的表名和行键格式
	get, err := hrpc.NewGetStr(ctx, "movies", "1_info")
	if err != nil {
		logrus.Errorf("创建Get请求失败: %v", err)
		return err
	}

	_, err = hbaseClient.Get(get)
	if err != nil {
		logrus.Errorf("HBase连接失败: %v", err)
		return err
	}

	logrus.Info("HBase连接成功")
	return nil
}

// GetClient 获取HBase客户端
func GetClient() gohbase.Client {
	return hbaseClient
}

// EnableCompression 启用压缩
func EnableCompression(compression string) error {
	// 这里添加压缩相关功能
	return nil
}
