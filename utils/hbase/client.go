package hbase

import (
	"context"
	"fmt"
	"gohbase/config"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var (
	hbaseClient gohbase.Client
	clientMu    sync.RWMutex

	// 连接池相关
	clientPool   []gohbase.Client
	poolSize     int = 5
	poolMu       sync.Mutex
	currentIndex int
)

// InitHBase 初始化HBase客户端和连接池
func InitHBase(conf *config.HBaseConfig) error {
	// 构建ZooKeeper连接字符串
	zkQuorum := fmt.Sprintf("%s:%s", conf.ZkQuorum, conf.ZkPort)

	// 创建主客户端
	hbaseClient = gohbase.NewClient(zkQuorum)

	// 初始化连接池
	clientPool = make([]gohbase.Client, poolSize)
	for i := 0; i < poolSize; i++ {
		clientPool[i] = gohbase.NewClient(zkQuorum)
	}

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

	logrus.Infof("HBase连接成功，连接池大小: %d", poolSize)
	return nil
}

// GetClient 获取HBase客户端
func GetClient() gohbase.Client {
	clientMu.RLock()
	defer clientMu.RUnlock()
	return hbaseClient
}

// GetPooledClient 从连接池获取客户端（用于高并发场景）
func GetPooledClient() gohbase.Client {
	poolMu.Lock()
	defer poolMu.Unlock()

	client := clientPool[currentIndex]
	currentIndex = (currentIndex + 1) % poolSize
	return client
}

// BatchPut 批量写入操作
func BatchPut(ctx context.Context, tableName string, puts []*hrpc.Mutate) error {
	if len(puts) == 0 {
		return nil
	}

	client := GetPooledClient()

	// 分批处理，每批最多100个操作
	batchSize := 100
	for i := 0; i < len(puts); i += batchSize {
		end := i + batchSize
		if end > len(puts) {
			end = len(puts)
		}

		batch := puts[i:end]
		for _, put := range batch {
			_, err := client.Put(put)
			if err != nil {
				logrus.Errorf("批量写入失败: %v", err)
				return err
			}
		}

		// 添加小延迟，避免过度压力
		if len(puts) > batchSize {
			time.Sleep(10 * time.Millisecond)
		}
	}

	return nil
}

// EnableCompression 启用压缩
func EnableCompression(compression string) error {
	// 这里添加压缩相关功能
	return nil
}

// CloseClients 关闭所有客户端连接
func CloseClients() {
	clientMu.Lock()
	defer clientMu.Unlock()

	if hbaseClient != nil {
		hbaseClient.Close()
	}

	poolMu.Lock()
	defer poolMu.Unlock()

	for _, client := range clientPool {
		if client != nil {
			client.Close()
		}
	}
}
