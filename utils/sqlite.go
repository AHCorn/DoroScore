package utils

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	_ "modernc.org/sqlite" // 导入纯Go版本的sqlite驱动
)

const dbFile = "./movie_index.db"

var (
	db   *sql.DB
	once sync.Once
)

// InitDB 初始化数据库连接，并创建基础表。
func InitDB() (*sql.DB, error) {
	var err error
	once.Do(func() {
		db, err = sql.Open("sqlite", dbFile)
		if err != nil {
			logrus.Errorf("打开SQLite数据库失败: %v", err)
			return
		}

		if err = createTables(db); err != nil {
			logrus.Errorf("创建表失败: %v", err)
			db.Close()
			db = nil // 失败时将db重置为nil
			return
		}
		logrus.Info("SQLite数据库初始化成功。")
	})

	if err != nil {
		return nil, fmt.Errorf("SQLite初始化失败: %w", err)
	}
	if db == nil {
		return nil, fmt.Errorf("初始化后数据库连接为nil")
	}

	return db, nil
}

// GetDB 返回数据库单例连接。
func GetDB() (*sql.DB, error) {
	if db == nil {
		return InitDB()
	}
	return db, nil
}

// createTables 创建索引所需的基础表。
func createTables(db *sql.DB) error {
	// 电影信息主表，只包含ID和用于索引的标题
	movieIndexTable := `
    CREATE TABLE IF NOT EXISTS movie_index (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id TEXT NOT NULL UNIQUE,
        title TEXT
    );`

	// 注意: FTS5表在构建时动态创建，以优化批量插入性能。
	if _, err := db.Exec(movieIndexTable); err != nil {
		return fmt.Errorf("创建movie_index表失败: %w", err)
	}

	return nil
}

// ResetDatabase 删除数据库文件并重置连接。
func ResetDatabase() error {
	if db != nil {
		if err := db.Close(); err != nil {
			logrus.Warnf("重置前关闭数据库时出错: %v", err)
		}
		db = nil
	}
	// 允许再次执行InitDB中的once.Do
	once = sync.Once{}
	return os.Remove(dbFile)
}
