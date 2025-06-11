# DoroScore Spark 批处理模块

本模块用于对 HBase 中的电影评分数据进行批处理计算，主要功能包括：

- 从 HBase 读取电影评分数据
- 使用 Spark 计算每部电影的平均评分
- 将计算结果更新回 HBase 的统计数据中

## 项目结构

```
Spark/
├── pom.xml                         # Maven 配置文件
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── doroscore/
│   │   │           └── MovieRatingProcessor.java  # 主程序类
│   │   └── resources/
│   │       └── log4j.properties    # 日志配置文件
└── README.md                       # 项目说明文档
```

## 环境要求

- JDK 8+
- Maven 3.6+
- Spark 3.3.x
- HBase 2.4.x
- 已部署的 HBase 服务

## 配置说明

在运行前需要修改以下配置：

1. `MovieRatingProcessor.java` 中的 HBase 连接配置
   ```java
   hbaseConf.set("hbase.zookeeper.quorum", "localhost"); // 修改为实际的 ZooKeeper 地址
   hbaseConf.set("hbase.zookeeper.property.clientPort", "2181"); // 修改为实际的端口
   ```

2. 根据实际需求调整 Spark 配置
   ```java
   SparkConf sparkConf = new SparkConf()
           .setAppName("DoroScore Movie Rating Calculator")
           .setMaster("local[*]"); // 生产环境改为集群地址
   ```

## 构建和运行

### 构建项目

```bash
cd Spark
mvn clean package
```

### 本地运行

```bash
java -jar target/movie-rating-processor-1.0-SNAPSHOT.jar
```

### 在 Spark 集群上运行

```bash
spark-submit --class com.doroscore.MovieRatingProcessor \
  --master yarn \
  --deploy-mode cluster \
  target/movie-rating-processor-1.0-SNAPSHOT.jar
```

## 数据处理流程

1. 从 HBase 的 `movies` 表读取所有 `{movieId}_ratings` 行
2. 解析每个评分数据 (格式: `{rating}:{userId}:{timestamp}`)
3. 按电影 ID 分组，计算平均评分和评分数量
4. 将计算结果写回 HBase 的 `{movieId}_stats` 行中

## 结果验证

处理完成后，可以通过 HBase Shell 验证结果：

```bash
echo "scan 'movies', { FILTER => \"PrefixFilter('_stats')\" }" | hbase shell
```

## 定期执行

建议通过 cron 或其他调度系统定期执行此批处理任务，以保持统计数据的更新。 