<div align="center">

# <img style="width:60px;height:60px;" src="https://github.com/user-attachments/assets/9d187e0a-0b50-4756-a3e7-3de6bd3ae7a1"> DoroScore
#### **简体中文** | <a href="https://github.com/AHCorn/DoroScore/blob/main/README_EN.md"> English </a>

与我的 Hbase 数据库作业通信的桥梁

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)
![GitHub stars](https://img.shields.io/github/stars/ahcorn/doroscore?style=for-the-badge)
![GitHub issues](https://img.shields.io/github/issues/ahcorn/doroscore?style=for-the-badge)
![GitHub pull requests](https://img.shields.io/github/issues-pr/ahcorn/doroscore?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/ahcorn/doroscore?style=for-the-badge)

</div>



## 📕 使用说明

这个是我的作业，可以参考，想直接用的话得自己建一个对应的表

要启动的话，安好依赖直接 ``` go run main.go ``` 就可以了

默认运行在本机 5000 端口

### 接口信息
- `GET /api/movies` - 获取电影列表
- `GET /api/movies/:id` - 获取电影详情
- `GET /api/movies/random` - 获取随机电影
- `POST /api/movies/random` - 获取随机电影
- `GET /api/movies/search` - 搜索电影
- `GET /api/ratings/movie/:id` - 获取电影评分
- `GET /api/system/logs` - 获取系统日志
- `GET /api/system/cache` - 获取缓存统计信息 

<br>

## 🗒 备注

名字是随便起的，非商业用途，若有侵权请联系删除。
