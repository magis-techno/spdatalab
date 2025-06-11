# Makefile 使用指南

## 概述

项目中的Makefile提供了完整的开发环境管理和Sprint 2功能的便捷命令。所有复杂的配置都通过`.env`文件管理，使用起来非常简单。

## 🚀 快速开始

### 1. 环境配置

```bash
# 1. 复制配置模板
cp env.example .env

# 2. 编辑配置文件（设置你的实际参数）
vim .env  # 或使用其他编辑器

# 3. 启动开发环境
make up

# 4. 查看所有可用命令
make help
```

### 2. 配置示例

编辑`.env`文件，设置你的实际参数：

```bash
# FDW远程数据库配置
REMOTE_TRAJ_HOST=192.168.1.100
REMOTE_MAP_HOST=192.168.1.101
FDW_USER=myuser
FDW_PASSWORD=mypassword

# 测试配置
TEST_DATASET_FILE=data/my_dataset.json
TEST_BATCH_SIZE=100
TEST_INSERT_BATCH_SIZE=50
```

## 📋 命令分类

### 🐳 环境管理

| 命令 | 功能 | 用途 |
|------|------|------|
| `make up` | 启动开发环境 | 启动PostgreSQL、PgAdmin等服务 |
| `make down` | 停止开发环境 | 停止所有Docker服务 |
| `make init-db` | 初始化数据库 | 首次使用时创建数据库结构 |
| `make clean-bbox` | 清理bbox表 | 清理clips_bbox相关数据 |
| `make psql` | 进入数据库命令行 | 调试和查询数据库 |

### 🔗 FDW远程连接管理

| 命令 | 功能 | 前置条件 |
|------|------|----------|
| `make setup-fdw` | 设置FDW连接 | 需要配置.env文件 |
| `make cleanup-fdw` | 清理FDW连接 | 无 |
| `make test-fdw` | 测试FDW连接 | FDW已设置 |

### 🎯 Sprint 2测试和演示

| 命令 | 功能 | 前置条件 |
|------|------|----------|
| `make demo-sprint2` | 运行演示脚本 | 无（仅展示命令格式） |
| `make test-sprint2` | 运行功能测试 | 需要测试数据集文件 |
| `make test-sprint2-full` | 运行完整测试 | 需要数据库连接和测试数据 |
| `make cleanup-sprint2` | 清理测试资源 | 无 |

### 📊 分表模式操作

| 命令 | 功能 | 前置条件 |
|------|------|----------|
| `make list-tables` | 列出bbox表 | 数据库连接 |
| `make create-view` | 创建统一视图 | 存在分表 |
| `make maintain-view` | 维护统一视图 | 无 |
| `make process-partitioned` | 分表模式处理 | 测试数据集文件 |

## 💡 使用场景

### 场景1: 首次环境搭建

```bash
# 1. 配置环境
cp env.example .env
vim .env  # 设置你的参数

# 2. 启动环境
make up

# 3. 初始化数据库
make init-db

# 4. 设置FDW连接（如果需要）
make setup-fdw

# 5. 测试连接
make test-fdw
```

### 场景2: Sprint 2功能测试

```bash
# 1. 确保环境配置正确
make up

# 2. 运行演示（了解功能）
make demo-sprint2

# 3. 运行功能测试
make test-sprint2

# 4. 运行完整测试（如果有数据）
make test-sprint2-full
```

### 场景3: 分表模式数据处理

```bash
# 1. 查看当前表状态
make list-tables

# 2. 处理数据（自动创建分表）
make process-partitioned

# 3. 创建统一视图
make create-view

# 4. 验证结果
make list-tables
```

### 场景4: 问题排查和清理

```bash
# 1. 进入数据库调试
make psql

# 2. 清理测试资源
make cleanup-sprint2

# 3. 清理FDW连接
make cleanup-fdw

# 4. 清理bbox数据
make clean-bbox
```

## ⚙️ 环境变量说明

### 必需配置

```bash
# FDW连接参数（如果使用远程数据库）
FDW_USER=your_username
FDW_PASSWORD=your_password
REMOTE_TRAJ_HOST=your_traj_host
REMOTE_MAP_HOST=your_map_host

# 测试数据文件（如果运行测试）
TEST_DATASET_FILE=path/to/your/dataset.json
```

### 可选配置

```bash
# 测试参数调优
TEST_BATCH_SIZE=100           # 处理批次大小
TEST_INSERT_BATCH_SIZE=50     # 插入批次大小

# 开发配置
PYTHONPATH=src               # Python模块路径
SPDATALAB_LOG_LEVEL=INFO     # 日志级别
```

## 🔧 故障排除

### 常见问题

1. **`.env文件不存在`**
   ```bash
   # 解决方法
   cp env.example .env
   vim .env  # 编辑配置
   ```

2. **FDW连接失败**
   ```bash
   # 检查网络连接
   ping your_remote_host
   
   # 检查配置
   cat .env | grep REMOTE
   
   # 重新设置
   make cleanup-fdw
   make setup-fdw
   ```

3. **测试数据文件不存在**
   ```bash
   # 检查文件路径
   ls -la $TEST_DATASET_FILE
   
   # 修改.env中的路径
   vim .env
   ```

4. **权限问题**
   ```bash
   # 检查Docker权限
   docker ps
   
   # 重启环境
   make down
   make up
   ```

### 调试技巧

```bash
# 1. 查看环境变量
make psql
\! env | grep -E "(FDW|TEST|REMOTE)"

# 2. 检查数据库连接
make test-fdw

# 3. 查看详细日志
docker compose -f docker/docker-compose.yml logs workspace

# 4. 手动运行命令
docker compose -f docker/docker-compose.yml exec workspace bash
# 然后在容器中运行Python命令
```

## 📝 最佳实践

1. **配置管理**
   - 始终使用`.env`文件管理配置
   - 不要将敏感信息提交到git
   - 定期备份配置文件

2. **测试流程**
   - 先运行`demo-sprint2`了解功能
   - 使用小数据集进行初始测试
   - 逐步扩大测试规模

3. **开发调试**
   - 使用`make psql`进行数据库调试
   - 查看容器日志排查问题
   - 及时清理测试资源

4. **生产部署**
   - 仔细配置生产环境参数
   - 测试FDW连接稳定性
   - 监控分表处理性能

---

*更新时间: 2024-12-19*
*适用版本: Sprint 2* 