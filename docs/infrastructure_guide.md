# 🏗️ 基础设施使用指南

## 📋 **使用场景区分**

### 🐳 **Makefile操作（开发环境管理）**

**适用场景：** 本地开发、测试、环境搭建

```bash
# 启动开发环境
make up              # 启动PostgreSQL + PgAdmin + workspace容器

# 初始化本地数据库（第一次使用）
make init-db         # 创建基础表结构

# 直接连接数据库（调试用）
make psql           # 进入PostgreSQL命令行

# 关闭环境
make down           # 停止所有容器
```

### 🚀 **命令行操作（业务数据处理）**

**适用场景：** 数据处理、生产任务、日常业务

```bash
# 数据集管理
spdatalab build-dataset --index-file data.jsonl --dataset-name my_dataset --output dataset.json

# 边界框处理
spdatalab process-bbox --input dataset.json --batch 1000

# 完整工作流程
spdatalab build-dataset-with-bbox --index-file data.jsonl --dataset-name my_dataset --output dataset.json

# 空间连接分析
spdatalab spatial-join --left-table clips_bbox --right-table intersections
```

## 🎯 **核心设计理念**

### ✅ **推荐：直接远端连接**
```python
# 当前生产级方案 - 直接连接远端数据库
from spdatalab.fusion import quick_spatial_join
result, stats = quick_spatial_join(num_bbox=100)
```

### ❌ **不推荐：FDW映射**
- FDW引入额外的网络开销
- 增加了配置复杂性
- 实际性能不如直接连接

## 📂 **SQL文件使用说明**

### 🔧 **主要SQL文件**

| 文件 | 用途 | 何时使用 |
|------|------|----------|
| `cleanup_clips_bbox.sql` | 清理重建clips_bbox表 | **推荐使用** - 完整且有验证 |
| `00_init_local_pg.sql` | 初始化数据库 | **Makefile自动调用** - 首次搭建 |
| `01_fdw_remote.sql` | FDW远程映射 | **建议废弃** - 效果不好 |

### 💡 **最佳实践**

1. **首次环境搭建**：
   ```bash
   make up
   make init-db
   ```

2. **清理表数据**：
   ```bash
   make psql
   \i sql/cleanup_clips_bbox.sql
   ```

3. **日常数据处理**：
   ```bash
   spdatalab build-dataset-with-bbox [参数]
   ```

## 🚫 **废弃功能**

### FDW相关功能标记为废弃
- `make fdw-init` - 不推荐使用
- `01_fdw_remote.sql` - 计划移除
- 原因：性能不佳，增加复杂性

## 🔄 **迁移建议**

如果您当前在使用FDW方式：

1. **停止使用FDW映射**
2. **使用生产级空间连接模块**：
   ```python
   from spdatalab.fusion import quick_spatial_join
   ```
3. **简化数据库配置** 