# BBox叠置分析示例

这个目录包含了bbox数据空间叠置分析的完整示例，从数据分析到QGIS可视化的全流程。

## 📁 目录结构

```
bbox_examples/
├── README.md                           # 本文档
├── bbox_overlap_analysis.py            # 主分析脚本
├── sql/                                # SQL脚本集合
│   ├── create_analysis_tables.sql      # 创建分析结果表
│   ├── overlap_analysis.sql            # 核心叠置分析查询
│   └── qgis_views.sql                  # QGIS兼容视图
└── (生成的样式文件会在运行后出现)
```

## 🎯 功能特性

### 🔍 空间叠置分析
- **智能重叠检测**：基于PostGIS空间函数的高效重叠检测
- **热点识别**：自动识别重叠数量最高的区域
- **多维度统计**：支持按城市、数据集、时间等维度过滤分析
- **可配置阈值**：支持自定义最小重叠面积等参数

### 📊 结果存储
- **专业表结构**：遵循数据库最佳实践的结果存储
- **完整索引**：优化查询性能的多维度索引
- **JSON参数存储**：完整记录分析参数便于重现
- **时间序列支持**：支持历史分析对比

### 🎨 QGIS集成
- **即插即用**：自动生成QGIS兼容的视图
- **多层次可视化**：底图、热点、详情三层展示
- **智能分级**：自动按密度和面积分级
- **专业样式**：内置推荐的颜色方案和符号设置
- **🛡️ 优雅退出**：支持 `Ctrl+C` 安全中断，自动清理资源

## 🚀 快速开始

### 1. 基础使用

```bash
# 1️⃣ 首先查看城市建议（推荐）
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --suggest-city

# 2️⃣ 估算特定城市的分析时间
python examples/dataset/bbox_examples/bbox_overlap_analysis.py \
    --city beijing --estimate-time

# 3️⃣ 执行指定城市的分析（推荐方式）
python examples/dataset/bbox_examples/bbox_overlap_analysis.py \
    --city beijing \
    --min-overlap-area 0.0001 \
    --top-n 15

# 🎯 简化模式：只要相交就算重叠（忽略面积阈值）
python examples/dataset/bbox_examples/bbox_overlap_analysis.py \
    --city beijing \
    --intersect-only \
    --top-n 15

# ⚠️ 全量分析（不推荐，可能耗时很久）
python examples/dataset/bbox_examples/bbox_overlap_analysis.py \
    --min-overlap-area 0.0001 \
    --top-n 20
```

### 2. QGIS可视化

```bash
# 运行QGIS可视化指南（包含演示分析）
python examples/visualization/qgis_bbox_overlap_guide.py --demo-mode

# 基于现有分析生成QGIS指南
python examples/visualization/qgis_bbox_overlap_guide.py \
    --analysis-id your_analysis_id
```

### 3. 手动SQL执行

如果你更喜欢直接使用SQL：

```bash
# 1. 创建表结构
psql -d postgres -f sql/create_analysis_tables.sql

# 2. 手动编辑 sql/overlap_analysis.sql 中的参数
# 3. 执行分析
psql -d postgres -f sql/overlap_analysis.sql

# 4. 创建QGIS视图
psql -d postgres -f sql/qgis_views.sql
```

## 📋 参数说明

### 命令行参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--city` | string | None | 城市过滤条件（🎯 强烈推荐） |
| `--subdatasets` | list | None | 子数据集过滤列表 |
| `--min-overlap-area` | float | 0.0 | 最小重叠面积阈值（平方度） |
| `--top-n` | int | 20 | 返回的热点数量 |
| `--analysis-id` | string | 自动生成 | 自定义分析ID |
| `--suggest-city` | flag | False | 显示城市分析建议并退出 |
| `--estimate-time` | flag | False | 估算分析时间并退出 |
| `--refresh-view` | flag | False | 强制刷新统一视图 |
| `--intersect-only` | flag | False | 🎯 简化模式：只要相交就算重叠 |

### 分析参数解释

- **最小重叠面积**：过滤掉面积太小的重叠区域，避免噪音
- **网格精度**：0.001度（约100米），用于聚合相邻重叠区域
- **热点阈值**：最少2个重叠才认定为热点
- **排序规则**：按重叠数量降序，相同时按面积降序

### 🎯 核心优化条件

- **相同城市约束**：`a.city_id = b.city_id` - 只分析同一城市内的bbox重叠
- **数据质量过滤**：`a.all_good = true AND b.all_good = true` - 只分析质量合格的数据
- **几何有效性**：排除完全相同的几何对象，确保是真实的重叠而非重复数据
- **空值处理**：排除city_id为NULL的记录，确保地理位置明确

### 🎯 简化重叠模式

当使用`--intersect-only`参数时，启用简化重叠检测：

**标准模式**：
```sql
WHERE ST_Intersects(a.geometry, b.geometry)
AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > min_overlap_area
```

**简化模式**：
```sql
WHERE ST_Intersects(a.geometry, b.geometry)
-- 忽略面积阈值条件
```

**适用场景**：
- 🔍 **调试分析**：快速检查是否有任何空间重叠
- 🚀 **初步探索**：了解数据分布和重叠模式
- ⚡ **性能优化**：减少复杂的面积计算
- 🧪 **问题排查**：排除面积阈值过滤导致的结果为空

**使用建议**：
```bash
# 先用简化模式快速检查
python bbox_overlap_analysis.py --city A263 --intersect-only --top-n 5

# 如果有结果，再用标准模式精细分析
python bbox_overlap_analysis.py --city A263 --min-overlap-area 0.0001 --top-n 20
```

### ⚡ 性能优化建议

| 数据量范围 | 预估时间 | 推荐做法 | 示例命令 |
|------------|----------|----------|----------|
| < 1,000 | 🚀 很快 (<30秒) | 直接分析 | `--city small_city` |
| 1,000-10,000 | ⚡ 较快 (<2分钟) | 指定城市 | `--city medium_city` |
| 10,000-50,000 | ⏰ 中等 (2-10分钟) | 指定城市 | `--city large_city` |
| 50,000-100,000 | ⏳ 较长 (10-30分钟) | 分批分析 | `--city huge_city --top-n 10` |
| > 100,000 | ⚠️ 很长 (>30分钟) | 强烈建议分城市 | `--suggest-city` |

**💡 最佳实践**：
1. 总是先运行 `--suggest-city` 查看推荐城市
2. 使用 `--estimate-time` 预估分析时间
3. 优先分析数据量适中、质量较高的城市
4. 避免全量分析，除非确实需要
5. **长时间分析可使用 `Ctrl+C` 安全退出**

### 🛡️ 优雅退出功能

当分析任务运行时间较长时，支持安全中断：

```bash
# 启动分析
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --city beijing

# 输出示例：
# 🚀 开始叠置分析: bbox_overlap_20231217_143025
# 💡 可以使用 Ctrl+C 安全退出
# ⚡ 执行空间叠置分析SQL...

# 使用 Ctrl+C 中断时：
# 🛑 收到退出信号 (SIGINT)
# 🔄 正在安全退出...
# 📝 当前分析ID: bbox_overlap_20231217_143025
# ⏱️ 已运行时间: 0:02:15.123456
# 🧹 清理资源中...
# ✅ 数据库连接已关闭
# ✅ 优雅退出完成
```

**退出处理特性**：
- ✅ **安全清理**：自动关闭数据库连接
- ✅ **状态保存**：显示当前分析ID和运行时间
- ✅ **跨平台支持**：Windows、Linux、MacOS
- ✅ **多信号支持**：`SIGINT`、`SIGTERM`、`SIGBREAK`
- ✅ **资源监控**：确保无资源泄漏

## 🧹 数据清理管理

### 清理功能概览

为方便管理分析结果和释放存储空间，提供了完整的清理工具：

#### **1. 列出分析结果**
```bash
# 列出所有分析结果
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --list-results

# 按模式过滤
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --list-results --cleanup-pattern "test%"
```

#### **2. 清理分析结果**
```bash
# 试运行模式（安全预览）
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup --cleanup-pattern "test%"

# 实际执行清理
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup --cleanup-pattern "test%" --confirm-cleanup

# 按ID清理
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup --cleanup-ids "bbox_overlap_20231201_100000" --confirm-cleanup

# 清理7天前的数据
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup --cleanup-older-than 7 --confirm-cleanup
```

#### **3. 清理QGIS视图**
```bash
# 试运行模式
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup-views

# 实际执行
python examples/dataset/bbox_examples/bbox_overlap_analysis.py --cleanup-views --confirm-cleanup
```

### 专用清理工具

提供了独立的清理脚本：`cleanup_analysis_data.py`

```bash
# 列出所有分析结果
python examples/dataset/bbox_examples/cleanup_analysis_data.py --list

# 按模式清理（试运行）
python examples/dataset/bbox_examples/cleanup_analysis_data.py --cleanup-results --pattern "test%" --dry-run

# 实际执行清理
python examples/dataset/bbox_examples/cleanup_analysis_data.py --cleanup-results --pattern "test%" --confirm

# 清理QGIS视图
python examples/dataset/bbox_examples/cleanup_analysis_data.py --cleanup-views --confirm
```

### 安全特性

- 🛡️ **默认试运行**：所有清理操作默认为预览模式
- 📋 **详细预览**：显示将要删除的具体内容
- 🔍 **多种过滤**：支持按ID、模式、时间过滤
- ✅ **确认机制**：必须明确使用`--confirm`才实际删除
- 📊 **清理统计**：显示详细的删除统计信息

## 📊 输出结果

### 1. 数据库表

**主结果表**：`bbox_overlap_analysis_results`
- 存储所有分析结果和详细统计
- 包含几何数据和JSON参数
- 支持历史查询和对比分析

**QGIS视图架构**：

🏗️ **基础数据层**（由bbox.py管理）：
- `clips_bbox_unified_qgis` - 原始bbox数据统一视图（复用bbox.py中的标准视图）

📊 **分析结果层**（由叠置分析创建）：
- `qgis_bbox_overlap_hotspots` - 主热点视图
- `qgis_bbox_overlap_summary` - 汇总统计视图  
- `qgis_bbox_overlap_details` - 详细信息视图

### 2. 控制台输出

```
📋 发现 8 个bbox分表
📊 统一视图包含 3,022,339 条bbox记录
📈 数据概况: 15 个子数据集, 6 个城市
📊 质量分布: 2,845,221 合格 (94.1%), 177,118 不合格
🏙️ TOP 5城市质量分布:
   shanghai: 1,245,880/1,320,156 (94.4%)
   beijing: 892,445/951,022 (93.8%)
   guangzhou: 458,332/487,651 (94.0%)
   shenzhen: 248,564/263,510 (94.3%)
💡 只有all_good=true的数据会参与叠置分析

✅ 叠置分析完成，发现 12 个重叠热点

TOP 5 重叠热点:
   hotspot_rank  overlap_count  total_overlap_area  subdataset_count  scene_count density_level
0             1             15              0.0067                 4           22   High Density
1             2             11              0.0043                 3           18   High Density
2             3              8              0.0035                 3           14 Medium Density
3             4              6              0.0028                 2           11 Medium Density
4             5              4              0.0018                 2            8 Low Density

📋 数据库连接信息:
   host: local_pg
   port: 5432
   database: postgres
   username: postgres

🎨 可视化建议:
   • 主键: qgis_id
   • 几何列: geometry
   • 按 density_level 字段设置颜色
   • 显示 overlap_count 标签
   • 使用 analysis_id = 'your_analysis_id' 过滤
```

## 🎨 QGIS可视化指南

### 数据库连接

1. 打开QGIS Desktop
2. 浏览器面板 → PostgreSQL → 新建连接
3. 输入连接参数（见上方输出）
4. 测试连接并保存

### 推荐图层顺序

1. **底图**：`clips_bbox_unified_qgis` 🏗️
   - **来源**：复用bbox.py中的标准统一视图
   - **用途**：显示所有bbox作为背景参考
   - **样式**：浅色填充，细边框，透明度30%
   - **主键**：qgis_id

2. **热点区域**：`qgis_bbox_overlap_hotspots` 📊 
   - **来源**：叠置分析结果专用视图
   - **用途**：按`density_level`分类着色的主要分析图层
   - **过滤**：`analysis_id = 'your_analysis_id'`
   - **主键**：qgis_id

3. **热点详情**：`qgis_bbox_overlap_details` 🔍
   - **来源**：分析结果详情视图
   - **用途**：按`overlap_count`分级符号的详细分析
   - **样式**：圆形符号，大小5-25像素
   - **主键**：qgis_id

### 样式配置

**密度等级颜色方案**：
- Very High Density: 深红色 (#8B0000)
- High Density: 红色 (#DC143C)  
- Medium Density: 橙色 (#FF8C00)
- Low Density: 黄色 (#FFD700)
- Single Overlap: 浅黄色 (#FFFFE0)

**标签设置**：
- 主标签：`overlap_count` 字段
- 辅助标签：`rank_label` 表达式
- 位置：几何中心，背景框

## 🔧 高级用法

### 1. 批量分析

```python
from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer

analyzer = BBoxOverlapAnalyzer()

# 为不同城市运行分析
cities = ['beijing', 'shanghai', 'guangzhou']
for city in cities:
    analysis_id = analyzer.run_overlap_analysis(
        city_filter=city,
        analysis_id=f"overlap_{city}_20241201"
    )
    print(f"完成 {city} 分析: {analysis_id}")
```

### 2. 自定义SQL分析

你可以修改 `sql/overlap_analysis.sql` 来实现自定义的分析逻辑：

```sql
-- 例：只分析特定数据集的重叠
WHERE a.subdataset_name IN ('lane_change', 'overtaking')
  AND b.subdataset_name IN ('lane_change', 'overtaking')

-- 例：按时间范围过滤
WHERE a.timestamp BETWEEN 1640995200 AND 1672531199  -- 2022年

-- 例：更严格的重叠面积阈值
AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0.001
```

### 3. 结果导出

```python
# 导出为GeoJSON
import geopandas as gpd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg://postgres:postgres@local_pg:5432/postgres")
gdf = gpd.read_postgis(
    "SELECT * FROM qgis_bbox_overlap_hotspots WHERE analysis_id = 'your_id'",
    engine,
    geom_col='geometry'
)
gdf.to_file("overlap_hotspots.geojson", driver="GeoJSON")
```

## 🐛 故障排除

### 常见问题

1. **统一视图不存在**
   ```
   解决：运行脚本会自动创建，或手动执行bbox.py中的create_qgis_compatible_unified_view()
   ```

2. **没有找到重叠**
   ```
   解决：降低min_overlap_area阈值，检查数据是否在同一坐标系
   ```

3. **QGIS连接失败**
   ```
   解决：检查Docker容器状态，确认端口5432未被占用
   ```

4. **几何数据无效**
   ```
   解决：在分析前运行ST_IsValid检查，使用ST_MakeValid修复
   ```

### 性能优化

1. **大数据量处理**：
   - 使用城市或区域过滤减少数据量
   - 调整网格精度参数
   - 考虑使用空间索引

2. **内存优化**：
   - 分批处理大数据集
   - 定期清理临时分析结果
   - 使用合适的top_n限制

## 🤝 贡献

欢迎提交改进建议和bug报告！

### 扩展想法

- [ ] 支持时间序列重叠分析
- [ ] 添加重叠形状复杂度分析
- [ ] 集成更多可视化样式模板
- [ ] 支持导出为Web地图

## 📚 相关文档

- [BBox模块文档](../../../docs/dataset_management.md)
- [空间分析指南](../../../docs/spatial_join_usage_guide.md)
- [QGIS可视化指南](../../visualization/)
- [数据库连接配置](../../../sql/README_FDW.md)

---

**最后更新**: 2024年12月
**维护者**: spdatalab团队
