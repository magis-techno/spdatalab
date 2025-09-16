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

## 🚀 快速开始

### 1. 基础使用

```bash
# 运行默认分析
cd /path/to/spdatalab
python examples/dataset/bbox_examples/bbox_overlap_analysis.py

# 指定城市和参数
python examples/dataset/bbox_examples/bbox_overlap_analysis.py \
    --city beijing \
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
| `--city` | string | None | 城市过滤条件 |
| `--subdatasets` | list | None | 子数据集过滤列表 |
| `--min-overlap-area` | float | 0.0 | 最小重叠面积阈值（平方度） |
| `--top-n` | int | 20 | 返回的热点数量 |
| `--analysis-id` | string | 自动生成 | 自定义分析ID |

### 分析参数解释

- **最小重叠面积**：过滤掉面积太小的重叠区域，避免噪音
- **网格精度**：0.001度（约100米），用于聚合相邻重叠区域
- **热点阈值**：最少2个重叠才认定为热点
- **排序规则**：按重叠数量降序，相同时按面积降序

## 📊 输出结果

### 1. 数据库表

**主结果表**：`bbox_overlap_analysis_results`
- 存储所有分析结果和详细统计
- 包含几何数据和JSON参数
- 支持历史查询和对比分析

**QGIS视图**：
- `qgis_bbox_overlap_hotspots` - 主热点视图
- `qgis_bbox_overlap_summary` - 汇总统计视图  
- `qgis_bbox_overlap_details` - 详细信息视图

### 2. 控制台输出

```
✅ 叠置分析完成，发现 15 个重叠热点

TOP 10 重叠热点:
   hotspot_rank  overlap_count  total_overlap_area  subdataset_count  scene_count density_level
0             1             12              0.0045                 4           18   High Density
1             2              8              0.0032                 3           12 Medium Density
2             3              6              0.0028                 2            9 Medium Density
...

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
```

## 🎨 QGIS可视化指南

### 数据库连接

1. 打开QGIS Desktop
2. 浏览器面板 → PostgreSQL → 新建连接
3. 输入连接参数（见上方输出）
4. 测试连接并保存

### 推荐图层顺序

1. **底图**：`clips_bbox_unified_qgis`
   - 显示所有bbox作为背景
   - 浅色填充，细边框
   - 透明度30%

2. **热点区域**：`qgis_bbox_overlap_hotspots` 
   - 按`density_level`分类着色
   - 主要分析图层
   - 过滤：`analysis_id = 'your_analysis_id'`

3. **热点详情**：`qgis_bbox_overlap_details`
   - 按`overlap_count`分级符号
   - 圆形符号，大小5-25像素
   - 用于详细分析

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
