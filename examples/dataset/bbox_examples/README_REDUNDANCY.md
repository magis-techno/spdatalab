# 空间冗余分析使用指南

## 快速开始

### 1. 首次使用：创建表
```bash
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --create-table
```

### 2. 生成网格数据
```bash
python examples/dataset/bbox_examples/batch_grid_analysis.py
```

### 3. 运行冗余分析
```bash
# 默认分析（top 1%，自动使用最新日期，快速模式）
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py

# 按 scene 数量排序（会增加几秒启动时间）
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --sort-by-scenes

# 分析 top 5%
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --top-percent 5

# 指定分析日期（分析历史数据）
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --analysis-date 2025-10-09

# 指定城市
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --cities A263 B001

# 导出 CSV
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --export-csv

# 组合使用
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --sort-by-scenes --analysis-date 2025-10-09 --export-csv
```

---

## 输出解读

### 单个城市结果
```
✓ A263: 冗余指数 16.9 (1.0%面积[10/1000grid] → 16.9%场景[3380/20000])
```

**含义**：
- **冗余指数 16.9**：数据集中度较高
- **1.0%面积**：top 1% 的网格占总网格面积的比例
- **10/1000grid**：top 1% = 10 个网格（总共 1000 个）
- **16.9%场景**：这 10 个网格包含了 16.9% 的场景
- **3380/20000**：包含 3380 个场景（总共 20000 个）

### 冗余指数分级
| 冗余指数 | 级别 | 含义 |
|---------|------|------|
| < 5 | 低 | 数据分布均匀 |
| 5-10 | 正常 | 适度集中 |
| 10-20 | 中等 | 较为集中 |
| ≥ 20 | 高 | 严重集中 |

---

## 计算方法

### 核心公式
```
area_pct = (top_n_grids / total_grids) × 100%
scene_pct = (hotspot_scenes / total_scenes) × 100%
redundancy_index = scene_pct / area_pct
```

### 面积计算
- **单个 grid**：0.002° × 0.002° ≈ 0.049 km² (约 200m × 200m)
- **总面积**：total_grids × 0.049 km²
- **热点面积**：top_n_grids × 0.049 km²

### Scene 去重
所有统计都使用 `COUNT(DISTINCT scene_token)`，确保同一场景的多个 bbox 只计数一次。

---

## 常见用例

### 用例 1：识别数据采集集中的区域
**目的**：找出哪些城市/区域的数据过度集中

```bash
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --export-csv
```

查看 CSV 中 `redundancy_index >= 20` 的城市。

### 用例 2：对比不同比例的集中度
**目的**：了解数据在不同空间尺度上的分布

```bash
# Top 1% 分析
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --top-percent 1 --export-csv

# Top 5% 分析
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --top-percent 5 --export-csv

# 对比两个 CSV 文件
```

### 用例 3：单个城市详细分析
**目的**：深入了解特定城市的数据分布

```bash
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --cities A263
```

---

## 数据表说明

### city_grid_density
存储每个城市的网格密度数据。

**关键字段**：
- `city_id`：城市ID
- `grid_x, grid_y`：网格坐标
- `bbox_count`：该网格中的 bbox 数量
- `scene_count`：该网格中的场景数量（去重）
- `geometry`：网格的几何形状（可在 QGIS 中可视化）

### 查询示例
```sql
-- 查看某城市的网格分布
SELECT 
    city_id, 
    COUNT(*) as total_grids,
    SUM(bbox_count) as total_bboxes,
    SUM(scene_count) as total_scenes
FROM city_grid_density
WHERE city_id = 'A263' AND analysis_date = CURRENT_DATE
GROUP BY city_id;

-- 查看最密集的 10 个网格
SELECT 
    city_id, 
    grid_x, 
    grid_y, 
    bbox_count, 
    scene_count
FROM city_grid_density
WHERE city_id = 'A263' AND analysis_date = CURRENT_DATE
ORDER BY bbox_count DESC
LIMIT 10;
```

---

## QGIS 可视化

### 加载网格数据
1. 在 QGIS 中添加 PostGIS 连接
2. 加载 `city_grid_density` 表
3. 设置样式：
   - **颜色**：按 `bbox_count` 分级（热力图）
   - **标签**：显示 `scene_count`
   - **过滤**：`analysis_date = CURRENT_DATE AND city_id = 'A263'`

### 可视化效果
- 红色区域：高密度（数据集中）
- 蓝色区域：低密度（数据分散）

---

## 故障排查

### 问题：没有找到城市数据
**原因**：未生成网格数据

**解决**：
```bash
python examples/dataset/bbox_examples/batch_grid_analysis.py
```

### 问题：area_percentage 不接近 top_percent
**原因**：网格数据不完整或脚本未更新

**解决**：
1. 检查 `city_grid_density` 表是否有数据
2. 确认使用最新版本的脚本
3. 查看 `REDUNDANCY_ANALYSIS_TEST.md` 进行详细诊断

### 问题：某些城市显示"无数据"
**原因**：该城市没有有效的 bbox 或网格数据

**检查**：
```sql
-- 检查 bbox 数据
SELECT COUNT(*) FROM clips_bbox_unified 
WHERE city_id = 'xxx' AND all_good = true;

-- 检查网格数据
SELECT COUNT(*) FROM city_grid_density 
WHERE city_id = 'xxx' AND analysis_date = CURRENT_DATE;
```

---

## 性能说明

### 数据量参考
- **356 个城市**：约 2-5 分钟
- **单个大城市**（20000+ scenes）：约 10-30 秒

### 优化建议
- 如果只关心部分城市，使用 `--cities` 参数
- 大规模分析可以分批进行

---

## 相关文档

- **测试说明**：`REDUNDANCY_ANALYSIS_TEST.md` - 详细测试步骤和验证方法
- **改动记录**：`CHANGELOG_REDUNDANCY_FIX.md` - 修复详情和技术说明
- **批量分析**：`batch_grid_analysis.py` - 批量生成网格数据

---

## 技术支持

如遇到问题或有改进建议，请提供：
1. 完整的错误信息
2. 使用的命令
3. 相关城市的数据量（scene 数量、grid 数量）

