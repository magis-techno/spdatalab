# 空间冗余分析修复 - 改动记录

## 修复日期
2025-10-10

## 修复文件
- `examples/dataset/bbox_examples/analyze_spatial_redundancy.py`

---

## 核心问题

### 原有问题
面积计算使用了不一致的度量单位：
- **分母**：所有 bbox 的几何面积总和（小多边形，单位：km²）
- **分子**：top grid 的网格面积总和（200m × 200m 矩形，单位：km²）

虽然单位相同，但语义不同：
- bbox 是实际标注的小多边形（可能只有几平方米）
- grid 是规则的 200m × 200m 矩形（约 0.04 km²）

**导致的结果**：area_pct 计算错误，出现 43% 这种不合理的值（期望应该是 1%）

### 解决方案
统一使用 grid 面积：
- **分母**：所有有数据的 grid 总面积
- **分子**：top N% grid 总面积
- **计算公式**：`area_pct = (top_n / total_grid_count) × 100%`

---

## 详细改动

### 1. 添加常量定义（第 44-50 行）
```python
# 常量定义
GRID_SIZE_DEGREES = 0.002        # 0.002度 × 0.002度
KM_PER_DEGREE = 111.0            # 1度 ≈ 111km（赤道附近）
SINGLE_GRID_AREA_KM2 = (GRID_SIZE_DEGREES * KM_PER_DEGREE) ** 2  # ≈ 0.049 km²
```

### 2. 修改 `calculate_city_redundancy` 函数

#### 移除 bbox 面积计算（第 107-113 行）
**修改前**：
```python
SELECT 
    COUNT(DISTINCT scene_token) as total_scenes,
    COUNT(*) as total_bboxes,
    SUM(ST_Area(geometry::geography)) / 1000000.0 as total_area_km2  # ← 删除
FROM clips_bbox_unified
```

**修改后**：
```python
SELECT 
    COUNT(DISTINCT scene_token) as total_scenes,
    COUNT(*) as total_bboxes
FROM clips_bbox_unified
```

#### 简化热点查询（第 134-148 行）
**修改前**：
```python
SELECT 
    COUNT(DISTINCT b.scene_token) as hotspot_scenes,
    COUNT(b.*) as hotspot_bboxes,
    SUM(ST_Area(tg.geometry::geography)) / 1000000.0 as hotspot_area_km2  # ← 删除
FROM top_grids tg
```

**修改后**：
```python
SELECT 
    COUNT(DISTINCT b.scene_token) as hotspot_scenes,
    COUNT(b.*) as hotspot_bboxes
FROM top_grids tg
```

#### 改用 grid 面积计算（第 158-175 行）
**修改前**：
```python
total_area = float(total.total_area_km2) if total.total_area_km2 else 0.001
hotspot_area = float(hotspot.hotspot_area_km2) if hotspot.hotspot_area_km2 else 0.001
area_pct = (hotspot_area / total_area) * 100
```

**修改后**：
```python
# 分母：所有有数据的 grid 的总面积
total_grid_area_km2 = grid_count * SINGLE_GRID_AREA_KM2

# 分子：top N% grid 的总面积
hotspot_grid_area_km2 = top_n * SINGLE_GRID_AREA_KM2

# 面积百分比（理论上应该接近 top_percent）
area_pct = (top_n / grid_count) * 100 if grid_count > 0 else 0
```

#### 返回值增加字段（第 177-191 行）
新增：
- `total_grid_area_km2`：总网格面积
- `hotspot_grid_area_km2`：热点网格面积

### 3. 城市列表按 scene 数量排序（第 230-243 行）

**修改前**：
```python
result = conn.execute(text("""
    SELECT DISTINCT city_id FROM city_grid_density
    WHERE analysis_date = CURRENT_DATE
    ORDER BY city_id
"""))
```

**修改后**：
```python
# 按 scene 数量从多到少排序
result = conn.execute(text("""
    SELECT 
        cgd.city_id,
        COUNT(DISTINCT cbu.scene_token) as scene_count
    FROM city_grid_density cgd
    LEFT JOIN clips_bbox_unified cbu 
        ON cgd.city_id = cbu.city_id AND cbu.all_good = true
    WHERE cgd.analysis_date = CURRENT_DATE
    GROUP BY cgd.city_id
    ORDER BY scene_count DESC, cgd.city_id
"""))
```

### 4. 优化输出格式

#### 单个城市输出（第 261-263 行）
**修改前**：
```python
print(f"✓ {city_id}: 冗余指数 {metrics['redundancy_index']} "
      f"({metrics['area_percentage']:.1f}%面积 → {metrics['scene_percentage']:.1f}%场景)")
```

**修改后**：
```python
print(f"✓ {city_id}: 冗余指数 {metrics['redundancy_index']} "
      f"({metrics['area_percentage']:.1f}%面积[{metrics['top_n_grids']}/{metrics['total_grids']}grid] "
      f"→ {metrics['scene_percentage']:.1f}%场景[{metrics['hotspot_scenes']}/{metrics['total_scenes']}])")
```

#### 汇总统计增加字段（第 277-282 行）
新增：
```python
print(f"总场景数: {df['total_scenes'].sum():,}")
print(f"总网格数: {df['total_grids'].sum():,}")
```

#### Top 5 输出更详细（第 298-300 行）
**修改前**：
```python
print(f"  {i}. {row.city_id}: 冗余指数 {row.redundancy_index} "
      f"({row.area_percentage:.1f}%面积包含{row.scene_percentage:.1f}%场景)")
```

**修改后**：
```python
print(f"  {i}. {row.city_id}: 冗余指数 {row.redundancy_index} "
      f"({row.area_percentage:.1f}%面积[{row.top_n_grids}grid/{row.total_grid_area_km2:.1f}km²] "
      f"包含{row.scene_percentage:.1f}%场景[{row.hotspot_scenes}/{row.total_scenes}])")
```

#### 添加计算方法说明（第 309-312 行）
```python
print(f"\n💡 计算方法说明:")
print(f"   - 面积计算：使用网格(grid)面积统一计算")
print(f"   - 单个grid：{GRID_SIZE_DEGREES}° × {GRID_SIZE_DEGREES}° ≈ {SINGLE_GRID_AREA_KM2:.3f} km²")
print(f"   - 冗余指数：scene占比 / 面积占比（越高表示数据越集中）")
```

---

## 影响评估

### 破坏性改动
✅ **无破坏性改动**
- CSV 导出格式兼容（增加了新字段，但不影响现有字段）
- 命令行参数完全兼容
- 数据库表结构无变化

### 结果变化
- `area_percentage` 值大幅变化（从错误的 43% 变为正确的 1%）
- `redundancy_index` 相应变化（从 0.39 变为 16.9）
- 城市显示顺序变化（按 scene 数量排序）

---

## 验证方法

### 快速验证
```bash
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py
```

检查输出中：
1. ✅ `area_percentage` 是否接近 1.0%
2. ✅ 城市是否按 scene 数量排序
3. ✅ 冗余指数是否合理（不再是小于 1 的值）

### 详细验证
参考 `REDUNDANCY_ANALYSIS_TEST.md` 文档

---

## 技术说明

### 为什么修复后 area_pct ≈ top_percent？

因为修复后的计算公式：
```
area_pct = (top_n / grid_count) × 100%

其中：
top_n = int(grid_count × top_percent / 100)

代入得：
area_pct ≈ top_percent
```

小幅偏差来自整数取整。

### 冗余指数的新解释

修复前（错误）：
```
redundancy = scene_pct / (bbox_area_pct)
```
语义不清晰，结果无意义。

修复后（正确）：
```
redundancy = scene_pct / (grid_area_pct)
```
**清晰含义**：
- redundancy = 1：数据均匀分布
- redundancy = 10：1% 的地理空间包含 10% 的场景
- redundancy = 20：严重集中，需要关注

---

## 相关文档

- **测试说明**：`REDUNDANCY_ANALYSIS_TEST.md`
- **原始脚本**：`analyze_spatial_redundancy.py`
- **批量分析**：`batch_grid_analysis.py`

---

## 后续建议

1. ✅ 在远端测试验证修复效果
2. 考虑添加 Jupyter Notebook 可视化分析
3. 考虑添加时间序列分析（对比不同日期的冗余度变化）
4. 考虑添加子数据集级别的冗余分析

