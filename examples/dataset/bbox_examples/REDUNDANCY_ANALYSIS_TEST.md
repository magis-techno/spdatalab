# 空间冗余分析修复 - 测试说明

## 修复内容

### 问题诊断
原代码存在面积计算不一致的问题：
- **分母**：使用所有 bbox 的几何面积总和（小多边形面积）
- **分子**：使用 top grid 的网格面积总和（200m × 200m 矩形）
- **结果**：导致 area_pct 出现 43% 这种不合理的值（期望应该接近 1%）

### 修复方案
采用统一使用 Grid 面积的方案：
- **分母**：所有有数据的 grid 总面积 = grid_count × 0.049 km²
- **分子**：top N% grid 总面积 = top_n × 0.049 km²
- **面积百分比**：area_pct = (top_n / grid_count) × 100%

### 其他改进
1. 城市列表按 scene 数量从多到少排序
2. 输出信息更详细（显示 grid 数量和具体统计）
3. 添加计算方法说明

---

## 测试步骤

### 1. 基本功能测试

```bash
# 运行默认分析（top 1%）
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py
```

**预期结果**：
- 城市按 scene 数量从多到少显示
- 每个城市的 `area_percentage` 应该**接近 1.0%**（不再是 43%）
- 输出格式类似：
  ```
  ✓ A0: 冗余指数 16.9 (1.0%面积[10/1000grid] → 16.9%场景[1690/10000])
  ```

### 2. 不同百分比测试

```bash
# 测试 top 5%
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --top-percent 5
```

**预期结果**：
- `area_percentage` 应该**接近 5.0%**
- 冗余指数 = scene_percentage / 5.0

### 3. 指定城市测试

```bash
# 测试特定城市
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --cities A263 B001
```

**预期结果**：
- 只分析指定的城市
- area_percentage 仍然接近 1.0%

### 4. CSV 导出测试

```bash
# 导出详细报告
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --export-csv
```

**预期结果**：
- 生成 `redundancy_report_top1pct.csv` 文件
- CSV 包含新增字段：
  - `total_grid_area_km2`：总网格面积
  - `hotspot_grid_area_km2`：热点网格面积

---

## 验证标准

### ✅ 修复成功的标志

对于 **top 1%** 分析：

| 指标 | 修复前（错误） | 修复后（正确） |
|------|--------------|---------------|
| area_percentage | 43.0% | ~1.0% |
| 冗余指数计算 | 16.9 / 43 = 0.39 | 16.9 / 1.0 = 16.9 |

**示例对比**：
```
修复前: ✓ A0: 冗余指数 0.39 (43.0%面积 → 16.9%场景) ❌
修复后: ✓ A0: 冗余指数 16.9 (1.0%面积[10/1000grid] → 16.9%场景[1690/10000]) ✅
```

### 数值合理性检查

1. **area_percentage 检查**：
   - 应该 ≈ `top_percent` 参数值
   - 允许小幅偏差（因为取整：`top_n = int(grid_count * top_percent / 100)`）

2. **冗余指数解读**：
   - **< 5**：数据分布较均匀
   - **5-10**：正常的空间集中度
   - **10-20**：中度冗余，数据较集中
   - **≥ 20**：严重冗余，数据高度集中在少数区域

3. **城市排序检查**：
   - 输出提示应该显示 "按scene数量排序"
   - 第一个城市应该是 scene 数量最多的

---

## 示例输出

修复后的正常输出应该类似：

```
🚀 空间冗余分析 (Top 1.0%)
============================================================
📊 分析所有城市: 共 356 个（按scene数量排序）

🔄 计算冗余度指标...

✓ A263: 冗余指数 18.5 (1.0%面积[15/1500grid] → 18.5%场景[3700/20000])
✓ B001: 冗余指数 12.3 (1.0%面积[12/1200grid] → 12.3%场景[2460/20000])
✓ A0: 冗余指数 16.9 (1.0%面积[10/1000grid] → 16.9%场景[1690/10000])
...

============================================================
📈 汇总统计
============================================================
分析城市数: 356
总场景数: 6,500,000
总网格数: 450,000
平均冗余指数: 11.25
中位数: 9.80
范围: 2.50 ~ 35.20

冗余度分级:
  - 严重冗余 (≥20): 45 个城市
  - 中度冗余 (10-20): 123 个城市
  - 合理范围 (<10): 188 个城市

🔝 Top 5 高冗余城市:
  1. A123: 冗余指数 35.2 (1.0%面积[8grid/3.9km²] 包含35.2%场景[7040/20000])
  2. B456: 冗余指数 28.7 (1.0%面积[10grid/4.9km²] 包含28.7%场景[5740/20000])
  ...

💡 计算方法说明:
   - 面积计算：使用网格(grid)面积统一计算
   - 单个grid：0.002° × 0.002° ≈ 0.049 km²
   - 冗余指数：scene占比 / 面积占比（越高表示数据越集中）

💡 下一步:
   - 在Jupyter Notebook中进行可视化分析
   - 在QGIS中加载 city_grid_density 表查看空间分布
============================================================
```

---

## 故障排查

### 如果 area_percentage 仍然异常

1. **检查 grid 数据是否正确**：
   ```sql
   SELECT city_id, COUNT(*) as grid_count
   FROM city_grid_density
   WHERE analysis_date = CURRENT_DATE
   GROUP BY city_id
   ORDER BY grid_count DESC
   LIMIT 10;
   ```

2. **检查 scene 数据**：
   ```sql
   SELECT city_id, COUNT(DISTINCT scene_token) as scene_count
   FROM clips_bbox_unified
   WHERE all_good = true
   GROUP BY city_id
   ORDER BY scene_count DESC
   LIMIT 10;
   ```

3. **验证单个城市的计算**：
   ```bash
   python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --cities <city_id>
   ```

---

## 反馈格式

请在测试后反馈以下信息：

1. **基本测试结果**：
   - area_percentage 是否接近 top_percent 值？
   - 城市是否按 scene 数量排序？

2. **前 5 个城市的输出**（复制完整输出）

3. **任何异常情况或错误信息**

4. **CSV 文件的前几行**（如果导出了）

---

## 技术细节

### 单个 grid 面积计算
```python
GRID_SIZE_DEGREES = 0.002  # 度
KM_PER_DEGREE = 111.0      # 约 111km/度（赤道附近）
SINGLE_GRID_AREA_KM2 = (0.002 × 111)² = 0.222² ≈ 0.049 km²
```

### 冗余指数公式
```
area_pct = (top_n_grids / total_grids) × 100%
scene_pct = (hotspot_scenes / total_scenes) × 100%
redundancy_index = scene_pct / area_pct
```

**解释**：
- 如果 1% 的面积包含 1% 的 scene → redundancy = 1（完全均匀）
- 如果 1% 的面积包含 20% 的 scene → redundancy = 20（高度集中）

