# 批量分析脚本测试说明

## 📋 修改内容

修改了 `batch_top1_analysis.py`，现在支持两种分析模式：

### 1. **百分比模式**（默认）
返回每个城市前X%最密集的网格

### 2. **固定数量模式**
返回每个城市前N个最密集的网格

---

## 🧪 测试命令

### **测试1：前1%热点（默认）**
```bash
cd /workspace/examples/dataset/bbox_examples
python batch_top1_analysis.py --top-percent 1 --cities A72 --max-cities 1
```

**预期结果**：
- 城市A72有约58000个热点网格
- 应该返回前580个（1%）热点
- 数据保存到 `city_hotspots` 表

---

### **测试2：前5%热点**
```bash
python batch_top1_analysis.py --top-percent 5 --cities A72 --max-cities 1
```

**预期结果**：
- 应该返回前2900个（5%）热点

---

### **测试3：固定数量（前10个）**
```bash
python batch_top1_analysis.py --top-n 10 --cities A72 --max-cities 1
```

**预期结果**：
- 只返回10个最密集的网格

---

### **测试4：批量分析多个城市（前1%）**
```bash
python batch_top1_analysis.py --top-percent 1 --max-cities 3
```

**预期结果**：
- 分析前3个城市（A72, A200, A252）
- 每个城市返回前1%热点

---

## 🔍 验证结果

### 1. 检查数据量
```sql
-- 查看今天的分析结果
SELECT 
    city_id,
    COUNT(*) as hotspot_count,
    MAX(bbox_count) as max_density,
    AVG(bbox_count) as avg_density
FROM city_hotspots
WHERE analysis_time::date = CURRENT_DATE
GROUP BY city_id
ORDER BY hotspot_count DESC;
```

### 2. 查看具体数据
```sql
-- 查看某个城市的热点分布
SELECT 
    city_id,
    bbox_count,
    grid_coords,
    ST_AsText(geometry) as geom
FROM city_hotspots
WHERE city_id = 'A72'
  AND analysis_time::date = CURRENT_DATE
ORDER BY bbox_count DESC
LIMIT 10;
```

---

## ✅ 验证要点

1. **数量验证**：
   - `--top-percent 1`: 应该约等于总热点数×1%
   - `--top-n 10`: 应该正好是10条

2. **质量验证**：
   - 所有热点的 `bbox_count` 应该从高到低排列
   - `grid_coords` 应该是 `(x,y)` 格式
   - `geometry` 字段应该有效

3. **JSON格式验证**：
   - 不应该有任何JSON解析错误
   - `calculate_area` 应该是 `false`（小写）
   - `grid_coords` 应该是 `"(100,200)"` 格式（不含SQL操作符）

---

## 🎯 关键改进

1. ✅ 修复JSON布尔值格式（`False` → `false`）
2. ✅ 修复grid_coords拼接（避免SQL操作符出现在JSON中）
3. ✅ 支持百分比和固定数量两种模式
4. ✅ 提取逻辑改为获取最新分析的所有热点（不限rank=1）
5. ✅ 默认输出表名改为 `city_hotspots`（更通用）

---

## 📊 完整分析流程（远端测试）

```bash
# 1. 测试单个城市
python batch_top1_analysis.py --top-percent 1 --cities A72 --max-cities 1

# 2. 如果成功，批量分析所有城市
python batch_top1_analysis.py --top-percent 1

# 3. 分析完成后查看统计
psql -h local_pg -U postgres -d postgres -c "
SELECT 
    COUNT(DISTINCT city_id) as city_count,
    COUNT(*) as total_hotspots,
    AVG(bbox_count) as avg_density,
    MAX(bbox_count) as max_density
FROM city_hotspots
WHERE analysis_time::date = CURRENT_DATE;
"
```

---

## 🐛 如果遇到问题

### 问题1：JSON解析错误
**解决**：删除旧表重新开始
```bash
cd /workspace/examples/one_time
python drop_analysis_table.py
```

### 问题2：数据量不对
**检查**：
```sql
-- 检查analysis_params
SELECT 
    analysis_id,
    analysis_params,
    COUNT(*) as count
FROM bbox_overlap_analysis_results
WHERE analysis_time::date = CURRENT_DATE
GROUP BY analysis_id, analysis_params
ORDER BY analysis_time DESC;
```

---

## 💡 参数组合示例

| 场景 | 命令 | 说明 |
|------|------|------|
| 前1%热点 | `--top-percent 1` | 默认，适合大多数分析 |
| 前5%热点 | `--top-percent 5` | 更全面的覆盖 |
| 最密集的10个 | `--top-n 10` | 只关注极端热点 |
| 最密集的1个 | `--top-n 1` | 旧版行为，只要TOP1 |


