# Grid多模态相似性分析 - 测试说明

## 测试环境要求

本脚本需要在远端环境（服务器）运行，因为需要：
1. 访问 PostgreSQL 数据库（包含 city_grid_density 表）
2. 访问多模态API（需要内网访问权限）
3. 环境变量配置（.env文件）

## 测试前准备

### 1. 确认数据表存在

```bash
# 进入容器
docker exec -it spdatalab_container bash

# 连接数据库检查
psql -U postgres -d postgres -c "SELECT COUNT(*) FROM city_grid_density;"

# 如果表不存在，先运行：
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --create-table
python examples/dataset/bbox_examples/batch_grid_analysis.py
```

### 2. 确认多模态API配置

检查 `.env` 文件是否包含以下配置：

```bash
# 查看配置（隐藏敏感信息）
grep MULTIMODAL .env | sed 's/=.*/=***/'

# 应该包含：
# MULTIMODAL_PROJECT=***
# MULTIMODAL_API_KEY=***
# MULTIMODAL_USERNAME=***
# MULTIMODAL_API_BASE_URL=***
```

### 3. 查看可用城市

```bash
# 查看哪些城市有grid数据
psql -U postgres -d postgres -c "
SELECT 
    city_id, 
    COUNT(*) as grid_count,
    MAX(bbox_count) as max_bbox_count
FROM city_grid_density 
WHERE analysis_date = (SELECT MAX(analysis_date) FROM city_grid_density)
GROUP BY city_id 
ORDER BY grid_count DESC 
LIMIT 10;
"
```

## 测试步骤

### 测试1: 基础功能测试

```bash
# 使用最简单的命令测试
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
    --city A72

# 预期输出：
# ✅ 数据库连接成功
# ✅ 选择Grid成功
# ✅ 提取Dataset成功
# ✅ API调用成功
# ✅ 相似度分析完成
```

**检查点：**
- [ ] 脚本能正常启动
- [ ] 数据库连接成功
- [ ] 找到目标Grid
- [ ] 提取到dataset列表
- [ ] API调用成功
- [ ] 返回检索结果
- [ ] 显示相似度分析

### 测试2: 参数测试

```bash
# 测试不同参数组合
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
    --city A72 \
    --grid-rank 2 \
    --query-text "夜晚" \
    --max-results 50 \
    --top-n 5

# 预期输出：
# 📍 选择Grid: A72 城市, Rank #2
# 🔍 查询文本: '夜晚'
# 📊 显示Top 5结果
```

**检查点：**
- [ ] 参数正确传递
- [ ] Grid排名选择正确
- [ ] 查询文本生效
- [ ] 结果数量符合预期

### 测试3: 错误处理测试

```bash
# 测试不存在的城市
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
    --city NONEXIST

# 预期输出：
# ❌ 未找到城市 NONEXIST 的Grid数据

# 测试过大的grid排名
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
    --city A72 \
    --grid-rank 999

# 预期输出：
# ❌ 未找到城市 A72 的Grid数据
# 💡 提示：尝试降低 --grid-rank 参数
```

**检查点：**
- [ ] 错误消息清晰
- [ ] 提供有用的提示
- [ ] 脚本正常退出（不崩溃）

### 测试4: 不同城市测试

如果有多个城市数据，测试不同城市：

```bash
# 获取城市列表
cities=$(psql -U postgres -d postgres -t -c "
SELECT DISTINCT city_id 
FROM city_grid_density 
WHERE analysis_date = (SELECT MAX(analysis_date) FROM city_grid_density)
LIMIT 3;
" | tr -d ' ')

# 循环测试
for city in $cities; do
    echo "测试城市: $city"
    python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
        --city $city \
        --max-results 20
done
```

## 预期输出示例

### 成功案例输出

```
🚀 Grid多模态相似性分析
============================================================

🔌 连接数据库...
✅ 数据库连接成功

🎯 选择目标Grid
============================================================
📍 选择Grid: A72 城市, Rank #1
   分析日期: 2025-10-09
   Grid坐标: (1234, 5678)
   BBox数量: 156
   Scene数量: 67
   Dataset数量: 8

📦 提取Grid内的数据
============================================================
✅ 提取完成:
   Dataset数量: 8
   Scene数量: 67
   子数据集数量: 5
   总记录数: 156

📋 Grid内的数据集 (前10个):
   1. dataset_a_2025/05/29/16:31:23 (15 scenes)
   2. dataset_b_2025/05/30/10:15:30 (12 scenes)
   ...

🔧 初始化多模态API...
✅ API配置加载成功

🔍 调用多模态API
============================================================
📝 查询参数:
   查询文本: '白天'
   Collection: ddi_collection_camera_encoded_1
   城市过滤: A72
   Dataset过滤: 8 个
   最大结果数: 100
✅ API调用成功: 返回 76 条结果

📊 相似度分析
============================================================
📈 相似度统计:
   范围: 0.234 ~ 0.867
   平均: 0.542
   中位数: 0.556
   样本数: 76

📊 相似度分布直方图:
   0.2-0.3: ████ (6, 7.9%)
   0.3-0.4: ████████ (12, 15.8%)
   0.4-0.5: ████████████ (18, 23.7%)
   0.5-0.6: ████████████████ (25, 32.9%)
   0.6-0.7: ████████ (11, 14.5%)
   0.7-0.8: ████ (3, 3.9%)
   0.8-0.9: █ (1, 1.3%)

📦 按Dataset分组:
   Top 5 Dataset (按平均相似度):
   1. dataset_a_2025/05/29/16:31:23
      结果数: 18, 平均相似度: 0.645, 最高: 0.867
   ...

🔝 Top 10 最相似结果:
   1. 相似度: 0.867
      Dataset: dataset_a_2025/05/29/16:31:23
      Timestamp: 1748507506699
      图片路径: obs://yw-xmodalitys-gy1/xmodalitys_img/xxx/camera_1/xxx.jpg
   ...

============================================================
✅ 分析完成
============================================================
📍 Grid: A72 (1234, 5678)
📦 Dataset数量: 8
🔍 检索结果: 76
📊 相似度范围: 0.234 ~ 0.867

💡 后续可以:
   1. 尝试不同的查询文本（--query-text）
   2. 分析其他排名的grid（--grid-rank）
   3. 下载图片进行视觉相似性分析（待实现）
============================================================
```

## 常见问题排查

### 问题1: 数据库连接失败

```
❌ 数据库连接失败: could not connect to server
```

**排查步骤：**
```bash
# 1. 检查容器状态
docker ps | grep postgres

# 2. 测试数据库连接
psql -U postgres -h localhost -d postgres -c "SELECT 1;"

# 3. 检查环境变量
echo $DATABASE_URL
```

### 问题2: 未找到Grid数据

```
❌ 未找到城市 A72 的Grid数据
```

**排查步骤：**
```bash
# 1. 确认表存在
psql -U postgres -d postgres -c "\dt city_grid_density"

# 2. 查看表内容
psql -U postgres -d postgres -c "SELECT * FROM city_grid_density LIMIT 5;"

# 3. 检查指定城市
psql -U postgres -d postgres -c "
SELECT COUNT(*) FROM city_grid_density WHERE city_id = 'A72';
"

# 4. 如果没有数据，运行生成脚本
python examples/dataset/bbox_examples/batch_grid_analysis.py
```

### 问题3: API调用失败

```
❌ API调用失败: 401 Unauthorized
```

**排查步骤：**
```bash
# 1. 检查API配置
grep MULTIMODAL .env

# 2. 测试API连接（使用curl）
curl -X POST "$MULTIMODAL_API_BASE_URL/xmodalitys/retrieve" \
  -H "Authorization: Bearer $MULTIMODAL_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"text":"test","collection":"ddi_collection_camera_encoded_1","count":1}'

# 3. 检查API密钥有效期
```

### 问题4: 返回结果为空

```
⚠️ API未返回结果
```

**可能原因和解决方案：**

1. **查询文本不匹配**
   ```bash
   # 尝试更通用的查询词
   --query-text "道路"
   --query-text "车辆"
   ```

2. **Dataset过滤过严**
   ```bash
   # 检查grid内有多少dataset
   # 如果只有1-2个，可能匹配不到
   ```

3. **城市过滤错误**
   ```bash
   # 确认城市代码格式
   psql -U postgres -d postgres -c "
   SELECT DISTINCT city_id FROM clips_bbox_unified LIMIT 10;
   "
   ```

## 测试报告模板

完成测试后，请记录以下信息反馈：

```markdown
## 测试环境
- 日期: YYYY-MM-DD
- 服务器: [服务器名称]
- Python版本: [版本号]

## 测试结果

### 基础功能测试
- [ ] 通过 / [ ] 失败
- 错误信息: [如果失败，记录错误]

### 参数测试
- [ ] 通过 / [ ] 失败
- 测试参数: [记录使用的参数]
- 观察结果: [结果是否符合预期]

### 不同城市测试
- 测试城市: [城市列表]
- 结果: [每个城市的测试情况]

### 性能观察
- 数据库查询耗时: [秒]
- API调用耗时: [秒]
- 总耗时: [秒]

### 其他发现
[记录任何异常情况、改进建议等]
```

## 后续优化建议

根据测试结果，可以考虑：

1. **参数优化**
   - 调整默认的 max_results
   - 优化查询文本列表

2. **错误处理**
   - 增加更详细的错误提示
   - 添加重试机制

3. **性能优化**
   - 缓存grid查询结果
   - 批量处理dataset

4. **功能扩展**
   - 添加图片下载功能
   - 实现视觉相似度分析
   - 支持结果导出

