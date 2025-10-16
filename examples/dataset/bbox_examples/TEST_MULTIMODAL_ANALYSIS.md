# Grid多模态相似性分析 - 快速测试指南

## 快速测试

### 基础测试
```bash
# 最简单的测试命令
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py --city A72
```

### 带参数测试
```bash
python examples/dataset/bbox_examples/analyze_grid_multimodal_similarity.py \
    --city A72 \
    --grid-rank 2 \
    --query-text "夜晚" \
    --max-results 200 \
    --top-n 15
```

## 前置条件

1. **数据表准备**
```bash
# 如果 city_grid_density 表不存在
python examples/dataset/bbox_examples/analyze_spatial_redundancy.py --create-table
python examples/dataset/bbox_examples/batch_grid_analysis.py
```

2. **API配置**（`.env` 文件）
```bash
MULTIMODAL_PROJECT=your_project
MULTIMODAL_API_KEY=your_key
MULTIMODAL_USERNAME=your_username
MULTIMODAL_API_BASE_URL=https://api.example.com
```

## 常见问题

### 问题1: 未找到Grid数据
```
❌ 未找到城市 A72 的Grid数据
```
**解决**: 运行 `batch_grid_analysis.py` 生成grid数据

### 问题2: API返回0结果
**已解决**: 默认只使用城市过滤（不使用dataset过滤）

### 问题3: 列名错误
**已解决**: 使用正确的列名 `data_name`

## 调试工具

如果遇到API问题，使用调试脚本：
```bash
python examples/dataset/bbox_examples/debug_multimodal_api.py
```

调试脚本会测试：
- 基础API功能
- 城市过滤
- Dataset过滤
- 参数组合

## 预期输出示例

```
🚀 Grid多模态相似性分析
============================================================
✅ 数据库连接成功
📍 选择Grid: A72 城市, Rank #1
   BBox数量: 3047
   Scene数量: 1962
✅ 提取完成: Dataset数量: 1962
💡 提示: 已禁用dataset过滤，只使用城市过滤
✅ API调用成功: 返回 100 条结果
📊 相似度分析
   范围: 0.234 ~ 0.867
   平均: 0.542
✅ 分析完成
```

## 参考文档

详细使用指南请参考: [GRID_MULTIMODAL_ANALYSIS_GUIDE.md](GRID_MULTIMODAL_ANALYSIS_GUIDE.md)
