# SPDataLab 文档

空间数据处理工具包的完整文档。

## 📖 文档导航

### 基础使用
- **[CLI 使用指南](cli_usage_guide.md)** - 命令行工具完整教程
- **[数据集管理](dataset_management.md)** - 数据集构建和管理
- **[BBox处理指南](bbox_integration_guide.md)** - 边界框处理专门指南

### 高级功能  
- **[空间连接指南](spatial_join.md)** - 空间数据处理和连接
- **[进度跟踪指南](progress_tracking_guide.md)** - 智能进度跟踪和失败恢复
- **[基础设施指南](infrastructure_guide.md)** - 环境搭建和配置

## 🚀 快速开始

> **环境搭建**：请先参考[项目根目录README](../README.md)完成Docker环境配置

```bash
# 构建数据集
python -m spdatalab.cli build-dataset \
  --index-file data.txt \
  --dataset-name "my_dataset" \
  --output dataset.parquet \
  --format parquet

# 处理边界框
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 1000

# 空间连接分析（当前版本专注于polygon相交）
python -m spdatalab.cli spatial-join --right-table intersections --num-bbox 2000
```

## 💡 使用建议

- **新用户**：从CLI使用指南开始
- **大数据处理**：参考进度跟踪指南  
- **空间分析**：查看空间连接指南
- **生产部署**：参考基础设施指南

## 🔧 故障排除

- 查看各指南的故障排除部分
- 使用 `--help` 查看命令参数
- 启用详细日志进行调试 