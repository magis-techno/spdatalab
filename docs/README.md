# SPDataLab 文档

空间数据处理工具包的完整文档。

## 📖 文档导航

### 核心功能
- **[CLI 使用指南](cli_usage_guide.md)** - 命令行工具完整教程
- **[数据集管理](dataset_management.md)** - 数据集构建和管理
- **[BBox处理指南](bbox_integration_guide.md)** - 边界框处理专门指南
- **[空间连接指南](spatial_join.md)** - 空间数据处理和连接
- **[基础设施指南](infrastructure_guide.md)** - 环境搭建和配置
- **[Notebook 使用与规范](notebook_guide.md)** - Notebook 工作流、nbstripout、回归测试

### 专业功能
- **[收费站分析](toll_station_analysis.md)** - 收费站轨迹分析
- **[轨迹道路分析](trajectory_road_analysis_guide.md)** - 轨迹道路元素分析
- **[集成轨迹分析](integrated_trajectory_analysis_guide.md)** - 综合轨迹分析流程

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