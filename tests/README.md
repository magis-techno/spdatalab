# Tests 目录

## 📁 测试文件

- `test_bbox_integration.py` - bbox功能集成测试
- `test_dataset_manager.py` - 数据集管理功能测试
- `test_dataset_manager_parquet.py` - Parquet格式处理测试
- `test_integrated_trajectory_analysis.py` - 集成轨迹分析测试
- `test_scene_list_generator.py` - 场景列表生成测试
- `test_trajectory_lane_analysis.py` - 轨迹车道分析测试
- `conftest.py` - pytest配置文件

## 🚀 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_bbox_integration.py

# 运行测试并显示详细信息
pytest -v
```