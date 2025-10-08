# Scripts 目录

## 📁 核心文件

### 测试脚本 (tests/)
- `test_bbox_integration.py` - bbox功能测试
- `test_dataset_manager.py` - 数据集管理测试
- `test_dataset_manager_parquet.py` - Parquet功能测试
- `test_integrated_trajectory_analysis.py` - 集成分析测试
- `test_scene_list_generator.py` - 场景列表生成测试
- `test_trajectory_lane_analysis.py` - 轨迹车道分析测试

### 维护脚本 (scripts/)
- `database/clean_toll_station_analysis.sql` - 清理收费站分析表
- `database/database_backup.py` - 数据库备份工具
- `database/database_migration.py` - 数据库迁移工具
- `cleanup/clear_cache.py` - 清理缓存表
- `cleanup/clear_polygon_tables.py` - 清理polygon表
- `cleanup/postgresql_cleanup.py` - PostgreSQL清理工具
- `cleanup/test_cleanup_demo.py` - 清理功能测试
- `diagnostics/check_disk_space.py` - 磁盘空间检查
- `diagnostics/quick_space_check.py` - 快速空间检查

### 示例脚本 (scripts/examples/core/)
- `spatial_join_production_example.py` - 空间连接示例
- `toll_station_analysis_example.py` - 收费站分析示例
- `polygon_trajectory_query_example.py` - 轨迹查询示例

## 🚀 快速使用

```bash
# 运行测试
pytest tests/

# 清理数据
python scripts/cleanup/clear_cache.py

# 检查磁盘空间
python scripts/diagnostics/check_disk_space.py

# 数据库备份
python scripts/database/database_backup.py

# 查看示例
python scripts/examples/core/spatial_join_production_example.py
```