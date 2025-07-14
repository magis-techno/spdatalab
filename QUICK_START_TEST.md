# 轨迹集成分析系统 - 快速开始测试

## 1. 立即测试（推荐）

### 运行快速测试脚本
```bash
python quick_test_integration.py
```

这个脚本会：
1. 自动创建测试GeoJSON数据
2. 验证数据格式
3. 执行集成分析
4. 检查结果
5. 提供清理选项

## 2. 手动测试步骤

### 步骤1：创建测试GeoJSON
```bash
python -c "
import json
from datetime import datetime

test_data = {
    'type': 'FeatureCollection',
    'features': [
        {
            'type': 'Feature',
            'properties': {
                'scene_id': 'manual_test_001',
                'data_name': 'manual_trajectory_001',
                'start_time': int(datetime.now().timestamp()),
                'end_time': int(datetime.now().timestamp()) + 300,
                'avg_speed': 15.5,
                'max_speed': 25.0,
                'avp_flag': 1
            },
            'geometry': {
                'type': 'LineString',
                'coordinates': [
                    [116.3974, 39.9093],
                    [116.4074, 39.9093],
                    [116.4074, 39.9193],
                    [116.4174, 39.9193],
                    [116.4174, 39.9293]
                ]
            }
        }
    ]
}

with open('manual_test.geojson', 'w', encoding='utf-8') as f:
    json.dump(test_data, f, ensure_ascii=False, indent=2)
    
print('创建测试文件: manual_test.geojson')
"
```

### 步骤2：验证数据格式
```bash
python -m spdatalab.fusion.trajectory_data_processor \
    --input manual_test.geojson \
    --validate-only \
    --verbose
```

### 步骤3：执行集成分析
```bash
python -m spdatalab.fusion.trajectory_integrated_analysis \
    --input-geojson manual_test.geojson \
    --output-prefix manual_test \
    --enable-road-analysis \
    --buffer-distance 3.0 \
    --batch-size 1 \
    --verbose
```

### 步骤4：检查结果
```sql
-- 在PostgreSQL中查看结果
SELECT 
    analysis_id,
    trajectory_count,
    success_count,
    error_count,
    total_lanes,
    total_intersections,
    total_roads
FROM trajectory_integrated_analysis
ORDER BY created_at DESC
LIMIT 5;
```

## 3. 使用现有轨迹数据测试

### 从trajectory.py生成轨迹
```bash
# 使用现有scene_id列表生成轨迹
python -m spdatalab.dataset.trajectory \
    --input your_scene_list.txt \
    --table test_trajectories \
    --batch-size 10 \
    --verbose
```

### 导出为GeoJSON
```bash
python -c "
import geopandas as gpd
from sqlalchemy import create_engine

eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')
gdf = gpd.read_postgis('''
    SELECT scene_id, data_name, avg_speed, geometry
    FROM test_trajectories
    LIMIT 10
''', eng, geom_col='geometry')

gdf.to_file('exported_trajectories.geojson', driver='GeoJSON')
print(f'导出 {len(gdf)} 条轨迹')
"
```

### 分析导出的轨迹
```bash
python -m spdatalab.fusion.trajectory_integrated_analysis \
    --input-geojson exported_trajectories.geojson \
    --output-prefix exported_test \
    --enable-road-analysis \
    --buffer-distance 3.0 \
    --batch-size 5 \
    --verbose
```

## 4. 预期输出

### 成功运行的输出示例
```
2024-01-20 10:00:00 - trajectory_integrated_analysis - INFO - 开始轨迹集成分析
2024-01-20 10:00:01 - trajectory_data_processor - INFO - 加载GeoJSON文件: test.geojson
2024-01-20 10:00:01 - trajectory_data_processor - INFO - 验证通过: 3条轨迹
2024-01-20 10:00:02 - trajectory_integrated_analysis - INFO - 执行道路分析
2024-01-20 10:00:05 - trajectory_road_analysis - INFO - 道路分析完成: 找到25个lane
2024-01-20 10:00:05 - trajectory_integrated_analysis - INFO - 保存结果到数据库
2024-01-20 10:00:06 - trajectory_integrated_analysis - INFO - 分析完成: test_20240120_100000
```

### 数据库结果示例
```sql
-- trajectory_integrated_analysis 表
analysis_id              | trajectory_count | success_count | error_count | total_lanes
test_20240120_100000     | 3               | 3             | 0           | 25

-- trajectory_integrated_summary 表
analysis_id              | trajectory_id | scene_id      | processing_status | road_analysis_id
test_20240120_100000     | traj_001     | test_001      | success          | road_20240120_100001
test_20240120_100000     | traj_002     | test_002      | success          | road_20240120_100002
```

## 5. 故障排除

### 常见错误和解决方案

1. **模块导入错误**
   ```bash
   # 确保在项目根目录
   cd /d%3A/Worksapce/code/spdatalab
   export PYTHONPATH=$PWD:$PYTHONPATH
   ```

2. **数据库连接失败**
   ```bash
   # 检查PostgreSQL容器
   docker ps | grep postgres
   # 重启容器
   docker restart local_pg
   ```

3. **Hive连接超时**
   ```bash
   # 检查网络连接
   python -c "from spdatalab.common.io_hive import hive_cursor; print('Hive连接正常')"
   ```

4. **GeoJSON格式错误**
   ```bash
   # 使用详细验证
   python -m spdatalab.fusion.trajectory_data_processor \
       --input your_file.geojson \
       --validate-only \
       --verbose
   ```

## 6. 完整功能测试

要进行完整的功能测试，请参考：
- `TRAJECTORY_INTEGRATION_TEST_GUIDE.md` - 详细测试指南
- `quick_test_integration.py` - 自动化测试脚本

## 7. 清理测试数据

```bash
# 清理测试文件
rm -f manual_test.geojson
rm -f exported_trajectories.geojson
rm -f quick_test_trajectories.geojson

# 清理数据库（谨慎使用）
python -c "
from sqlalchemy import create_engine, text
eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')
with eng.connect() as conn:
    conn.execute(text('DELETE FROM trajectory_integrated_analysis WHERE analysis_id LIKE \\'%test%\\''))
    conn.execute(text('DELETE FROM trajectory_integrated_summary WHERE analysis_id LIKE \\'%test%\\''))
    conn.commit()
    print('清理完成')
"
```

---

**推荐使用快速测试脚本开始：**
```bash
python quick_test_integration.py
```

这是最简单快捷的测试方式！ 