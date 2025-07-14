# 轨迹集成分析系统测试指南

## 系统概述

轨迹集成分析系统将轨迹道路分析和车道分析统一为一个端到端的处理流程，支持从GeoJSON格式的轨迹数据进行完整的空间分析。

## 测试前准备

### 1. 环境确认
```bash
# 确认在项目根目录
pwd
# 应该显示: /d%3A/Worksapce/code/spdatalab

# 确认Python环境
python -c "import spdatalab; print('模块可用')"
```

### 2. 数据库连接确认
```bash
# 测试本地PostgreSQL连接
python -c "from sqlalchemy import create_engine; eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres'); print('本地DB连接正常')"

# 测试Hive连接
python -c "from spdatalab.common.io_hive import hive_cursor; print('Hive连接配置正常')"
```

## 测试场景1：使用现有轨迹数据

### 步骤1：从trajectory.py生成轨迹数据
```bash
# 生成测试轨迹表（使用现有scene_id列表）
python -m spdatalab.dataset.trajectory \
    --input examples/test_scenes.txt \
    --table test_trajectories_integration \
    --batch-size 10 \
    --verbose
```

### 步骤2：查看生成的轨迹数据
```sql
-- 在PostgreSQL中查看轨迹表
SELECT 
    scene_id,
    data_name,
    start_time,
    end_time,
    avg_speed,
    ST_AsText(geometry) as geometry_wkt
FROM test_trajectories_integration
LIMIT 5;
```

### 步骤3：导出轨迹为GeoJSON
```bash
# 创建GeoJSON导出脚本
python -c "
import geopandas as gpd
from sqlalchemy import create_engine

# 连接数据库
eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')

# 读取轨迹数据
gdf = gpd.read_postgis('''
    SELECT 
        scene_id,
        data_name,
        start_time,
        end_time,
        avg_speed,
        max_speed,
        avp_ratio,
        geometry
    FROM test_trajectories_integration
    LIMIT 5
''', eng, geom_col='geometry')

# 导出GeoJSON
gdf.to_file('test_trajectories.geojson', driver='GeoJSON')
print(f'导出 {len(gdf)} 条轨迹到 test_trajectories.geojson')
"
```

### 步骤4：验证GeoJSON格式
```bash
# 验证GeoJSON数据格式
python -m spdatalab.fusion.trajectory_data_processor \
    --input test_trajectories.geojson \
    --validate-only \
    --verbose
```

### 步骤5：执行集成分析
```bash
# 运行轨迹集成分析
python -m spdatalab.fusion.trajectory_integrated_analysis \
    --input-geojson test_trajectories.geojson \
    --output-prefix test_integration_001 \
    --enable-road-analysis \
    --enable-lane-analysis \
    --buffer-distance 3.0 \
    --batch-size 2 \
    --verbose
```

## 测试场景2：使用手工构造的GeoJSON

### 步骤1：创建测试GeoJSON文件
```bash
# 创建测试GeoJSON数据
python -c "
import json
from datetime import datetime

# 构造测试轨迹（北京市区的示例坐标）
test_geojson = {
    'type': 'FeatureCollection',
    'features': [
        {
            'type': 'Feature',
            'properties': {
                'scene_id': 'test_scene_001',
                'data_name': 'test_trajectory_001',
                'start_time': int(datetime(2024, 1, 1, 10, 0, 0).timestamp()),
                'end_time': int(datetime(2024, 1, 1, 10, 5, 0).timestamp()),
                'avg_speed': 15.5,
                'max_speed': 25.0,
                'avp_flag': 1
            },
            'geometry': {
                'type': 'LineString',
                'coordinates': [
                    [116.3974, 39.9093],  # 天安门
                    [116.4074, 39.9093],  # 向东500m
                    [116.4074, 39.9193],  # 向北1km
                    [116.4174, 39.9193],  # 向东500m
                    [116.4174, 39.9293]   # 向北1km
                ]
            }
        },
        {
            'type': 'Feature',
            'properties': {
                'scene_id': 'test_scene_002',
                'data_name': 'test_trajectory_002',
                'start_time': int(datetime(2024, 1, 1, 11, 0, 0).timestamp()),
                'end_time': int(datetime(2024, 1, 1, 11, 8, 0).timestamp()),
                'avg_speed': 12.8,
                'max_speed': 22.0,
                'avp_flag': 0
            },
            'geometry': {
                'type': 'LineString',
                'coordinates': [
                    [116.4274, 39.9293],  # 接续上一条轨迹
                    [116.4374, 39.9293],  # 向东500m
                    [116.4374, 39.9393],  # 向北1km
                    [116.4474, 39.9393],  # 向东500m
                    [116.4474, 39.9493]   # 向北1km
                ]
            }
        }
    ]
}

# 保存测试文件
with open('test_manual_trajectories.geojson', 'w', encoding='utf-8') as f:
    json.dump(test_geojson, f, ensure_ascii=False, indent=2)

print('创建测试GeoJSON文件: test_manual_trajectories.geojson')
"
```

### 步骤2：验证和分析
```bash
# 验证GeoJSON格式
python -m spdatalab.fusion.trajectory_data_processor \
    --input test_manual_trajectories.geojson \
    --validate-only \
    --verbose

# 执行集成分析
python -m spdatalab.fusion.trajectory_integrated_analysis \
    --input-geojson test_manual_trajectories.geojson \
    --output-prefix test_manual_001 \
    --enable-road-analysis \
    --buffer-distance 5.0 \
    --batch-size 1 \
    --verbose
```

## 测试场景3：错误处理测试

### 步骤1：创建错误的GeoJSON文件
```bash
# 创建包含错误的GeoJSON文件
python -c "
import json

# 构造有错误的测试数据
error_geojson = {
    'type': 'FeatureCollection',
    'features': [
        {
            'type': 'Feature',
            'properties': {
                # 缺少必需的scene_id
                'data_name': 'error_trajectory_001',
                'avg_speed': 'invalid_speed'  # 错误的速度类型
            },
            'geometry': {
                'type': 'Point',  # 错误的几何类型
                'coordinates': [116.3974, 39.9093]
            }
        }
    ]
}

with open('test_error_trajectories.geojson', 'w', encoding='utf-8') as f:
    json.dump(error_geojson, f, ensure_ascii=False, indent=2)

print('创建错误测试GeoJSON文件: test_error_trajectories.geojson')
"
```

### 步骤2：测试错误处理
```bash
# 测试错误处理
python -m spdatalab.fusion.trajectory_data_processor \
    --input test_error_trajectories.geojson \
    --validate-only \
    --verbose
```

## 结果验证

### 1. 检查数据库表
```sql
-- 检查分析结果表
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

-- 检查轨迹信息表
SELECT 
    analysis_id,
    trajectory_id,
    scene_id,
    data_name,
    processing_status,
    road_analysis_id,
    lane_analysis_id
FROM trajectory_integrated_summary
ORDER BY created_at DESC
LIMIT 10;
```

### 2. 检查QGIS视图
```sql
-- 查看可用的QGIS视图
SELECT schemaname, viewname 
FROM pg_views 
WHERE viewname LIKE 'qgis_%'
ORDER BY viewname;
```

### 3. 查看处理日志
```bash
# 查看详细的处理日志
tail -50 trajectory_integration.log
```

## 性能测试

### 大批量数据测试
```bash
# 生成大量轨迹数据（如果有足够的scene_id）
python -m spdatalab.dataset.trajectory \
    --input large_scene_list.txt \
    --table large_trajectories_test \
    --batch-size 50

# 导出大量GeoJSON
python -c "
import geopandas as gpd
from sqlalchemy import create_engine

eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')
gdf = gpd.read_postgis('''
    SELECT scene_id, data_name, avg_speed, geometry
    FROM large_trajectories_test
    LIMIT 100
''', eng, geom_col='geometry')

gdf.to_file('large_test_trajectories.geojson', driver='GeoJSON')
print(f'导出 {len(gdf)} 条大批量测试轨迹')
"

# 执行大批量集成分析
python -m spdatalab.fusion.trajectory_integrated_analysis \
    --input-geojson large_test_trajectories.geojson \
    --output-prefix large_batch_test \
    --enable-road-analysis \
    --buffer-distance 3.0 \
    --batch-size 10 \
    --verbose
```

## 故障排除

### 常见问题

1. **数据库连接失败**
   ```bash
   # 检查PostgreSQL服务状态
   docker ps | grep postgres
   
   # 重启PostgreSQL容器
   docker restart local_pg
   ```

2. **Hive连接超时**
   ```bash
   # 检查网络连接
   ping your-hive-server
   
   # 检查Hive配置
   python -c "from spdatalab.common.io_hive import hive_cursor; print('配置正常')"
   ```

3. **GeoJSON格式错误**
   ```bash
   # 使用详细验证
   python -m spdatalab.fusion.trajectory_data_processor \
       --input your_file.geojson \
       --validate-only \
       --verbose
   ```

4. **内存不足**
   ```bash
   # 减少批处理大小
   python -m spdatalab.fusion.trajectory_integrated_analysis \
       --input-geojson your_file.geojson \
       --batch-size 1 \
       --verbose
   ```

### 日志分析
```bash
# 查看错误日志
grep -i "error\|exception\|fail" trajectory_integration.log

# 查看处理统计
grep -i "处理完成\|分析完成\|成功" trajectory_integration.log

# 查看性能指标
grep -i "耗时\|处理时间\|duration" trajectory_integration.log
```

## 清理测试数据

```bash
# 清理测试文件
rm -f test_*.geojson
rm -f large_test_*.geojson
rm -f trajectory_integration.log

# 清理数据库表（谨慎使用）
python -c "
from sqlalchemy import create_engine, text

eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')
with eng.connect() as conn:
    # 清理测试表
    conn.execute(text('DROP TABLE IF EXISTS test_trajectories_integration'))
    conn.execute(text('DROP TABLE IF EXISTS large_trajectories_test'))
    
    # 清理测试分析结果
    conn.execute(text('DELETE FROM trajectory_integrated_analysis WHERE analysis_id LIKE \\'test_%\\''))
    conn.execute(text('DELETE FROM trajectory_integrated_summary WHERE analysis_id LIKE \\'test_%\\''))
    
    conn.commit()
    print('清理完成')
"
```

## 测试检查清单

- [ ] 环境和依赖验证
- [ ] 数据库连接测试
- [ ] 轨迹数据生成测试
- [ ] GeoJSON格式验证
- [ ] 基本集成分析测试
- [ ] 错误处理测试
- [ ] 大批量数据测试
- [ ] 结果验证
- [ ] QGIS可视化测试
- [ ] 性能指标检查
- [ ] 清理测试数据

完成所有检查清单项目后，系统测试即告完成。 