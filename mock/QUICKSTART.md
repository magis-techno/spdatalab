# Mock数据环境快速开始

## 🚀 一键启动

```bash
# 1. 启动Mock环境（3个数据库 + 管理服务）
make mock-up

# 2. 等待服务启动完成（约30-60秒），然后初始化测试数据
make mock-init

# 3. 验证环境是否正常
make mock-check
```

## 📊 使用Mock数据

Mock环境启动后，你可以将代码中的数据库连接修改为：

```python
# 在你的配置文件中修改数据库连接
HIVE_DSN = "postgresql+psycopg://postgres:postgres@localhost:5434/business"  # 业务数据库
TRAJECTORY_DSN = "postgresql+psycopg://postgres:postgres@localhost:5433/trajectory"  # 轨迹数据库  
MAP_DSN = "postgresql+psycopg://postgres:postgres@localhost:5435/mapdb"  # 地图数据库
```

或者设置环境变量：
```bash
export BUSINESS_DSN="postgresql+psycopg://postgres:postgres@localhost:5434/business"
export TRAJECTORY_DSN="postgresql+psycopg://postgres:postgres@localhost:5433/trajectory"
export MAP_DSN="postgresql+psycopg://postgres:postgres@localhost:5435/mapdb"
```

## 🧪 测试你的代码

现在你可以直接运行项目代码，它会连接到本地Mock数据库：

```bash
# 例如：测试bbox.py中的功能
python -c "
from src.spdatalab.dataset.bbox import fetch_meta
result = fetch_meta(['scene_001', 'scene_002'])
print('Meta data:', result)
"

# 测试spatial_join功能
python -c "
from src.spdatalab.fusion.spatial_join_production import quick_spatial_join
result, stats = quick_spatial_join(10, city_filter='BJ1')
print('Spatial join result:', len(result), 'records')
"
```

## 🔄 数据管理

```bash
# 查看服务日志
make mock-logs

# 重置测试数据
make mock-reset

# 停止Mock环境
make mock-down
```

## 📋 可用的测试数据

Mock环境包含以下测试数据：

### 轨迹数据库 (localhost:5433)
- **表**: `public.ddi_data_points`
- **数据**: 轨迹点数据，包含多个城市和场景
- **字段**: `point_lla`, `dataset_name`, `workstage`, `scene_id`

### 业务数据库 (localhost:5434)
- **表**: `transform.ods_t_data_fragment_datalake`
- **数据**: 场景元数据，包含事件ID、城市ID、时间戳等
- **字段**: `id`, `origin_name`, `event_id`, `city_id`, `timestamp`

### 地图数据库 (localhost:5435)
- **表**: `public.full_intersection`
- **数据**: 路口几何数据，包含不同类型的路口
- **字段**: `id`, `intersectiontype`, `intersectionsubtype`, `wkb_geometry`

### 测试城市
- `BJ1`: 北京测试城市1
- `SH1`: 上海测试城市1  
- `GZ1`: 广州测试城市1
- `A72`, `B15`: Mock测试城市

## 🐛 问题排查

### 数据库连接失败
```bash
# 检查服务状态
docker ps | grep mock

# 查看服务日志
make mock-logs

# 手动连接测试
psql -h localhost -p 5433 -U postgres -d trajectory
```

### 数据为空
```bash
# 重新生成测试数据
make mock-reset
make mock-init
```

### 端口冲突
如果端口被占用，可以修改 `mock/docker-compose.mock.yml` 中的端口映射。

## 💡 开发建议

1. **配置管理**: 使用环境变量或配置文件来切换Mock/生产环境
2. **数据规模**: 根据测试需要调整数据规模 (`--scale small/medium/large`)
3. **数据重置**: 测试过程中数据有问题时，随时可以重置
4. **并行开发**: Mock环境与主开发环境独立，可以并行使用 