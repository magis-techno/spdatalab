# Mock数据系统开发文档

## 1. 架构概览

### 1.1 系统架构
```
┌─────────────────────────────────────────────────────────────┐
│                     Mock数据系统                            │
├─────────────────────────────────────────────────────────────┤
│  Application Layer                                          │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Mock Manager  │    │     Data Generators             │ │
│  │   Service       │    │  ┌─────────┐ ┌─────────┐       │ │
│  │                 │    │  │Trajectory│ │Business │       │ │
│  │  - Health Check │    │  │Generator │ │Generator│       │ │
│  │  - Data Control │    │  │         │ │         │       │ │
│  │  - Status API   │    │  └─────────┘ └─────────┘       │ │
│  └─────────────────┘    │  ┌─────────┐ ┌─────────┐       │ │
│                         │  │   Map   │ │  JSONL  │       │ │
│                         │  │Generator│ │Processor│       │ │
│  ┌─────────────────────┐ │  └─────────┘ └─────────┘       │ │
│  │  Configuration      │ └─────────────────────────────────┘ │
│  │  Management         │                                     │
│  └─────────────────────┘                                     │
├─────────────────────────────────────────────────────────────┤
│  Database Layer                                             │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │ Trajectory  │ │  Business   │ │      Map Database       ││
│  │ Database    │ │  Database   │ │                         ││
│  │             │ │             │ │  - PostGIS Extension    ││
│  │ PostGIS     │ │ PostgreSQL  │ │  - Intersection Data    ││
│  │ Port: 5433  │ │ Port: 5434  │ │  - Road Networks        ││
│  │             │ │             │ │  Port: 5435            ││
│  └─────────────┘ └─────────────┘ └─────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│  Infrastructure Layer                                       │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Docker Compose                             ││
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐││
│  │  │  Container  │ │  Container  │ │     Container       │││
│  │  │  Network    │ │   Volumes   │ │     Registry        │││
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### 1.2 核心组件

#### Mock Manager Service
- **职责**: 统一管理各个Mock数据库
- **技术栈**: Python + Flask + SQLAlchemy
- **功能**: 健康检查、数据生成控制、状态监控

#### Data Generators
- **轨迹生成器**: 生成GPS轨迹数据，支持多种移动模式
- **业务生成器**: 生成用户和配置数据
- **地图生成器**: 生成交叉口和道路网络数据
- **JSONL处理器**: 处理地址文件，支持去重和统计

#### Database Cluster
- **3个PostgreSQL实例**: 独立运行，端口隔离
- **PostGIS扩展**: 支持地理空间数据
- **数据持久化**: Docker卷管理

## 2. 目录结构

```
mock/
├── README.md                 # 快速开始指南
├── REQUIREMENTS.md           # 需求文档  
├── DEVELOPMENT.md           # 开发文档（本文档）
├── docker-compose.mock.yml  # Docker编排文件
├── Dockerfile.mock          # Mock服务镜像构建
├── requirements.txt         # Python依赖包
├── config/                  # 配置文件目录
│   ├── database/           # 数据库配置
│   │   ├── trajectory_init.sql
│   │   ├── business_init.sql
│   │   └── map_init.sql
│   └── app/               # 应用配置
│       └── config.py
├── src/                    # 源代码目录
│   ├── __init__.py
│   ├── mock_manager.py    # Mock管理服务
│   ├── generators/        # 数据生成器
│   │   ├── __init__.py
│   │   ├── trajectory.py  # 轨迹数据生成
│   │   ├── business.py    # 业务数据生成
│   │   ├── map_data.py    # 地图数据生成
│   │   └── jsonl_processor.py # JSONL处理
│   └── utils/             # 工具函数
│       ├── __init__.py
│       ├── database.py    # 数据库连接工具
│       └── geo_utils.py   # 地理数据工具
├── data/                  # 数据目录
│   ├── input/            # 输入数据文件
│   ├── output/           # 输出数据文件
│   └── templates/        # 数据模板
└── tests/                # 测试文件
    ├── __init__.py
    ├── test_generators.py
    ├── test_database.py
    └── test_integration.py
```

## 3. 开发环境搭建

### 3.1 环境要求
- **Docker**: 20.10+
- **Docker Compose**: 1.29+  
- **Python**: 3.9+（开发调试用）
- **Make**: GNU Make 4.0+（可选）

### 3.2 快速启动
```bash
# 克隆项目并进入目录
cd your_project_root

# 启动Mock环境
make mock-up

# 等待服务启动完成（约1-2分钟）
make mock-status

# 初始化测试数据
make mock-init
```

### 3.3 开发模式启动
```bash
# 仅启动数据库服务
docker-compose -f mock/docker-compose.mock.yml up -d mock_trajectory_db mock_business_db mock_map_db

# 本地运行Mock管理服务（便于调试）
cd mock
pip install -r requirements.txt
python src/mock_manager.py
```

## 4. 配置管理

### 4.1 数据库配置
文件位置: `mock/config/app/config.py`

```python
# 数据库连接配置
DATABASE_CONFIGS = {
    'trajectory': {
        'host': 'mock_trajectory_db',  # 容器内使用容器名
        'port': 5432,                  # 容器内端口
        'database': 'trajectory_db',
        'user': 'mockuser',
        'password': 'mockpass'
    },
    'business': {
        'host': 'mock_business_db',
        'port': 5432,
        'database': 'business_db', 
        'user': 'mockuser',
        'password': 'mockpass'
    },
    'map': {
        'host': 'mock_map_db',
        'port': 5432,
        'database': 'map_db',
        'user': 'mockuser', 
        'password': 'mockpass'
    }
}

# 数据生成配置
GENERATION_CONFIGS = {
    'trajectory': {
        'batch_size': 1000,
        'default_users': 10,
        'time_range_days': 7
    },
    'business': {
        'user_count': 100,
        'config_count': 50
    },
    'map': {
        'intersection_count': 200,
        'city_bounds': {
            'min_lat': 39.9,
            'max_lat': 40.0,
            'min_lng': 116.3,
            'max_lng': 116.5
        }
    }
}
```

### 4.2 Docker配置
文件位置: `mock/docker-compose.mock.yml`

关键配置项：
- **网络**: 所有服务使用`mock_network`内部网络
- **卷持久化**: 数据库数据持久化到`mock_*_data`卷
- **端口映射**: 数据库端口映射到宿主机
- **健康检查**: 数据库服务健康状态监控

## 5. 数据生成器开发

### 5.1 轨迹数据生成器
文件位置: `mock/src/generators/trajectory.py`

```python
class TrajectoryGenerator:
    def __init__(self, db_config):
        self.db_config = db_config
        
    def generate_user_trajectory(self, user_id, start_time, duration_hours):
        """生成单个用户的轨迹数据"""
        pass
        
    def generate_route_trajectory(self, start_point, end_point, transport_mode):
        """生成路径轨迹数据"""  
        pass
```

### 5.2 业务数据生成器
```python
class BusinessGenerator:
    def generate_users(self, count):
        """生成用户数据"""
        pass
        
    def generate_configurations(self, count):
        """生成配置数据"""
        pass
```

### 5.3 自定义生成器开发规范
1. **继承基类**: 所有生成器继承`BaseGenerator`
2. **配置驱动**: 支持通过配置文件调整生成参数
3. **批量处理**: 支持大批量数据生成
4. **错误处理**: 完善的异常处理机制
5. **日志记录**: 详细的生成过程日志

## 6. API接口开发

### 6.1 Mock Manager REST API

#### 健康检查
```http
GET /health
Response: {"status": "healthy", "databases": {...}}
```

#### 数据生成
```http
POST /generate/trajectory
Content-Type: application/json
{
    "user_count": 10,
    "time_range_days": 7
} 
Response: {"status": "success", "generated_count": 7000}
```

#### 数据清理
```http
DELETE /clean/trajectory
Response: {"status": "success", "deleted_count": 7000}
```

#### 状态查询
```http
GET /status
Response: {
    "databases": {
        "trajectory": {"status": "running", "record_count": 7000},
        "business": {"status": "running", "record_count": 150},
        "map": {"status": "running", "record_count": 200}
    }
}
```

### 6.2 API开发规范
1. **RESTful设计**: 遵循REST API设计原则
2. **统一响应格式**: JSON格式，统一状态码
3. **错误处理**: 详细的错误信息和状态码
4. **参数验证**: 输入参数严格验证
5. **日志记录**: API调用日志记录

## 7. 测试指南

### 7.1 单元测试
```bash
# 运行所有测试
cd mock
python -m pytest tests/

# 运行特定测试
python -m pytest tests/test_generators.py -v

# 测试覆盖率
python -m pytest --cov=src tests/
```

### 7.2 集成测试
```bash
# 启动测试环境
make mock-up

# 运行集成测试
python -m pytest tests/test_integration.py -v

# 清理测试环境
make mock-down
```

### 7.3 性能测试
```bash
# 数据生成性能测试
python tests/performance/test_generation_speed.py

# 数据库连接压力测试  
python tests/performance/test_db_connections.py
```

## 8. 部署指南

### 8.1 开发环境部署
```bash
# 快速启动
make mock-up

# 检查状态
make mock-status

# 查看日志
make mock-logs
```

### 8.2 生产环境部署
```bash
# 使用生产配置
docker-compose -f mock/docker-compose.mock.yml -f mock/docker-compose.prod.yml up -d

# 配置资源限制
# 在docker-compose.prod.yml中设置CPU和内存限制

# 设置数据备份
# 配置定时备份脚本
```

### 8.3 Docker配置优化
```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  mock_trajectory_db:
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: '0.5'
    restart: unless-stopped
```

## 9. 故障排除

### 9.1 常见问题

#### 数据库启动失败
**现象**: 容器启动后立即退出
**原因**: PostGIS扩展未安装或初始化脚本错误
**解决**: 
```bash
# 检查容器日志
docker logs mock_trajectory_db

# 手动连接数据库检查
docker exec mock_trajectory_db psql -U mockuser -d trajectory_db -c "SELECT PostGIS_version();"
```

#### 容器间网络连接失败
**现象**: Mock管理服务无法连接数据库
**原因**: 使用localhost而非容器名连接
**解决**: 配置文件中使用容器名作为主机名

#### Windows下Make命令不识别
**现象**: "make 无法识别" 错误
**解决方案**:
1. 使用Git Bash执行make命令
2. 安装Chocolatey和make工具
3. 直接使用docker-compose命令

### 9.2 调试技巧

#### 查看服务状态
```bash
# 查看所有容器状态
docker-compose -f mock/docker-compose.mock.yml ps

# 查看特定服务日志
docker-compose -f mock/docker-compose.mock.yml logs mock_manager

# 进入容器调试
docker exec -it mock_trajectory_db bash
```

#### 数据库调试
```bash
# 连接数据库
docker exec -it mock_trajectory_db psql -U mockuser -d trajectory_db

# 检查表结构
\dt
\d trajectories

# 查看数据统计
SELECT COUNT(*) FROM trajectories;
```

### 9.3 性能监控
```bash
# 查看容器资源使用
docker stats

# 查看数据库连接数
docker exec mock_trajectory_db psql -U mockuser -d trajectory_db -c "SELECT COUNT(*) FROM pg_stat_activity;"

# 监控数据库性能
docker exec mock_trajectory_db psql -U mockuser -d trajectory_db -c "SELECT * FROM pg_stat_database WHERE datname='trajectory_db';"
```

## 10. 扩展开发

### 10.1 添加新数据库类型
1. 在`docker-compose.mock.yml`中添加新服务
2. 创建数据库初始化脚本
3. 开发对应的数据生成器
4. 更新配置文件和API接口

### 10.2 添加新数据生成器
1. 在`src/generators/`下创建新生成器文件
2. 继承`BaseGenerator`基类
3. 实现必要的生成方法
4. 在`mock_manager.py`中注册新生成器

### 10.3 自定义配置
1. 修改`config/app/config.py`中的配置
2. 支持环境变量覆盖配置
3. 提供配置验证机制

## 11. 最佳实践

### 11.1 开发实践
- **代码规范**: 遵循PEP 8 Python代码规范
- **类型提示**: 使用Type Hints提高代码可读性
- **文档字符串**: 为所有公共方法添加docstring
- **错误处理**: 使用特定异常类型，避免捕获通用异常

### 11.2 数据库实践
- **连接池**: 使用连接池管理数据库连接
- **事务管理**: 批量操作使用事务
- **索引优化**: 为常用查询字段添加索引
- **定期清理**: 定期清理测试数据，避免数据库膨胀

### 11.3 容器实践
- **镜像优化**: 使用多阶段构建减小镜像大小
- **资源限制**: 设置合理的CPU和内存限制
- **健康检查**: 为所有服务配置健康检查
- **日志管理**: 配置适当的日志级别和轮转

## 12. 版本管理

### 12.1 版本规划
- **v1.0**: 基础功能实现（当前版本）
- **v1.1**: 性能优化和Bug修复
- **v1.2**: 新数据类型支持
- **v2.0**: 集群部署支持

### 12.2 发布流程
1. 功能开发和测试
2. 代码审查
3. 性能测试
4. 文档更新
5. 版本标签和发布 