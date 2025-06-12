# Mock数据系统需求文档

## 1. 项目背景

### 1.1 业务背景
为了实现开发和测试环境的数据隔离，需要构建一套Mock数据系统来模拟生产环境中的3个远程数据库和本地数据文件处理能力。

### 1.2 目标
- 提供稳定、可控的测试数据环境
- 支持快速的开发调试
- 减少对生产数据的依赖
- 提高开发效率和测试覆盖率

## 2. 功能需求

### 2.1 数据库Mock需求

#### 2.1.1 轨迹数据库 (Trajectory Database)
- **用途**: 存储GPS轨迹点数据
- **核心字段**: point_lla（经纬度几何字段）
- **数据特征**: 
  - 大量时序数据
  - 地理空间数据
  - 高频更新
- **Mock要求**: 
  - 支持PostGIS地理扩展
  - 生成真实的GPS轨迹数据
  - 模拟不同的移动模式（步行、驾车、公交等）

#### 2.1.2 业务数据库 (Business Database)
- **用途**: 存储业务元数据
- **数据特征**: 
  - 用户信息
  - 业务配置
  - 系统参数
- **Mock要求**: 
  - 关系型数据结构
  - 支持复杂查询
  - 数据一致性保证

#### 2.1.3 地图数据库 (Map Database)
- **用途**: 存储地图交叉口数据
- **数据特征**: 
  - 道路网络信息
  - 交叉口几何数据
  - 路网拓扑关系
- **Mock要求**: 
  - 支持PostGIS地理扩展
  - 真实的道路网络数据
  - 准确的地理坐标信息

### 2.2 本地数据处理需求

#### 2.2.1 JSONL地址文件处理
- **输入**: 包含地址信息的txt文件（JSONL格式）
- **特征**: 数据存在重复，需要去重处理
- **处理需求**: 
  - 解析JSONL格式数据
  - 统计重复数量
  - 数据清洗和标准化
  - 支持批量处理

## 3. 技术需求

### 3.1 部署需求
- **容器化**: 基于Docker的完整解决方案
- **编排**: Docker Compose管理多容器服务
- **集成**: 与现有Makefile无缝集成
- **跨平台**: 支持Windows、Linux、macOS

### 3.2 性能需求
- **启动时间**: 完整环境启动时间 < 2分钟
- **数据生成**: 支持大规模测试数据快速生成
- **资源占用**: 合理的CPU和内存使用
- **网络**: 容器间高效通信

### 3.3 数据库需求
- **类型**: PostgreSQL 13+
- **扩展**: PostGIS 3.0+（地理数据支持）
- **端口**: 
  - 轨迹数据库: 5433
  - 业务数据库: 5434  
  - 地图数据库: 5435
- **持久化**: 数据卷持久化存储

### 3.4 开发需求
- **语言**: Python 3.9+
- **框架**: 
  - 数据生成: Faker, GeoPandas
  - 数据库连接: psycopg2, SQLAlchemy
  - 地理处理: Shapely, PostGIS
- **包管理**: requirements.txt
- **国内优化**: 使用阿里云镜像源

## 4. 接口规格

### 4.1 Makefile命令接口
```bash
make mock-up          # 启动Mock环境
make mock-down        # 停止Mock环境  
make mock-init        # 初始化测试数据
make mock-clean       # 清理所有数据
make mock-status      # 查看服务状态
make mock-logs        # 查看服务日志
```

### 4.2 数据库连接接口
```
轨迹数据库: localhost:5433/trajectory_db
业务数据库: localhost:5434/business_db
地图数据库: localhost:5435/map_db
默认用户: mockuser/mockpass
```

### 4.3 Mock管理服务接口
- **健康检查**: HTTP GET /health
- **数据生成**: HTTP POST /generate/{db_type}
- **数据清理**: HTTP DELETE /clean/{db_type}
- **状态查询**: HTTP GET /status

## 5. 数据模型

### 5.1 轨迹数据表结构
```sql
CREATE TABLE trajectories (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    point_lla GEOMETRY(POINT, 4326),
    timestamp TIMESTAMP,
    speed REAL,
    heading REAL,
    accuracy REAL
);
```

### 5.2 业务数据表结构
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE configurations (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100),
    value TEXT,
    description TEXT,
    updated_at TIMESTAMP
);
```

### 5.3 地图数据表结构
```sql
CREATE TABLE intersections (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    location GEOMETRY(POINT, 4326),
    type VARCHAR(50),
    roads TEXT[],
    traffic_lights BOOLEAN
);
```

## 6. 质量要求

### 6.1 可用性
- 系统可用性 > 99%
- 支持快速重启和恢复
- 完善的错误处理机制

### 6.2 可维护性  
- 清晰的代码结构
- 完整的文档说明
- 标准化的开发流程

### 6.3 可扩展性
- 支持新数据库类型扩展
- 支持自定义数据生成器
- 支持配置化管理

## 7. 验收标准

### 7.1 功能验收
- [ ] 三个数据库容器正常启动
- [ ] 数据库连接测试通过
- [ ] 测试数据成功生成
- [ ] JSONL文件处理功能正常
- [ ] Makefile命令执行成功

### 7.2 性能验收  
- [ ] 环境启动时间 < 2分钟
- [ ] 数据生成速度 > 1000条/秒
- [ ] 内存使用 < 1GB
- [ ] CPU使用率 < 50%

### 7.3 兼容性验收
- [ ] Windows 10+ 环境测试通过
- [ ] Linux 环境测试通过
- [ ] Docker版本兼容性测试通过 