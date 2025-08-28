# 数据库连接问题修复指南

## 问题概述

当运行多模态轨迹检索命令时，遇到以下错误：

```
2025-08-28 20:26:11,934 - spdatalab.dataset.polygon_trajectory_query - ERROR - 创建轨迹表失败: discovered_trajectories, 错误: (psycopg.OperationalError) [Errno -3] Temporary failure in name resolution
```

**原因分析：** 代码中使用 `local_pg:5432` 作为PostgreSQL主机名，但系统无法解析此主机名。

## 快速修复流程

### 步骤1：运行诊断脚本

```bash
python database_connection_diagnostic.py
```

诊断脚本会：
- 检查DNS解析状态
- 测试各种数据库连接方案
- 检查Docker容器状态
- 提供修复建议

### 步骤2：应用修复方案

根据诊断结果，运行修复脚本：

```bash
# 使用localhost（推荐）
python fix_database_config.py --host localhost

# 或使用IP地址
python fix_database_config.py --host 127.0.0.1
```

### 步骤3：验证修复效果

```bash
python test_multimodal_database_fix.py --quick --cleanup
```

### 步骤4：正常使用

修复后，可以正常运行原始命令：

```bash
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
  --text 'bicycle crossing intersection' \
  --collection 'ddi_collection_camera_encoded_1' \
  --output-table 'discovered_trajectories' \
  --verbose
```

## 详细故障排除

### 情况1：PostgreSQL未安装或未运行

**症状：** 所有数据库连接测试均失败

**解决方案：**

#### 选项A：使用Docker（推荐）

```bash
# 启动PostgreSQL容器
docker run -d --name local_pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest

# 等待容器启动（约30秒）
docker logs local_pg

# 验证容器状态
docker ps
```

#### 选项B：本地安装PostgreSQL

**Windows:**
1. 下载PostgreSQL安装包：https://www.postgresql.org/download/windows/
2. 安装时设置用户名：`postgres`，密码：`postgres`
3. 确保服务运行在端口5432

**Linux/macOS:**
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib

# CentOS/RHEL
sudo yum install postgresql postgresql-server

# macOS
brew install postgresql
```

设置数据库：
```bash
sudo -u postgres psql
postgres=# ALTER USER postgres PASSWORD 'postgres';
postgres=# \q
```

### 情况2：端口被占用

**症状：** `Address already in use` 错误

**解决方案：**

1. 检查端口占用：
```bash
# Windows
netstat -ano | findstr :5432

# Linux/macOS
lsof -i :5432
```

2. 停止占用进程或使用其他端口：
```bash
# 使用其他端口启动PostgreSQL
docker run -d --name local_pg -p 5433:5432 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest
```

3. 修改代码中的端口配置（在修复脚本中添加端口参数支持）

### 情况3：防火墙阻止连接

**症状：** `Connection refused` 错误

**解决方案：**

**Windows:**
```bash
# 添加防火墙规则
netsh advfirewall firewall add rule name="PostgreSQL" dir=in action=allow protocol=TCP localport=5432
```

**Linux:**
```bash
# Ubuntu/Debian
sudo ufw allow 5432

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=5432/tcp
sudo firewall-cmd --reload
```

### 情况4：Docker网络问题

**症状：** Docker容器运行但无法连接

**解决方案：**

1. 检查容器网络：
```bash
docker inspect local_pg | grep IPAddress
```

2. 使用容器IP地址：
```bash
python fix_database_config.py --host <容器IP地址>
```

3. 重新创建容器：
```bash
docker stop local_pg
docker rm local_pg
docker run -d --name local_pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest
```

## 配置文件说明

### 原始配置

文件：`src/spdatalab/dataset/polygon_trajectory_query.py`

```python
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
```

### 修复后配置

```python
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"
```

## 常见错误信息

### 1. DNS解析失败
```
[Errno -3] Temporary failure in name resolution
```
**解决：** 运行修复脚本替换主机名

### 2. 连接被拒绝
```
[Errno 111] Connection refused
```
**解决：** 检查PostgreSQL服务状态

### 3. 认证失败
```
FATAL: password authentication failed
```
**解决：** 检查用户名密码配置

### 4. 数据库不存在
```
FATAL: database "postgres" does not exist
```
**解决：** 创建postgres数据库或使用其他数据库名

## 验证脚本说明

### 诊断脚本功能
- `database_connection_diagnostic.py`：全面诊断数据库连接问题
- 自动检测网络、DNS、Docker状态
- 提供个性化修复建议

### 修复脚本功能
- `fix_database_config.py`：自动修复数据库配置
- 支持多种主机名选项
- 自动备份原始文件

### 测试脚本功能
- `test_multimodal_database_fix.py`：验证修复效果
- 支持快速测试模式
- 自动清理测试数据

## 生产环境部署建议

### 1. 环境变量配置

创建 `.env` 文件：
```bash
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=your_secure_password
DATABASE_NAME=spdatalab
```

### 2. 配置管理

修改代码使用环境变量：
```python
import os
from dotenv import load_dotenv

load_dotenv()

LOCAL_DSN = f"postgresql+psycopg://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
```

### 3. 数据库安全

- 使用强密码
- 限制网络访问
- 启用SSL连接
- 定期备份数据

## 联系支持

如果遇到其他问题，请提供：
1. 错误信息完整日志
2. 诊断脚本运行结果
3. 系统环境信息（OS、Python版本等）
4. PostgreSQL版本和配置信息

## 附录：手动验证方法

### 使用psql客户端测试
```bash
# 测试连接
psql -h localhost -p 5432 -U postgres -d postgres

# 或使用容器内psql
docker exec -it local_pg psql -U postgres
```

### 使用Python直接测试
```python
import psycopg

try:
    conn = psycopg.connect(
        "host=localhost port=5432 user=postgres password=postgres dbname=postgres"
    )
    print("✅ 数据库连接成功")
    conn.close()
except Exception as e:
    print(f"❌ 数据库连接失败: {e}")
```

### 检查网络连通性
```bash
# 测试端口是否开放
telnet localhost 5432

# 或使用nc命令
nc -zv localhost 5432
```
