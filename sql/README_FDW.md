# Foreign Data Wrapper (FDW) 使用说明

## 概述

FDW (Foreign Data Wrapper) 允许PostgreSQL访问远程数据库的表，就像访问本地表一样。本项目使用FDW连接远程的trajectory和map数据库。

## 文件说明

| 文件 | 用途 |
|------|------|
| `init_fdw.sql` | FDW初始化脚本（需要手动配置） |
| `init_fdw.example.sql` | 配置示例文件 |
| `cleanup_fdw.sql` | FDW清理脚本 |

## 快速开始

### 1. 配置FDW连接

```bash
# 方法1: 直接修改init_fdw.sql
vim sql/init_fdw.sql

# 方法2: 从示例文件复制
cp sql/init_fdw.example.sql sql/init_fdw.sql
vim sql/init_fdw.sql
```

### 2. 修改连接参数

编辑 `sql/init_fdw.sql`，替换以下占位符：

```sql
-- Trajectory数据库
host 'REMOTE_TRAJ_HOST'     → host '192.168.1.100' 
dbname 'trajdb'             → dbname 'actual_traj_db'
user :fdw_user              → user 'traj_username'
password :fdw_pwd           → password 'traj_password'

-- Map数据库  
host 'REMOTE_MAP_HOST'      → host '192.168.1.101'
dbname 'mapdb'              → dbname 'actual_map_db'
user :fdw_user              → user 'map_username'
password :fdw_pwd           → password 'map_password'
```

### 3. 初始化FDW连接

```bash
# 启动数据库环境
make up

# 初始化FDW连接
make init-fdw

# 检查连接状态
make check-fdw
```

### 4. 验证连接

```bash
# 进入数据库
make psql

# 测试trajectory表
SELECT COUNT(*) FROM public.ddi_data_points;

# 测试map表
SELECT COUNT(*) FROM public.intersections;

# 退出
\q
```

## 管理命令

### Makefile命令

```bash
# 初始化FDW连接
make init-fdw

# 检查FDW状态
make check-fdw

# 清理FDW连接
make clean-fdw

# 查看所有命令
make help
```

### 手动SQL命令

```bash
# 直接运行初始化脚本
make psql
\i sql/init_fdw.sql

# 直接运行清理脚本
\i sql/cleanup_fdw.sql
```

## 故障排除

### 常见错误

1. **连接被拒绝**
   ```
   could not connect to server "traj_srv"
   ```
   - 检查网络连通性
   - 确认远程数据库正在运行
   - 检查防火墙设置

2. **认证失败**
   ```
   password authentication failed
   ```
   - 确认用户名和密码正确
   - 检查远程数据库的pg_hba.conf配置

3. **表不存在**
   ```
   relation "ddi_data_points" does not exist
   ```
   - 确认远程数据库中表名正确
   - 检查schema名称

4. **权限不足**
   ```
   permission denied for table
   ```
   - 确认远程用户有表的SELECT权限
   - 检查schema的USAGE权限

### 调试步骤

```bash
# 1. 检查网络连通性
ping REMOTE_HOST

# 2. 测试数据库连接
psql -h REMOTE_HOST -U USERNAME -d DATABASE

# 3. 检查FDW状态
make check-fdw

# 4. 查看详细错误信息
make psql
SELECT * FROM pg_foreign_server;
SELECT * FROM pg_foreign_tables;
```

## 性能注意事项

1. **查询优化**
   - FDW查询可能比本地查询慢
   - 使用WHERE条件减少数据传输
   - 避免不必要的JOIN操作

2. **网络影响**
   - 网络延迟影响查询性能
   - 大数据量查询考虑分批处理
   - 必要时考虑数据复制到本地

3. **监控和维护**
   - 定期检查FDW连接状态
   - 监控远程数据库性能
   - 必要时重新初始化连接

## 安全考虑

1. **密码管理**
   - 不要在Git中提交包含密码的配置文件
   - 考虑使用环境变量或密钥文件
   - 定期更换数据库密码

2. **网络安全**
   - 使用SSL加密连接
   - 限制数据库访问IP范围
   - 配置适当的防火墙规则

3. **权限控制**
   - 为FDW创建专用的数据库用户
   - 只授予必要的表访问权限
   - 定期审核用户权限

---

*最后更新: 2024-12-19* 