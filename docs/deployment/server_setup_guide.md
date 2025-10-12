# PostgreSQL服务器迁移配置指南

本指南将帮助你在新服务器上配置PostgreSQL，为数据库迁移做准备。

## 🚀 新服务器环境准备

### 1. 系统要求检查

```bash
# 检查系统信息
uname -a
cat /etc/os-release

# 检查可用空间（确保足够存储迁移的数据）
df -h

# 检查内存
free -h

# 检查CPU
nproc
```

### 2. PostgreSQL安装

#### Ubuntu/Debian系统
```bash
# 更新软件包列表
sudo apt update

# 安装PostgreSQL
sudo apt install postgresql postgresql-contrib

# 启动并启用PostgreSQL服务
sudo systemctl start postgresql
sudo systemctl enable postgresql

# 检查服务状态
sudo systemctl status postgresql
```

#### CentOS/RHEL系统
```bash
# 安装PostgreSQL仓库
sudo yum install -y postgresql-server postgresql-contrib

# 初始化数据库
sudo postgresql-setup initdb

# 启动并启用服务
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### Docker安装（推荐用于快速部署）
```bash
# 创建数据目录
mkdir -p /data/postgresql

# 运行PostgreSQL容器
docker run -d \
  --name postgres-migration \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=your_password \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -v /data/postgresql:/var/lib/postgresql/data \
  postgres:15

# 检查容器状态
docker ps
docker logs postgres-migration
```

### 3. PostgreSQL配置优化

#### 基本配置文件位置
```bash
# 查找配置文件
sudo find / -name "postgresql.conf" 2>/dev/null
sudo find / -name "pg_hba.conf" 2>/dev/null

# 常见位置
# Ubuntu: /etc/postgresql/*/main/
# CentOS: /var/lib/pgsql/*/data/
# Docker: /var/lib/postgresql/data/
```

#### 编辑postgresql.conf
```bash
sudo vim /etc/postgresql/15/main/postgresql.conf
```

关键配置项：
```ini
# 监听地址
listen_addresses = '*'

# 端口
port = 5432

# 内存配置（根据服务器内存调整）
shared_buffers = 256MB          # 系统内存的25%
effective_cache_size = 1GB      # 系统内存的75%
work_mem = 4MB
maintenance_work_mem = 64MB

# 连接配置
max_connections = 100

# 日志配置
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_statement = 'all'
log_duration = on

# 检查点配置
checkpoint_completion_target = 0.9
wal_buffers = 16MB

# 临时文件目录（确保有足够空间）
temp_tablespaces = ''
```

#### 编辑pg_hba.conf（访问控制）
```bash
sudo vim /etc/postgresql/15/main/pg_hba.conf
```

添加访问规则：
```
# 类型  数据库    用户      地址          方法
local   all       all                     peer
host    all       all       127.0.0.1/32  md5
host    all       all       ::1/128       md5
host    all       all       0.0.0.0/0     md5    # 允许所有IP（生产环境需要限制）
```

### 4. 创建迁移用户

```bash
# 切换到postgres用户
sudo -u postgres psql

-- 在PostgreSQL中执行
CREATE USER migration_user WITH PASSWORD 'secure_password';
ALTER USER migration_user CREATEDB;
ALTER USER migration_user CREATEROLE;
ALTER USER migration_user SUPERUSER;

-- 退出
\q
```

### 5. 防火墙配置

#### Ubuntu/Debian (ufw)
```bash
sudo ufw allow 5432/tcp
sudo ufw reload
```

#### CentOS/RHEL (firewalld)
```bash
sudo firewall-cmd --permanent --add-port=5432/tcp
sudo firewall-cmd --reload
```

### 6. 重启服务
```bash
sudo systemctl restart postgresql
```

## 🔧 性能优化建议

### 1. 磁盘配置
```bash
# 检查磁盘I/O性能
sudo hdparm -tT /dev/sda

# 如果可能，将数据目录放在SSD上
# 将WAL日志放在单独的磁盘上
```

### 2. 内存配置计算器
```bash
# 创建配置计算脚本
cat > /tmp/pg_config_calc.py << 'EOF'
#!/usr/bin/env python3
import psutil

# 获取系统内存
total_mem_gb = psutil.virtual_memory().total / (1024**3)

print(f"系统总内存: {total_mem_gb:.1f} GB")
print("\n推荐PostgreSQL配置:")
print(f"shared_buffers = {int(total_mem_gb * 0.25 * 1024)}MB")
print(f"effective_cache_size = {int(total_mem_gb * 0.75 * 1024)}MB")
print(f"work_mem = {max(4, int(total_mem_gb * 1024 / 100))}MB")
print(f"maintenance_work_mem = {int(total_mem_gb * 0.05 * 1024)}MB")
EOF

python3 /tmp/pg_config_calc.py
```

### 3. 连接池配置（可选）
```bash
# 安装pgbouncer
sudo apt install pgbouncer

# 配置文件: /etc/pgbouncer/pgbouncer.ini
# 详细配置请参考pgbouncer文档
```

## 🧪 迁移前测试

### 1. 连接测试
```bash
# 本地连接
psql -h localhost -U migration_user -d postgres

# 远程连接测试
psql -h your_server_ip -U migration_user -d postgres
```

### 2. 性能基准测试
```bash
# 使用pgbench进行基准测试
sudo -u postgres createdb pgbench_test
sudo -u postgres pgbench -i -s 50 pgbench_test
sudo -u postgres pgbench -c 10 -j 2 -t 1000 pgbench_test

# 清理测试数据库
sudo -u postgres dropdb pgbench_test
```

### 3. 磁盘空间监控
```bash
# 创建监控脚本
cat > /tmp/disk_monitor.sh << 'EOF'
#!/bin/bash
while true; do
    echo "$(date): $(df -h / | tail -1)"
    sleep 60
done
EOF

chmod +x /tmp/disk_monitor.sh
# 在后台运行监控
nohup /tmp/disk_monitor.sh > /tmp/disk_usage.log 2>&1 &
```

## 📋 迁移前检查清单

- [ ] PostgreSQL服务正常运行
- [ ] 网络连接正常（可以从源服务器访问目标服务器）
- [ ] 磁盘空间充足（至少是源数据库大小的2倍）
- [ ] 内存配置合理
- [ ] 防火墙规则正确
- [ ] 用户权限配置完成
- [ ] 备份目录创建并有写权限
- [ ] 性能基准测试完成

## 🚨 安全注意事项

1. **网络安全**
   - 使用VPN或专用网络进行迁移
   - 迁移完成后限制访问规则
   - 使用强密码

2. **权限控制**
   - 迁移完成后删除临时用户
   - 最小权限原则
   - 定期更新密码

3. **数据加密**
   - 启用SSL连接
   - 考虑数据库级别加密

## 📞 故障排除

### 常见问题

1. **连接被拒绝**
   ```bash
   # 检查服务状态
   sudo systemctl status postgresql
   
   # 检查端口监听
   sudo netstat -tlnp | grep 5432
   
   # 检查日志
   sudo tail -f /var/log/postgresql/postgresql-*.log
   ```

2. **内存不足**
   ```bash
   # 检查内存使用
   free -h
   
   # 调整PostgreSQL内存配置
   # 重启服务
   sudo systemctl restart postgresql
   ```

3. **磁盘空间不足**
   ```bash
   # 清理日志文件
   sudo find /var/log -name "*.log" -mtime +7 -delete
   
   # 清理临时文件
   sudo rm -rf /tmp/pg*
   ```

完成以上配置后，你的新服务器就准备好接收数据库迁移了！


