# PostgreSQL数据库迁移完整指南

本指南提供了完整的PostgreSQL数据库迁移流程，包括准备、执行、验证和测试步骤。

## 📋 迁移概览

### 迁移流程
1. **准备阶段** - 环境准备和配置
2. **备份阶段** - 源数据库备份
3. **迁移阶段** - 数据传输和恢复
4. **验证阶段** - 数据完整性检查
5. **测试阶段** - 功能测试和性能验证
6. **切换阶段** - 生产环境切换

## 🚀 快速开始

### 1. 环境准备

首先准备新服务器环境：
```bash
# 参考详细配置指南
cat troubleshooting/server_setup_guide.md

# 快速验证环境
python troubleshooting/quick_space_check.py
```

### 2. 执行迁移

```bash
# 方法1: 使用自动化迁移脚本
python troubleshooting/database_migration.py \
  --source-host old-server.com \
  --source-username postgres \
  --source-password old_password \
  --target-host new-server.com \
  --target-username postgres \
  --target-password new_password

# 方法2: 手动备份和恢复
python troubleshooting/database_backup.py \
  --host old-server.com \
  --username postgres \
  --password old_password \
  --backup-dir ./backups
```

### 3. 验证迁移

```bash
# 运行完整验证测试
python troubleshooting/migration_test.py \
  --source-host old-server.com \
  --source-username postgres \
  --source-password old_password \
  --target-host new-server.com \
  --target-username postgres \
  --target-password new_password
```

## 📝 详细迁移步骤

### 阶段1: 准备工作

#### 1.1 评估源数据库
```bash
# 检查数据库大小
psql -h source-host -U username -c "
SELECT 
    pg_database.datname,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS size
FROM pg_database
ORDER BY pg_database_size(pg_database.datname) DESC;
"

# 检查连接数
psql -h source-host -U username -c "
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';
"

# 检查长时间运行的查询
psql -h source-host -U username -c "
SELECT now() - pg_stat_activity.query_start AS duration, query 
FROM pg_stat_activity 
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
"
```

#### 1.2 新服务器准备
```bash
# 1. 按照服务器配置指南设置新环境
# 2. 确保磁盘空间足够（建议是源数据库大小的3倍）
# 3. 配置网络连接
# 4. 安装相同版本的PostgreSQL
```

#### 1.3 创建迁移配置文件
```bash
# 创建配置文件
cat > migration_config.json << 'EOF'
{
  "source": {
    "host": "old-server.com",
    "port": "5432",
    "username": "postgres",
    "password": "source_password"
  },
  "target": {
    "host": "new-server.com",
    "port": "5432", 
    "username": "postgres",
    "password": "target_password"
  },
  "options": {
    "backup_dir": "/backup",
    "parallel_jobs": 4,
    "compress": true
  }
}
EOF
```

### 阶段2: 备份执行

#### 2.1 全量备份
```bash
# 使用备份脚本
python troubleshooting/database_backup.py \
  --config-file migration_config.json \
  --backup-dir /backup/$(date +%Y%m%d)

# 手动备份（适用于大型数据库）
# 1. 备份全局对象
pg_dumpall --host=source-host --username=postgres --globals-only > globals.sql

# 2. 并行备份每个数据库
pg_dump --host=source-host --username=postgres --format=directory --jobs=4 --file=db1_backup database1
pg_dump --host=source-host --username=postgres --format=directory --jobs=4 --file=db2_backup database2
```

#### 2.2 增量备份（可选）
```bash
# 如果数据库很大，可以考虑WAL-E或Barman等工具
# 这里提供基本的WAL备份方法

# 启用WAL归档（在postgresql.conf中）
# wal_level = replica
# archive_mode = on
# archive_command = 'test ! -f /backup/wal/%f && cp %p /backup/wal/%f'

# 创建基础备份
pg_basebackup -h source-host -D /backup/basebackup -U postgres -P -W
```

### 阶段3: 迁移执行

#### 3.1 传输备份文件
```bash
# 方法1: rsync同步
rsync -avz --progress /backup/ target-host:/backup/

# 方法2: scp复制
scp -r /backup/* target-host:/backup/

# 方法3: 直接网络迁移（推荐）
python troubleshooting/database_migration.py --config-file migration_config.json
```

#### 3.2 恢复数据库
```bash
# 1. 恢复全局对象
psql -h target-host -U postgres < globals.sql

# 2. 创建数据库
createdb -h target-host -U postgres database1

# 3. 恢复数据
pg_restore -h target-host -U postgres -d database1 --jobs=4 db1_backup

# 或使用迁移脚本自动化
python troubleshooting/database_migration.py \
  --config-file migration_config.json \
  --databases database1 database2
```

### 阶段4: 验证数据

#### 4.1 自动化验证
```bash
# 运行完整验证套件
python troubleshooting/migration_test.py --config-file migration_config.json

# 生成验证报告
# 报告将保存为migration_test_report_TIMESTAMP.json
```

#### 4.2 手动验证关键数据
```bash
# 1. 检查表数量
psql -h target-host -U postgres -d database1 -c "
SELECT schemaname, count(*) as table_count 
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog') 
GROUP BY schemaname;
"

# 2. 检查关键表的行数
psql -h target-host -U postgres -d database1 -c "
SELECT 'users' as table_name, count(*) as row_count FROM users
UNION ALL
SELECT 'orders', count(*) FROM orders
UNION ALL  
SELECT 'products', count(*) FROM products;
"

# 3. 检查序列当前值
psql -h target-host -U postgres -d database1 -c "
SELECT sequence_name, last_value 
FROM information_schema.sequences s
JOIN pg_sequences ps ON s.sequence_name = ps.sequencename;
"
```

### 阶段5: 性能测试

#### 5.1 基准测试
```bash
# 使用pgbench进行基准测试
pgbench -h target-host -U postgres -i -s 100 test_bench
pgbench -h target-host -U postgres -c 10 -j 2 -t 1000 test_bench

# 自定义性能测试脚本
cat > performance_test.sql << 'EOF'
-- 测试查询性能
\timing on
SELECT count(*) FROM users;
SELECT * FROM orders WHERE created_at > now() - interval '1 day';
SELECT u.name, count(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100;
\timing off
EOF

psql -h target-host -U postgres -d database1 -f performance_test.sql
```

#### 5.2 应用程序连接测试
```bash
# 创建应用测试脚本
cat > app_connection_test.py << 'EOF'
#!/usr/bin/env python3
import psycopg2
import time

def test_connection():
    try:
        conn = psycopg2.connect(
            host="new-server.com",
            port="5432", 
            user="app_user",
            password="app_password",
            database="your_database"
        )
        cursor = conn.cursor()
        
        # 测试基本查询
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ 数据库连接成功: {version}")
        
        # 测试应用查询
        cursor.execute("SELECT count(*) FROM users;")
        count = cursor.fetchone()[0]
        print(f"✅ 用户表查询成功: {count} 条记录")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False

if __name__ == "__main__":
    test_connection()
EOF

python app_connection_test.py
```

### 阶段6: 切换准备

#### 6.1 应用程序配置更新
```bash
# 1. 更新应用配置文件中的数据库连接信息
# 2. 准备回滚计划
# 3. 通知相关人员

# 创建配置模板
cat > database_config_template.conf << 'EOF'
# 新数据库配置
DB_HOST=new-server.com
DB_PORT=5432
DB_NAME=your_database
DB_USER=app_user
DB_PASSWORD=new_password

# 连接池配置
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=30
DB_POOL_TIMEOUT=30
EOF
```

#### 6.2 切换检查清单
```bash
# 创建切换检查清单
cat > cutover_checklist.md << 'EOF'
# 数据库切换检查清单

## 切换前检查 (T-1小时)
- [ ] 数据迁移验证通过
- [ ] 性能测试通过  
- [ ] 应用程序测试通过
- [ ] 备份策略已配置
- [ ] 监控系统已更新
- [ ] 团队成员已通知

## 切换执行 (T-0)
- [ ] 停止应用程序写入
- [ ] 执行最后一次增量同步
- [ ] 更新应用程序配置
- [ ] 启动应用程序
- [ ] 验证应用程序功能

## 切换后验证 (T+30分钟)
- [ ] 应用程序正常运行
- [ ] 数据库性能正常
- [ ] 用户功能正常
- [ ] 监控指标正常
- [ ] 日志无异常

## 回滚计划
- [ ] 回滚步骤已准备
- [ ] 回滚时间窗口确定
- [ ] 回滚责任人确定
EOF
```

## 🛠️ 故障排除

### 常见问题及解决方案

#### 1. 迁移过程中断
```bash
# 查看迁移日志
tail -f migration_log_*.txt

# 恢复中断的迁移
python troubleshooting/database_migration.py \
  --config-file migration_config.json \
  --databases remaining_database_list
```

#### 2. 权限问题
```bash
# 检查用户权限
psql -h target-host -U postgres -c "\du"

# 重新授权
GRANT ALL PRIVILEGES ON DATABASE your_database TO app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO app_user;
```

#### 3. 性能问题
```bash
# 更新统计信息
psql -h target-host -U postgres -d database1 -c "ANALYZE;"

# 重建索引
psql -h target-host -U postgres -d database1 -c "REINDEX DATABASE database1;"

# 清理死元组
psql -h target-host -U postgres -d database1 -c "VACUUM FULL;"
```

#### 4. 连接问题
```bash
# 检查网络连接
telnet new-server.com 5432

# 检查防火墙
sudo ufw status
sudo firewall-cmd --list-all

# 检查PostgreSQL配置
grep -E "(listen_addresses|port)" /etc/postgresql/*/main/postgresql.conf
grep -E "host.*all" /etc/postgresql/*/main/pg_hba.conf
```

## 📊 监控和维护

### 设置监控
```bash
# 1. 设置数据库监控
# 2. 配置磁盘空间警报
# 3. 设置性能基线

# 示例监控脚本
cat > monitor_database.py << 'EOF'
#!/usr/bin/env python3
import psycopg2
import time

def monitor_connections():
    conn = psycopg2.connect(host="new-server.com", user="postgres", database="postgres")
    cursor = conn.cursor()
    
    cursor.execute("SELECT count(*) FROM pg_stat_activity;")
    active_connections = cursor.fetchone()[0]
    
    print(f"活跃连接数: {active_connections}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    while True:
        monitor_connections()
        time.sleep(60)
EOF
```

### 维护计划
```bash
# 创建维护脚本
cat > maintenance_tasks.sh << 'EOF'
#!/bin/bash

# 每日维护任务
echo "开始每日维护任务: $(date)"

# 更新统计信息
psql -h new-server.com -U postgres -d database1 -c "ANALYZE;"

# 清理死元组 
psql -h new-server.com -U postgres -d database1 -c "VACUUM;"

# 检查数据库大小
psql -h new-server.com -U postgres -c "
SELECT datname, pg_size_pretty(pg_database_size(datname)) 
FROM pg_database 
WHERE datistemplate = false;
"

echo "每日维护任务完成: $(date)"
EOF

chmod +x maintenance_tasks.sh

# 设置定时任务
echo "0 2 * * * /path/to/maintenance_tasks.sh" | crontab -
```

## 🎯 最佳实践

1. **提前规划** - 预估迁移时间窗口，准备充分的测试时间
2. **分步执行** - 先迁移小数据库，再处理大型数据库
3. **保持备份** - 迁移期间保留原数据库备份
4. **监控性能** - 密切监控迁移过程中的性能指标
5. **团队协作** - 确保团队成员了解迁移计划和应急流程
6. **文档记录** - 详细记录迁移过程和遇到的问题

## 📞 紧急联系

如果在迁移过程中遇到紧急问题：

1. 立即停止迁移过程
2. 保留所有日志文件
3. 联系数据库管理员
4. 准备回滚到原数据库

记住：数据安全是第一优先级！

