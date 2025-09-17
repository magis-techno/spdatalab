# 故障排查工具集

这个目录包含了用于排查PostgreSQL磁盘空间不足等问题的脚本工具。

## 脚本说明

### 1. quick_space_check.py - 快速空间检查
```bash
python troubleshooting/quick_space_check.py
```
- 快速检查所有磁盘分区的空间使用情况
- 检查临时目录大小
- 查找PostgreSQL临时文件
- 适合快速诊断问题

### 2. check_disk_space.py - 详细磁盘分析
```bash
python troubleshooting/check_disk_space.py
```
- 详细的磁盘使用情况分析
- 临时目录深度检查
- PostgreSQL目录位置建议
- 提供排查建议

### 3. postgresql_cleanup.py - PostgreSQL清理工具
```bash
python troubleshooting/postgresql_cleanup.py
```
- 查找和清理PostgreSQL临时文件
- 检查PostgreSQL进程状态
- 系统资源使用监控
- 提供维护命令建议

## 使用场景

### 磁盘空间不足错误
当遇到以下错误时使用这些工具：
```
(psycopg.errors.DiskFull) could not write to file "base/pgsql_tmp/pgsql_tmp1272612.195": No space left on device
```

### 推荐排查流程

1. **快速诊断**
   ```bash
   python troubleshooting/quick_space_check.py
   ```

2. **详细分析** 
   ```bash
   python troubleshooting/check_disk_space.py
   ```

3. **清理和维护**
   ```bash
   python troubleshooting/postgresql_cleanup.py
   ```

## 常见问题解决方案

### 1. PostgreSQL临时文件占用过多空间
- 运行 `postgresql_cleanup.py` 清理临时文件
- 检查长时间运行的查询
- 考虑调整PostgreSQL配置参数

### 2. 系统临时目录占用过多空间
- 清理系统临时目录 (C:\Windows\Temp, C:\Temp)
- 检查应用程序临时文件
- 配置自动清理策略

### 3. 数据目录磁盘空间不足
- 移动数据目录到其他磁盘
- 删除不必要的数据库/表
- 运行VACUUM清理死元组

## PostgreSQL维护命令

### 查看数据库大小
```sql
SELECT pg_database.datname, 
       pg_size_pretty(pg_database_size(pg_database.datname)) AS size 
FROM pg_database;
```

### 查看表空间使用
```sql
SELECT spcname, 
       pg_size_pretty(pg_tablespace_size(spcname)) 
FROM pg_tablespace;
```

### 清理死元组
```sql
VACUUM VERBOSE;
```

### 重建统计信息
```sql
ANALYZE;
```

## 预防措施

1. **监控磁盘空间**
   - 设置磁盘空间警报
   - 定期检查临时文件
   - 监控数据库增长

2. **配置优化**
   - 调整work_mem参数
   - 配置合适的temp_tablespace
   - 设置maintenance_work_mem

3. **定期维护**
   - 定期运行VACUUM
   - 清理日志文件
   - 监控长时间运行的查询

## 注意事项

- 在生产环境中运行清理脚本前，请先在测试环境验证
- 清理临时文件前确认没有正在运行的重要查询
- 备份重要数据后再进行大规模清理操作
- 建议在非业务高峰期进行维护操作
