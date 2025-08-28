# 多模态轨迹检索数据库连接修复 - 快速修复指南

## 问题描述

运行多模态轨迹检索命令时遇到数据库连接错误：
```
(psycopg.OperationalError) [Errno -3] Temporary failure in name resolution
```

## 快速修复（3步解决）

### 1. 运行诊断
```bash
python database_connection_diagnostic.py
```

### 2. 应用修复
```bash
python fix_database_config.py --host localhost
```

### 3. 验证效果
```bash
python test_multimodal_database_fix.py --quick --cleanup
```

## 重新测试原命令

修复完成后，重新运行原始命令：
```bash
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
  --text 'bicycle crossing intersection' \
  --collection 'ddi_collection_camera_encoded_1' \
  --output-table 'discovered_trajectories' \
  --verbose
```

## 脚本说明

| 脚本名称 | 功能 | 用途 |
|---------|------|------|
| `database_connection_diagnostic.py` | 诊断数据库连接问题 | 检测DNS、连接状态、Docker容器 |
| `fix_database_config.py` | 修复数据库配置 | 替换 local_pg 为 localhost |
| `test_multimodal_database_fix.py` | 验证修复效果 | 完整测试多模态轨迹检索功能 |

## 如果仍有问题

1. **PostgreSQL未运行：** 启动PostgreSQL或Docker容器
2. **端口被占用：** 检查端口5432是否被其他程序占用  
3. **权限问题：** 确保有数据库访问权限

详细故障排除请参考：`database_setup_troubleshooting_guide.md`

## 测试环境要求

- PostgreSQL 服务运行在 localhost:5432
- 用户名：postgres，密码：postgres
- 数据库：postgres
- Python包：psycopg, sqlalchemy, geopandas

---
**注意：** 修复脚本会自动备份原始文件，安全可靠。
