#!/usr/bin/env python3
"""
PostgreSQL清理脚本
用于清理临时文件和释放磁盘空间
"""

import os
import sys
import glob
import psutil
from pathlib import Path
import subprocess

def format_bytes(bytes):
    """格式化字节数为可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"

def find_postgresql_temp_files():
    """查找PostgreSQL临时文件"""
    print("=" * 80)
    print("查找PostgreSQL临时文件")
    print("=" * 80)
    
    # 常见的临时文件位置
    temp_patterns = [
        'C:\\Windows\\Temp\\pgsql_tmp*',
        'C:\\Temp\\pgsql_tmp*',
        '/tmp/pgsql_tmp*',
        '/var/tmp/pgsql_tmp*'
    ]
    
    found_files = []
    total_size = 0
    
    for pattern in temp_patterns:
        try:
            files = glob.glob(pattern)
            for file_path in files:
                try:
                    size = os.path.getsize(file_path)
                    found_files.append((file_path, size))
                    total_size += size
                    print(f"发现临时文件: {file_path} ({format_bytes(size)})")
                except OSError:
                    continue
        except Exception as e:
            print(f"搜索模式 {pattern} 时出错: {e}")
    
    print(f"\n总计找到 {len(found_files)} 个临时文件")
    print(f"总大小: {format_bytes(total_size)}")
    
    return found_files

def cleanup_temp_files(files, dry_run=True):
    """清理临时文件"""
    print("\n" + "=" * 80)
    print(f"清理临时文件 {'(预览模式)' if dry_run else '(实际执行)'}")
    print("=" * 80)
    
    if not files:
        print("没有找到需要清理的文件")
        return
    
    total_freed = 0
    
    for file_path, size in files:
        if dry_run:
            print(f"将删除: {file_path} ({format_bytes(size)})")
            total_freed += size
        else:
            try:
                os.remove(file_path)
                print(f"已删除: {file_path} ({format_bytes(size)})")
                total_freed += size
            except OSError as e:
                print(f"删除失败: {file_path} - {e}")
    
    print(f"\n{'预计' if dry_run else '实际'}释放空间: {format_bytes(total_freed)}")

def check_postgresql_processes():
    """检查PostgreSQL进程"""
    print("\n" + "=" * 80)
    print("PostgreSQL进程检查")
    print("=" * 80)
    
    pg_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'memory_info', 'cpu_percent']):
        try:
            if 'postgres' in proc.info['name'].lower():
                pg_processes.append(proc)
                print(f"PID: {proc.info['pid']}, "
                      f"进程: {proc.info['name']}, "
                      f"内存: {format_bytes(proc.info['memory_info'].rss)}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if not pg_processes:
        print("未找到PostgreSQL进程")
    else:
        print(f"\n找到 {len(pg_processes)} 个PostgreSQL进程")

def postgresql_maintenance_commands():
    """提供PostgreSQL维护命令"""
    print("\n" + "=" * 80)
    print("PostgreSQL维护命令建议")
    print("=" * 80)
    
    commands = [
        ("清理查询计划缓存", "SELECT pg_stat_reset();"),
        ("查看数据库大小", "SELECT pg_database.datname, pg_size_pretty(pg_database_size(pg_database.datname)) AS size FROM pg_database;"),
        ("查看表空间使用", "SELECT spcname, pg_size_pretty(pg_tablespace_size(spcname)) FROM pg_tablespace;"),
        ("清理过期的复制槽", "SELECT slot_name FROM pg_replication_slots WHERE active = false;"),
        ("分析表统计信息", "ANALYZE;"),
        ("清理死元组", "VACUUM;"),
        ("重建索引", "REINDEX DATABASE your_database_name;"),
        ("检查配置", "SHOW ALL;")
    ]
    
    for desc, cmd in commands:
        print(f"\n{desc}:")
        print(f"  {cmd}")

def check_system_resources():
    """检查系统资源使用情况"""
    print("\n" + "=" * 80)
    print("系统资源检查")
    print("=" * 80)
    
    # CPU使用率
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"CPU使用率: {cpu_percent}%")
    
    # 内存使用情况
    memory = psutil.virtual_memory()
    print(f"内存使用率: {memory.percent}%")
    print(f"可用内存: {format_bytes(memory.available)}")
    
    # 交换空间
    swap = psutil.swap_memory()
    print(f"交换空间使用率: {swap.percent}%")
    
    # 磁盘I/O
    disk_io = psutil.disk_io_counters()
    if disk_io:
        print(f"磁盘读取: {format_bytes(disk_io.read_bytes)}")
        print(f"磁盘写入: {format_bytes(disk_io.write_bytes)}")

def main():
    """主函数"""
    print("PostgreSQL清理和维护工具")
    print("=" * 80)
    
    # 检查系统资源
    check_system_resources()
    
    # 检查PostgreSQL进程
    check_postgresql_processes()
    
    # 查找临时文件
    temp_files = find_postgresql_temp_files()
    
    # 预览清理效果
    if temp_files:
        cleanup_temp_files(temp_files, dry_run=True)
        
        print("\n" + "=" * 40)
        response = input("是否执行实际清理? (y/N): ").strip().lower()
        if response == 'y':
            cleanup_temp_files(temp_files, dry_run=False)
        else:
            print("已取消清理操作")
    
    # 显示维护命令
    postgresql_maintenance_commands()
    
    print("\n" + "=" * 80)
    print("完成排查和清理")
    print("=" * 80)

if __name__ == "__main__":
    main()
