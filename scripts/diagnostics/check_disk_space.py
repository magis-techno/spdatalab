#!/usr/bin/env python3
"""
磁盘空间检查脚本
用于排查PostgreSQL磁盘空间不足问题
"""

import psutil
import os
import sys
from pathlib import Path

def format_bytes(bytes):
    """格式化字节数为可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"

def check_disk_usage():
    """检查所有磁盘分区的使用情况"""
    print("=" * 80)
    print("磁盘空间使用情况检查")
    print("=" * 80)
    
    partitions = psutil.disk_partitions()
    
    for partition in partitions:
        print(f"\n设备: {partition.device}")
        print(f"挂载点: {partition.mountpoint}")
        print(f"文件系统: {partition.fstype}")
        
        try:
            partition_usage = psutil.disk_usage(partition.mountpoint)
            total = partition_usage.total
            used = partition_usage.used
            free = partition_usage.free
            percent = (used / total) * 100
            
            print(f"总空间: {format_bytes(total)}")
            print(f"已使用: {format_bytes(used)} ({percent:.1f}%)")
            print(f"可用空间: {format_bytes(free)}")
            
            # 警告检查
            if percent > 90:
                print("⚠️  警告: 磁盘使用率超过90%")
            elif percent > 80:
                print("⚠️  注意: 磁盘使用率超过80%")
            else:
                print("✅ 磁盘空间充足")
                
        except PermissionError:
            print("❌ 无法访问此分区")
        
        print("-" * 40)

def check_temp_dirs():
    """检查临时目录的使用情况"""
    print("\n" + "=" * 80)
    print("临时目录检查")
    print("=" * 80)
    
    temp_dirs = [
        os.environ.get('TEMP', ''),
        os.environ.get('TMP', ''),
        '/tmp',
        'C:\\Windows\\Temp',
        'C:\\Temp'
    ]
    
    for temp_dir in temp_dirs:
        if temp_dir and os.path.exists(temp_dir):
            print(f"\n临时目录: {temp_dir}")
            try:
                usage = psutil.disk_usage(temp_dir)
                print(f"可用空间: {format_bytes(usage.free)}")
                
                # 计算目录大小
                total_size = 0
                file_count = 0
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        try:
                            file_path = os.path.join(root, file)
                            total_size += os.path.getsize(file_path)
                            file_count += 1
                        except (OSError, PermissionError):
                            continue
                
                print(f"目录大小: {format_bytes(total_size)}")
                print(f"文件数量: {file_count}")
                
            except (PermissionError, OSError) as e:
                print(f"❌ 无法访问: {e}")

def check_postgresql_dirs():
    """检查PostgreSQL可能的数据目录"""
    print("\n" + "=" * 80)
    print("PostgreSQL目录检查")
    print("=" * 80)
    
    # 常见的PostgreSQL数据目录
    pg_dirs = [
        'C:\\Program Files\\PostgreSQL\\*\\data',
        'C:\\PostgreSQL\\*\\data',
        '/var/lib/postgresql',
        '/usr/local/var/postgres',
        '/opt/homebrew/var/postgres'
    ]
    
    print("常见PostgreSQL数据目录位置:")
    for pg_dir in pg_dirs:
        print(f"  - {pg_dir}")
    
    print("\n建议检查命令:")
    print("1. 查看PostgreSQL配置: SHOW data_directory;")
    print("2. 查看临时文件目录: SHOW temp_tablespace;")
    print("3. 查看日志目录: SHOW log_directory;")

def main():
    """主函数"""
    print("PostgreSQL磁盘空间排查工具")
    print(f"运行时间: {psutil.boot_time()}")
    print(f"Python版本: {sys.version}")
    
    check_disk_usage()
    check_temp_dirs()
    check_postgresql_dirs()
    
    print("\n" + "=" * 80)
    print("排查建议:")
    print("=" * 80)
    print("1. 检查PostgreSQL数据目录磁盘空间")
    print("2. 清理PostgreSQL临时文件 (pgsql_tmp*)")
    print("3. 清理系统临时目录")
    print("4. 检查PostgreSQL日志文件大小")
    print("5. 考虑增加磁盘空间或移动数据目录")

if __name__ == "__main__":
    main()
