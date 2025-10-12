#!/usr/bin/env python3
"""
数据库迁移脚本
用于将PostgreSQL数据库从源服务器迁移到目标服务器
"""

import os
import sys
import subprocess
import argparse
import time
import json
from pathlib import Path
from datetime import datetime

class DatabaseMigrator:
    def __init__(self, source_config, target_config):
        self.source = source_config
        self.target = target_config
        self.migration_log = []
        
    def log(self, message, level="INFO"):
        """记录迁移日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        self.migration_log.append(log_entry)
        
    def run_command(self, command, env=None, timeout=None):
        """运行命令"""
        self.log(f"执行命令: {' '.join(command) if isinstance(command, list) else command}")
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                env=env,
                timeout=timeout
            )
            
            if result.stdout:
                self.log(f"输出: {result.stdout.strip()}")
            return result
            
        except subprocess.CalledProcessError as e:
            self.log(f"命令执行失败: {e}", "ERROR")
            if e.stderr:
                self.log(f"错误详情: {e.stderr}", "ERROR")
            raise e
        except subprocess.TimeoutExpired as e:
            self.log(f"命令执行超时: {e}", "ERROR")
            raise e
    
    def test_connection(self, config, label):
        """测试数据库连接"""
        self.log(f"测试{label}数据库连接...")
        
        env = os.environ.copy()
        if config.get('password'):
            env['PGPASSWORD'] = config['password']
        
        command = [
            'psql',
            f"--host={config['host']}",
            f"--port={config['port']}",
            f"--username={config['username']}",
            '--command=SELECT version();'
        ]
        
        try:
            result = self.run_command(command, env=env, timeout=30)
            self.log(f"✅ {label}数据库连接成功")
            return True
        except Exception as e:
            self.log(f"❌ {label}数据库连接失败: {e}", "ERROR")
            return False
    
    def get_source_databases(self):
        """获取源数据库列表"""
        self.log("获取源数据库列表...")
        
        env = os.environ.copy()
        if self.source.get('password'):
            env['PGPASSWORD'] = self.source['password']
        
        command = [
            'psql',
            f"--host={self.source['host']}",
            f"--port={self.source['port']}",
            f"--username={self.source['username']}",
            '--list',
            '--tuples-only',
            '--no-align'
        ]
        
        try:
            result = self.run_command(command, env=env)
            databases = []
            
            for line in result.stdout.strip().split('\n'):
                if '|' in line and line.strip():
                    parts = line.split('|')
                    db_name = parts[0].strip()
                    # 跳过系统数据库
                    if db_name and db_name not in ['template0', 'template1', 'postgres']:
                        databases.append(db_name)
            
            self.log(f"发现数据库: {', '.join(databases)}")
            return databases
            
        except Exception as e:
            self.log(f"获取数据库列表失败: {e}", "ERROR")
            return []
    
    def create_target_database(self, database_name):
        """在目标服务器创建数据库"""
        self.log(f"在目标服务器创建数据库: {database_name}")
        
        env = os.environ.copy()
        if self.target.get('password'):
            env['PGPASSWORD'] = self.target['password']
        
        # 首先检查数据库是否已存在
        check_command = [
            'psql',
            f"--host={self.target['host']}",
            f"--port={self.target['port']}",
            f"--username={self.target['username']}",
            '--dbname=postgres',
            f"--command=SELECT 1 FROM pg_database WHERE datname='{database_name}';"
        ]
        
        try:
            result = self.run_command(check_command, env=env)
            if '1' in result.stdout:
                self.log(f"数据库 {database_name} 已存在，跳过创建")
                return True
        except Exception:
            pass  # 如果检查失败，继续尝试创建
        
        # 创建数据库
        create_command = [
            'psql',
            f"--host={self.target['host']}",
            f"--port={self.target['port']}",
            f"--username={self.target['username']}",
            '--dbname=postgres',
            f"--command=CREATE DATABASE {database_name};"
        ]
        
        try:
            self.run_command(create_command, env=env)
            self.log(f"✅ 数据库 {database_name} 创建成功")
            return True
        except Exception as e:
            self.log(f"❌ 数据库 {database_name} 创建失败: {e}", "ERROR")
            return False
    
    def migrate_database(self, database_name, backup_dir=None):
        """迁移单个数据库"""
        self.log(f"开始迁移数据库: {database_name}")
        
        # 1. 备份源数据库
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if backup_dir:
            backup_file = Path(backup_dir) / f"{database_name}_migration_{timestamp}.backup"
        else:
            backup_file = Path(f"/tmp/{database_name}_migration_{timestamp}.backup")
        
        self.log(f"备份数据库到: {backup_file}")
        
        source_env = os.environ.copy()
        if self.source.get('password'):
            source_env['PGPASSWORD'] = self.source['password']
        
        dump_command = [
            'pg_dump',
            f"--host={self.source['host']}",
            f"--port={self.source['port']}",
            f"--username={self.source['username']}",
            '--verbose',
            '--format=custom',
            '--clean',
            '--no-owner',
            '--no-privileges',
            f"--file={backup_file}",
            database_name
        ]
        
        try:
            self.run_command(dump_command, env=source_env, timeout=3600)  # 1小时超时
            self.log(f"✅ 数据库备份完成: {backup_file.stat().st_size / (1024*1024):.2f} MB")
        except Exception as e:
            self.log(f"❌ 数据库备份失败: {e}", "ERROR")
            return False
        
        # 2. 在目标服务器创建数据库
        if not self.create_target_database(database_name):
            return False
        
        # 3. 恢复到目标数据库
        self.log(f"恢复数据库: {database_name}")
        
        target_env = os.environ.copy()
        if self.target.get('password'):
            target_env['PGPASSWORD'] = self.target['password']
        
        restore_command = [
            'pg_restore',
            f"--host={self.target['host']}",
            f"--port={self.target['port']}",
            f"--username={self.target['username']}",
            f"--dbname={database_name}",
            '--verbose',
            '--clean',
            '--if-exists',
            '--no-owner',
            '--no-privileges',
            str(backup_file)
        ]
        
        try:
            self.run_command(restore_command, env=target_env, timeout=7200)  # 2小时超时
            self.log(f"✅ 数据库 {database_name} 迁移完成")
            
            # 清理备份文件（可选）
            if backup_file.exists():
                backup_file.unlink()
                self.log(f"清理备份文件: {backup_file}")
                
            return True
            
        except Exception as e:
            self.log(f"❌ 数据库 {database_name} 恢复失败: {e}", "ERROR")
            return False
    
    def migrate_globals(self):
        """迁移全局对象（角色、表空间等）"""
        self.log("迁移全局对象...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        globals_file = Path(f"/tmp/globals_migration_{timestamp}.sql")
        
        # 1. 备份全局对象
        source_env = os.environ.copy()
        if self.source.get('password'):
            source_env['PGPASSWORD'] = self.source['password']
        
        dump_command = [
            'pg_dumpall',
            f"--host={self.source['host']}",
            f"--port={self.source['port']}",
            f"--username={self.source['username']}",
            '--globals-only',
            '--verbose',
            f"--file={globals_file}"
        ]
        
        try:
            self.run_command(dump_command, env=source_env)
            self.log(f"✅ 全局对象备份完成")
        except Exception as e:
            self.log(f"❌ 全局对象备份失败: {e}", "ERROR")
            return False
        
        # 2. 恢复全局对象
        target_env = os.environ.copy()
        if self.target.get('password'):
            target_env['PGPASSWORD'] = self.target['password']
        
        restore_command = [
            'psql',
            f"--host={self.target['host']}",
            f"--port={self.target['port']}",
            f"--username={self.target['username']}",
            '--dbname=postgres',
            f"--file={globals_file}"
        ]
        
        try:
            self.run_command(restore_command, env=target_env)
            self.log(f"✅ 全局对象迁移完成")
            
            # 清理备份文件
            if globals_file.exists():
                globals_file.unlink()
                
            return True
            
        except Exception as e:
            self.log(f"❌ 全局对象恢复失败: {e}", "ERROR")
            return False
    
    def verify_migration(self, database_name):
        """验证迁移结果"""
        self.log(f"验证数据库迁移: {database_name}")
        
        # 获取源数据库统计信息
        source_env = os.environ.copy()
        if self.source.get('password'):
            source_env['PGPASSWORD'] = self.source['password']
        
        source_stats_command = [
            'psql',
            f"--host={self.source['host']}",
            f"--port={self.source['port']}",
            f"--username={self.source['username']}",
            f"--dbname={database_name}",
            '--tuples-only',
            '--no-align',
            "--command=SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables ORDER BY schemaname, tablename;"
        ]
        
        # 获取目标数据库统计信息
        target_env = os.environ.copy()
        if self.target.get('password'):
            target_env['PGPASSWORD'] = self.target['password']
        
        target_stats_command = [
            'psql',
            f"--host={self.target['host']}",
            f"--port={self.target['port']}",
            f"--username={self.target['username']}",
            f"--dbname={database_name}",
            '--tuples-only',
            '--no-align',
            "--command=SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables ORDER BY schemaname, tablename;"
        ]
        
        try:
            source_result = self.run_command(source_stats_command, env=source_env)
            target_result = self.run_command(target_stats_command, env=target_env)
            
            source_tables = set(line.split('|')[1] for line in source_result.stdout.strip().split('\n') if '|' in line)
            target_tables = set(line.split('|')[1] for line in target_result.stdout.strip().split('\n') if '|' in line)
            
            if source_tables == target_tables:
                self.log(f"✅ 数据库 {database_name} 验证通过 - 表数量匹配 ({len(source_tables)} 个表)")
                return True
            else:
                missing_tables = source_tables - target_tables
                extra_tables = target_tables - source_tables
                
                if missing_tables:
                    self.log(f"❌ 缺少表: {', '.join(missing_tables)}", "ERROR")
                if extra_tables:
                    self.log(f"⚠️ 多余表: {', '.join(extra_tables)}", "WARNING")
                    
                return False
                
        except Exception as e:
            self.log(f"❌ 验证失败: {e}", "ERROR")
            return False
    
    def save_migration_log(self, log_file=None):
        """保存迁移日志"""
        if not log_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"migration_log_{timestamp}.txt"
        
        with open(log_file, 'w', encoding='utf-8') as f:
            for entry in self.migration_log:
                f.write(entry + '\n')
        
        self.log(f"迁移日志已保存: {log_file}")
    
    def run_migration(self, databases=None, backup_dir=None, migrate_globals=True):
        """执行完整迁移流程"""
        self.log("开始数据库迁移流程")
        
        # 1. 测试连接
        if not self.test_connection(self.source, "源"):
            return False
        
        if not self.test_connection(self.target, "目标"):
            return False
        
        # 2. 获取数据库列表
        if not databases:
            databases = self.get_source_databases()
        
        if not databases:
            self.log("没有找到需要迁移的数据库", "ERROR")
            return False
        
        # 3. 迁移全局对象
        if migrate_globals:
            if not self.migrate_globals():
                self.log("全局对象迁移失败，但继续迁移数据库", "WARNING")
        
        # 4. 迁移每个数据库
        success_count = 0
        failed_databases = []
        
        for database in databases:
            try:
                if self.migrate_database(database, backup_dir):
                    success_count += 1
                    # 验证迁移
                    self.verify_migration(database)
                else:
                    failed_databases.append(database)
            except Exception as e:
                self.log(f"数据库 {database} 迁移异常: {e}", "ERROR")
                failed_databases.append(database)
        
        # 5. 迁移总结
        self.log("=" * 80)
        self.log("迁移完成总结")
        self.log("=" * 80)
        self.log(f"总数据库数: {len(databases)}")
        self.log(f"成功迁移: {success_count}")
        self.log(f"失败数量: {len(failed_databases)}")
        
        if failed_databases:
            self.log(f"失败的数据库: {', '.join(failed_databases)}", "ERROR")
        
        # 6. 保存日志
        self.save_migration_log()
        
        return len(failed_databases) == 0

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PostgreSQL数据库迁移工具')
    
    # 源数据库配置
    parser.add_argument('--source-host', required=True, help='源数据库主机')
    parser.add_argument('--source-port', default='5432', help='源数据库端口')
    parser.add_argument('--source-username', required=True, help='源数据库用户名')
    parser.add_argument('--source-password', help='源数据库密码')
    
    # 目标数据库配置
    parser.add_argument('--target-host', required=True, help='目标数据库主机')
    parser.add_argument('--target-port', default='5432', help='目标数据库端口')
    parser.add_argument('--target-username', required=True, help='目标数据库用户名')
    parser.add_argument('--target-password', help='目标数据库密码')
    
    # 迁移选项
    parser.add_argument('--databases', nargs='+', help='要迁移的数据库列表（不指定则迁移所有）')
    parser.add_argument('--backup-dir', help='备份文件目录')
    parser.add_argument('--no-globals', action='store_true', help='跳过全局对象迁移')
    parser.add_argument('--config-file', help='从JSON配置文件读取参数')
    
    args = parser.parse_args()
    
    # 从配置文件读取参数
    if args.config_file:
        with open(args.config_file, 'r') as f:
            config = json.load(f)
        
        source_config = config.get('source', {})
        target_config = config.get('target', {})
        
        # 命令行参数覆盖配置文件
        for key, value in vars(args).items():
            if value is not None and key.startswith('source_'):
                source_config[key.replace('source_', '')] = value
            elif value is not None and key.startswith('target_'):
                target_config[key.replace('target_', '')] = value
    else:
        source_config = {
            'host': args.source_host,
            'port': args.source_port,
            'username': args.source_username,
            'password': args.source_password
        }
        
        target_config = {
            'host': args.target_host,
            'port': args.target_port,
            'username': args.target_username,
            'password': args.target_password
        }
    
    # 创建迁移器
    migrator = DatabaseMigrator(source_config, target_config)
    
    # 执行迁移
    success = migrator.run_migration(
        databases=args.databases,
        backup_dir=args.backup_dir,
        migrate_globals=not args.no_globals
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()


