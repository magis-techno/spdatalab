#!/usr/bin/env python3
"""
数据库备份脚本
用于PostgreSQL数据库迁移前的数据备份
"""

import os
import sys
import subprocess
import datetime
from pathlib import Path
import argparse

def run_command(command, check=True):
    """运行命令并返回结果"""
    print(f"执行命令: {command}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=check
        )
        if result.stdout:
            print(f"输出: {result.stdout}")
        if result.stderr and result.returncode != 0:
            print(f"错误: {result.stderr}")
        return result
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {e}")
        return None

def create_backup_directory(backup_dir):
    """创建备份目录"""
    backup_path = Path(backup_dir)
    backup_path.mkdir(parents=True, exist_ok=True)
    print(f"备份目录: {backup_path.absolute()}")
    return backup_path

def backup_database(host, port, username, database, backup_dir, password=None):
    """备份数据库"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{database}_backup_{timestamp}.sql"
    backup_path = Path(backup_dir) / backup_file
    
    # 设置环境变量
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    # 构建pg_dump命令
    dump_command = [
        'pg_dump',
        f'--host={host}',
        f'--port={port}',
        f'--username={username}',
        '--verbose',
        '--clean',
        '--no-owner',
        '--no-privileges',
        '--format=custom',
        f'--file={backup_path}',
        database
    ]
    
    print(f"开始备份数据库: {database}")
    print(f"备份文件: {backup_path}")
    
    try:
        result = subprocess.run(
            dump_command,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✅ 数据库备份成功!")
        print(f"备份文件大小: {backup_path.stat().st_size / (1024*1024):.2f} MB")
        return str(backup_path)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ 备份失败: {e}")
        if e.stderr:
            print(f"错误详情: {e.stderr}")
        return None

def backup_schema_only(host, port, username, database, backup_dir, password=None):
    """仅备份数据库结构"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    schema_file = f"{database}_schema_{timestamp}.sql"
    schema_path = Path(backup_dir) / schema_file
    
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    dump_command = [
        'pg_dump',
        f'--host={host}',
        f'--port={port}',
        f'--username={username}',
        '--verbose',
        '--schema-only',
        '--clean',
        '--no-owner',
        '--no-privileges',
        f'--file={schema_path}',
        database
    ]
    
    print(f"开始备份数据库结构: {database}")
    
    try:
        subprocess.run(dump_command, env=env, check=True)
        print("✅ 数据库结构备份成功!")
        return str(schema_path)
    except subprocess.CalledProcessError as e:
        print(f"❌ 结构备份失败: {e}")
        return None

def backup_data_only(host, port, username, database, backup_dir, password=None):
    """仅备份数据"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    data_file = f"{database}_data_{timestamp}.sql"
    data_path = Path(backup_dir) / data_file
    
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    dump_command = [
        'pg_dump',
        f'--host={host}',
        f'--port={port}',
        f'--username={username}',
        '--verbose',
        '--data-only',
        '--no-owner',
        '--no-privileges',
        f'--file={data_path}',
        database
    ]
    
    print(f"开始备份数据: {database}")
    
    try:
        subprocess.run(dump_command, env=env, check=True)
        print("✅ 数据备份成功!")
        return str(data_path)
    except subprocess.CalledProcessError as e:
        print(f"❌ 数据备份失败: {e}")
        return None

def backup_globals(host, port, username, backup_dir, password=None):
    """备份全局对象(角色、表空间等)"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    globals_file = f"globals_backup_{timestamp}.sql"
    globals_path = Path(backup_dir) / globals_file
    
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    dump_command = [
        'pg_dumpall',
        f'--host={host}',
        f'--port={port}',
        f'--username={username}',
        '--verbose',
        '--globals-only',
        f'--file={globals_path}'
    ]
    
    print("开始备份全局对象...")
    
    try:
        subprocess.run(dump_command, env=env, check=True)
        print("✅ 全局对象备份成功!")
        return str(globals_path)
    except subprocess.CalledProcessError as e:
        print(f"❌ 全局对象备份失败: {e}")
        return None

def get_database_info(host, port, username, password=None):
    """获取数据库信息"""
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    # 获取数据库列表
    list_command = [
        'psql',
        f'--host={host}',
        f'--port={port}',
        f'--username={username}',
        '--list',
        '--tuples-only'
    ]
    
    try:
        result = subprocess.run(list_command, env=env, capture_output=True, text=True, check=True)
        databases = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                db_name = line.split('|')[0].strip()
                if db_name and db_name not in ['template0', 'template1']:
                    databases.append(db_name)
        return databases
    except subprocess.CalledProcessError as e:
        print(f"❌ 获取数据库列表失败: {e}")
        return []

def create_migration_script(backup_files, target_host, target_port, target_username):
    """创建迁移脚本"""
    script_content = f"""#!/bin/bash
# PostgreSQL数据库迁移脚本
# 生成时间: {datetime.datetime.now()}

echo "开始数据库迁移..."

# 目标服务器信息
TARGET_HOST="{target_host}"
TARGET_PORT="{target_port}"
TARGET_USER="{target_username}"

# 备份文件列表
"""
    
    for backup_type, file_path in backup_files.items():
        if file_path:
            script_content += f'\n# {backup_type}\n'
            script_content += f'{backup_type.upper()}_FILE="{file_path}"\n'
    
    script_content += """
# 恢复函数
restore_backup() {
    local file=$1
    local database=$2
    
    echo "恢复 $file 到数据库 $database"
    
    if [[ $file == *.sql ]]; then
        psql --host=$TARGET_HOST --port=$TARGET_PORT --username=$TARGET_USER --dbname=$database < "$file"
    else
        pg_restore --host=$TARGET_HOST --port=$TARGET_PORT --username=$TARGET_USER --dbname=$database --verbose "$file"
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ $file 恢复成功"
    else
        echo "❌ $file 恢复失败"
    fi
}

# 执行迁移
echo "请确保目标服务器已安装PostgreSQL并可以连接"
echo "开始执行迁移..."

# 这里添加具体的恢复命令
# restore_backup "$BACKUP_FILE" "your_database_name"

echo "迁移完成!"
"""
    
    script_path = Path("troubleshooting") / "migration_script.sh"
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    # 设置执行权限
    os.chmod(script_path, 0o755)
    print(f"✅ 迁移脚本已生成: {script_path}")
    return str(script_path)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PostgreSQL数据库备份工具')
    parser.add_argument('--host', default='localhost', help='数据库主机地址')
    parser.add_argument('--port', default='5432', help='数据库端口')
    parser.add_argument('--username', required=True, help='数据库用户名')
    parser.add_argument('--password', help='数据库密码')
    parser.add_argument('--database', help='要备份的数据库名(不指定则备份所有)')
    parser.add_argument('--backup-dir', default='./backups', help='备份目录')
    parser.add_argument('--schema-only', action='store_true', help='仅备份结构')
    parser.add_argument('--data-only', action='store_true', help='仅备份数据')
    parser.add_argument('--target-host', help='目标服务器地址(用于生成迁移脚本)')
    parser.add_argument('--target-port', default='5432', help='目标服务器端口')
    parser.add_argument('--target-username', help='目标服务器用户名')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("PostgreSQL数据库备份工具")
    print("=" * 80)
    
    # 创建备份目录
    backup_dir = create_backup_directory(args.backup_dir)
    
    # 获取数据库列表
    if not args.database:
        databases = get_database_info(args.host, args.port, args.username, args.password)
        print(f"发现数据库: {', '.join(databases)}")
    else:
        databases = [args.database]
    
    backup_files = {}
    
    # 备份全局对象
    globals_file = backup_globals(args.host, args.port, args.username, backup_dir, args.password)
    if globals_file:
        backup_files['globals'] = globals_file
    
    # 备份每个数据库
    for database in databases:
        print(f"\n处理数据库: {database}")
        
        if args.schema_only:
            schema_file = backup_schema_only(args.host, args.port, args.username, database, backup_dir, args.password)
            if schema_file:
                backup_files[f'{database}_schema'] = schema_file
                
        elif args.data_only:
            data_file = backup_data_only(args.host, args.port, args.username, database, backup_dir, args.password)
            if data_file:
                backup_files[f'{database}_data'] = data_file
        else:
            backup_file = backup_database(args.host, args.port, args.username, database, backup_dir, args.password)
            if backup_file:
                backup_files[f'{database}_full'] = backup_file
    
    # 生成迁移脚本
    if args.target_host and args.target_username:
        migration_script = create_migration_script(backup_files, args.target_host, args.target_port, args.target_username)
        backup_files['migration_script'] = migration_script
    
    print("\n" + "=" * 80)
    print("备份完成!")
    print("=" * 80)
    print("备份文件:")
    for backup_type, file_path in backup_files.items():
        if file_path:
            print(f"  {backup_type}: {file_path}")
    
    print(f"\n备份目录: {backup_dir.absolute()}")
    print("下一步: 将备份文件传输到目标服务器并执行恢复")

if __name__ == "__main__":
    main()

