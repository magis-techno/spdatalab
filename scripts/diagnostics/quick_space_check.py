#!/usr/bin/env python3
"""
快速空间检查脚本
用于快速诊断磁盘空间问题
"""

import subprocess
import sys
import os

def run_powershell_command(command):
    """运行PowerShell命令"""
    try:
        result = subprocess.run(
            ["powershell", "-Command", command],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {e}")
        return None

def quick_disk_check():
    """快速磁盘检查"""
    print("=" * 60)
    print("快速磁盘空间检查")
    print("=" * 60)
    
    # Windows磁盘空间检查
    if os.name == 'nt':
        command = """
        Get-WmiObject -Class Win32_LogicalDisk | 
        Select-Object DeviceID, 
        @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, 
        @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}, 
        @{Name="UsedSpace(GB)";Expression={[math]::Round(($_.Size-$_.FreeSpace)/1GB,2)}}, 
        @{Name="PercentFree";Expression={[math]::Round(($_.FreeSpace/$_.Size)*100,2)}} | 
        Format-Table -AutoSize
        """
        
        result = run_powershell_command(command)
        if result:
            print(result)
        else:
            print("无法获取磁盘信息")
    
    else:
        # Linux/Unix系统
        try:
            result = subprocess.run(['df', '-h'], capture_output=True, text=True, check=True)
            print(result.stdout)
        except subprocess.CalledProcessError:
            print("无法获取磁盘信息")

def check_temp_directories():
    """检查临时目录"""
    print("\n" + "=" * 60)
    print("临时目录检查")
    print("=" * 60)
    
    temp_dirs = []
    
    if os.name == 'nt':
        # Windows临时目录
        temp_dirs = [
            os.environ.get('TEMP', ''),
            os.environ.get('TMP', ''),
            'C:\\Windows\\Temp',
            'C:\\Temp'
        ]
    else:
        # Linux/Unix临时目录
        temp_dirs = ['/tmp', '/var/tmp', '/usr/tmp']
    
    for temp_dir in temp_dirs:
        if temp_dir and os.path.exists(temp_dir):
            print(f"\n临时目录: {temp_dir}")
            
            if os.name == 'nt':
                # Windows目录大小检查
                command = f'''
                $path = "{temp_dir}"
                $size = (Get-ChildItem -Path $path -Recurse -ErrorAction SilentlyContinue | 
                        Measure-Object -Property Length -Sum).Sum
                if ($size) {{
                    "{0:N2} GB" -f ($size / 1GB)
                }} else {{
                    "无法计算大小"
                }}
                '''
                result = run_powershell_command(command)
                if result:
                    print(f"  大小: {result}")
            else:
                # Linux目录大小检查
                try:
                    result = subprocess.run(['du', '-sh', temp_dir], 
                                          capture_output=True, text=True, check=True)
                    print(f"  大小: {result.stdout.strip()}")
                except subprocess.CalledProcessError:
                    print("  无法计算大小")

def find_large_files():
    """查找大文件"""
    print("\n" + "=" * 60)
    print("查找大文件 (>1GB)")
    print("=" * 60)
    
    if os.name == 'nt':
        # Windows查找大文件
        command = '''
        Get-ChildItem -Path C:\\ -Recurse -ErrorAction SilentlyContinue | 
        Where-Object {$_.Length -gt 1GB} | 
        Sort-Object Length -Descending | 
        Select-Object -First 10 FullName, @{Name="Size(GB)";Expression={[math]::Round($_.Length/1GB,2)}} | 
        Format-Table -AutoSize
        '''
        
        print("正在搜索C盘大文件...")
        result = run_powershell_command(command)
        if result:
            print(result)
        else:
            print("搜索完成，未找到或无法访问")
    
    else:
        # Linux查找大文件
        try:
            result = subprocess.run([
                'find', '/', '-type', 'f', '-size', '+1G', 
                '-exec', 'ls', '-lh', '{}', ';', '2>/dev/null'
            ], capture_output=True, text=True, timeout=30)
            
            if result.stdout:
                print(result.stdout)
            else:
                print("未找到大文件")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            print("搜索超时或失败")

def check_postgresql_temp():
    """检查PostgreSQL临时文件"""
    print("\n" + "=" * 60)
    print("检查PostgreSQL临时文件")
    print("=" * 60)
    
    if os.name == 'nt':
        # Windows PostgreSQL临时文件
        patterns = [
            'C:\\Windows\\Temp\\pgsql_tmp*',
            'C:\\Temp\\pgsql_tmp*'
        ]
        
        for pattern in patterns:
            command = f'Get-ChildItem -Path "{pattern}" -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum'
            result = run_powershell_command(command)
            if result and 'Sum' in result:
                print(f"模式 {pattern}: 找到临时文件")
    
    else:
        # Linux PostgreSQL临时文件
        patterns = ['/tmp/pgsql_tmp*', '/var/tmp/pgsql_tmp*']
        
        for pattern in patterns:
            try:
                result = subprocess.run(['ls', '-la', pattern], 
                                      capture_output=True, text=True)
                if result.stdout:
                    print(f"模式 {pattern}:")
                    print(result.stdout)
            except subprocess.CalledProcessError:
                continue

def main():
    """主函数"""
    print("PostgreSQL磁盘空间快速诊断工具")
    print("运行平台:", "Windows" if os.name == 'nt' else "Unix/Linux")
    
    quick_disk_check()
    check_temp_directories()
    check_postgresql_temp()
    
    print("\n" + "=" * 60)
    print("快速诊断完成")
    print("=" * 60)
    print("建议:")
    print("1. 如果C盘空间不足，清理临时文件")
    print("2. 检查PostgreSQL数据目录所在磁盘")
    print("3. 清理PostgreSQL临时文件(pgsql_tmp*)")
    print("4. 考虑移动数据到其他磁盘")

if __name__ == "__main__":
    main()
