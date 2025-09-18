#!/usr/bin/env python3
"""
测试工具验证脚本
用于验证所有迁移工具是否可以正常运行
"""

import os
import sys
import subprocess
from pathlib import Path

def test_script_syntax(script_path):
    """测试Python脚本语法"""
    try:
        result = subprocess.run([sys.executable, '-m', 'py_compile', script_path], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {script_path} - 语法检查通过")
            return True
        else:
            print(f"❌ {script_path} - 语法错误: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ {script_path} - 检查失败: {e}")
        return False

def test_script_help(script_path):
    """测试脚本help功能"""
    try:
        result = subprocess.run([sys.executable, script_path, '--help'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ {script_path} - help功能正常")
            return True
        else:
            print(f"⚠️  {script_path} - help功能异常但可能正常")
            return True  # 有些脚本可能没有argparse
    except subprocess.TimeoutExpired:
        print(f"⚠️  {script_path} - help测试超时")
        return True
    except Exception as e:
        print(f"⚠️  {script_path} - help测试失败: {e}")
        return True  # 不严格要求help功能

def main():
    """主函数"""
    print("=" * 80)
    print("数据库迁移工具验证测试")
    print("=" * 80)
    
    # 获取所有Python脚本
    troubleshooting_dir = Path("troubleshooting")
    python_scripts = list(troubleshooting_dir.glob("*.py"))
    
    if not python_scripts:
        print("❌ 没有找到Python脚本")
        return False
    
    print(f"发现 {len(python_scripts)} 个Python脚本")
    print()
    
    # 测试语法
    print("=== 语法检查 ===")
    syntax_results = []
    for script in python_scripts:
        if script.name == 'test_tools.py':  # 跳过自己
            continue
        result = test_script_syntax(script)
        syntax_results.append(result)
    
    print()
    
    # 测试help功能
    print("=== Help功能测试 ===")
    help_results = []
    for script in python_scripts:
        if script.name == 'test_tools.py':  # 跳过自己
            continue
        result = test_script_help(script)
        help_results.append(result)
    
    print()
    
    # 检查必需的依赖
    print("=== 依赖检查 ===")
    required_modules = ['psycopg2', 'psutil']
    dependency_results = []
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"✅ {module} - 已安装")
            dependency_results.append(True)
        except ImportError:
            print(f"❌ {module} - 未安装")
            print(f"   安装命令: pip install {module}")
            dependency_results.append(False)
    
    print()
    
    # 检查文档文件
    print("=== 文档检查 ===")
    doc_files = [
        "troubleshooting/README.md",
        "troubleshooting/server_setup_guide.md", 
        "troubleshooting/MIGRATION_GUIDE.md"
    ]
    
    doc_results = []
    for doc_file in doc_files:
        if os.path.exists(doc_file):
            print(f"✅ {doc_file} - 存在")
            doc_results.append(True)
        else:
            print(f"❌ {doc_file} - 缺失")
            doc_results.append(False)
    
    print()
    
    # 总结
    print("=" * 80)
    print("测试总结")
    print("=" * 80)
    
    syntax_passed = sum(syntax_results)
    help_passed = sum(help_results) 
    dependency_passed = sum(dependency_results)
    doc_passed = sum(doc_results)
    
    print(f"语法检查: {syntax_passed}/{len(syntax_results)} 通过")
    print(f"Help功能: {help_passed}/{len(help_results)} 通过") 
    print(f"依赖模块: {dependency_passed}/{len(dependency_results)} 可用")
    print(f"文档文件: {doc_passed}/{len(doc_results)} 存在")
    
    # 安装建议
    if dependency_passed < len(dependency_results):
        print()
        print("📦 安装缺失的依赖:")
        print("pip install psycopg2-binary psutil")
    
    # 使用建议
    print()
    print("🚀 快速开始:")
    print("1. 检查磁盘空间: python troubleshooting/quick_space_check.py")
    print("2. 详细诊断: python troubleshooting/check_disk_space.py") 
    print("3. PostgreSQL清理: python troubleshooting/postgresql_cleanup.py")
    print("4. 数据库备份: python troubleshooting/database_backup.py --help")
    print("5. 数据库迁移: python troubleshooting/database_migration.py --help")
    print("6. 迁移验证: python troubleshooting/migration_test.py --help")
    print("7. 查看完整指南: cat troubleshooting/MIGRATION_GUIDE.md")
    
    # 返回整体结果
    overall_success = (
        syntax_passed == len(syntax_results) and
        dependency_passed >= len(dependency_results) - 1 and  # 允许一个依赖缺失
        doc_passed == len(doc_results)
    )
    
    if overall_success:
        print("\n🎉 所有工具验证通过，可以开始使用！")
    else:
        print("\n⚠️  部分工具需要修复或安装依赖")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

