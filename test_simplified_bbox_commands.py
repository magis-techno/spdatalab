#!/usr/bin/env python3
"""
测试简化后的bbox两阶段命令
验证修改后的命令参数和功能正常工作
"""

import subprocess
import sys
import json
import tempfile
import os
from pathlib import Path

def run_command(cmd, check=True):
    """运行命令并返回结果"""
    print(f"🔧 运行命令: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        if result.stdout:
            print(f"✅ 输出: {result.stdout.strip()}")
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ 命令失败: {e}")
        if e.stderr:
            print(f"错误信息: {e.stderr}")
        return e

def test_command_help():
    """测试命令帮助信息"""
    print("\n📋 测试命令帮助信息...")
    
    commands = [
        ["python", "-m", "spdatalab", "build_dataset", "--help"],
        ["python", "-m", "spdatalab", "process_bbox", "--help"]
    ]
    
    for cmd in commands:
        print(f"\n测试: {cmd[3]}")
        result = run_command(cmd, check=False)
        if result.returncode == 0:
            print(f"✅ {cmd[3]} 命令存在且帮助正常")
        else:
            print(f"❌ {cmd[3]} 命令有问题")

def test_process_bbox_parameters():
    """测试process_bbox命令的新参数"""
    print("\n🔧 测试process_bbox命令参数...")
    
    # 测试help输出是否包含新参数
    result = run_command(["python", "-m", "spdatalab", "process_bbox", "--help"], check=False)
    
    if result.returncode == 0:
        help_text = result.stdout
        
        # 检查关键参数是否存在
        required_params = [
            "--parallel",
            "--workers", 
            "--no-partitioning",
            "--input"
        ]
        
        missing_params = []
        for param in required_params:
            if param not in help_text:
                missing_params.append(param)
        
        if not missing_params:
            print("✅ 所有必需参数都存在")
        else:
            print(f"❌ 缺少参数: {missing_params}")
            
        # 检查默认行为描述
        if "默认启用分表" in help_text or "默认启用" in help_text:
            print("✅ 帮助信息显示默认启用分表模式")
        else:
            print("⚠️  帮助信息未明确说明默认启用分表模式")
    else:
        print("❌ 无法获取process_bbox帮助信息")

def test_removed_command():
    """测试一体化命令是否已被移除"""
    print("\n🗑️  测试一体化命令是否已移除...")
    
    result = run_command(["python", "-m", "spdatalab", "build_dataset_with_bbox", "--help"], check=False)
    
    if result.returncode != 0:
        print("✅ build_dataset_with_bbox 命令已成功移除")
    else:
        print("❌ build_dataset_with_bbox 命令仍然存在！")

def create_test_dataset():
    """创建一个简单的测试数据集文件"""
    print("\n📄 创建测试数据集文件...")
    
    test_dataset = {
        "name": "test_dataset",
        "description": "测试用数据集",
        "subdatasets": [
            {
                "name": "test_sub1",
                "obs_path": "test_path_1",
                "scene_count": 3,
                "duplication_factor": 1,
                "scene_ids": ["scene_001", "scene_002", "scene_003"]
            }
        ],
        "total_scenes": 3,
        "total_unique_scenes": 3
    }
    
    test_file = "test_dataset.json"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_dataset, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 测试数据集文件已创建: {test_file}")
    return test_file

def test_process_bbox_dry_run(test_file):
    """测试process_bbox命令的参数传递（不实际执行）"""
    print("\n🧪 测试process_bbox命令参数传递...")
    
    # 测试基础命令（会失败但可以看到参数解析）
    test_commands = [
        ["python", "-m", "spdatalab", "process_bbox", "--input", test_file, "--help"],
        ["python", "-m", "spdatalab", "process_bbox", "--input", test_file, "--parallel", "--workers", "2", "--help"]
    ]
    
    for cmd in test_commands:
        print(f"\n测试命令: {' '.join(cmd[:-1])}")  # 不显示--help
        result = run_command(cmd, check=False)
        # 由于加了--help，会显示帮助而不是实际执行

def cleanup_test_files():
    """清理测试文件"""
    print("\n🧹 清理测试文件...")
    test_files = ["test_dataset.json"]
    
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"✅ 已删除: {file}")

def main():
    """主测试函数"""
    print("🧪 开始测试简化后的bbox命令...")
    print("=" * 60)
    
    try:
        # 1. 测试命令帮助
        test_command_help()
        
        # 2. 测试process_bbox参数
        test_process_bbox_parameters()
        
        # 3. 测试一体化命令是否已移除
        test_removed_command()
        
        # 4. 创建测试文件
        test_file = create_test_dataset()
        
        # 5. 测试参数传递
        test_process_bbox_dry_run(test_file)
        
        print("\n" + "=" * 60)
        print("🎉 测试完成！")
        print("\n📋 简化后的bbox使用方法:")
        print("# 第一阶段：构建数据集")
        print("python -m spdatalab build_dataset --input data.txt --dataset-name 'my_dataset' --output dataset.json")
        print("\n# 第二阶段：处理边界框（默认启用分表模式）")
        print("python -m spdatalab process_bbox --input dataset.json")
        print("\n# 高性能并行处理")
        print("python -m spdatalab process_bbox --input dataset.json --parallel --workers 4")
        
    finally:
        # 清理测试文件
        cleanup_test_files()

if __name__ == "__main__":
    main()
