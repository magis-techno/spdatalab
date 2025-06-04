#!/usr/bin/env python3
"""
示例：使用spdatalab CLI的完整工作流程
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """运行命令并显示结果"""
    print(f"\n{'='*50}")
    print(f"执行: {description}")
    print(f"命令: {' '.join(cmd)}")
    print(f"{'='*50}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✅ 执行成功")
        if result.stdout:
            print("输出:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("❌ 执行失败")
        print(f"错误: {e}")
        if e.stdout:
            print("标准输出:")
            print(e.stdout)
        if e.stderr:
            print("错误输出:")
            print(e.stderr)
        return False

def example_cli_workflow_json():
    """使用CLI的JSON格式完整工作流程示例"""
    print("=== 使用CLI的JSON格式工作流程示例 ===")
    
    # 模拟数据文件（实际使用时需要真实的索引文件）
    index_file = "data/example_index.txt"
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # 创建示例索引文件（如果不存在）
    if not Path(index_file).exists():
        Path("data").mkdir(exist_ok=True)
        with open(index_file, 'w') as f:
            f.write("obs://example/path1/file1.jsonl@duplicate10\n")
            f.write("obs://example/path2/file2.jsonl@duplicate5\n")
        print(f"创建了示例索引文件: {index_file}")
    
    # 方法1：分步执行
    print("\n--- 方法1：分步执行 ---")
    
    # 步骤1：构建数据集
    cmd1 = [
        "python", "-m", "spdatalab.cli", "build-dataset",
        "--index-file", index_file,
        "--dataset-name", "example_dataset_json",
        "--description", "CLI示例数据集（JSON格式）",
        "--output", "output/example_dataset.json",
        "--format", "json"
    ]
    
    if run_command(cmd1, "构建JSON格式数据集"):
        # 步骤2：处理边界框
        cmd2 = [
            "python", "-m", "spdatalab.cli", "process-bbox",
            "--input", "output/example_dataset.json",
            "--batch", "500",
            "--insert-batch", "500",
            "--buffer-meters", "50"
        ]
        
        run_command(cmd2, "处理边界框数据")
    
    # 方法2：一键式完整工作流程
    print("\n--- 方法2：一键式完整工作流程 ---")
    
    cmd3 = [
        "python", "-m", "spdatalab.cli", "build-dataset-with-bbox",
        "--index-file", index_file,
        "--dataset-name", "example_dataset_complete",
        "--description", "完整工作流程示例",
        "--output", "output/complete_dataset.json",
        "--format", "json",
        "--batch", "500",
        "--insert-batch", "500",
        "--buffer-meters", "100",
        "--precise-buffer"  # 使用精确缓冲区
    ]
    
    run_command(cmd3, "一键式完整工作流程")

def example_cli_workflow_parquet():
    """使用CLI的Parquet格式完整工作流程示例"""
    print("=== 使用CLI的Parquet格式工作流程示例 ===")
    
    index_file = "data/example_index.txt"
    
    # 一键式Parquet工作流程
    cmd = [
        "python", "-m", "spdatalab.cli", "build-dataset-with-bbox",
        "--index-file", index_file,
        "--dataset-name", "example_dataset_parquet",
        "--description", "Parquet格式示例数据集",
        "--output", "output/example_dataset.parquet",
        "--format", "parquet",
        "--batch", "1000",
        "--insert-batch", "1000",
        "--buffer-meters", "75"
    ]
    
    run_command(cmd, "Parquet格式完整工作流程")

def example_dataset_operations():
    """演示数据集操作命令"""
    print("=== 数据集操作示例 ===")
    
    dataset_file = "output/example_dataset.json"
    
    if not Path(dataset_file).exists():
        print(f"数据集文件不存在: {dataset_file}")
        print("请先运行JSON工作流程示例")
        return
    
    # 查看数据集信息
    cmd1 = [
        "python", "-m", "spdatalab.cli", "dataset-info",
        "--dataset-file", dataset_file
    ]
    
    run_command(cmd1, "查看数据集信息")
    
    # 列出场景ID
    cmd2 = [
        "python", "-m", "spdatalab.cli", "list-scenes",
        "--dataset-file", dataset_file,
        "--output", "output/scene_ids.txt"
    ]
    
    run_command(cmd2, "导出场景ID列表")
    
    # 生成包含倍增的场景ID列表
    cmd3 = [
        "python", "-m", "spdatalab.cli", "generate-scene-ids",
        "--dataset-file", dataset_file,
        "--output", "output/scene_ids_with_duplicates.txt"
    ]
    
    run_command(cmd3, "生成包含倍增的场景ID列表")

def example_bbox_only():
    """只处理边界框的示例"""
    print("=== 只处理边界框示例 ===")
    
    # 处理已存在的JSON数据集
    dataset_file = "output/example_dataset.json"
    if Path(dataset_file).exists():
        cmd1 = [
            "python", "-m", "spdatalab.cli", "process-bbox",
            "--input", dataset_file,
            "--batch", "200",
            "--insert-batch", "200",
            "--buffer-meters", "30",
            "--precise-buffer"
        ]
        
        run_command(cmd1, "处理JSON数据集的边界框（精确模式）")
    
    # 处理已存在的Parquet数据集
    parquet_file = "output/example_dataset.parquet"
    if Path(parquet_file).exists():
        cmd2 = [
            "python", "-m", "spdatalab.cli", "process-bbox",
            "--input", parquet_file,
            "--batch", "1000",
            "--insert-batch", "500",
            "--buffer-meters", "100"
        ]
        
        run_command(cmd2, "处理Parquet数据集的边界框（快速模式）")

def show_cli_help():
    """显示CLI帮助信息"""
    print("=== CLI帮助信息 ===")
    
    # 显示主帮助
    cmd1 = ["python", "-m", "spdatalab.cli", "--help"]
    run_command(cmd1, "主CLI帮助")
    
    # 显示特定命令帮助
    commands = [
        "build-dataset",
        "process-bbox", 
        "build-dataset-with-bbox",
        "dataset-info"
    ]
    
    for command in commands:
        cmd = ["python", "-m", "spdatalab.cli", command, "--help"]
        run_command(cmd, f"{command} 命令帮助")

def main():
    """主函数：演示不同的CLI使用方式"""
    import argparse
    
    parser = argparse.ArgumentParser(description="spdatalab CLI使用示例")
    parser.add_argument(
        '--mode', 
        choices=['json', 'parquet', 'operations', 'bbox-only', 'help'], 
        default='json',
        help='运行模式'
    )
    
    args = parser.parse_args()
    
    # 确保输出目录存在
    Path("output").mkdir(exist_ok=True)
    
    print("spdatalab CLI 使用示例")
    print("=" * 60)
    
    if args.mode == 'json':
        example_cli_workflow_json()
    elif args.mode == 'parquet':
        example_cli_workflow_parquet()
    elif args.mode == 'operations':
        example_dataset_operations()
    elif args.mode == 'bbox-only':
        example_bbox_only()
    elif args.mode == 'help':
        show_cli_help()
    
    print("\n" + "=" * 60)
    print("示例完成！")
    
    # 显示生成的文件
    print("\n生成的文件:")
    output_dir = Path("output")
    if output_dir.exists():
        for file in output_dir.iterdir():
            if file.is_file():
                print(f"  - {file}")

if __name__ == "__main__":
    main() 