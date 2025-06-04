#!/usr/bin/env python3
"""
快速启动脚本：从索引文件到边界框处理的完整工作流程
"""

import sys
import argparse
from pathlib import Path

# 添加src路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run as bbox_run

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="从索引文件到边界框处理的完整工作流程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # JSON格式工作流程
  python scripts/run_bbox_workflow.py --index data/train_index.txt --output output/train_dataset.json --format json --name train_v1

  # Parquet格式工作流程  
  python scripts/run_bbox_workflow.py --index data/val_index.txt --output output/val_dataset.parquet --format parquet --name val_v1

  # 只生成数据集，不处理边界框
  python scripts/run_bbox_workflow.py --index data/test_index.txt --output output/test_dataset.json --name test_v1 --skip-bbox

  # 只处理边界框（数据集文件已存在）
  python scripts/run_bbox_workflow.py --bbox-only --input output/existing_dataset.json
        """
    )
    
    # 输入选项
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--index', help='索引文件路径')
    input_group.add_argument('--bbox-only', action='store_true', help='只处理边界框（数据集文件已存在）')
    
    # 输出选项
    parser.add_argument('--output', help='输出数据集文件路径')
    parser.add_argument('--input', help='已存在的数据集文件路径（用于--bbox-only模式）')
    
    # 数据集选项
    parser.add_argument('--name', help='数据集名称')
    parser.add_argument('--description', default='', help='数据集描述')
    parser.add_argument('--format', choices=['json', 'parquet'], default='json', help='数据集保存格式')
    
    # 处理选项
    parser.add_argument('--skip-bbox', action='store_true', help='跳过边界框处理')
    parser.add_argument('--batch', type=int, default=1000, help='处理批次大小')
    parser.add_argument('--insert-batch', type=int, default=1000, help='插入批次大小')
    
    # 解析参数
    args = parser.parse_args()
    
    # 验证参数
    if args.bbox_only:
        if not args.input:
            parser.error("--bbox-only 模式需要指定 --input 参数")
    else:
        if not args.output:
            parser.error("需要指定 --output 参数")
        if not args.name:
            parser.error("需要指定 --name 参数")
    
    try:
        if args.bbox_only:
            # 只处理边界框
            process_bbox_only(args)
        else:
            # 完整工作流程
            process_full_workflow(args)
            
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)

def process_full_workflow(args):
    """处理完整工作流程"""
    print("=== 开始完整工作流程 ===")
    
    # 步骤1：构建数据集
    print(f"1. 从索引文件构建数据集: {args.index}")
    manager = DatasetManager()
    
    dataset = manager.build_dataset_from_index(
        index_file=args.index,
        dataset_name=args.name,
        description=args.description
    )
    
    # 显示数据集统计信息
    stats = manager.get_dataset_stats(dataset)
    print("数据集统计信息:")
    print(f"  - 数据集名称: {stats['dataset_name']}")
    print(f"  - 子数据集数量: {stats['subdataset_count']}")
    print(f"  - 唯一场景数: {stats['total_unique_scenes']}")
    print(f"  - 总场景数(含倍增): {stats['total_scenes_with_duplicates']}")
    
    # 步骤2：保存数据集
    print(f"2. 保存数据集到: {args.output} ({args.format}格式)")
    
    # 确保输出目录存在
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    
    manager.save_dataset(dataset, args.output, format=args.format)
    print(f"数据集已保存: {args.output}")
    
    # 步骤3：处理边界框（如果需要）
    if not args.skip_bbox:
        print("3. 开始处理边界框数据...")
        process_bbox(args.output, args.batch, args.insert_batch)
    else:
        print("3. 跳过边界框处理")
    
    print("=== 完整工作流程完成 ===")

def process_bbox_only(args):
    """只处理边界框"""
    print("=== 开始边界框处理 ===")
    
    input_file = args.input
    if not Path(input_file).exists():
        raise FileNotFoundError(f"数据集文件不存在: {input_file}")
    
    print(f"输入数据集文件: {input_file}")
    
    # 显示数据集信息
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(input_file)
        stats = manager.get_dataset_stats(dataset)
        
        print("数据集统计信息:")
        print(f"  - 数据集名称: {stats['dataset_name']}")
        print(f"  - 子数据集数量: {stats['subdataset_count']}")
        print(f"  - 唯一场景数: {stats['total_unique_scenes']}")
        print(f"  - 总场景数(含倍增): {stats['total_scenes_with_duplicates']}")
        
    except Exception as e:
        print(f"警告: 无法加载数据集信息: {str(e)}")
    
    # 处理边界框
    batch = args.batch if hasattr(args, 'batch') else 1000
    insert_batch = args.insert_batch if hasattr(args, 'insert_batch') else 1000
    
    process_bbox(input_file, batch, insert_batch)
    
    print("=== 边界框处理完成 ===")

def process_bbox(input_file, batch_size, insert_batch_size):
    """处理边界框数据"""
    print(f"开始处理边界框，批次大小: {batch_size}, 插入批次大小: {insert_batch_size}")
    
    try:
        bbox_run(
            input_path=input_file,
            batch=batch_size,
            insert_batch=insert_batch_size
        )
        print("边界框处理完成")
        
    except Exception as e:
        print(f"边界框处理失败: {str(e)}")
        raise

def show_examples():
    """显示使用示例"""
    examples = """
使用示例:

1. 完整的JSON工作流程:
   python scripts/run_bbox_workflow.py \\
     --index data/train_index.txt \\
     --output output/train_dataset.json \\
     --format json \\
     --name "training_dataset_v1" \\
     --description "训练数据集版本1" \\
     --batch 1000 \\
     --insert-batch 500

2. 完整的Parquet工作流程:
   python scripts/run_bbox_workflow.py \\
     --index data/validation_index.txt \\
     --output output/val_dataset.parquet \\
     --format parquet \\
     --name "validation_dataset_v1" \\
     --batch 2000 \\
     --insert-batch 1000

3. 只生成数据集，不处理边界框:
   python scripts/run_bbox_workflow.py \\
     --index data/test_index.txt \\
     --output output/test_dataset.json \\
     --name "test_dataset_v1" \\
     --skip-bbox

4. 只处理已存在数据集的边界框:
   python scripts/run_bbox_workflow.py \\
     --bbox-only \\
     --input output/existing_dataset.json \\
     --batch 1500 \\
     --insert-batch 750

5. 处理大型数据集（优化批次大小）:
   python scripts/run_bbox_workflow.py \\
     --index data/large_index.txt \\
     --output output/large_dataset.parquet \\
     --format parquet \\
     --name "large_dataset_v1" \\
     --batch 500 \\
     --insert-batch 200
    """
    print(examples)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("bbox工作流程快速启动脚本")
        print("使用 -h 参数查看详细帮助信息")
        print("使用 --examples 参数查看使用示例")
        sys.exit(0)
    
    if len(sys.argv) == 2 and sys.argv[1] == '--examples':
        show_examples()
        sys.exit(0)
    
    main() 