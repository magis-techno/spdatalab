#!/usr/bin/env python3
"""
并行处理性能测试脚本

用于测试和对比顺序处理与并行处理的性能差异
"""

import time
import sys
import argparse
from pathlib import Path

def test_sequential_processing(dataset_file, batch_size=1000, insert_batch_size=1000):
    """测试顺序处理性能"""
    print("🔄 开始顺序处理测试...")
    
    try:
        from src.spdatalab.dataset.bbox import run_with_partitioning
        
        start_time = time.time()
        
        run_with_partitioning(
            input_path=dataset_file,
            batch=batch_size,
            insert_batch=insert_batch_size,
            work_dir="./test_logs/sequential",
            create_unified_view_flag=True,
            maintain_view_only=False,
            use_parallel=False
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"✅ 顺序处理完成，耗时: {processing_time:.2f} 秒")
        return processing_time
        
    except Exception as e:
        print(f"❌ 顺序处理失败: {str(e)}")
        return None

def test_parallel_processing(dataset_file, batch_size=1000, insert_batch_size=1000, max_workers=None):
    """测试并行处理性能"""
    print(f"🚀 开始并行处理测试 (workers: {max_workers or 'auto'})...")
    
    try:
        from src.spdatalab.dataset.bbox import run_with_partitioning
        
        start_time = time.time()
        
        run_with_partitioning(
            input_path=dataset_file,
            batch=batch_size,
            insert_batch=insert_batch_size,
            work_dir="./test_logs/parallel",
            create_unified_view_flag=True,
            maintain_view_only=False,
            use_parallel=True,
            max_workers=max_workers
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"✅ 并行处理完成，耗时: {processing_time:.2f} 秒")
        return processing_time
        
    except Exception as e:
        print(f"❌ 并行处理失败: {str(e)}")
        return None

def compare_performance(sequential_time, parallel_time):
    """比较性能差异"""
    if sequential_time and parallel_time:
        speedup = sequential_time / parallel_time
        time_saved = sequential_time - parallel_time
        
        print(f"\n📊 性能对比结果:")
        print(f"  - 顺序处理: {sequential_time:.2f} 秒")
        print(f"  - 并行处理: {parallel_time:.2f} 秒")
        print(f"  - 性能提升: {speedup:.2f}x")
        print(f"  - 节省时间: {time_saved:.2f} 秒")
        
        if speedup > 1:
            print(f"🎉 并行处理效果显著，提升 {speedup:.1f} 倍!")
        else:
            print(f"⚠️  并行处理未显示明显优势，可能数据量太小或存在瓶颈")
    else:
        print("❌ 无法进行性能对比，某个测试失败")

def main():
    parser = argparse.ArgumentParser(description="并行处理性能测试")
    parser.add_argument('--dataset-file', required=True, help='数据集文件路径')
    parser.add_argument('--batch-size', type=int, default=1000, help='处理批次大小')
    parser.add_argument('--insert-batch-size', type=int, default=1000, help='插入批次大小')
    parser.add_argument('--max-workers', type=int, help='最大并行worker数量')
    parser.add_argument('--test-mode', choices=['sequential', 'parallel', 'both'], 
                       default='both', help='测试模式')
    parser.add_argument('--skip-cleanup', action='store_true', help='跳过清理步骤')
    
    args = parser.parse_args()
    
    # 验证数据集文件存在
    if not Path(args.dataset_file).exists():
        print(f"❌ 数据集文件不存在: {args.dataset_file}")
        return
    
    print("🧪 并行处理性能测试开始")
    print("=" * 60)
    print(f"数据集文件: {args.dataset_file}")
    print(f"处理批次大小: {args.batch_size}")
    print(f"插入批次大小: {args.insert_batch_size}")
    print(f"测试模式: {args.test_mode}")
    
    # 创建测试日志目录
    Path("./test_logs/sequential").mkdir(parents=True, exist_ok=True)
    Path("./test_logs/parallel").mkdir(parents=True, exist_ok=True)
    
    sequential_time = None
    parallel_time = None
    
    # 执行测试
    if args.test_mode in ['sequential', 'both']:
        print("\n" + "="*60)
        sequential_time = test_sequential_processing(
            args.dataset_file, args.batch_size, args.insert_batch_size
        )
    
    if args.test_mode in ['parallel', 'both']:
        print("\n" + "="*60)
        parallel_time = test_parallel_processing(
            args.dataset_file, args.batch_size, args.insert_batch_size, args.max_workers
        )
    
    # 比较结果
    if args.test_mode == 'both':
        print("\n" + "="*60)
        compare_performance(sequential_time, parallel_time)
    
    print("\n✅ 性能测试完成")

if __name__ == "__main__":
    main() 