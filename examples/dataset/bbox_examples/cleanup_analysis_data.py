#!/usr/bin/env python3
"""
分析数据清理工具
===============

专门用于清理bbox叠置分析产生的数据和视图。

功能特性：
- 列出所有分析结果
- 按条件批量删除分析结果
- 清理QGIS视图
- 安全的试运行模式
- 详细的清理统计

使用示例：
    # 列出所有分析结果
    python cleanup_analysis_data.py --list
    
    # 按模式清理
    python cleanup_analysis_data.py --pattern "bbox_overlap_2023%" --dry-run
    
    # 实际执行清理
    python cleanup_analysis_data.py --pattern "bbox_overlap_2023%" --confirm
    
    # 清理7天前的结果
    python cleanup_analysis_data.py --older-than 7 --confirm
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='清理BBox叠置分析数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 列出所有分析结果
  python cleanup_analysis_data.py --list
  
  # 按模式试运行清理
  python cleanup_analysis_data.py --pattern "test%" --dry-run
  
  # 实际执行按ID清理
  python cleanup_analysis_data.py --ids "bbox_overlap_20231201_100000" --confirm
  
  # 清理7天前的数据
  python cleanup_analysis_data.py --older-than 7 --confirm
  
  # 清理所有QGIS视图
  python cleanup_analysis_data.py --cleanup-views --confirm
        """
    )
    
    # 操作类型参数（互斥）
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--list', action='store_true', 
                             help='列出所有分析结果')
    action_group.add_argument('--cleanup-results', action='store_true', 
                             help='清理分析结果数据')
    action_group.add_argument('--cleanup-views', action='store_true', 
                             help='清理QGIS视图')
    
    # 清理条件参数
    condition_group = parser.add_argument_group('清理条件')
    condition_group.add_argument('--pattern', 
                                help='按模式清理（支持SQL LIKE语法，如"test%"）')
    condition_group.add_argument('--ids', nargs='+', 
                                help='按analysis_id清理（可指定多个）')
    condition_group.add_argument('--older-than', type=int, 
                                help='清理N天前的结果')
    
    # 执行控制参数
    control_group = parser.add_argument_group('执行控制')
    exec_group = control_group.add_mutually_exclusive_group()
    exec_group.add_argument('--dry-run', action='store_true', default=True,
                           help='试运行模式，不实际删除（默认）')
    exec_group.add_argument('--confirm', action='store_true',
                           help='确认执行实际删除')
    
    args = parser.parse_args()
    
    print("🧹 BBox叠置分析数据清理工具")
    print("=" * 50)
    
    # 初始化分析器
    try:
        analyzer = BBoxOverlapAnalyzer()
        print("✅ 分析器初始化成功")
    except Exception as e:
        print(f"❌ 分析器初始化失败: {e}")
        return 1
    
    try:
        if args.list:
            # 列出分析结果
            print("\n📋 查询所有分析结果")
            print("-" * 30)
            
            pattern = args.pattern if args.pattern else None
            df = analyzer.list_analysis_results(pattern)
            
            if not df.empty:
                print(f"\n📊 统计摘要:")
                print(f"   分析总数: {len(df)}")
                print(f"   记录总数: {df['hotspot_count'].sum()}")
                print(f"   总重叠数: {df['total_overlaps'].sum()}")
                
                # 按日期分组统计
                df['date'] = df['created_at'].dt.date
                date_stats = df.groupby('date').agg({
                    'analysis_id': 'count',
                    'hotspot_count': 'sum'
                }).rename(columns={'analysis_id': 'analyses_count'})
                
                print(f"\n📅 按日期分布:")
                print(date_stats.to_string())
            
        elif args.cleanup_results:
            # 清理分析结果
            print("\n🗑️ 清理分析结果数据")
            print("-" * 30)
            
            if not any([args.pattern, args.ids, args.older_than]):
                print("⚠️ 请指定清理条件: --pattern, --ids, 或 --older-than")
                return 1
            
            result = analyzer.cleanup_analysis_results(
                analysis_ids=args.ids,
                pattern=args.pattern,
                older_than_days=args.older_than,
                dry_run=not args.confirm
            )
            
            if args.dry_run and result.get("would_delete", 0) > 0:
                print(f"\n💡 这是试运行模式，未实际删除数据")
                print(f"💡 使用 --confirm 参数执行实际删除")
            
        elif args.cleanup_views:
            # 清理QGIS视图
            print("\n🎨 清理QGIS视图")
            print("-" * 30)
            
            success = analyzer.cleanup_qgis_views(confirm=args.confirm)
            
            if not args.confirm:
                print(f"\n💡 这是试运行模式，未实际删除视图")
                print(f"💡 使用 --confirm 参数执行实际删除")
        
        print("\n✅ 操作完成")
        return 0
        
    except Exception as e:
        print(f"\n❌ 操作失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
