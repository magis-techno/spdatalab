#!/usr/bin/env python3
# STATUS: one_time - 清理bbox_overlap_analysis_results表中有问题的旧数据
"""
清理bbox_overlap_analysis_results表中的旧分析数据

这个脚本会清理包含错误JSON格式的分析记录（Python布尔值 False/True 而不是 JSON的 false/true）

使用方法：
    # 清理今天所有的数据
    python examples/one_time/cleanup_old_analysis_data.py --today
    
    # 清理所有数据
    python examples/one_time/cleanup_old_analysis_data.py --all
    
    # 只查看但不删除
    python examples/one_time/cleanup_old_analysis_data.py --dry-run
"""

import sys
from pathlib import Path
import argparse

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.bbox import LOCAL_DSN
from sqlalchemy import create_engine, text

def cleanup_analysis_data(mode='today', dry_run=False):
    """清理分析数据
    
    Args:
        mode: 'today' - 只清理今天的数据, 'all' - 清理所有数据
        dry_run: 如果为True，只查看不删除
    """
    print("🧹 清理bbox_overlap_analysis_results表中的旧数据")
    print("=" * 60)
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        # 先查看有多少数据
        if mode == 'today':
            count_sql = text("""
                SELECT COUNT(*) as total_count,
                       COUNT(*) FILTER (WHERE analysis_time::date = CURRENT_DATE) as today_count
                FROM bbox_overlap_analysis_results;
            """)
            where_clause = "WHERE analysis_time::date = CURRENT_DATE"
            scope_desc = "今天的"
        else:
            count_sql = text("""
                SELECT COUNT(*) as total_count,
                       COUNT(*) as today_count
                FROM bbox_overlap_analysis_results;
            """)
            where_clause = ""
            scope_desc = "所有"
        
        result = conn.execute(count_sql).fetchone()
        
        print(f"📊 当前数据统计:")
        print(f"   总记录数: {result.total_count}")
        if mode == 'today':
            print(f"   今天记录数: {result.today_count}")
        
        # 查看要删除的数据样本
        sample_sql = text(f"""
            SELECT 
                analysis_id,
                analysis_time,
                analysis_params
            FROM bbox_overlap_analysis_results 
            {where_clause}
            ORDER BY analysis_time DESC
            LIMIT 5;
        """)
        
        samples = conn.execute(sample_sql).fetchall()
        
        if samples:
            print(f"\n📋 {scope_desc}数据样本 (前5条):")
            for i, sample in enumerate(samples, 1):
                print(f"\n{i}. Analysis ID: {sample.analysis_id}")
                print(f"   Time: {sample.analysis_time}")
                print(f"   Params: {sample.analysis_params[:100]}...")
        else:
            print(f"\n✅ 没有找到{scope_desc}数据")
            return True
        
        # 检查是否有问题的JSON
        if where_clause:
            json_where = f"{where_clause} AND (analysis_params LIKE '%False%' OR analysis_params LIKE '%True%')"
        else:
            json_where = "WHERE (analysis_params LIKE '%False%' OR analysis_params LIKE '%True%')"
        
        check_json_sql = text(f"""
            SELECT COUNT(*) as bad_json_count
            FROM bbox_overlap_analysis_results 
            {json_where};
        """)
        
        bad_json_result = conn.execute(check_json_sql).fetchone()
        
        if bad_json_result.bad_json_count > 0:
            print(f"\n⚠️ 发现 {bad_json_result.bad_json_count} 条包含错误JSON格式的记录")
            print(f"   (包含Python布尔值 False/True 而不是 JSON的 false/true)")
        
        # 执行删除
        if dry_run:
            print(f"\n🔍 [DRY RUN] 模拟模式，不会实际删除数据")
            if mode == 'today':
                print(f"   将要删除: 今天的 {result.today_count} 条记录")
            else:
                print(f"   将要删除: 所有 {result.total_count} 条记录")
            return True
        
        # 确认删除
        print(f"\n⚠️ 警告: 即将删除 {scope_desc}{result.today_count if mode == 'today' else result.total_count} 条记录")
        confirm = input("   确认删除? (yes/no): ")
        
        if confirm.lower() != 'yes':
            print("❌ 取消删除操作")
            return False
        
        # 执行删除
        delete_sql = text(f"""
            DELETE FROM bbox_overlap_analysis_results 
            {where_clause};
        """)
        
        conn.execute(delete_sql)
        conn.commit()
        
        print(f"✅ 删除完成")
        
        # 验证删除结果
        verify_sql = text("""
            SELECT COUNT(*) as remaining_count
            FROM bbox_overlap_analysis_results;
        """)
        
        verify_result = conn.execute(verify_sql).fetchone()
        print(f"📊 剩余记录数: {verify_result.remaining_count}")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='清理bbox_overlap_analysis_results表中的旧数据')
    parser.add_argument('--today', action='store_true', help='只清理今天的数据（默认）')
    parser.add_argument('--all', action='store_true', help='清理所有数据')
    parser.add_argument('--dry-run', action='store_true', help='只查看不删除（模拟运行）')
    
    args = parser.parse_args()
    
    # 确定清理模式
    if args.all:
        mode = 'all'
    else:
        mode = 'today'  # 默认只清理今天的
    
    print(f"🎯 清理模式: {mode}")
    print(f"🔍 模拟运行: {args.dry_run}")
    print()
    
    success = cleanup_analysis_data(mode=mode, dry_run=args.dry_run)
    
    if success:
        print("\n" + "=" * 60)
        print("🎉 清理完成！")
        if not args.dry_run:
            print("\n📝 下一步:")
            print("   1. 运行分析生成新数据:")
            print("      cd examples/dataset/bbox_examples")
            print("      python run_overlap_analysis.py --city A72 --top-n 1")
            print("   2. 运行批量分析:")
            print("      python batch_top1_analysis.py --cities A72 --max-cities 1")
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())
