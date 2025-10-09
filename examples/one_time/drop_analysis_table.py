#!/usr/bin/env python3
# STATUS: one_time - 删除bbox_overlap_analysis_results表
"""
删除bbox_overlap_analysis_results表

⚠️ 警告：这会删除整个表及其所有数据！

使用方法：
    # 删除表
    python examples/one_time/drop_analysis_table.py
    
    # 只查看不删除
    python examples/one_time/drop_analysis_table.py --dry-run
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

def drop_analysis_table(dry_run=False):
    """删除bbox_overlap_analysis_results表"""
    
    table_name = "bbox_overlap_analysis_results"
    
    print(f"🗑️ 删除表: {table_name}")
    print("=" * 60)
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        # 检查表是否存在
        check_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        table_exists = conn.execute(check_sql).scalar()
        
        if not table_exists:
            print(f"✅ 表 {table_name} 不存在，无需删除")
            return True
        
        # 查看表的基本信息
        info_sql = text(f"""
            SELECT 
                COUNT(*) as total_rows,
                pg_size_pretty(pg_total_relation_size('{table_name}')) as table_size
            FROM {table_name};
        """)
        
        info = conn.execute(info_sql).fetchone()
        
        print(f"📊 表信息:")
        print(f"   表名: {table_name}")
        print(f"   记录数: {info.total_rows:,}")
        print(f"   表大小: {info.table_size}")
        
        # 查看最近的数据
        sample_sql = text(f"""
            SELECT 
                analysis_id,
                analysis_time,
                COUNT(*) OVER() as total_analyses
            FROM {table_name}
            ORDER BY analysis_time DESC
            LIMIT 3;
        """)
        
        samples = conn.execute(sample_sql).fetchall()
        
        if samples:
            print(f"\n📋 最近的分析记录:")
            for i, sample in enumerate(samples, 1):
                print(f"   {i}. {sample.analysis_id} - {sample.analysis_time}")
        
        if dry_run:
            print(f"\n🔍 [DRY RUN] 模拟模式，不会实际删除表")
            print(f"   将要执行: DROP TABLE {table_name} CASCADE;")
            return True
        
        # 确认删除
        print(f"\n⚠️ 警告: 即将删除整个表 {table_name}")
        print(f"   这将删除 {info.total_rows:,} 条记录")
        print(f"   表占用空间: {info.table_size}")
        confirm = input("   确认删除? (yes/no): ")
        
        if confirm.lower() != 'yes':
            print("❌ 取消删除操作")
            return False
        
        # 执行删除
        drop_sql = text(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        print(f"\n🗑️ 正在删除表...")
        conn.execute(drop_sql)
        conn.commit()
        
        print(f"✅ 表 {table_name} 已删除")
        
        # 验证删除结果
        verify_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        still_exists = conn.execute(verify_sql).scalar()
        
        if not still_exists:
            print(f"✅ 验证成功: 表已完全删除")
            return True
        else:
            print(f"⚠️ 警告: 表可能未完全删除")
            return False

def main():
    parser = argparse.ArgumentParser(description='删除bbox_overlap_analysis_results表')
    parser.add_argument('--dry-run', action='store_true', help='只查看不删除（模拟运行）')
    
    args = parser.parse_args()
    
    print(f"🔍 模拟运行: {args.dry_run}")
    print()
    
    success = drop_analysis_table(dry_run=args.dry_run)
    
    if success:
        print("\n" + "=" * 60)
        print("🎉 操作完成！")
        if not args.dry_run:
            print("\n📝 下一步:")
            print("   表会在下次运行分析时自动重建")
            print("   运行分析:")
            print("      cd examples/dataset/bbox_examples")
            print("      python run_overlap_analysis.py --city A72 --top-n 1")
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())
