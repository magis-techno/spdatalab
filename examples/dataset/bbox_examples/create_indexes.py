#!/usr/bin/env python3
"""
BBox性能索引创建脚本
==================

为bbox分表创建优化索引，提升overlap分析性能

使用方法：
    python examples/dataset/bbox_examples/create_indexes.py
    
    # 只优化前10个表（快速模式）
    python examples/dataset/bbox_examples/create_indexes.py --quick
    
    # 指定表数量
    python examples/dataset/bbox_examples/create_indexes.py --limit 20
    
    # 只检查，不创建
    python examples/dataset/bbox_examples/create_indexes.py --check-only
"""

import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict, Tuple

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import list_bbox_tables, LOCAL_DSN
    from sqlalchemy import create_engine, text
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import list_bbox_tables, LOCAL_DSN
    from sqlalchemy import create_engine, text

import pandas as pd

class BBoxIndexOptimizer:
    """BBox分表索引优化器"""
    
    def __init__(self, dsn: str = LOCAL_DSN):
        self.dsn = dsn
        self.engine = create_engine(dsn, future=True)
        
    def get_table_stats(self, table_name: str) -> Dict:
        """获取表的统计信息"""
        try:
            with self.engine.connect() as conn:
                # 获取记录数
                count_sql = text(f"SELECT COUNT(*) FROM {table_name};")
                row_count = conn.execute(count_sql).scalar()
                
                # 获取索引信息
                index_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_indexes,
                        COUNT(*) FILTER (WHERE indexdef ILIKE '%gist%') as spatial_indexes,
                        COUNT(*) FILTER (WHERE indexdef ILIKE '%city_id%') as city_indexes,
                        COUNT(*) FILTER (WHERE indexdef ILIKE '%all_good%') as quality_indexes,
                        BOOL_OR(indexdef ILIKE '%city_id%all_good%' OR indexdef ILIKE '%all_good%city_id%') as has_composite
                    FROM pg_indexes 
                    WHERE tablename = '{table_name}' AND schemaname = 'public';
                """)
                
                index_stats = conn.execute(index_sql).fetchone()
                
                return {
                    'row_count': row_count,
                    'total_indexes': index_stats.total_indexes,
                    'spatial_indexes': index_stats.spatial_indexes,
                    'city_indexes': index_stats.city_indexes,
                    'quality_indexes': index_stats.quality_indexes,
                    'has_composite': index_stats.has_composite,
                    'needs_optimization': not (index_stats.has_composite and index_stats.spatial_indexes > 0)
                }
                
        except Exception as e:
            return {
                'row_count': 0,
                'error': str(e),
                'needs_optimization': True
            }
    
    def create_indexes_for_table(self, table_name: str) -> Tuple[bool, List[str], str]:
        """为单个表创建索引
        
        Returns:
            (success, created_indexes, message)
        """
        created_indexes = []
        
        try:
            with self.engine.connect() as conn:
                # 1. 🚀 核心复合索引：(city_id, all_good)
                composite_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_city_good 
                    ON {table_name} (city_id, all_good) 
                    WHERE city_id IS NOT NULL;
                """)
                conn.execute(composite_index_sql)
                created_indexes.append("city_good_composite")
                
                # 2. 🚀 空间索引检查和创建
                check_spatial_sql = text(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = '{table_name}' 
                        AND indexdef ILIKE '%gist%geometry%'
                    );
                """)
                
                has_spatial = conn.execute(check_spatial_sql).scalar()
                if not has_spatial:
                    spatial_index_sql = text(f"""
                        CREATE INDEX idx_{table_name}_geom 
                        ON {table_name} USING GIST (geometry);
                    """)
                    conn.execute(spatial_index_sql)
                    created_indexes.append("spatial_geometry")
                
                # 3. 🚀 部分索引：高质量数据的空间索引
                partial_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_quality_geom 
                    ON {table_name} USING GIST (geometry) 
                    WHERE all_good = true AND city_id IS NOT NULL;
                """)
                conn.execute(partial_index_sql)
                created_indexes.append("quality_spatial")
                
                # 4. 🚀 主键索引检查
                check_id_sql = text(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = '{table_name}' 
                        AND indexdef ILIKE '%\\bid\\b%'
                        AND indexdef NOT ILIKE '%city_id%'
                    );
                """)
                
                has_id_index = conn.execute(check_id_sql).scalar()
                if not has_id_index:
                    id_index_sql = text(f"""
                        CREATE INDEX IF NOT EXISTS idx_{table_name}_id 
                        ON {table_name} (id);
                    """)
                    conn.execute(id_index_sql)
                    created_indexes.append("id_primary")
                
                conn.commit()
                
                return True, created_indexes, "成功"
                
        except Exception as e:
            return False, created_indexes, f"失败: {str(e)}"
    
    def check_all_tables(self, limit: int = None) -> pd.DataFrame:
        """检查所有表的索引状态"""
        print("🔍 检查bbox分表索引状态...")
        
        # 获取分表列表（排除视图）
        all_tables = list_bbox_tables(self.engine)
        
        # 过滤出真正的表，排除视图
        bbox_tables = []
        
        # 获取所有视图名称，避免对视图进行索引操作
        with self.engine.connect() as conn:
            views_sql = text("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'clips_bbox%';
            """)
            view_names = {row[0] for row in conn.execute(views_sql).fetchall()}
        
        for table in all_tables:
            if table.startswith('clips_bbox_') and table != 'clips_bbox':
                # 排除视图
                if table not in view_names:
                    bbox_tables.append(table)
                else:
                    print(f"⏭️ 跳过视图: {table}")
        
        print(f"📋 找到 {len(bbox_tables)} 个实际分表（排除 {len(view_names)} 个视图）")
        
        if limit:
            bbox_tables = bbox_tables[:limit]
            print(f"📋 检查前 {len(bbox_tables)} 个分表")
        else:
            print(f"📋 检查所有 {len(bbox_tables)} 个分表")
        
        results = []
        
        for i, table_name in enumerate(bbox_tables, 1):
            print(f"[{i}/{len(bbox_tables)}] 检查 {table_name}...")
            
            stats = self.get_table_stats(table_name)
            
            # 计算优化状态
            if 'error' in stats:
                status = "❌ 错误"
            elif stats.get('has_composite') and stats.get('spatial_indexes', 0) > 0:
                status = "🚀 已优化"
            elif stats.get('spatial_indexes', 0) > 0 and stats.get('city_indexes', 0) > 0:
                status = "⚡ 部分优化"
            elif stats.get('spatial_indexes', 0) > 0:
                status = "📍 仅空间索引"
            else:
                status = "❌ 需要优化"
            
            results.append({
                'table_name': table_name,
                'row_count': stats.get('row_count', 0),
                'total_indexes': stats.get('total_indexes', 0),
                'spatial_indexes': stats.get('spatial_indexes', 0),
                'city_indexes': stats.get('city_indexes', 0),
                'has_composite': stats.get('has_composite', False),
                'status': status,
                'needs_optimization': stats.get('needs_optimization', True),
                'error': stats.get('error', '')
            })
        
        return pd.DataFrame(results)
    
    def optimize_tables(self, limit: int = None, dry_run: bool = False) -> Dict:
        """优化分表索引"""
        print("🚀 开始bbox分表索引优化...")
        
        # 获取需要优化的表
        df = self.check_all_tables(limit)
        need_optimization = df[df['needs_optimization']].copy()
        
        if need_optimization.empty:
            print("✅ 所有表都已优化，无需额外操作")
            return {
                'total_tables': len(df),
                'optimized_tables': 0,
                'failed_tables': 0,
                'skipped_tables': len(df)
            }
        
        print(f"📋 需要优化 {len(need_optimization)} 个表")
        
        if dry_run:
            print("🧪 试运行模式 - 只检查，不执行")
            return {
                'total_tables': len(df),
                'would_optimize': len(need_optimization),
                'dry_run': True
            }
        
        # 执行优化
        optimized = 0
        failed = 0
        
        for i, row in need_optimization.iterrows():
            table_name = row['table_name']
            print(f"\n[{optimized + failed + 1}/{len(need_optimization)}] 🔧 优化表: {table_name}")
            print(f"   当前记录数: {row['row_count']:,}")
            
            start_time = time.time()
            success, created_indexes, message = self.create_indexes_for_table(table_name)
            elapsed = time.time() - start_time
            
            if success:
                print(f"   ✅ {message} (耗时: {elapsed:.1f}s)")
                print(f"   📊 创建索引: {', '.join(created_indexes)}")
                optimized += 1
            else:
                print(f"   ❌ {message}")
                failed += 1
        
        print(f"\n✅ 索引优化完成！")
        print(f"   成功优化: {optimized} 个表")
        if failed > 0:
            print(f"   优化失败: {failed} 个表")
        
        return {
            'total_tables': len(df),
            'optimized_tables': optimized,
            'failed_tables': failed,
            'skipped_tables': len(df) - len(need_optimization)
        }
    
    def get_optimization_summary(self) -> Dict:
        """获取优化摘要"""
        df = self.check_all_tables()
        
        summary = df['status'].value_counts().to_dict()
        
        # 计算百分比
        total = len(df)
        for status in summary:
            summary[status] = {
                'count': summary[status],
                'percentage': round(100 * summary[status] / total, 1)
            }
        
        return {
            'total_tables': total,
            'status_distribution': summary,
            'needs_optimization': len(df[df['needs_optimization']]),
            'already_optimized': len(df[~df['needs_optimization']])
        }

def main():
    parser = argparse.ArgumentParser(description='BBox分表索引优化工具')
    parser.add_argument('--quick', action='store_true', help='快速模式：只优化前10个表')
    parser.add_argument('--limit', type=int, help='限制处理的表数量')
    parser.add_argument('--check-only', action='store_true', help='只检查状态，不创建索引')
    parser.add_argument('--dry-run', action='store_true', help='试运行模式')
    
    args = parser.parse_args()
    
    print("🎯 BBox分表索引优化工具")
    print("=" * 50)
    
    optimizer = BBoxIndexOptimizer()
    
    # 确定处理表的数量
    limit = None
    if args.quick:
        limit = 10
        print("🚀 快速模式：处理前10个表")
    elif args.limit:
        limit = args.limit
        print(f"📊 限制模式：处理前{limit}个表")
    else:
        print("📋 完整模式：处理所有表")
    
    if args.check_only:
        # 只检查状态
        print("\n🔍 检查索引状态...")
        df = optimizer.check_all_tables(limit)
        
        print("\n📊 索引状态报告:")
        print(df[['table_name', 'row_count', 'total_indexes', 'status']].to_string(index=False))
        
        # 显示摘要
        summary = optimizer.get_optimization_summary()
        print(f"\n📈 优化摘要:")
        print(f"   总表数: {summary['total_tables']}")
        print(f"   需要优化: {summary['needs_optimization']}")
        print(f"   已优化: {summary['already_optimized']}")
        
        print(f"\n📊 状态分布:")
        for status, info in summary['status_distribution'].items():
            print(f"   {status}: {info['count']} ({info['percentage']}%)")
        
    else:
        # 执行优化
        result = optimizer.optimize_tables(limit, args.dry_run)
        
        if not args.dry_run:
            print(f"\n🎯 测试建议:")
            print(f"   python examples/dataset/bbox_examples/run_overlap_analysis.py --city A86 --intersect-only --top-n 5")

if __name__ == "__main__":
    main()
