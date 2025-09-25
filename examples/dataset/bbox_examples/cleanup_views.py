#!/usr/bin/env python3
"""
清理旧视图脚本
==============

删除不需要的bbox视图，避免干扰索引优化

使用方法：
    python examples/dataset/bbox_examples/cleanup_views.py
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
    from sqlalchemy import create_engine, text
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN
    from sqlalchemy import create_engine, text

def cleanup_bbox_views():
    """清理bbox相关视图"""
    print("🧹 清理bbox相关视图...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 要清理的视图列表
    views_to_remove = [
        'clips_bbox_unified_qgis',    # 旧的QGIS视图
        'clips_bbox_unified_mat',     # 物化视图（如果存在）
        'qgis_bbox_overlap_hotspots', # 重叠分析结果视图
        'bbox_index_stats'            # 索引统计视图（如果存在）
    ]
    
    try:
        with engine.connect() as conn:
            # 首先检查哪些视图存在
            check_views_sql = text("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = ANY(:view_names);
            """)
            
            existing_views = conn.execute(check_views_sql, {'view_names': views_to_remove}).fetchall()
            existing_view_names = [row[0] for row in existing_views]
            
            if not existing_view_names:
                print("✅ 没有找到需要清理的视图")
                return
            
            print(f"📋 找到以下视图需要清理:")
            for view_name in existing_view_names:
                print(f"   - {view_name}")
            
            # 删除视图
            for view_name in existing_view_names:
                print(f"🗑️ 删除视图: {view_name}")
                
                # 先尝试删除普通视图
                try:
                    drop_sql = text(f"DROP VIEW IF EXISTS {view_name} CASCADE;")
                    conn.execute(drop_sql)
                    print(f"   ✅ 普通视图删除成功")
                except Exception as e:
                    # 如果是物化视图，尝试删除物化视图
                    try:
                        drop_mat_sql = text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                        conn.execute(drop_mat_sql)
                        print(f"   ✅ 物化视图删除成功")
                    except Exception as e2:
                        print(f"   ❌ 删除失败: {str(e2)}")
            
            conn.commit()
            print(f"✅ 视图清理完成")
            
            # 验证清理结果
            remaining_views = conn.execute(check_views_sql, {'view_names': views_to_remove}).fetchall()
            if remaining_views:
                print(f"⚠️ 仍有 {len(remaining_views)} 个视图未删除:")
                for row in remaining_views:
                    print(f"   - {row[0]}")
            else:
                print(f"✅ 所有目标视图已成功删除")
            
    except Exception as e:
        print(f"❌ 清理过程中出现错误: {str(e)}")

def check_remaining_views():
    """检查剩余的bbox相关视图"""
    print("\n🔍 检查剩余的bbox相关视图...")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    try:
        with engine.connect() as conn:
            check_sql = text("""
                SELECT 
                    table_name,
                    CASE 
                        WHEN table_name LIKE '%unified%' THEN '统一视图'
                        WHEN table_name LIKE '%overlap%' THEN '分析视图'
                        WHEN table_name LIKE '%qgis%' THEN 'QGIS视图'
                        ELSE '其他视图'
                    END as view_type
                FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%bbox%'
                ORDER BY view_type, table_name;
            """)
            
            remaining_views = conn.execute(check_sql).fetchall()
            
            if remaining_views:
                print(f"📋 剩余的bbox相关视图:")
                current_type = None
                for row in remaining_views:
                    if row.view_type != current_type:
                        current_type = row.view_type
                        print(f"\n{current_type}:")
                    print(f"   - {row.table_name}")
                
                # 检查是否有我们需要保留的视图
                needed_views = ['clips_bbox_unified']
                for view_name in needed_views:
                    exists = any(row.table_name == view_name for row in remaining_views)
                    if exists:
                        print(f"\n✅ 必需视图 {view_name} 存在")
                    else:
                        print(f"\n⚠️ 必需视图 {view_name} 不存在，需要创建")
                        print(f"   运行: python -m spdatalab create-unified-view")
            else:
                print("📭 没有找到bbox相关视图")
                print("⚠️ 需要创建统一视图: python -m spdatalab create-unified-view")
        
    except Exception as e:
        print(f"❌ 检查过程中出现错误: {str(e)}")

if __name__ == "__main__":
    print("🎯 BBox视图清理工具")
    print("=" * 40)
    
    cleanup_bbox_views()
    check_remaining_views()
    
    print("\n💡 接下来可以:")
    print("   1. 重新运行索引优化: python examples/dataset/bbox_examples/create_indexes.py --quick")
    print("   2. 确保统一视图存在: python -m spdatalab create-unified-view")
    print("   3. 测试overlap分析: python examples/dataset/bbox_examples/run_overlap_analysis.py --city A86 --intersect-only")
