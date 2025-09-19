#!/usr/bin/env python3
"""
清理多余的统一视图，只保留基础视图

删除：
- clips_bbox_unified_qgis (QGIS兼容视图，多余)
- clips_bbox_unified_mat (物化视图，多余)

保留：
- clips_bbox_unified (基础视图，用于分析)
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.dataset.bbox import LOCAL_DSN
    from sqlalchemy import create_engine, text
    
    print("🧹 清理多余的统一视图")
    print("=" * 50)
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    # 要删除的视图
    views_to_remove = [
        ("clips_bbox_unified_qgis", "view"),
        ("clips_bbox_unified_mat", "materialized view")
    ]
    
    with engine.connect() as conn:
        print("\n📋 检查现有视图:")
        for view_name, view_type in views_to_remove:
            if view_type == "materialized view":
                check_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE matviewname = '{view_name}'
                        AND schemaname = 'public'
                    );
                """)
            else:
                check_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = '{view_name}'
                    );
                """)
            
            exists = conn.execute(check_sql).scalar()
            print(f"   {view_name} ({view_type}): {'存在' if exists else '不存在'}")
            
            if exists:
                try:
                    count_sql = text(f"SELECT COUNT(*) FROM {view_name};")
                    count = conn.execute(count_sql).scalar()
                    print(f"      记录数: {count:,}")
                except:
                    print(f"      记录数: 无法统计")
        
        # 检查基础视图
        print(f"\n📊 检查基础视图:")
        base_check_sql = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = 'clips_bbox_unified'
            );
        """)
        
        base_exists = conn.execute(base_check_sql).scalar()
        print(f"   clips_bbox_unified (基础视图): {'存在' if base_exists else '不存在'}")
        
        if not base_exists:
            print("⚠️ 基础视图不存在，需要先创建基础视图")
            return
        
        print(f"\n🗑️ 开始删除多余视图:")
        for view_name, view_type in views_to_remove:
            try:
                if view_type == "materialized view":
                    drop_sql = text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
                else:
                    drop_sql = text(f"DROP VIEW IF EXISTS {view_name};")
                
                conn.execute(drop_sql)
                print(f"✅ 删除 {view_name}")
            except Exception as e:
                print(f"⚠️ 删除 {view_name} 失败: {str(e)}")
        
        conn.commit()
        
        print(f"\n✅ 清理完成!")
        print(f"💡 现在只保留 clips_bbox_unified 基础视图")
        print(f"💡 分析性能应该显著提升")

except Exception as e:
    print(f"❌ 清理失败: {e}")
    import traceback
    traceback.print_exc()
