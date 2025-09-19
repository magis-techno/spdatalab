#!/usr/bin/env python3
"""
检查数据库中的视图和表状态
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))

from sqlalchemy import create_engine, text
from src.spdatalab.common.db import LOCAL_DSN

def check_database_objects():
    """检查数据库中的对象状态"""
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        print("🔍 检查数据库对象状态")
        print("=" * 50)
        
        # 1. 检查所有bbox相关的表
        bbox_tables_sql = text("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_name LIKE '%bbox%' 
            AND table_schema = 'public'
            ORDER BY table_name;
        """)
        
        print("\n📊 BBox相关的表:")
        bbox_tables = conn.execute(bbox_tables_sql).fetchall()
        for table_name, table_type in bbox_tables:
            print(f"   {table_name} ({table_type})")
        
        # 2. 检查所有视图
        views_sql = text("""
            SELECT table_name as view_name
            FROM information_schema.views 
            WHERE table_schema = 'public'
            AND table_name LIKE '%bbox%'
            ORDER BY table_name;
        """)
        
        print("\n👁️ BBox相关的视图:")
        views = conn.execute(views_sql).fetchall()
        for (view_name,) in views:
            print(f"   {view_name} (VIEW)")
            
        # 3. 检查分析结果相关对象
        analysis_sql = text("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE (table_name LIKE '%overlap%' OR table_name LIKE '%qgis%')
            AND table_schema = 'public'
            ORDER BY table_name;
        """)
        
        print("\n📈 分析结果相关对象:")
        analysis_objects = conn.execute(analysis_sql).fetchall()
        for table_name, table_type in analysis_objects:
            print(f"   {table_name} ({table_type})")
            
        # 4. 检查特定对象的存在性
        print("\n🎯 关键对象检查:")
        
        # 检查clips_bbox_unified
        check_unified_sql = text("""
            SELECT 
                CASE 
                    WHEN EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = 'clips_bbox_unified') THEN 'VIEW'
                    WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'clips_bbox_unified') THEN 'TABLE'
                    ELSE 'NOT_EXISTS'
                END as object_type;
        """)
        
        unified_status = conn.execute(check_unified_sql).scalar()
        print(f"   clips_bbox_unified: {unified_status}")
        
        # 检查qgis_bbox_overlap_hotspots
        check_qgis_sql = text("""
            SELECT 
                CASE 
                    WHEN EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = 'qgis_bbox_overlap_hotspots') THEN 'VIEW'
                    WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'qgis_bbox_overlap_hotspots') THEN 'TABLE'
                    ELSE 'NOT_EXISTS'
                END as object_type;
        """)
        
        qgis_status = conn.execute(check_qgis_sql).scalar()
        print(f"   qgis_bbox_overlap_hotspots: {qgis_status}")
        
        # 5. 如果视图存在，检查记录数
        if unified_status == 'VIEW':
            try:
                count_sql = text("SELECT COUNT(*) FROM clips_bbox_unified;")
                count = conn.execute(count_sql).scalar()
                print(f"   clips_bbox_unified 记录数: {count:,}")
            except Exception as e:
                print(f"   clips_bbox_unified 查询失败: {str(e)[:100]}")
                
        if qgis_status in ['VIEW', 'TABLE']:
            try:
                count_sql = text("SELECT COUNT(*) FROM qgis_bbox_overlap_hotspots;")
                count = conn.execute(count_sql).scalar()
                print(f"   qgis_bbox_overlap_hotspots 记录数: {count:,}")
            except Exception as e:
                print(f"   qgis_bbox_overlap_hotspots 查询失败: {str(e)[:100]}")

if __name__ == "__main__":
    try:
        check_database_objects()
    except Exception as e:
        print(f"❌ 检查失败: {str(e)}")
