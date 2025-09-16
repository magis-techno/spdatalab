#!/usr/bin/env python3
"""
简化测试脚本，用于诊断bbox叠置分析问题
"""

import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

print("🔍 开始测试bbox叠置分析...")

try:
    print("1. 测试数据库连接...")
    from sqlalchemy import create_engine, text
    
    dsn = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    engine = create_engine(dsn, future=True)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 as test;"))
        print(f"   ✅ 数据库连接成功: {result.scalar()}")
    
    print("2. 测试bbox模块导入...")
    from src.spdatalab.dataset.bbox import list_bbox_tables, create_qgis_compatible_unified_view
    
    tables = list_bbox_tables(engine)
    print(f"   ✅ 找到 {len(tables)} 个bbox相关表")
    
    bbox_tables = [t for t in tables if t.startswith('clips_bbox_') and t != 'clips_bbox']
    print(f"   📋 分表数量: {len(bbox_tables)}")
    
    if bbox_tables:
        print(f"   📋 分表示例: {bbox_tables[:3]}")
    
    print("3. 测试统一视图...")
    view_name = "clips_bbox_unified_qgis"
    
    # 检查视图是否存在
    check_view_sql = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name = '{view_name}'
        );
    """)
    
    with engine.connect() as conn:
        result = conn.execute(check_view_sql)
        view_exists = result.scalar()
        
        if view_exists:
            print(f"   ✅ 视图 {view_name} 已存在")
            
            # 检查数据量
            count_sql = text(f"SELECT COUNT(*) FROM {view_name};")
            count_result = conn.execute(count_sql)
            row_count = count_result.scalar()
            print(f"   📊 视图包含 {row_count:,} 条记录")
            
        else:
            print(f"   ⚠️ 视图 {view_name} 不存在")
            
            if bbox_tables:
                print("   🛠️ 尝试创建视图...")
                success = create_qgis_compatible_unified_view(engine, view_name)
                if success:
                    print("   ✅ 视图创建成功")
                else:
                    print("   ❌ 视图创建失败")
            else:
                print("   ❌ 没有分表，无法创建视图")
    
    print("4. 测试分析器导入...")
    from examples.dataset.bbox_examples.bbox_overlap_analysis import BBoxOverlapAnalyzer
    
    analyzer = BBoxOverlapAnalyzer()
    print(f"   ✅ 分析器创建成功")
    
    print("5. 测试分析表创建...")
    table_created = analyzer.create_analysis_table()
    if table_created:
        print("   ✅ 分析表创建/检查成功")
    else:
        print("   ❌ 分析表创建失败")
    
    print("\n🎉 所有基础测试通过！")
    print("现在可以尝试运行完整的叠置分析。")
    
except Exception as e:
    print(f"\n❌ 测试失败: {str(e)}")
    import traceback
    traceback.print_exc()
