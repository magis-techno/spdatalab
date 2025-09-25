#!/usr/bin/env python3
"""
BBox数据诊断脚本
===============

用于快速诊断bbox分表数据状态和统一视图问题

使用方法：
    python examples/dataset/bbox_examples/diagnose_bbox_data.py
"""

import sys
from pathlib import Path

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

def diagnose_bbox_data():
    """诊断bbox数据状态"""
    print("🔍 BBox数据诊断报告")
    print("=" * 60)
    
    eng = create_engine(LOCAL_DSN, future=True)
    
    try:
        # 1. 检查分表状态
        print("\n📋 1. 分表状态检查")
        all_tables = list_bbox_tables(eng)
        bbox_tables = [t for t in all_tables if t.startswith('clips_bbox_') and t != 'clips_bbox']
        
        print(f"   总计bbox相关表: {len(all_tables)}")
        print(f"   分表数量: {len(bbox_tables)}")
        
        if bbox_tables:
            print("   分表列表:")
            for table in bbox_tables[:10]:  # 只显示前10个
                print(f"     - {table}")
            if len(bbox_tables) > 10:
                print(f"     ... 还有 {len(bbox_tables) - 10} 个分表")
        else:
            print("   ❌ 没有发现bbox分表")
            return
        
        # 2. 检查数据量
        print("\n📊 2. 数据量统计")
        total_records = 0
        
        with eng.connect() as conn:
            for table in bbox_tables[:5]:  # 检查前5个表的数据量
                try:
                    count_sql = text(f"SELECT COUNT(*) FROM {table};")
                    count = conn.execute(count_sql).scalar()
                    total_records += count
                    print(f"   {table}: {count:,} 条记录")
                except Exception as e:
                    print(f"   {table}: 查询失败 - {str(e)}")
            
            if len(bbox_tables) > 5:
                print(f"   ... (仅显示前5个表的数据)")
        
        print(f"   样本总记录数: {total_records:,}")
        
        # 3. 检查统一视图状态
        print("\n🔍 3. 统一视图状态")
        view_name = "clips_bbox_unified"
        
        with eng.connect() as conn:
            # 检查视图是否存在
            check_view_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = '{view_name}'
                );
            """)
            
            view_exists = conn.execute(check_view_sql).scalar()
            
            if view_exists:
                print(f"   ✅ 统一视图 {view_name} 存在")
                
                try:
                    # 检查视图数据
                    view_count_sql = text(f"SELECT COUNT(*) FROM {view_name};")
                    view_count = conn.execute(view_count_sql).scalar()
                    print(f"   📊 视图记录数: {view_count:,}")
                    
                    # 检查城市分布
                    city_sql = text(f"""
                        SELECT 
                            city_id,
                            COUNT(*) as count,
                            COUNT(*) FILTER (WHERE all_good = true) as good_count
                        FROM {view_name} 
                        WHERE city_id IS NOT NULL
                        GROUP BY city_id
                        ORDER BY count DESC
                        LIMIT 5;
                    """)
                    city_results = conn.execute(city_sql).fetchall()
                    
                    if city_results:
                        print("   🏙️ TOP 5城市数据分布:")
                        for city in city_results:
                            print(f"     {city.city_id}: {city.count:,} 条记录 ({city.good_count:,} 优质)")
                    
                except Exception as e:
                    print(f"   ⚠️ 视图数据查询失败: {str(e)}")
                    print("   💡 建议重新创建统一视图")
                    
            else:
                print(f"   ❌ 统一视图 {view_name} 不存在")
                print("   💡 运行以下命令创建: python -m spdatalab create-unified-view")
        
        # 4. 提供建议
        print("\n💡 4. 诊断建议")
        
        if not bbox_tables:
            print("   ❌ 没有bbox分表，请先运行数据导入")
            print("   📝 命令: python -m spdatalab process-bbox --input your_dataset.json")
        elif not view_exists:
            print("   🔧 缺少统一视图，建议创建")
            print("   📝 命令: python -m spdatalab create-unified-view")
        elif total_records == 0:
            print("   ⚠️ 分表为空，检查数据导入是否完成")
        else:
            print("   ✅ 数据状态正常，可以进行overlap分析")
            print("   📝 测试命令: python examples/dataset/bbox_examples/bbox_overlap_analysis.py --suggest-city")
        
    except Exception as e:
        print(f"\n❌ 诊断过程中出现错误: {str(e)}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    diagnose_bbox_data()
