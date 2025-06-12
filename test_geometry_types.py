#!/usr/bin/env python3
"""
测试几何类型表结构
"""

import sys
from pathlib import Path
from sqlalchemy import text

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.fusion.toll_station_analysis import TollStationAnalyzer
    print("✅ 模块导入成功")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

def main():
    """测试几何类型表结构"""
    print("🧪 测试几何类型表结构")
    print("-" * 40)
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 检查表结构
        print("1️⃣ 检查表结构...")
        
        # 检查收费站表结构
        toll_table_sql = text(f"""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns 
            WHERE table_name = '{analyzer.config.toll_station_table}'
            ORDER BY ordinal_position
        """)
        
        with analyzer.local_engine.connect() as conn:
            toll_columns = conn.execute(toll_table_sql).fetchall()
        
        print(f"   {analyzer.config.toll_station_table} 表结构:")
        for col in toll_columns:
            print(f"     {col[0]}: {col[1]} ({col[2]})")
        
        # 检查轨迹表结构
        traj_table_sql = text(f"""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns 
            WHERE table_name = '{analyzer.config.trajectory_results_table}'
            ORDER BY ordinal_position
        """)
        
        with analyzer.local_engine.connect() as conn:
            traj_columns = conn.execute(traj_table_sql).fetchall()
        
        print(f"   {analyzer.config.trajectory_results_table} 表结构:")
        for col in traj_columns:
            print(f"     {col[0]}: {col[1]} ({col[2]})")
        
        # 测试几何插入
        print("2️⃣ 测试几何数据插入...")
        
        # 测试收费站几何插入
        test_toll_sql = text(f"""
            INSERT INTO {analyzer.config.toll_station_table} 
            (analysis_id, intersection_id, intersectiontype, geometry)
            VALUES (
                'test_geom', 
                999999, 
                2, 
                ST_GeomFromText('POINT(116.3974 39.9093)', 4326)
            )
            ON CONFLICT (analysis_id, intersection_id) DO UPDATE SET
            geometry = EXCLUDED.geometry
        """)
        
        with analyzer.local_engine.connect() as conn:
            conn.execute(test_toll_sql)
            conn.commit()
        
        print("   ✅ 收费站几何插入成功")
        
        # 测试轨迹几何插入
        test_traj_sql = text(f"""
            INSERT INTO {analyzer.config.trajectory_results_table} 
            (analysis_id, toll_station_id, dataset_name, trajectory_count, point_count, geometry)
            VALUES (
                'test_geom', 
                999999, 
                'test_dataset', 
                10, 
                10,
                ST_GeomFromText('LINESTRING(116.3974 39.9093, 116.3975 39.9094, 116.3976 39.9095)', 4326)
            )
            ON CONFLICT (analysis_id, toll_station_id, dataset_name) DO UPDATE SET
            geometry = EXCLUDED.geometry
        """)
        
        with analyzer.local_engine.connect() as conn:
            conn.execute(test_traj_sql)
            conn.commit()
        
        print("   ✅ 轨迹几何插入成功")
        
        # 验证几何数据
        print("3️⃣ 验证几何数据...")
        
        verify_sql = text(f"""
            SELECT 
                'toll_station' as table_type,
                ST_AsText(geometry) as geom_text,
                ST_GeometryType(geometry) as geom_type
            FROM {analyzer.config.toll_station_table}
            WHERE analysis_id = 'test_geom'
            
            UNION ALL
            
            SELECT 
                'trajectory' as table_type,
                ST_AsText(geometry) as geom_text,
                ST_GeometryType(geometry) as geom_type
            FROM {analyzer.config.trajectory_results_table}
            WHERE analysis_id = 'test_geom'
        """)
        
        with analyzer.local_engine.connect() as conn:
            results = conn.execute(verify_sql).fetchall()
        
        for result in results:
            print(f"   {result[0]}: {result[2]} - {result[1][:50]}...")
        
        # 清理测试数据
        print("4️⃣ 清理测试数据...")
        
        cleanup_sqls = [
            text(f"DELETE FROM {analyzer.config.toll_station_table} WHERE analysis_id = 'test_geom'"),
            text(f"DELETE FROM {analyzer.config.trajectory_results_table} WHERE analysis_id = 'test_geom'")
        ]
        
        with analyzer.local_engine.connect() as conn:
            for sql in cleanup_sqls:
                conn.execute(sql)
            conn.commit()
        
        print("   ✅ 测试数据清理完成")
        
        print("\n✅ 几何类型表结构测试通过！")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 