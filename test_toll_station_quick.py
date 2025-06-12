#!/usr/bin/env python3
"""
快速测试收费站分析功能
"""

import sys
from pathlib import Path
from sqlalchemy import text

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.fusion.toll_station_analysis import TollStationAnalyzer, TollStationAnalysisConfig
    print("✅ 模块导入成功")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

def main():
    """快速测试"""
    print("🧪 收费站分析快速测试")
    print("-" * 40)
    
    try:
        # 先清理旧数据
        print("1️⃣ 清理旧数据...")
        analyzer = TollStationAnalyzer()
        
        with analyzer.local_engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {analyzer.config.toll_station_table} WHERE analysis_id LIKE 'test_%'"))
            conn.execute(text(f"DELETE FROM {analyzer.config.trajectory_results_table} WHERE analysis_id LIKE 'test_%'"))
            conn.commit()
        
        # 检查远程收费站几何类型
        print("2️⃣ 检查收费站几何类型...")
        check_sql = text("""
            SELECT 
                id,
                ST_GeometryType(wkb_geometry) as geom_type,
                ST_AsText(wkb_geometry) as geom_sample
            FROM full_intersection
            WHERE intersectiontype = 2
            AND wkb_geometry IS NOT NULL
            ORDER BY id
            LIMIT 3
        """)
        
        with analyzer.remote_engine.connect() as conn:
            geom_samples = conn.execute(check_sql).fetchall()
        
        for sample in geom_samples:
            print(f"   ID {sample[0]}: {sample[1]} - {sample[2][:80]}...")
        
        # 测试收费站查找和保存
        print("3️⃣ 测试收费站查找...")
        toll_stations_df, analysis_id = analyzer.find_toll_stations(limit=2)
        print(f"   找到 {len(toll_stations_df)} 个收费站")
        
        # 检查保存结果
        with analyzer.local_engine.connect() as conn:
            saved_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {analyzer.config.toll_station_table} 
                WHERE analysis_id = '{analysis_id}'
            """)).scalar()
            
            # 检查几何类型
            if saved_count > 0:
                geom_check = conn.execute(text(f"""
                    SELECT 
                        intersection_id,
                        ST_GeometryType(geometry) as saved_geom_type,
                        ST_AsText(geometry) as saved_geom
                    FROM {analyzer.config.toll_station_table} 
                    WHERE analysis_id = '{analysis_id}'
                    LIMIT 1
                """)).fetchone()
                
                if geom_check:
                    print(f"   保存了 {saved_count} 个收费站")
                    print(f"   几何类型: {geom_check[1]}")
                    print(f"   几何示例: {geom_check[2][:80]}...")
                else:
                    print(f"   保存了 {saved_count} 个收费站，但无几何数据")
            else:
                print("   ❌ 没有保存任何收费站")
        
        print(f"\n✅ 测试完成！分析ID: {analysis_id}")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 