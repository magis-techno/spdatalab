#!/usr/bin/env python3
"""
测试完整轨迹片段保存功能
验证当一个轨迹点落入收费站时，保存该轨迹的完整片段
"""

import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.fusion.toll_station_analysis import TollStationAnalyzer
    print("✅ 模块导入成功")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

def analyze_trajectory_completeness(analyzer, analysis_id: str):
    """分析轨迹完整性"""
    print("\n🔍 分析轨迹完整性...")
    
    # 1. 获取收费站数据
    with analyzer.local_engine.connect() as conn:
        toll_stations = pd.read_sql(text(f"""
            SELECT intersection_id, ST_AsText(geometry) as geometry_wkt
            FROM {analyzer.config.toll_station_table}
            WHERE analysis_id = '{analysis_id}'
        """), conn)
    
    if toll_stations.empty:
        print("❌ 没有找到收费站数据")
        return
    
    # 2. 对每个收费站分析轨迹完整性
    for _, toll_station in toll_stations.iterrows():
        toll_station_id = toll_station['intersection_id']
        geometry_wkt = toll_station['geometry_wkt']
        
        print(f"\n📊 收费站 {toll_station_id} 的轨迹分析:")
        
        # 2.1 获取该收费站的所有轨迹片段
        with analyzer.local_engine.connect() as conn:
            trajectory_results = pd.read_sql(text(f"""
                SELECT 
                    dataset_name,
                    scene_token,
                    point_count,
                    trajectory_geometry
                FROM {analyzer.config.trajectory_results_table}
                WHERE analysis_id = '{analysis_id}'
                AND toll_station_id = '{toll_station_id}'
            """), conn)
        
        if trajectory_results.empty:
            print(f"   ⚠️ 没有找到轨迹数据")
            continue
        
        print(f"   找到 {len(trajectory_results)} 个轨迹片段")
        
        # 2.2 分析每个轨迹片段
        for _, trajectory in trajectory_results.iterrows():
            dataset_name = trajectory['dataset_name']
            scene_token = trajectory['scene_token']
            point_count = trajectory['point_count']
            
            # 2.3 获取原始轨迹数据
            with analyzer.trajectory_engine.connect() as conn:
                original_trajectory = pd.read_sql(text(f"""
                    SELECT 
                        COUNT(*) as total_points,
                        COUNT(CASE WHEN ST_Intersects(
                            ST_SetSRID(ST_GeomFromText('{geometry_wkt}'), 4326),
                            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                        ) THEN 1 END) as intersecting_points
                    FROM {analyzer.config.trajectory_table}
                    WHERE dataset_name = '{dataset_name}'
                    AND scene_token = '{scene_token}'
                """), conn)
            
            if not original_trajectory.empty:
                total_points = original_trajectory['total_points'].iloc[0]
                intersecting_points = original_trajectory['intersecting_points'].iloc[0]
                
                print(f"\n   轨迹片段: {dataset_name} @ {scene_token}")
                print(f"   - 总点数: {total_points}")
                print(f"   - 收费站内点数: {intersecting_points}")
                print(f"   - 收费站外点数: {total_points - intersecting_points}")
                print(f"   - 保存的点数: {point_count}")
                
                # 验证完整性
                if point_count == total_points:
                    print("   ✅ 轨迹完整性验证通过")
                else:
                    print("   ⚠️ 轨迹完整性验证失败")
                    print(f"     保存的点数 ({point_count}) 与总点数 ({total_points}) 不匹配")

def main():
    """测试完整轨迹片段保存功能"""
    print("🧪 测试完整轨迹片段保存功能")
    print("-" * 40)
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 1. 查找收费站
        print("1️⃣ 查找收费站...")
        toll_stations_df, analysis_id = analyzer.find_toll_stations(limit=2)
        print(f"   找到 {len(toll_stations_df)} 个收费站")
        
        if toll_stations_df.empty:
            print("❌ 没有收费站数据，退出测试")
            return
        
        # 2. 分析轨迹
        print("\n2️⃣ 分析轨迹数据...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(analysis_id)
        print(f"   找到 {len(trajectory_results)} 个轨迹片段")
        
        # 3. 分析轨迹完整性
        analyze_trajectory_completeness(analyzer, analysis_id)
        
        print(f"\n✅ 测试完成！分析ID: {analysis_id}")
        print("💡 验证了完整轨迹片段的保存功能")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 