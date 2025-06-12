#!/usr/bin/env python3
"""
简化的收费站分析测试（包含几何数据）
"""

import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

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
    """简化的测试流程"""
    print("🧪 收费站分析简化测试")
    print("-" * 40)
    
    try:
        # 配置
        config = TollStationAnalysisConfig()
        config.buffer_distance_meters = 1000.0  # 1公里缓冲区
        analyzer = TollStationAnalyzer(config)
        
        # 1. 查找收费站
        print("1️⃣ 查找收费站...")
        toll_stations_df, analysis_id = analyzer.find_toll_stations(limit=2)
        print(f"   找到 {len(toll_stations_df)} 个收费站")
        
        # 检查收费站保存
        with analyzer.local_engine.connect() as conn:
            toll_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {analyzer.config.toll_station_table} 
                WHERE analysis_id = '{analysis_id}'
            """)).scalar()
        print(f"   保存了 {toll_count} 个收费站")
        
        # 2. 分析轨迹
        print("2️⃣ 分析轨迹...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(analysis_id)
        print(f"   找到 {len(trajectory_results)} 个数据集-收费站组合")
        
        # 检查轨迹保存
        with analyzer.local_engine.connect() as conn:
            traj_stats = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(geometry) as with_geom
                FROM {analyzer.config.trajectory_results_table} 
                WHERE analysis_id = '{analysis_id}'
            """)).fetchone()
        
        print(f"   保存了 {traj_stats[0]} 个轨迹分析结果")
        print(f"   其中 {traj_stats[1]} 个有几何数据")
        
        # 3. 检查几何质量
        print("3️⃣ 检查几何质量...")
        
        # 收费站几何
        with analyzer.local_engine.connect() as conn:
            toll_geom = conn.execute(text(f"""
                SELECT 
                    COUNT(geometry) as geom_count
                FROM {analyzer.config.toll_station_table}
                WHERE analysis_id = '{analysis_id}'
            """)).fetchone()
        
        print(f"   收费站: {toll_geom[0]}个几何")
        
        # 轨迹几何样本
        with analyzer.local_engine.connect() as conn:
            traj_sample = pd.read_sql(text(f"""
                SELECT 
                    dataset_name,
                    point_count,
                    CASE WHEN geometry IS NOT NULL THEN 'YES' ELSE 'NO' END as has_geom
                FROM {analyzer.config.trajectory_results_table}
                WHERE analysis_id = '{analysis_id}'
                ORDER BY point_count DESC
                LIMIT 3
            """), conn)
        
        print("   轨迹样本:")
        for _, row in traj_sample.iterrows():
            print(f"     {row['dataset_name']}: {row['point_count']}点, 几何={row['has_geom']}")
        
        # 4. 汇总
        summary = analyzer.get_analysis_summary(analysis_id)
        print("4️⃣ 分析汇总:")
        print(f"   收费站数量: {summary.get('total_toll_stations', 0)}")
        print(f"   数据集数量: {summary.get('unique_datasets', 0)}")
        print(f"   总轨迹点数: {summary.get('total_points', 0):,}")
        
        print(f"\n✅ 测试完成！分析ID: {analysis_id}")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 