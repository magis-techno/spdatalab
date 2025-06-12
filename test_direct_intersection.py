#!/usr/bin/env python3
"""
测试直接几何相交功能（无缓冲区）
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
    """测试直接几何相交"""
    print("🧪 测试直接几何相交（无缓冲区）")
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
        
        # 检查收费站保存
        with analyzer.local_engine.connect() as conn:
            toll_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {analyzer.config.toll_station_table} 
                WHERE analysis_id = '{analysis_id}'
            """)).scalar()
        print(f"   保存了 {toll_count} 个收费站")
        
        # 2. 直接几何相交分析
        print("2️⃣ 直接几何相交分析...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(analysis_id)
        print(f"   找到 {len(trajectory_results)} 个数据集-收费站组合")
        
        # 3. 检查结果
        print("3️⃣ 检查分析结果...")
        
        if not trajectory_results.empty:
            # 显示前几个结果
            print("   前3个结果:")
            for _, row in trajectory_results.head(3).iterrows():
                print(f"     {row['dataset_name']} @ 收费站{row['toll_station_id']}: {row['point_count']}点")
            
            # 检查保存到数据库的结果
            with analyzer.local_engine.connect() as conn:
                saved_count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {analyzer.config.trajectory_results_table} 
                    WHERE analysis_id = '{analysis_id}'
                """)).scalar()
                
                geom_count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {analyzer.config.trajectory_results_table} 
                    WHERE analysis_id = '{analysis_id}' AND geometry IS NOT NULL
                """)).scalar()
            
            print(f"   保存了 {saved_count} 个轨迹分析结果")
            print(f"   其中 {geom_count} 个有几何数据")
        else:
            print("   ⚠️ 没有找到相交的轨迹数据")
            print("   这可能是因为:")
            print("     - 收费站几何与轨迹数据不在同一区域")
            print("     - 收费站几何范围太小，没有轨迹点直接相交")
        
        # 4. 分析汇总
        print("4️⃣ 分析汇总...")
        summary = analyzer.get_analysis_summary(analysis_id)
        print(f"   收费站数量: {summary.get('total_toll_stations', 0)}")
        print(f"   数据集数量: {summary.get('unique_datasets', 0)}")
        print(f"   总轨迹点数: {summary.get('total_points', 0):,}")
        
        print(f"\n✅ 测试完成！分析ID: {analysis_id}")
        print("💡 使用直接几何相交，无缓冲区扩展")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 