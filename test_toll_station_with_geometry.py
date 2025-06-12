#!/usr/bin/env python3
"""
测试收费站分析功能（包含几何数据）
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
    print("✅ 成功导入收费站分析模块")
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    sys.exit(1)

def test_with_geometry():
    """测试包含几何数据的完整分析流程"""
    print("🧪 测试包含几何数据的收费站分析...")
    
    try:
        # 使用更大的缓冲区距离
        config = TollStationAnalysisConfig()
        config.buffer_distance_meters = 1000.0  # 1公里缓冲区
        
        analyzer = TollStationAnalyzer(config)
        
        # 步骤1: 查找收费站
        print("1️⃣ 查找收费站...")
        toll_stations_df, analysis_id = analyzer.find_toll_stations(limit=3)
        
        if toll_stations_df.empty:
            print("❌ 没有找到收费站")
            return False
        
        print(f"✅ 找到 {len(toll_stations_df)} 个收费站，分析ID: {analysis_id}")
        
        # 检查收费站保存结果
        check_toll_sql = text(f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(intersection_geometry) as geom_count,
                COUNT(buffered_geometry) as buffer_count
            FROM {analyzer.config.toll_station_table} 
            WHERE analysis_id = '{analysis_id}'
        """)
        
        with analyzer.local_engine.connect() as conn:
            toll_stats = conn.execute(check_toll_sql).fetchone()
        
        print(f"   保存的收费站: {toll_stats[0]} 个")
        print(f"   有原始几何: {toll_stats[1]} 个")
        print(f"   有缓冲几何: {toll_stats[2]} 个")
        
        if toll_stats[0] == 0:
            print("❌ 收费站数据没有保存")
            return False
        
        # 步骤2: 分析轨迹数据
        print("2️⃣ 分析轨迹数据...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(analysis_id, use_buffer=True)
        
        if trajectory_results.empty:
            print("⚠️ 没有找到轨迹数据")
        else:
            print(f"✅ 找到 {len(trajectory_results)} 个数据集-收费站组合")
            
            # 显示轨迹结果详情
            for _, row in trajectory_results.head(3).iterrows():
                print(f"   数据集: {row['dataset_name']}")
                print(f"     轨迹点数: {row['point_count']}")
                print(f"     几何长度: {len(row.get('trajectory_geometry', '')) if row.get('trajectory_geometry') else 0}")
        
        # 检查轨迹保存结果
        check_traj_sql = text(f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(trajectory_geometry) as geom_count,
                AVG(point_count) as avg_points
            FROM {analyzer.config.trajectory_results_table} 
            WHERE analysis_id = '{analysis_id}'
        """)
        
        with analyzer.local_engine.connect() as conn:
            traj_stats = conn.execute(check_traj_sql).fetchone()
        
        print(f"   保存的轨迹分析: {traj_stats[0]} 个")
        print(f"   有轨迹几何: {traj_stats[1]} 个")
        print(f"   平均轨迹点数: {traj_stats[2]:.1f}" if traj_stats[2] else "N/A")
        
        # 步骤3: 导出QGIS视图
        print("3️⃣ 导出QGIS视图...")
        export_info = analyzer.export_results_for_qgis(analysis_id)
        
        for view_type, view_name in export_info.items():
            print(f"   {view_type}: {view_name}")
        
        # 步骤4: 获取分析汇总
        print("4️⃣ 获取分析汇总...")
        summary = analyzer.get_analysis_summary(analysis_id)
        
        print("   分析汇总:")
        for key, value in summary.items():
            if key != 'analysis_time':
                print(f"     {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_geometry_quality():
    """检查几何数据质量"""
    print("\n🔍 检查几何数据质量...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 检查最近的分析结果
        recent_analysis_sql = text(f"""
            SELECT DISTINCT analysis_id 
            FROM {analyzer.config.toll_station_table} 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        with analyzer.local_engine.connect() as conn:
            recent_result = conn.execute(recent_analysis_sql).fetchone()
        
        if not recent_result:
            print("⚠️ 没有找到分析结果")
            return False
        
        analysis_id = recent_result[0]
        print(f"📊 检查分析ID: {analysis_id}")
        
        # 检查收费站几何
        toll_geom_sql = text(f"""
            SELECT 
                intersection_id,
                CASE WHEN intersection_geometry IS NOT NULL THEN 'YES' ELSE 'NO' END as has_orig_geom,
                CASE WHEN buffered_geometry IS NOT NULL THEN 'YES' ELSE 'NO' END as has_buffer_geom,
                CASE WHEN buffered_geometry IS NOT NULL THEN LENGTH(buffered_geometry) ELSE 0 END as buffer_length
            FROM {analyzer.config.toll_station_table}
            WHERE analysis_id = '{analysis_id}'
            ORDER BY intersection_id
        """)
        
        with analyzer.local_engine.connect() as conn:
            toll_geom_df = pd.read_sql(toll_geom_sql, conn)
        
        print("📍 收费站几何检查:")
        for _, row in toll_geom_df.iterrows():
            print(f"   收费站 {row['intersection_id']}: 原始几何={row['has_orig_geom']}, 缓冲几何={row['has_buffer_geom']} (长度:{row['buffer_length']})")
        
        # 检查轨迹几何
        traj_geom_sql = text(f"""
            SELECT 
                dataset_name,
                toll_station_id,
                point_count,
                CASE WHEN trajectory_geometry IS NOT NULL THEN 'YES' ELSE 'NO' END as has_traj_geom,
                CASE WHEN trajectory_geometry IS NOT NULL THEN LENGTH(trajectory_geometry) ELSE 0 END as traj_length
            FROM {analyzer.config.trajectory_results_table}
            WHERE analysis_id = '{analysis_id}'
            ORDER BY point_count DESC
            LIMIT 5
        """)
        
        with analyzer.local_engine.connect() as conn:
            traj_geom_df = pd.read_sql(traj_geom_sql, conn)
        
        print("🚗 轨迹几何检查（前5个）:")
        for _, row in traj_geom_df.iterrows():
            print(f"   {row['dataset_name']} @ 收费站{row['toll_station_id']}: {row['point_count']}点, 几何={row['has_traj_geom']} (长度:{row['traj_length']})")
        
        return True
        
    except Exception as e:
        print(f"❌ 几何质量检查失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("🧪 收费站分析几何数据测试")
    print("=" * 60)
    
    # 测试完整流程
    test_ok = test_with_geometry()
    
    # 检查几何质量
    geom_ok = check_geometry_quality()
    
    # 总结
    print("\n" + "=" * 60)
    print("🔍 测试总结:")
    print(f"   完整流程测试: {'✅' if test_ok else '❌'}")
    print(f"   几何质量检查: {'✅' if geom_ok else '❌'}")
    
    if test_ok and geom_ok:
        print("\n🎉 所有测试通过！收费站分析功能（含几何）正常工作")
        print("\n💡 使用建议:")
        print("   - 使用 make clean-toll-station 清理分析表")
        print("   - 使用 spdatalab analyze-toll-stations 运行分析")
        print("   - 在QGIS中连接本地数据库查看几何结果")
    else:
        print("\n⚠️ 仍有问题需要解决")

if __name__ == "__main__":
    main() 