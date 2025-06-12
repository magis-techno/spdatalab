#!/usr/bin/env python3
"""
修复收费站分析表结构和测试保存功能
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

def drop_and_recreate_tables():
    """删除并重新创建收费站分析表"""
    print("🔄 删除并重新创建收费站分析表...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 删除现有表
        drop_tables_sql = [
            text(f"DROP TABLE IF EXISTS {analyzer.config.toll_station_table} CASCADE"),
            text(f"DROP TABLE IF EXISTS {analyzer.config.trajectory_results_table} CASCADE")
        ]
        
        with analyzer.local_engine.connect() as conn:
            for drop_sql in drop_tables_sql:
                try:
                    conn.execute(drop_sql)
                    print(f"   删除表: {drop_sql}")
                except Exception as e:
                    print(f"   删除表失败: {e}")
            conn.commit()
        
        # 重新初始化表
        analyzer._init_analysis_tables()
        print("✅ 表重建完成")
        
        return analyzer
        
    except Exception as e:
        print(f"❌ 表重建失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_complete_analysis():
    """测试完整的分析流程"""
    print("\n🧪 测试完整的分析流程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 步骤1: 查找收费站（限制5个）
        print("1️⃣ 查找收费站...")
        toll_stations_df, analysis_id = analyzer.find_toll_stations(limit=5)
        
        if toll_stations_df.empty:
            print("❌ 没有找到收费站")
            return False
        
        print(f"✅ 找到 {len(toll_stations_df)} 个收费站，分析ID: {analysis_id}")
        
        # 检查保存结果
        check_toll_stations_sql = text(f"""
            SELECT COUNT(*) as count FROM {analyzer.config.toll_station_table} 
            WHERE analysis_id = '{analysis_id}'
        """)
        
        with analyzer.local_engine.connect() as conn:
            toll_count = conn.execute(check_toll_stations_sql).scalar()
        
        print(f"   保存的收费站数量: {toll_count}")
        
        if toll_count == 0:
            print("❌ 收费站数据没有保存成功")
            return False
        
        # 步骤2: 分析轨迹数据
        print("2️⃣ 分析轨迹数据...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(analysis_id, use_buffer=True)
        
        if trajectory_results.empty:
            print("⚠️ 没有找到轨迹数据")
        else:
            print(f"✅ 找到 {len(trajectory_results)} 个数据集-收费站组合")
        
        # 检查轨迹保存结果
        check_trajectories_sql = text(f"""
            SELECT COUNT(*) as count FROM {analyzer.config.trajectory_results_table} 
            WHERE analysis_id = '{analysis_id}'
        """)
        
        with analyzer.local_engine.connect() as conn:
            traj_count = conn.execute(check_trajectories_sql).scalar()
        
        print(f"   保存的轨迹分析结果数量: {traj_count}")
        
        # 步骤3: 获取分析汇总
        print("3️⃣ 获取分析汇总...")
        summary = analyzer.get_analysis_summary(analysis_id)
        print(f"   分析汇总: {summary}")
        
        return True
        
    except Exception as e:
        print(f"❌ 完整分析测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_save_process():
    """详细调试保存过程"""
    print("\n🔍 详细调试保存过程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 手动查询一个收费站进行测试
        toll_station_sql = text("""
            SELECT 
                id as intersection_id,
                intersectiontype,
                intersectionsubtype,
                ST_AsText(wkb_geometry) as intersection_geometry
            FROM full_intersection
            WHERE intersectiontype = 2
            AND wkb_geometry IS NOT NULL
            ORDER BY id
            LIMIT 1
        """)
        
        with analyzer.remote_engine.connect() as conn:
            test_station = pd.read_sql(toll_station_sql, conn)
        
        if test_station.empty:
            print("❌ 没有找到测试收费站")
            return False
        
        print(f"✅ 找到测试收费站: ID {test_station.iloc[0]['intersection_id']}")
        
        # 手动保存测试
        analysis_id = "debug_test_001"
        
        print("🔄 手动保存测试...")
        
        # 准备保存数据
        row = test_station.iloc[0]
        
        # 生成缓冲区
        buffered_geometry = None
        if row.get('intersection_geometry'):
            try:
                buffer_sql = text(f"""
                    SELECT ST_AsText(
                        ST_Buffer(
                            ST_GeomFromText('{row['intersection_geometry']}', 4326)::geography,
                            {analyzer.config.buffer_distance_meters}
                        )::geometry
                    ) as buffered_geom
                """)
                with analyzer.remote_engine.connect() as conn:
                    buffer_result = conn.execute(buffer_sql).fetchone()
                    if buffer_result and buffer_result[0]:
                        buffered_geometry = buffer_result[0]
                        print(f"✅ 缓冲区生成成功，长度: {len(buffered_geometry)}")
            except Exception as e:
                print(f"❌ 缓冲区生成失败: {e}")
        
        # 准备保存记录
        record = {
            'analysis_id': analysis_id,
            'intersection_id': int(row['intersection_id']),
            'intersectiontype': int(row['intersectiontype']) if pd.notna(row['intersectiontype']) else None,
            'intersectionsubtype': int(row['intersectionsubtype']) if pd.notna(row['intersectionsubtype']) else None,
            'intersection_geometry': row.get('intersection_geometry'),
            'buffered_geometry': buffered_geometry
        }
        
        print(f"📝 准备保存的记录: {record}")
        
        # 保存到数据库
        save_df = pd.DataFrame([record])
        
        try:
            with analyzer.local_engine.connect() as conn:
                save_df.to_sql(
                    analyzer.config.toll_station_table,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                conn.commit()
            print("✅ 手动保存成功")
            
            # 验证保存结果
            verify_sql = text(f"""
                SELECT * FROM {analyzer.config.toll_station_table} 
                WHERE analysis_id = '{analysis_id}'
            """)
            
            with analyzer.local_engine.connect() as conn:
                saved_data = pd.read_sql(verify_sql, conn)
            
            print(f"✅ 验证保存结果: {len(saved_data)} 条记录")
            if not saved_data.empty:
                print(f"   保存的数据: {saved_data.iloc[0].to_dict()}")
            
            return True
            
        except Exception as e:
            print(f"❌ 手动保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    except Exception as e:
        print(f"❌ 调试保存过程失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("🔧 收费站分析表修复和测试")
    print("=" * 60)
    
    # 步骤1: 重建表
    analyzer = drop_and_recreate_tables()
    if not analyzer:
        print("❌ 表重建失败，退出")
        return
    
    # 步骤2: 调试保存过程
    save_ok = debug_save_process()
    if not save_ok:
        print("❌ 保存过程有问题，退出")
        return
    
    # 步骤3: 测试完整流程
    analysis_ok = test_complete_analysis()
    
    # 总结
    print("\n" + "=" * 60)
    print("🔍 修复和测试总结:")
    print(f"   表重建: {'✅' if analyzer else '❌'}")
    print(f"   保存测试: {'✅' if save_ok else '❌'}")
    print(f"   完整分析: {'✅' if analysis_ok else '❌'}")
    
    if analyzer and save_ok and analysis_ok:
        print("\n🎉 所有测试通过！收费站分析功能已修复")
    else:
        print("\n⚠️ 仍有问题需要解决")

if __name__ == "__main__":
    main() 