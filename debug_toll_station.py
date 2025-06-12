#!/usr/bin/env python3
"""
收费站分析调试脚本

逐步检查每个环节，找出问题所在
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

def debug_find_toll_stations():
    """调试收费站查找过程"""
    print("🔍 调试收费站查找过程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 直接查询收费站（不保存）
        toll_station_sql = text("""
            SELECT 
                id as intersection_id,
                intersectiontype,
                intersectionsubtype,
                ST_AsText(wkb_geometry) as intersection_geometry,
                ST_IsValid(wkb_geometry) as is_valid_geom,
                ST_GeometryType(wkb_geometry) as geom_type
            FROM full_intersection
            WHERE intersectiontype = 2
            AND wkb_geometry IS NOT NULL
            ORDER BY id
            LIMIT 5
        """)
        
        with analyzer.remote_engine.connect() as conn:
            toll_stations_df = pd.read_sql(toll_station_sql, conn)
        
        if toll_stations_df.empty:
            print("❌ 没有找到收费站数据")
            return None
        
        print(f"✅ 找到 {len(toll_stations_df)} 个收费站")
        
        # 检查每个收费站的几何信息
        for i, row in toll_stations_df.iterrows():
            print(f"\n📍 收费站 {i+1}:")
            print(f"   ID: {row['intersection_id']}")
            print(f"   类型: {row['intersectiontype']}")
            print(f"   子类型: {row['intersectionsubtype']}")
            print(f"   几何有效: {row['is_valid_geom']}")
            print(f"   几何类型: {row['geom_type']}")
            
            geom = row['intersection_geometry']
            if geom:
                print(f"   几何长度: {len(geom)} 字符")
                print(f"   几何预览: {geom[:100]}...")
            else:
                print(f"   ❌ 几何为空")
        
        return toll_stations_df
        
    except Exception as e:
        print(f"❌ 查找收费站失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def debug_buffer_generation(toll_stations_df):
    """调试缓冲区生成过程"""
    if toll_stations_df is None or toll_stations_df.empty:
        print("⚠️ 跳过缓冲区测试（没有收费站数据）")
        return None
    
    print("\n🔄 调试缓冲区生成过程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 测试第一个收费站的缓冲区生成
        first_station = toll_stations_df.iloc[0]
        geometry_wkt = first_station['intersection_geometry']
        
        if not geometry_wkt:
            print("❌ 第一个收费站没有几何数据")
            return None
        
        print(f"🎯 测试收费站ID: {first_station['intersection_id']}")
        
        # 测试缓冲区生成
        buffer_sql = text(f"""
            SELECT 
                ST_AsText(
                    ST_Buffer(
                        ST_GeomFromText('{geometry_wkt}', 4326)::geography,
                        {analyzer.config.buffer_distance_meters}
                    )::geometry
                ) as buffered_geom,
                ST_Area(
                    ST_Buffer(
                        ST_GeomFromText('{geometry_wkt}', 4326)::geography,
                        {analyzer.config.buffer_distance_meters}
                    )
                ) as buffer_area
        """)
        
        with analyzer.remote_engine.connect() as conn:
            buffer_result = conn.execute(buffer_sql).fetchone()
        
        if buffer_result and buffer_result[0]:
            buffered_geom = buffer_result[0]
            buffer_area = buffer_result[1]
            print(f"✅ 缓冲区生成成功")
            print(f"   缓冲区面积: {buffer_area:.2f} 平方米")
            print(f"   缓冲区几何长度: {len(buffered_geom)} 字符")
            print(f"   缓冲区几何预览: {buffered_geom[:100]}...")
            return buffered_geom
        else:
            print("❌ 缓冲区生成失败")
            return None
            
    except Exception as e:
        print(f"❌ 缓冲区生成过程失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def debug_trajectory_query(buffered_geom):
    """调试轨迹查询过程"""
    if not buffered_geom:
        print("⚠️ 跳过轨迹查询测试（没有缓冲区几何）")
        return None
    
    print("\n🚗 调试轨迹查询过程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 首先检查轨迹表基本信息
        print("📊 检查轨迹表基本信息...")
        
        basic_info_sql = text("""
            SELECT 
                COUNT(*) as total_count,
                COUNT(point_lla) as geom_count,
                COUNT(DISTINCT dataset_name) as dataset_count,
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM public.ddi_data_points
            LIMIT 1
        """)
        
        with analyzer.remote_engine.connect() as conn:
            basic_info = conn.execute(basic_info_sql).fetchone()
        
        if basic_info:
            print(f"   总记录数: {basic_info[0]:,}")
            print(f"   有几何的记录: {basic_info[1]:,}")
            print(f"   数据集数量: {basic_info[2]:,}")
            print(f"   时间范围: {basic_info[3]} - {basic_info[4]}")
        
        # 测试空间相交查询
        print("\n🎯 测试空间相交查询...")
        
        # 先测试一个简单的相交查询
        simple_intersect_sql = text(f"""
            SELECT COUNT(*) as intersect_count
            FROM public.ddi_data_points 
            WHERE ST_Intersects(
                point_lla, 
                ST_GeomFromText('{buffered_geom}', 4326)
            )
        """)
        
        with analyzer.remote_engine.connect() as conn:
            intersect_count = conn.execute(simple_intersect_sql).scalar()
        
        print(f"   相交的轨迹点数: {intersect_count:,}")
        
        if intersect_count > 0:
            print("✅ 找到了相交的轨迹数据！")
            
            # 获取详细的数据集信息
            detailed_sql = text(f"""
                SELECT 
                    dataset_name,
                    COUNT(*) as trajectory_count,
                    COUNT(*) as point_count,
                    MIN(timestamp) as min_timestamp,
                    MAX(timestamp) as max_timestamp,
                    COUNT(CASE WHEN workstage = 2 THEN 1 END) as workstage_2_count
                FROM public.ddi_data_points 
                WHERE ST_Intersects(
                    point_lla, 
                    ST_GeomFromText('{buffered_geom}', 4326)
                )
                GROUP BY dataset_name
                ORDER BY trajectory_count DESC
                LIMIT 10
            """)
            
            with analyzer.remote_engine.connect() as conn:
                trajectory_results = pd.read_sql(detailed_sql, conn)
            
            print(f"\n📋 找到的数据集:")
            for _, row in trajectory_results.iterrows():
                print(f"   {row['dataset_name']}: {row['trajectory_count']} 个轨迹点")
            
            return trajectory_results
        else:
            print("⚠️ 没有找到相交的轨迹数据")
            
            # 检查可能的原因
            print("\n🔍 检查可能的原因...")
            
            # 检查轨迹数据的空间范围
            bbox_sql = text("""
                SELECT 
                    ST_XMin(ST_Extent(point_lla)) as min_x,
                    ST_YMin(ST_Extent(point_lla)) as min_y,
                    ST_XMax(ST_Extent(point_lla)) as max_x,
                    ST_YMax(ST_Extent(point_lla)) as max_y
                FROM public.ddi_data_points
                WHERE point_lla IS NOT NULL
            """)
            
            with analyzer.remote_engine.connect() as conn:
                bbox_result = conn.execute(bbox_sql).fetchone()
            
            if bbox_result:
                print(f"   轨迹数据空间范围:")
                print(f"   X: {bbox_result[0]:.6f} - {bbox_result[2]:.6f}")
                print(f"   Y: {bbox_result[1]:.6f} - {bbox_result[3]:.6f}")
            
            # 检查收费站和轨迹数据的坐标系统是否一致
            crs_sql = text(f"""
                SELECT 
                    ST_SRID(ST_GeomFromText('{buffered_geom}', 4326)) as buffer_srid,
                    ST_SRID(point_lla) as point_srid
                FROM public.ddi_data_points
                WHERE point_lla IS NOT NULL
                LIMIT 1
            """)
            
            with analyzer.remote_engine.connect() as conn:
                crs_result = conn.execute(crs_sql).fetchone()
            
            if crs_result:
                print(f"   坐标系统:")
                print(f"   缓冲区SRID: {crs_result[0]}")
                print(f"   轨迹点SRID: {crs_result[1]}")
            
            return None
            
    except Exception as e:
        print(f"❌ 轨迹查询过程失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def debug_data_saving():
    """调试数据保存过程"""
    print("\n💾 调试数据保存过程...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 检查本地数据库表
        check_tables_sql = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name IN ('toll_station_analysis', 'toll_station_trajectories')
            AND table_schema = 'public'
        """)
        
        with analyzer.local_engine.connect() as conn:
            existing_tables = conn.execute(check_tables_sql).fetchall()
        
        table_names = [row[0] for row in existing_tables]
        print(f"✅ 存在的表: {table_names}")
        
        # 检查表内容
        for table_name in table_names:
            count_sql = text(f"SELECT COUNT(*) FROM {table_name}")
            with analyzer.local_engine.connect() as conn:
                count = conn.execute(count_sql).scalar()
            print(f"   {table_name}: {count} 条记录")
            
            if count > 0:
                # 显示最近的几条记录
                sample_sql = text(f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT 3")
                with analyzer.local_engine.connect() as conn:
                    sample_data = pd.read_sql(sample_sql, conn)
                print(f"   最近的记录:")
                for _, row in sample_data.iterrows():
                    print(f"     分析ID: {row.get('analysis_id', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据保存检查失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主调试函数"""
    print("🐛 收费站分析详细调试")
    print("=" * 60)
    
    # 步骤1: 调试收费站查找
    toll_stations_df = debug_find_toll_stations()
    
    # 步骤2: 调试缓冲区生成
    buffered_geom = debug_buffer_generation(toll_stations_df)
    
    # 步骤3: 调试轨迹查询
    trajectory_results = debug_trajectory_query(buffered_geom)
    
    # 步骤4: 调试数据保存
    save_ok = debug_data_saving()
    
    # 总结调试结果
    print("\n" + "=" * 60)
    print("🔍 调试总结:")
    print(f"   收费站查找: {'✅' if toll_stations_df is not None and not toll_stations_df.empty else '❌'}")
    print(f"   缓冲区生成: {'✅' if buffered_geom else '❌'}")
    print(f"   轨迹查询: {'✅' if trajectory_results is not None and not trajectory_results.empty else '❌'}")
    print(f"   数据保存: {'✅' if save_ok else '❌'}")
    
    if trajectory_results is not None and not trajectory_results.empty:
        print("\n🎉 核心功能正常！问题可能在数据保存环节")
        print("💡 建议: 运行一次完整的分析，并检查日志输出")
    elif buffered_geom:
        print("\n⚠️ 缓冲区生成正常，但没有找到相交的轨迹数据")
        print("💡 可能的原因:")
        print("   - 收费站位置与轨迹数据不在同一区域")
        print("   - 坐标系统不匹配")
        print("   - 缓冲区距离太小")
    else:
        print("\n❌ 基础功能有问题")
        print("💡 需要检查数据库配置和数据完整性")

if __name__ == "__main__":
    main() 