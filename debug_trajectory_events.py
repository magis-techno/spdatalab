#!/usr/bin/env python3
"""
轨迹事件数据诊断脚本
用于检查事件点的几何位置和字段数据质量
"""

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import argparse
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def analyze_trajectory_events(trajectory_table: str, events_table: str) -> dict:
    """分析轨迹事件数据质量
    
    Args:
        trajectory_table: 轨迹表名
        events_table: 事件表名
        
    Returns:
        分析结果字典
    """
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        results = {
            'trajectory_stats': {},
            'events_stats': {},
            'geometry_analysis': {},
            'data_quality': {}
        }
        
        # 1. 分析轨迹表
        print("\n=== 分析轨迹表 ===")
        traj_sql = text(f"""
            SELECT 
                COUNT(*) as total_trajectories,
                AVG(ST_Length(geometry::geography)) as avg_length_meters,
                MIN(ST_Length(geometry::geography)) as min_length_meters,
                MAX(ST_Length(geometry::geography)) as max_length_meters,
                COUNT(CASE WHEN avg_speed IS NOT NULL THEN 1 END) as trajectories_with_speed,
                COUNT(CASE WHEN avp_ratio IS NOT NULL THEN 1 END) as trajectories_with_avp
            FROM {trajectory_table}
        """)
        
        with eng.connect() as conn:
            result = conn.execute(traj_sql).fetchone()
            results['trajectory_stats'] = dict(result._mapping)
        
        # 2. 分析事件表
        print("\n=== 分析事件表 ===")
        events_sql = text(f"""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT scene_id) as scenes_with_events,
                COUNT(CASE WHEN event_type = 'avp_change' THEN 1 END) as avp_changes,
                COUNT(CASE WHEN event_type = 'speed_spike' THEN 1 END) as speed_spikes,
                COUNT(CASE WHEN from_value IS NOT NULL THEN 1 END) as events_with_from_value,
                COUNT(CASE WHEN to_value IS NOT NULL THEN 1 END) as events_with_to_value,
                COUNT(CASE WHEN speed_value IS NOT NULL THEN 1 END) as events_with_speed_value
            FROM {events_table}
        """)
        
        with eng.connect() as conn:
            result = conn.execute(events_sql).fetchone()
            results['events_stats'] = dict(result._mapping)
        
        # 3. 几何分析：检查事件点是否在轨迹上
        print("\n=== 几何分析 ===")
        geom_sql = text(f"""
            SELECT 
                e.scene_id,
                e.event_type,
                e.from_value,
                e.to_value,
                e.speed_value,
                ST_X(e.geometry) as event_lon,
                ST_Y(e.geometry) as event_lat,
                ST_X(ST_StartPoint(t.geometry)) as traj_start_lon,
                ST_Y(ST_StartPoint(t.geometry)) as traj_start_lat,
                ST_X(ST_EndPoint(t.geometry)) as traj_end_lon,
                ST_Y(ST_EndPoint(t.geometry)) as traj_end_lat,
                ST_Distance(e.geometry, t.geometry) as distance_to_trajectory
            FROM {events_table} e
            JOIN {trajectory_table} t ON e.scene_id = t.scene_id
            LIMIT 10
        """)
        
        with eng.connect() as conn:
            geom_df = pd.read_sql(geom_sql, conn)
            results['geometry_analysis'] = geom_df.to_dict('records')
        
        # 4. 数据质量检查
        print("\n=== 数据质量检查 ===")
        quality_sql = text(f"""
            SELECT 
                'avp_change' as event_type,
                COUNT(*) as total_count,
                COUNT(CASE WHEN from_value IS NULL THEN 1 END) as missing_from_value,
                COUNT(CASE WHEN to_value IS NULL THEN 1 END) as missing_to_value,
                COUNT(CASE WHEN from_value = to_value THEN 1 END) as invalid_changes
            FROM {events_table}
            WHERE event_type = 'avp_change'
            
            UNION ALL
            
            SELECT 
                'speed_spike' as event_type,
                COUNT(*) as total_count,
                COUNT(CASE WHEN speed_value IS NULL THEN 1 END) as missing_speed_value,
                COUNT(CASE WHEN speed_mean IS NULL THEN 1 END) as missing_speed_mean,
                COUNT(CASE WHEN z_score IS NULL THEN 1 END) as missing_z_score
            FROM {events_table}
            WHERE event_type = 'speed_spike'
        """)
        
        with eng.connect() as conn:
            quality_df = pd.read_sql(quality_sql, conn)
            results['data_quality'] = quality_df.to_dict('records')
        
        return results
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}")
        return {}

def print_analysis_results(results: dict):
    """打印分析结果"""
    print("\n" + "="*50)
    print("轨迹事件数据分析报告")
    print("="*50)
    
    # 轨迹统计
    if 'trajectory_stats' in results:
        print("\n【轨迹表统计】")
        stats = results['trajectory_stats']
        print(f"总轨迹数: {stats.get('total_trajectories', 0)}")
        print(f"平均轨迹长度: {stats.get('avg_length_meters', 0):.2f} 米")
        print(f"最短轨迹长度: {stats.get('min_length_meters', 0):.2f} 米")
        print(f"最长轨迹长度: {stats.get('max_length_meters', 0):.2f} 米")
        print(f"包含速度信息的轨迹: {stats.get('trajectories_with_speed', 0)}")
        print(f"包含AVP信息的轨迹: {stats.get('trajectories_with_avp', 0)}")
    
    # 事件统计
    if 'events_stats' in results:
        print("\n【事件表统计】")
        stats = results['events_stats']
        print(f"总事件数: {stats.get('total_events', 0)}")
        print(f"包含事件的场景数: {stats.get('scenes_with_events', 0)}")
        print(f"AVP变化事件: {stats.get('avp_changes', 0)}")
        print(f"速度突变事件: {stats.get('speed_spikes', 0)}")
        print(f"包含from_value的事件: {stats.get('events_with_from_value', 0)}")
        print(f"包含to_value的事件: {stats.get('events_with_to_value', 0)}")
        print(f"包含speed_value的事件: {stats.get('events_with_speed_value', 0)}")
    
    # 几何分析
    if 'geometry_analysis' in results:
        print("\n【几何分析样本】")
        geom_data = results['geometry_analysis']
        if geom_data:
            print("前10个事件的几何位置分析:")
            for i, event in enumerate(geom_data[:5]):  # 只显示前5个
                print(f"  事件{i+1}: {event['event_type']}")
                print(f"    事件坐标: ({event['event_lon']:.6f}, {event['event_lat']:.6f})")
                print(f"    轨迹起点: ({event['traj_start_lon']:.6f}, {event['traj_start_lat']:.6f})")
                print(f"    轨迹终点: ({event['traj_end_lon']:.6f}, {event['traj_end_lat']:.6f})")
                print(f"    到轨迹距离: {event['distance_to_trajectory']:.6f} 度")
                if event['event_type'] == 'avp_change':
                    print(f"    AVP变化: {event['from_value']} -> {event['to_value']}")
                elif event['event_type'] == 'speed_spike':
                    print(f"    速度值: {event['speed_value']}")
                print()
    
    # 数据质量
    if 'data_quality' in results:
        print("\n【数据质量分析】")
        quality_data = results['data_quality']
        for item in quality_data:
            print(f"\n{item['event_type']} 事件质量:")
            print(f"  总数: {item['total_count']}")
            if item['event_type'] == 'avp_change':
                print(f"  缺少from_value: {item.get('missing_from_value', 0)}")
                print(f"  缺少to_value: {item.get('missing_to_value', 0)}")
                print(f"  无效变化(from=to): {item.get('invalid_changes', 0)}")
            elif item['event_type'] == 'speed_spike':
                print(f"  缺少speed_value: {item.get('missing_speed_value', 0)}")
                print(f"  缺少speed_mean: {item.get('missing_speed_mean', 0)}")
                print(f"  缺少z_score: {item.get('missing_z_score', 0)}")

def main():
    parser = argparse.ArgumentParser(description='轨迹事件数据诊断工具')
    parser.add_argument('--trajectory-table', required=True, help='轨迹表名')
    parser.add_argument('--events-table', required=True, help='事件表名')
    
    args = parser.parse_args()
    
    print(f"开始分析轨迹表: {args.trajectory_table}")
    print(f"开始分析事件表: {args.events_table}")
    
    results = analyze_trajectory_events(args.trajectory_table, args.events_table)
    
    if results:
        print_analysis_results(results)
    else:
        print("分析失败，请检查表名和数据库连接")

if __name__ == '__main__':
    main() 