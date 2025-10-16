#!/usr/bin/env python3
"""
调试脚本：导出Grid轨迹点到数据库表
===============================

用途：
1. 从city_hotspots获取指定grid
2. 查询该grid内的高质量轨迹点（workstage=2）
3. 为每个点计算特征（速度、加速度、航向角变化等）
4. 导出到debug表，方便QGIS可视化分析

使用方法：
    # 导出A72城市top 1的grid轨迹点
    python examples/dataset/bbox_examples/debug_export_grid_points.py --city A72 --grid-rank 1
    
    # 导出指定grid ID的轨迹点
    python examples/dataset/bbox_examples/debug_export_grid_points.py --grid-id 12963
    
    # 限制返回点数（避免数据量过大）
    python examples/dataset/bbox_examples/debug_export_grid_points.py --city A72 --limit 10000

输出表：
    debug_grid_trajectory_points - 轨迹点表（带特征和几何）
    
QGIS可视化：
    1. 加载 debug_grid_trajectory_points 表
    2. 按 dataset_name 分类着色
    3. 按 twist_linear 字段大小设置点大小
    4. 查看属性表的特征值
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

def get_top_grid(engine, city_id: str = None, grid_rank: int = 1, grid_id: int = None):
    """获取热点grid信息"""
    print(f"\n📊 获取热点Grid...")
    
    if grid_id:
        sql = text("""
            SELECT 
                id as grid_id,
                city_id,
                analysis_id,
                bbox_count,
                subdataset_count,
                scene_count,
                grid_coords,
                ST_AsText(geometry) as geometry_wkt,
                ST_XMin(geometry) as xmin,
                ST_YMin(geometry) as ymin,
                ST_XMax(geometry) as xmax,
                ST_YMax(geometry) as ymax
            FROM city_hotspots
            WHERE id = :grid_id;
        """)
        params = {'grid_id': grid_id}
    else:
        where_clause = f"WHERE city_id = :city_id" if city_id else ""
        sql = text(f"""
            SELECT 
                id as grid_id,
                city_id,
                analysis_id,
                bbox_count,
                subdataset_count,
                scene_count,
                grid_coords,
                ST_AsText(geometry) as geometry_wkt,
                ST_XMin(geometry) as xmin,
                ST_YMin(geometry) as ymin,
                ST_XMax(geometry) as xmax,
                ST_YMax(geometry) as ymax
            FROM city_hotspots
            {where_clause}
            ORDER BY bbox_count DESC
            LIMIT 1 OFFSET :offset;
        """)
        params = {'offset': grid_rank - 1}
        if city_id:
            params['city_id'] = city_id
    
    with engine.connect() as conn:
        result = pd.read_sql(sql, conn, params=params)
    
    if result.empty:
        print("❌ 未找到符合条件的grid")
        return None
    
    grid = result.iloc[0]
    print(f"✅ Grid #{grid['grid_id']}")
    print(f"   城市: {grid['city_id']}")
    print(f"   bbox数量: {grid['bbox_count']}")
    print(f"   子数据集数: {grid['subdataset_count']}")
    print(f"   场景数: {grid['scene_count']}")
    print(f"   坐标范围: [{grid['xmin']:.6f}, {grid['ymin']:.6f}] - [{grid['xmax']:.6f}, {grid['ymax']:.6f}]")
    
    return grid

def query_trajectory_points(engine, geometry_wkt: str, limit: int = None):
    """查询grid内的轨迹点"""
    print(f"\n🔍 查询Grid内的轨迹点...")
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    sql = text(f"""
        SELECT 
            dataset_name,
            vehicle_id,
            timestamp,
            twist_linear as speed,
            yaw,
            pitch,
            roll,
            workstage,
            ST_X(point_lla) as lon,
            ST_Y(point_lla) as lat
        FROM {POINT_TABLE}
        WHERE ST_Intersects(point_lla, ST_GeomFromText(:geometry_wkt, 4326))
          AND workstage = 2
          AND point_lla IS NOT NULL
          AND twist_linear IS NOT NULL
        ORDER BY dataset_name, timestamp
        {limit_clause};
    """)
    
    with engine.connect() as conn:
        points_df = pd.read_sql(sql, conn, params={'geometry_wkt': geometry_wkt})
    
    print(f"✅ 查询到 {len(points_df)} 个轨迹点")
    if not points_df.empty:
        print(f"   轨迹数: {points_df['dataset_name'].nunique()}")
        print(f"   时间范围: {points_df['timestamp'].min()} - {points_df['timestamp'].max()}")
        print(f"   速度范围: {points_df['speed'].min():.2f} - {points_df['speed'].max():.2f} m/s")
    
    return points_df

def calculate_point_features(points_df: pd.DataFrame):
    """为每个点计算特征"""
    print(f"\n📐 计算点特征...")
    
    # 按轨迹分组计算
    features_list = []
    
    for dataset_name, group in points_df.groupby('dataset_name'):
        group = group.sort_values('timestamp').reset_index(drop=True)
        
        n_points = len(group)
        
        # 初始化特征列
        group['point_index'] = range(n_points)
        group['distance_to_next'] = 0.0
        group['time_gap'] = 0.0
        group['acceleration'] = 0.0
        group['yaw_change_rate'] = 0.0
        group['cumulative_distance'] = 0.0
        
        # 计算逐点特征
        cumulative_dist = 0.0
        
        for i in range(n_points):
            if i < n_points - 1:
                # 距离到下一点
                dist = haversine_distance(
                    group.iloc[i]['lat'], group.iloc[i]['lon'],
                    group.iloc[i+1]['lat'], group.iloc[i+1]['lon']
                )
                group.at[i, 'distance_to_next'] = dist
                cumulative_dist += dist
                
                # 时间间隔
                time_gap = group.iloc[i+1]['timestamp'] - group.iloc[i]['timestamp']
                group.at[i, 'time_gap'] = time_gap
                
                # 加速度
                if time_gap > 0:
                    speed_diff = group.iloc[i+1]['speed'] - group.iloc[i]['speed']
                    group.at[i, 'acceleration'] = speed_diff / time_gap
                    
                    # 航向角变化率
                    yaw_diff = group.iloc[i+1]['yaw'] - group.iloc[i]['yaw']
                    group.at[i, 'yaw_change_rate'] = yaw_diff / time_gap
            
            group.at[i, 'cumulative_distance'] = cumulative_dist
        
        features_list.append(group)
    
    result_df = pd.concat(features_list, ignore_index=True)
    
    print(f"✅ 特征计算完成")
    print(f"   加速度范围: {result_df['acceleration'].min():.2f} - {result_df['acceleration'].max():.2f} m/s²")
    print(f"   航向角变化率范围: {result_df['yaw_change_rate'].min():.4f} - {result_df['yaw_change_rate'].max():.4f} rad/s")
    
    return result_df

def haversine_distance(lat1, lon1, lat2, lon2):
    """计算两点间的Haversine距离（米）"""
    R = 6371000  # 地球半径（米）
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c

def export_to_database(engine, points_df: pd.DataFrame, grid_id: int):
    """导出到数据库调试表"""
    print(f"\n💾 导出到数据库...")
    
    # 创建调试表
    create_table_sql = text("""
        DROP TABLE IF EXISTS debug_grid_trajectory_points CASCADE;
        
        CREATE TABLE debug_grid_trajectory_points (
            id SERIAL PRIMARY KEY,
            grid_id INTEGER NOT NULL,
            dataset_name TEXT NOT NULL,
            vehicle_id TEXT,
            timestamp BIGINT NOT NULL,
            point_index INTEGER NOT NULL,
            
            -- 位置信息
            lon DOUBLE PRECISION NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            geometry GEOMETRY(Point, 4326),
            
            -- 运动状态
            speed DOUBLE PRECISION,
            yaw DOUBLE PRECISION,
            pitch DOUBLE PRECISION,
            roll DOUBLE PRECISION,
            workstage INTEGER,
            
            -- 计算特征
            distance_to_next DOUBLE PRECISION,
            time_gap DOUBLE PRECISION,
            acceleration DOUBLE PRECISION,
            yaw_change_rate DOUBLE PRECISION,
            cumulative_distance DOUBLE PRECISION,
            
            -- 元数据
            export_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_debug_grid_points_grid ON debug_grid_trajectory_points(grid_id);
        CREATE INDEX idx_debug_grid_points_dataset ON debug_grid_trajectory_points(dataset_name);
        CREATE INDEX idx_debug_grid_points_geom ON debug_grid_trajectory_points USING GIST(geometry);
    """)
    
    with engine.connect() as conn:
        conn.execute(create_table_sql)
        conn.commit()
    
    print(f"✅ 调试表已创建")
    
    # 准备插入数据
    insert_sql = text("""
        INSERT INTO debug_grid_trajectory_points (
            grid_id, dataset_name, vehicle_id, timestamp, point_index,
            lon, lat, geometry,
            speed, yaw, pitch, roll, workstage,
            distance_to_next, time_gap, acceleration, yaw_change_rate, cumulative_distance
        ) VALUES (
            :grid_id, :dataset_name, :vehicle_id, :timestamp, :point_index,
            :lon, :lat, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
            :speed, :yaw, :pitch, :roll, :workstage,
            :distance_to_next, :time_gap, :acceleration, :yaw_change_rate, :cumulative_distance
        );
    """)
    
    # 批量插入
    batch_size = 1000
    total_batches = (len(points_df) + batch_size - 1) // batch_size
    
    for i in range(0, len(points_df), batch_size):
        batch = points_df.iloc[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        records = []
        for _, row in batch.iterrows():
            records.append({
                'grid_id': int(grid_id),
                'dataset_name': row['dataset_name'],
                'vehicle_id': row.get('vehicle_id'),
                'timestamp': int(row['timestamp']),
                'point_index': int(row['point_index']),
                'lon': float(row['lon']),
                'lat': float(row['lat']),
                'speed': float(row['speed']),
                'yaw': float(row['yaw']),
                'pitch': float(row['pitch']) if pd.notna(row['pitch']) else None,
                'roll': float(row['roll']) if pd.notna(row['roll']) else None,
                'workstage': int(row['workstage']),
                'distance_to_next': float(row['distance_to_next']),
                'time_gap': float(row['time_gap']),
                'acceleration': float(row['acceleration']),
                'yaw_change_rate': float(row['yaw_change_rate']),
                'cumulative_distance': float(row['cumulative_distance'])
            })
        
        with engine.connect() as conn:
            conn.execute(insert_sql, records)
            conn.commit()
        
        print(f"   批次 {batch_num}/{total_batches} 已保存 ({len(records)} 条)")
    
    print(f"✅ 总共导出 {len(points_df)} 个点")
    
    # 统计信息
    stats_sql = text("""
        SELECT 
            COUNT(*) as total_points,
            COUNT(DISTINCT dataset_name) as trajectory_count,
            MIN(speed) as min_speed,
            MAX(speed) as max_speed,
            AVG(speed) as avg_speed,
            MIN(acceleration) as min_accel,
            MAX(acceleration) as max_accel,
            AVG(ABS(acceleration)) as avg_abs_accel,
            MAX(cumulative_distance) as max_trajectory_length
        FROM debug_grid_trajectory_points
        WHERE grid_id = :grid_id;
    """)
    
    with engine.connect() as conn:
        stats = pd.read_sql(stats_sql, conn, params={'grid_id': grid_id})
    
    print(f"\n📊 导出统计:")
    stat = stats.iloc[0]
    print(f"   总点数: {stat['total_points']}")
    print(f"   轨迹数: {stat['trajectory_count']}")
    print(f"   速度: {stat['min_speed']:.2f} - {stat['max_speed']:.2f} m/s (平均: {stat['avg_speed']:.2f})")
    print(f"   加速度: {stat['min_accel']:.2f} - {stat['max_accel']:.2f} m/s² (平均绝对值: {stat['avg_abs_accel']:.2f})")
    print(f"   最长轨迹: {stat['max_trajectory_length']:.1f} 米")

def print_qgis_guide(grid_id: int):
    """打印QGIS使用指南"""
    print(f"\n" + "="*70)
    print(f"🎨 QGIS可视化指南")
    print("="*70)
    print(f"""
1️⃣ 加载数据表：
   - 表名: debug_grid_trajectory_points
   - 几何列: geometry
   - 主键: id
   - 过滤条件: grid_id = {grid_id}

2️⃣ 推荐样式设置：
   
   【方案1：按轨迹着色】
   - 样式类型: 分类 (Categorized)
   - 字段: dataset_name
   - 颜色: 随机色
   - 点大小: 2-3像素
   
   【方案2：按速度着色】
   - 样式类型: 渐变 (Graduated)
   - 字段: speed
   - 颜色方案: Reds (低速) → Greens (高速)
   - 分级方式: Natural Breaks (5-7级)
   
   【方案3：按加速度着色】
   - 样式类型: 渐变 (Graduated)
   - 字段: acceleration
   - 颜色方案: RdBu (红色=刹车, 蓝色=加速)
   - 范围: -3 到 +3 m/s²

3️⃣ 标签设置：
   - 字段: dataset_name
   - 显示条件: $scale < 1000
   - 背景: 半透明白色

4️⃣ 属性表查看：
   - timestamp: 时间戳
   - speed: 瞬时速度 (m/s)
   - acceleration: 加速度 (m/s²)
   - yaw_change_rate: 航向角变化率 (rad/s)
   - cumulative_distance: 累计距离 (m)
   - distance_to_next: 到下一点距离 (m)

5️⃣ 高级分析：
   - 使用 "按表达式选择" 过滤异常点
   - 例: acceleration > 2 (强加速点)
   - 例: speed < 1 (低速/停车点)
   - 例: yaw_change_rate > 0.5 (转弯点)
""")

def main():
    parser = argparse.ArgumentParser(
        description='导出Grid轨迹点到调试表',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    grid_group = parser.add_mutually_exclusive_group(required=True)
    grid_group.add_argument('--city', help='城市ID（如：A72）')
    grid_group.add_argument('--grid-id', type=int, help='指定Grid ID')
    
    parser.add_argument('--grid-rank', type=int, default=1,
                       help='城市内的Grid排名（默认：1，即最热点）')
    parser.add_argument('--limit', type=int, 
                       help='限制返回点数（默认：不限制）')
    
    args = parser.parse_args()
    
    print("🔬 Grid轨迹点调试导出工具")
    print("="*70)
    
    # 创建数据库引擎
    engine = create_engine(LOCAL_DSN, future=True, pool_pre_ping=True)
    
    try:
        # 1. 获取Grid信息
        grid = get_top_grid(
            engine, 
            city_id=args.city, 
            grid_rank=args.grid_rank,
            grid_id=args.grid_id
        )
        
        if grid is None:
            return 1
        
        # 2. 查询轨迹点
        points_df = query_trajectory_points(
            engine,
            grid['geometry_wkt'],
            limit=args.limit
        )
        
        if points_df.empty:
            print("\n⚠️ 该Grid内没有轨迹点")
            return 1
        
        # 3. 计算特征
        points_with_features = calculate_point_features(points_df)
        
        # 4. 导出到数据库
        export_to_database(engine, points_with_features, grid['grid_id'])
        
        # 5. 打印QGIS指南
        print_qgis_guide(grid['grid_id'])
        
        print(f"\n✅ 导出完成！")
        print(f"💡 现在可以在QGIS中加载 debug_grid_trajectory_points 表进行可视化分析")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ 导出失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        engine.dispose()

if __name__ == "__main__":
    sys.exit(main())

