#!/usr/bin/env python3
"""
轨迹道路分析SQL调试脚本

用于生成可在DataGrip中直接使用的SQL语句，方便调试
"""

import sys
import json
import argparse
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from spdatalab.fusion.trajectory_road_analysis import TrajectoryRoadAnalysisConfig


def load_trajectories_from_geojson(geojson_file: str):
    """从GeoJSON文件加载轨迹数据"""
    with open(geojson_file, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    trajectories = []
    
    # 处理FeatureCollection
    if geojson_data.get('type') == 'FeatureCollection':
        features = geojson_data.get('features', [])
    elif geojson_data.get('type') == 'Feature':
        features = [geojson_data]
    else:
        features = [{'geometry': geojson_data, 'properties': {}}]
    
    for i, feature in enumerate(features):
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})
        
        trajectory_id = properties.get('id') or properties.get('name') or f"trajectory_{i+1:03d}"
        
        if geometry.get('type') == 'LineString':
            coordinates = geometry.get('coordinates', [])
            points = [f"{coord[0]} {coord[1]}" for coord in coordinates]
            trajectory_wkt = f"LINESTRING({', '.join(points)})"
            trajectories.append((trajectory_id, trajectory_wkt))
    
    return trajectories


def generate_debug_sql(trajectory_wkt: str, config: TrajectoryRoadAnalysisConfig):
    """生成调试SQL语句"""
    
    print("=" * 80)
    print("轨迹道路分析SQL调试语句")
    print("=" * 80)
    
    print(f"\n-- 原始轨迹几何:")
    print(f"-- {trajectory_wkt}")
    
    # 1. 创建轨迹缓冲区
    print(f"\n-- 步骤1: 创建轨迹缓冲区 (膨胀{config.buffer_distance}m)")
    buffer_sql = f"""
SELECT 
    ST_AsText(
        ST_Buffer(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
            {config.buffer_distance}
        )::geometry
    ) as buffer_geom;
"""
    print(buffer_sql)
    
    # 2. 验证几何有效性
    print(f"\n-- 步骤2: 验证几何有效性")
    validation_sql = f"""
SELECT 
    ST_IsValid(ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)) as trajectory_valid,
    ST_GeometryType(ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)) as trajectory_type,
    ST_AsText(ST_Envelope(ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326))) as trajectory_bbox;
"""
    print(validation_sql)
    
    # 3. 检查表结构和列信息
    print(f"\n-- 步骤3a: 检查lane表结构")
    lane_structure_sql = f"""
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_name = '{config.lane_table}'
ORDER BY ordinal_position;
"""
    print(lane_structure_sql)
    
    print(f"\n-- 步骤3b: 检查intersection表结构")
    intersection_structure_sql = f"""
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_name = '{config.intersection_table}'
ORDER BY ordinal_position;
"""
    print(intersection_structure_sql)
    
    print(f"\n-- 步骤3c: 检查lane表基本信息（使用wkb_geometry）")
    lane_table_sql = f"""
SELECT 
    COUNT(*) as total_lanes,
    COUNT(CASE WHEN wkb_geometry IS NOT NULL THEN 1 END) as lanes_with_geom,
    ST_AsText(ST_Extent(wkb_geometry)) as table_extent
FROM {config.lane_table}
WHERE wkb_geometry IS NOT NULL;
"""
    print(lane_table_sql)
    
    print(f"\n-- 步骤3d: 检查intersection表基本信息（使用wkb_geometry）")
    intersection_table_sql = f"""
SELECT 
    COUNT(*) as total_intersections,
    COUNT(CASE WHEN wkb_geometry IS NOT NULL THEN 1 END) as intersections_with_geom,
    ST_AsText(ST_Extent(wkb_geometry)) as table_extent
FROM {config.intersection_table}
WHERE wkb_geometry IS NOT NULL;
"""
    print(intersection_table_sql)
    
    print(f"\n-- 步骤3e: 检查可能的几何列名")
    geometry_columns_sql = f"""
SELECT 
    f_table_name,
    f_geometry_column,
    coord_dimension,
    srid,
    type
FROM geometry_columns 
WHERE f_table_name IN ('{config.lane_table}', '{config.intersection_table}');
"""
    print(geometry_columns_sql)
    
    # 4. 使用WITH子句的完整查询
    print(f"\n-- 步骤4: 使用WITH子句的完整lane查询")
    lane_with_buffer_sql = f"""
WITH buffer_geom AS (
    SELECT 
        ST_Buffer(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
            {config.buffer_distance}
        )::geometry as buffer_geometry
)
SELECT 
    l.id as lane_id,
    l.roadid as road_id,
    ST_AsText(l.wkb_geometry) as geometry_wkt,
    ST_Distance(
        b.buffer_geometry::geography,
        l.wkb_geometry::geography
    ) as distance
FROM {config.lane_table} l, buffer_geom b
WHERE ST_Intersects(
    b.buffer_geometry,
    l.wkb_geometry
)
AND l.wkb_geometry IS NOT NULL
LIMIT 10;
"""
    print(lane_with_buffer_sql)
    
    print(f"\n-- 步骤5: 使用WITH子句的完整intersection查询")
    intersection_with_buffer_sql = f"""
WITH buffer_geom AS (
    SELECT 
        ST_Buffer(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
            {config.buffer_distance}
        )::geometry as buffer_geometry
)
SELECT 
    i.id as intersection_id,
    i.intersectiontype,
    i.intersectionsubtype,
    ST_AsText(i.wkb_geometry) as geometry_wkt
FROM {config.intersection_table} i, buffer_geom b
WHERE ST_Intersects(
    b.buffer_geometry,
    i.wkb_geometry
)
AND i.wkb_geometry IS NOT NULL
LIMIT 10;
"""
    print(intersection_with_buffer_sql)
    
    # 5. 简化的空间查询（用于快速测试）
    print(f"\n-- 步骤6: 简化的空间查询（不计算距离）")
    simple_lane_sql = f"""
SELECT 
    l.id as lane_id,
    l.roadid as road_id,
    ST_AsText(l.wkb_geometry) as geometry_wkt
FROM {config.lane_table} l
WHERE ST_DWithin(
    l.wkb_geometry::geography,
    ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
    {config.buffer_distance}
)
AND l.wkb_geometry IS NOT NULL
LIMIT 10;
"""
    print(simple_lane_sql)
    
    # 6. 检查特定区域的数据
    print(f"\n-- 步骤7: 检查轨迹附近的数据密度")
    density_sql = f"""
SELECT 
    COUNT(*) as nearby_lanes
FROM {config.lane_table}
WHERE ST_DWithin(
    wkb_geometry::geography,
    ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
    1000  -- 1km范围内
)
AND wkb_geometry IS NOT NULL;
"""
    print(density_sql)
    
    print("\n" + "=" * 80)
    print("调试建议:")
    print("1. 依次执行上述SQL语句")
    print("2. 首先执行步骤3a-3e检查表结构和列信息")
    print("3. 如果wkb_geometry列不存在，请根据步骤3e的结果使用正确的几何列名")
    print("4. 如果步骤1-4都正常，但步骤5-6无结果，可能是空间索引问题")
    print("5. 如果步骤7显示附近有数据，但步骤4-6无结果，检查几何投影和缓冲区大小")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='轨迹道路分析SQL调试工具')
    parser.add_argument('--geojson', type=str, required=True, help='GeoJSON文件路径')
    parser.add_argument('--trajectory-index', type=int, default=0, help='要调试的轨迹索引（默认第一个）')
    parser.add_argument('--output', type=str, help='输出SQL文件路径')
    
    args = parser.parse_args()
    
    # 加载轨迹数据
    trajectories = load_trajectories_from_geojson(args.geojson)
    
    if not trajectories:
        print("错误: 没有找到有效的轨迹数据")
        return 1
    
    if args.trajectory_index >= len(trajectories):
        print(f"错误: 轨迹索引 {args.trajectory_index} 超出范围，总共有 {len(trajectories)} 个轨迹")
        return 1
    
    trajectory_id, trajectory_wkt = trajectories[args.trajectory_index]
    config = TrajectoryRoadAnalysisConfig()
    
    print(f"生成轨迹 '{trajectory_id}' 的调试SQL...")
    
    if args.output:
        # 重定向输出到文件
        with open(args.output, 'w', encoding='utf-8') as f:
            import sys
            old_stdout = sys.stdout
            sys.stdout = f
            generate_debug_sql(trajectory_wkt, config)
            sys.stdout = old_stdout
        print(f"SQL已保存到: {args.output}")
    else:
        generate_debug_sql(trajectory_wkt, config)
    
    return 0


if __name__ == '__main__':
    sys.exit(main()) 