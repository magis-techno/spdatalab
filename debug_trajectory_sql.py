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
    
    # 4. 使用WITH子句的road查询（新策略）
    print(f"\n-- 步骤4: 使用WITH子句的完整road查询（新策略）")
    road_with_buffer_sql = f"""
WITH buffer_geom AS (
    SELECT 
        ST_Buffer(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
            {config.buffer_distance}
        )::geometry as buffer_geometry
)
SELECT 
    r.id as road_id,
    ST_AsText(r.wkb_geometry) as geometry_wkt
FROM {config.road_table} r, buffer_geom b
WHERE ST_Intersects(
    b.buffer_geometry,
    r.wkb_geometry
)
AND r.wkb_geometry IS NOT NULL
LIMIT 10;
"""
    print(road_with_buffer_sql)
    
    print(f"\n-- 步骤4b: road链路扩展查询示例（前向）")
    road_chain_sql = f"""
WITH RECURSIVE road_chain AS (
    SELECT 
        rnr.roadid,
        rnr.nextroadid,
        0 as depth,
        0 as distance,
        COALESCE(rnr.length, 0) as current_length
    FROM {config.roadnextroad_table} rnr
    WHERE rnr.roadid IN (
        -- 这里应该是相交的road_id列表
        SELECT r.id FROM {config.road_table} r 
        WHERE ST_Intersects(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326),
            r.wkb_geometry
        ) LIMIT 5
    )
    AND rnr.nextroadid IS NOT NULL
    
    UNION ALL
    
    SELECT 
        rnr.roadid,
        rnr.nextroadid,
        rc.depth + 1,
        rc.distance + rc.current_length,
        COALESCE(rnr.length, 0) as current_length
    FROM {config.roadnextroad_table} rnr
    JOIN road_chain rc ON rnr.roadid = rc.nextroadid
    WHERE rc.distance + rc.current_length <= 500.0
    AND rc.depth < 10
    AND rnr.nextroadid IS NOT NULL
)
SELECT DISTINCT 
    rc.nextroadid as road_id,
    rc.depth as chain_depth,
    rc.distance
FROM road_chain rc
WHERE rc.nextroadid IS NOT NULL
LIMIT 20;
"""
    print(road_chain_sql)
    
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
    print(f"\n-- 步骤6: intersection的inroad/outroad查询")
    intersection_roads_sql = f"""
-- 查找intersection的inroad
SELECT 
    igr.roadid as road_id,
    igr.intersectionid,
    'intersection_inroad' as road_type
FROM {config.intersection_inroad_table} igr
WHERE igr.intersectionid IN (
    -- 这里应该是相交的intersection_id列表
    SELECT i.id FROM {config.intersection_table} i 
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326),
        i.wkb_geometry
    ) LIMIT 5
)
LIMIT 10;

-- 查找intersection的outroad
SELECT 
    ior.roadid as road_id,
    ior.intersectionid,
    'intersection_outroad' as road_type
FROM {config.intersection_outroad_table} ior
WHERE ior.intersectionid IN (
    -- 这里应该是相交的intersection_id列表
    SELECT i.id FROM {config.intersection_table} i 
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326),
        i.wkb_geometry
    ) LIMIT 5
)
LIMIT 10;
"""
    print(intersection_roads_sql)
    
    print(f"\n-- 步骤7: 根据road收集所有lane（最终步骤）")
    lanes_from_roads_sql = f"""
SELECT 
    l.id as lane_id,
    l.roadid as road_id,
    ST_AsText(l.wkb_geometry) as geometry_wkt,
    l.intersectionid,
    l.isintersectioninlane,
    l.isintersectionoutlane
FROM {config.lane_table} l
WHERE l.roadid IN (
    -- 这里应该是所有收集到的road_id列表
    -- 包括：相交road + road链路 + intersection roads
    SELECT DISTINCT road_id FROM (
        -- 相交road
        SELECT r.id as road_id FROM {config.road_table} r 
        WHERE ST_Intersects(
            ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326),
            r.wkb_geometry
        )
        LIMIT 10
    ) combined_roads
)
AND l.wkb_geometry IS NOT NULL
LIMIT 100;
"""
    print(lanes_from_roads_sql)
    
    # 6. 检查特定区域的数据
    print(f"\n-- 步骤8: 检查轨迹附近的数据密度")
    density_sql = f"""
SELECT 
    COUNT(*) as nearby_roads
FROM {config.road_table}
WHERE ST_DWithin(
    wkb_geometry::geography,
    ST_SetSRID(ST_GeomFromText('{trajectory_wkt}'), 4326)::geography,
    1000  -- 1km范围内
)
AND wkb_geometry IS NOT NULL;
"""
    print(density_sql)
    
    print("\n" + "=" * 80)
    print("基于Road策略的调试建议:")
    print("1. 依次执行上述SQL语句")
    print("2. 首先执行步骤3a-3e检查表结构和列信息")
    print("3. 如果wkb_geometry列不存在，请根据步骤3e的结果使用正确的几何列名")
    print("4. 新策略的执行顺序：")
    print("   - 步骤4: 查找相交road（比lane查询快得多）")
    print("   - 步骤4b: 扩展road链路（前后500m/100m）")
    print("   - 步骤5: 查找相交intersection")
    print("   - 步骤6: 补齐intersection的inroad/outroad")
    print("   - 步骤7: 根据所有road收集对应的lane（最终步骤）")
    print("5. 性能优化：")
    print("   - road数量比lane少很多，查询速度显著提升")
    print("   - 递归查询限制在road级别，复杂度大幅降低")
    print("   - 最后的lane查询范围已被限定，查询时间可控")
    print("6. 如果步骤8显示附近有数据，但步骤4-7无结果，检查几何投影和缓冲区大小")
    print("7. 预期性能提升：查询时间从35s降低到10s以内")
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