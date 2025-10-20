#!/usr/bin/env python3
"""
诊断Grid聚类问题的临时脚本

快速检查：
1. 轨迹点数据是否正常
2. 切分是否成功
3. 质量过滤的具体原因
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
from shapely import wkt
from sqlalchemy import create_engine, text

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.grid_trajectory_clustering import (
    GridTrajectoryClusterer, 
    ClusterConfig
)

def diagnose_grid_clustering(city_id='A72', grid_rank=1):
    """诊断grid聚类问题"""
    
    print("="*70)
    print("🔬 Grid聚类问题诊断")
    print("="*70)
    
    # 创建聚类器
    config = ClusterConfig(
        query_limit=50000,
        min_distance=50.0,
        max_duration=15.0,
        min_points=5
    )
    clusterer = GridTrajectoryClusterer(config)
    
    # 1. 加载grid
    print(f"\n1️⃣ 加载Grid...")
    grids_df = clusterer.load_hotspot_grids(city_id=city_id, limit=1)
    
    if grids_df.empty:
        print("❌ 没有找到grid")
        return
    
    grid = grids_df.iloc[0]
    grid_id = grid['grid_id']
    geometry = grid['geometry']
    
    print(f"✅ Grid #{grid_id}")
    
    # 2. 查询轨迹点
    print(f"\n2️⃣ 查询轨迹点...")
    points_df = clusterer.query_trajectory_points(geometry, grid_id)
    
    if points_df.empty:
        print("❌ 没有轨迹点")
        return
    
    print(f"✅ 查询到 {len(points_df)} 个点，{points_df['dataset_name'].nunique()} 条轨迹")
    
    # 检查数据
    print(f"\n📊 数据检查:")
    print(f"  timestamp范围: {points_df['timestamp'].min()} - {points_df['timestamp'].max()}")
    
    # 检测timestamp单位
    ts_min = points_df['timestamp'].min()
    ts_max = points_df['timestamp'].max()
    if ts_min < 1e10 and ts_max < 1e10:
        print(f"  timestamp单位: 秒级")
    elif ts_min < 1e13 and ts_max < 1e13:
        print(f"  timestamp单位: 毫秒级")
    elif ts_min < 1e16 and ts_max < 1e16:
        print(f"  timestamp单位: 微秒级")
    else:
        print(f"  timestamp单位: 混合/纳秒级 ⚠️")
    
    print(f"  lon范围: [{points_df['lon'].min():.6f}, {points_df['lon'].max():.6f}]")
    print(f"  lat范围: [{points_df['lat'].min():.6f}, {points_df['lat'].max():.6f}]")
    print(f"  twist_linear范围: [{points_df['twist_linear'].min():.2f}, {points_df['twist_linear'].max():.2f}] m/s")
    
    # 抽样检查几条轨迹
    print(f"\n3️⃣ 抽样检查前3条轨迹:")
    for i, (dataset_name, group) in enumerate(points_df.groupby('dataset_name')):
        if i >= 3:
            break
        
        group = group.sort_values('timestamp')
        
        # 自动检测timestamp单位
        sample_ts = group['timestamp'].median()
        if sample_ts < 1e10:
            scale = 1  # 秒
        elif sample_ts < 1e13:
            scale = 1e3  # 毫秒
        elif sample_ts < 1e16:
            scale = 1e6  # 微秒
        else:
            scale = 1e9  # 纳秒
        
        # 统一为秒
        timestamps_sec = group['timestamp'] / scale
        time_span = timestamps_sec.max() - timestamps_sec.min()
        
        # 计算总距离
        total_dist = 0
        for j in range(1, len(group)):
            lat1, lon1 = group.iloc[j-1]['lat'], group.iloc[j-1]['lon']
            lat2, lon2 = group.iloc[j]['lat'], group.iloc[j]['lon']
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            total_dist += dist
        
        print(f"\n  轨迹 {i+1}: {dataset_name}")
        print(f"    点数: {len(group)}")
        print(f"    时间跨度: {time_span:.2f}秒")
        print(f"    总距离: {total_dist:.2f}米")
        print(f"    平均速度: {group['twist_linear'].mean():.2f} m/s")
        
        # 判断是否能切分
        if len(group) < config.min_points:
            print(f"    ⚠️ 点数不足（< {config.min_points}）")
        elif time_span < config.max_duration:
            print(f"    ⚠️ 时间跨度太短（< {config.max_duration}秒）")
        elif total_dist < config.min_distance:
            print(f"    ⚠️ 距离太短（< {config.min_distance}米）")
        else:
            print(f"    ✅ 应该能切分")
    
    # 4. 尝试切分
    print(f"\n4️⃣ 尝试切分轨迹...")
    segments = clusterer.segment_trajectories(points_df)
    print(f"✅ 切分得到 {len(segments)} 个轨迹段")
    
    if not segments:
        print("❌ 切分失败！没有生成任何轨迹段")
        print("\n🔍 可能原因:")
        print("  1. 每条轨迹点数都 < min_points (5)")
        print("  2. 轨迹时间跨度都太短")
        print("  3. 轨迹距离都太短")
        return
    
    # 5. 质量过滤
    print(f"\n5️⃣ 质量过滤检查:")
    quality_stats = {}
    
    for segment in segments[:10]:  # 只检查前10个
        is_valid, reason = clusterer.filter_segment_quality(segment)
        quality_stats[reason] = quality_stats.get(reason, 0) + 1
    
    print(f"  前10个轨迹段的质量分布:")
    for reason, count in sorted(quality_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"    {reason}: {count}")
    
    # 6. 完整统计
    print(f"\n6️⃣ 完整质量统计:")
    all_quality_stats = {}
    valid_count = 0
    
    for segment in segments:
        is_valid, reason = clusterer.filter_segment_quality(segment)
        all_quality_stats[reason] = all_quality_stats.get(reason, 0) + 1
        if is_valid:
            valid_count += 1
    
    print(f"  总轨迹段数: {len(segments)}")
    print(f"  有效轨迹段: {valid_count} ({valid_count/len(segments)*100:.1f}%)")
    print(f"\n  质量分布:")
    for reason, count in sorted(all_quality_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"    {reason:20s}: {count:5d} ({count/len(segments)*100:.1f}%)")
    
    # 7. 建议
    print(f"\n💡 建议:")
    if valid_count == 0:
        if 'stationary' in all_quality_stats and all_quality_stats['stationary'] > len(segments) * 0.5:
            print("  ⚠️ 大部分轨迹段被判定为原地不动")
            print("     建议：降低 --min-movement 参数（当前10米）")
        
        if 'gps_jump' in all_quality_stats and all_quality_stats['gps_jump'] > len(segments) * 0.3:
            print("  ⚠️ 大量GPS跳点")
            print("     建议：增加 --max-jump 参数（当前100米）")
        
        if 'insufficient_points' in all_quality_stats:
            print("  ⚠️ 轨迹段点数不足")
            print("     建议：降低 --min-points 参数（当前5）")
    else:
        print("  ✅ 有有效轨迹段，可以继续聚类")

def haversine_distance(lat1, lon1, lat2, lon2):
    """计算两点间距离（米）"""
    R = 6371000
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='诊断Grid聚类问题')
    parser.add_argument('--city', default='A72', help='城市ID')
    parser.add_argument('--grid-rank', type=int, default=1, help='Grid排名')
    args = parser.parse_args()
    
    diagnose_grid_clustering(args.city, args.grid_rank)

