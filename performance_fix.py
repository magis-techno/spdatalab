#!/usr/bin/env python3
"""
性能优化补丁：解决polygon_trajectory_query卡死问题

使用方法：
1. python performance_fix.py
2. 或者手动应用下面的优化配置
"""

import logging

logger = logging.getLogger(__name__)

def create_optimized_config():
    """创建优化的配置"""
    from src.spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig
    
    # 性能优化配置
    optimized_config = PolygonTrajectoryConfig(
        # 大幅降低每polygon的点数限制
        limit_per_polygon=1000,      # 从10000降到1000
        
        # 更激进的分块策略
        batch_threshold=5,           # 从50降到5
        chunk_size=3,                # 从20降到3
        
        # 更短的超时时间
        query_timeout=60,            # 从300降到60秒
        
        # 其他优化
        batch_insert_size=500,       # 从1000降到500
        min_points_per_trajectory=3  # 从2增加到3，过滤无效轨迹
    )
    
    return optimized_config

def test_optimized_query():
    """测试优化后的查询"""
    print("=" * 60)
    print("🚀 测试性能优化配置")
    print("=" * 60)
    
    # 创建优化配置
    config = create_optimized_config()
    
    print("✅ 优化配置已创建:")
    print(f"   • 每polygon点数限制: {config.limit_per_polygon:,}")
    print(f"   • 批量查询阈值: {config.batch_threshold}")
    print(f"   • 分块大小: {config.chunk_size}")
    print(f"   • 查询超时: {config.query_timeout}秒")
    
    from src.spdatalab.dataset.polygon_trajectory_query import HighPerformancePolygonTrajectoryQuery
    
    try:
        # 使用优化配置创建查询器
        query_engine = HighPerformancePolygonTrajectoryQuery(config)
        print("✅ 优化查询器创建成功")
        
        return config
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return None

def apply_quick_fix():
    """应用快速修复：创建限制版查询函数"""
    print("=" * 60)
    print("🔧 应用快速性能修复")
    print("=" * 60)
    
    quick_fix_code = '''
def quick_polygon_query(geojson_file, output_table=None, max_points_per_polygon=500):
    """
    快速polygon查询 - 解决卡死问题
    
    Args:
        geojson_file: GeoJSON文件路径
        output_table: 输出表名（可选）
        max_points_per_polygon: 每polygon最大点数（默认500）
    """
    from src.spdatalab.dataset.polygon_trajectory_query import (
        PolygonTrajectoryConfig, 
        HighPerformancePolygonTrajectoryQuery,
        load_polygons_from_geojson
    )
    import time
    
    # 创建快速配置
    config = PolygonTrajectoryConfig(
        limit_per_polygon=max_points_per_polygon,
        batch_threshold=3,  # 很小的批量
        chunk_size=2,       # 很小的分块
        query_timeout=30    # 很短的超时
    )
    
    print(f"🚀 使用快速配置: 每polygon最多{max_points_per_polygon}个点")
    
    # 加载polygon
    polygons = load_polygons_from_geojson(geojson_file)
    if not polygons:
        print("❌ 未加载到polygon")
        return None
    
    print(f"📋 加载了 {len(polygons)} 个polygon")
    
    # 创建查询器
    query_engine = HighPerformancePolygonTrajectoryQuery(config)
    
    try:
        # 逐个处理polygon以避免卡死
        all_results = []
        
        for i, polygon in enumerate(polygons, 1):
            print(f"🔍 处理polygon {i}/{len(polygons)}: {polygon['id']}")
            
            start_time = time.time()
            
            # 单个polygon查询
            single_result, stats = query_engine.query_intersecting_trajectory_points([polygon])
            
            query_time = time.time() - start_time
            print(f"   ⏱️ 用时: {query_time:.2f}s, 获得: {len(single_result)} 个点")
            
            if not single_result.empty:
                all_results.append(single_result)
            
            # 避免查询过快
            if query_time < 0.5:
                time.sleep(0.1)
        
        # 合并结果
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            print(f"✅ 总计获得 {len(final_df)} 个轨迹点")
            
            # 构建轨迹
            trajectories, build_stats = query_engine.build_trajectories_from_points(final_df)
            print(f"✅ 构建了 {len(trajectories)} 条轨迹")
            
            # 保存到数据库（如果指定）
            if output_table:
                saved_count, save_stats = query_engine.save_trajectories_to_table(trajectories, output_table)
                print(f"✅ 保存了 {saved_count} 条轨迹到表: {output_table}")
            
            return {
                'trajectories': trajectories,
                'total_points': len(final_df),
                'total_trajectories': len(trajectories)
            }
        else:
            print("⚠️ 未找到任何轨迹点")
            return None
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return None

# 使用示例：
# result = quick_polygon_query("data/uturn_poi_20250716.geojson", "test_table", max_points_per_polygon=200)
'''
    
    # 保存快速修复代码
    with open('quick_polygon_fix.py', 'w', encoding='utf-8') as f:
        f.write(quick_fix_code)
    
    print("✅ 快速修复代码已保存到: quick_polygon_fix.py")
    print("\n使用方法:")
    print("1. from quick_polygon_fix import quick_polygon_query")
    print("2. result = quick_polygon_query('your_file.geojson', 'table_name', max_points_per_polygon=200)")

if __name__ == "__main__":
    print("🔧 Polygon轨迹查询性能优化工具")
    print("=" * 60)
    
    # 测试优化配置
    config = test_optimized_query()
    
    if config:
        print("\n✅ 建议使用以下优化参数运行：")
        print(f"python src/spdatalab/dataset/polygon_trajectory_query.py \\")
        print(f"  --input data/uturn_poi_20250716.geojson \\")
        print(f"  --table utrun_polygon_of_interest_trajectires \\")
        print(f"  --limit-per-polygon {config.limit_per_polygon} \\")
        print(f"  --batch-threshold {config.batch_threshold} \\")
        print(f"  --chunk-size {config.chunk_size} \\")
        print(f"  --timeout {config.query_timeout}")
    
    # 创建快速修复方案
    apply_quick_fix() 