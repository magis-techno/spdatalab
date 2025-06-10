"""
空间连接分析示例
=================

展示如何使用新的预存缓存功能进行高效的空间分析：
1. 构建相交关系缓存
2. 进行多维度统计分析
3. 获取详细相交信息

推荐工作流程：
- 首次使用：构建缓存 → 进行分析
- 后续分析：直接基于缓存进行各种维度的分析
- 数据更新：重新构建缓存
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin, 
    SpatialJoinConfig,
    build_cache,
    analyze_cached_intersections,
    get_available_cities,
    get_intersection_types_summary,
    explain_intersection_types
)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def explore_data_format():
    """探索实际的数据格式"""
    print("🔍 数据格式探索")
    print("=" * 40)
    
    try:
        # 1. 获取可用城市列表
        print("🏙️ 可用城市列表:")
        cities_df = get_available_cities()
        if not cities_df.empty and 'city_id' in cities_df.columns:
            print(cities_df.head(10).to_string(index=False))
            # 选择第一个城市作为示例
            sample_city = cities_df.iloc[0]['city_id'] if len(cities_df) > 0 else None
            print(f"\n💡 示例城市ID: {sample_city}")
        else:
            print(cities_df.to_string(index=False))
            sample_city = None
        
        # 2. 获取路口类型说明
        print(f"\n📖 路口类型说明:")
        explanation_df = explain_intersection_types()
        print(explanation_df.to_string(index=False))
        
        # 3. 获取路口类型汇总
        print(f"\n🚦 路口类型汇总:")
        types_df = get_intersection_types_summary()
        if not types_df.empty and 'intersectiontype' in types_df.columns:
            print(types_df.head(15).to_string(index=False))
        else:
            print(types_df.to_string(index=False))
        
        # 4. 获取bbox样本数据
        print(f"\n📋 bbox数据样本:")
        spatial_join = ProductionSpatialJoin()
        sample_data = spatial_join._fetch_bbox_data(3, None)
        if not sample_data.empty:
            print(sample_data.to_string(index=False))
        else:
            print("❌ 未找到bbox数据")
            
        return sample_city
            
    except Exception as e:
        print(f"❌ 数据探索失败: {e}")
        return None


def demo_cache_workflow(sample_city=None):
    """演示完整的缓存工作流程"""
    
    print("🚀 空间连接分析演示")
    print("=" * 60)
    
    # 配置
    config = SpatialJoinConfig(
        batch_threshold=200,
        chunk_size=50,
        enable_cache_table=True
    )
    
    # 初始化空间连接器
    spatial_join = ProductionSpatialJoin(config)
    
    # 1. 构建缓存
    print("\n📊 第1步：构建相交关系缓存")
    print("-" * 40)
    
    city = sample_city  # 使用从探索中获得的实际城市ID
    num_bbox = 20       # 减少数量，方便调试
    
    if city:
        print(f"使用城市过滤: {city}")
    else:
        print("不使用城市过滤")
    
    try:
        cached_count, build_stats = spatial_join.build_intersection_cache(
            num_bbox=num_bbox,
            city_filter=city,
            force_rebuild=False  # 如果已有缓存则跳过
        )
        
        print(f"✅ 缓存构建完成！")
        print(f"   - 缓存记录数: {cached_count}")
        print(f"   - 构建耗时: {build_stats['build_time']:.2f}秒")
        print(f"   - 使用策略: {build_stats['strategy']}")
        
    except Exception as e:
        print(f"❌ 缓存构建失败: {e}")
        return
    
    # 2. 基础统计分析
    print(f"\n📈 第2步：基础统计分析")
    print("-" * 40)
    
    try:
        # 总体统计
        overall_stats = spatial_join.analyze_intersections(city_filter=city)
        print("总体统计:")
        print(overall_stats.to_string(index=False))
        
    except Exception as e:
        print(f"❌ 基础统计失败: {e}")
    
    # 3. 按路口类型分组分析
    print(f"\n🏗️ 第3步：按路口类型分组分析")
    print("-" * 40)
    
    try:
        type_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["intersection_type"]
        )
        
        print("按路口类型统计:")
        print(type_analysis.to_string(index=False))
        
        # 找出最常见的路口类型
        if not type_analysis.empty:
            top_type = type_analysis.loc[type_analysis['intersection_count'].idxmax()]
            from src.spdatalab.fusion.spatial_join_production import INTERSECTION_TYPE_MAPPING
            type_name = INTERSECTION_TYPE_MAPPING.get(top_type['intersection_type'], '未知')
            print(f"\n💡 最常见路口类型: {top_type['intersection_type']}({type_name}) - {top_type['intersection_count']}个相交")
        
    except Exception as e:
        print(f"❌ 路口类型分析失败: {e}")
    
    # 3.5. 按路口子类型分组分析
    print(f"\n🏗️ 第3.5步：按路口子类型分组分析")
    print("-" * 40)
    
    try:
        subtype_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["intersection_subtype"]
        )
        
        print("按路口子类型统计:")
        print(subtype_analysis.to_string(index=False))
        
        # 找出最常见的路口子类型
        if not subtype_analysis.empty:
            top_subtype = subtype_analysis.loc[subtype_analysis['intersection_count'].idxmax()]
            from src.spdatalab.fusion.spatial_join_production import INTERSECTION_SUBTYPE_MAPPING
            subtype_name = INTERSECTION_SUBTYPE_MAPPING.get(top_subtype['intersection_subtype'], '未知')
            print(f"\n💡 最常见路口子类型: {top_subtype['intersection_subtype']}({subtype_name}) - {top_subtype['intersection_count']}个相交")
        
    except Exception as e:
        print(f"❌ 路口子类型分析失败: {e}")
    
    # 4. 按场景分组分析（展示前10个）
    print(f"\n🎬 第4步：按场景分组分析")
    print("-" * 40)
    
    try:
        scene_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["scene_token"]
        )
        
        print("按场景统计 (前10个):")
        print(scene_analysis.head(10).to_string(index=False))
        
        # 找出相交最多的场景
        if not scene_analysis.empty:
            top_scene = scene_analysis.loc[scene_analysis['intersection_count'].idxmax()]
            print(f"\n💡 相交最多的场景: {top_scene['scene_token']} ({top_scene['intersection_count']}个相交)")
        
    except Exception as e:
        print(f"❌ 场景分析失败: {e}")
    
    # 5. 特定路口类型分析
    print(f"\n🎯 第5步：特定路口类型深度分析")
    print("-" * 40)
    
    try:
        # 先查看实际有哪些路口类型（数字）
        type_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["intersection_type"]
        )
        
        if not type_analysis.empty:
            print("实际的路口类型及其数量:")
            print(type_analysis.to_string(index=False))
            
            # 选择前几个最常见的路口类型进行分析（使用实际的数字）
            top_types = type_analysis.nlargest(3, 'intersection_count')['intersection_type'].tolist()
            print(f"\n选择分析的路口类型: {top_types}")
            
            specific_analysis = spatial_join.analyze_intersections(
                city_filter=city,
                intersection_types=top_types,  # 使用实际的数字类型
                group_by=["intersection_type", "scene_token"]
            )
        else:
            print("未找到路口类型数据")
            specific_analysis = None
        
        if specific_analysis is not None and not specific_analysis.empty:
            print(f"特定路口类型分析 (前15个):")
            print(specific_analysis.head(15).to_string(index=False))
        elif specific_analysis is not None:
            print("未找到指定类型的路口数据")
        
    except Exception as e:
        print(f"❌ 特定类型分析失败: {e}")
    
    # 6. 获取详细信息
    print(f"\n🔍 第6步：获取详细相交信息")
    print("-" * 40)
    
    try:
        details = spatial_join.get_intersection_details(
            city_filter=city,
            limit=5  # 只取5条做演示
        )
        
        print("详细相交信息 (前5条):")
        from src.spdatalab.fusion.spatial_join_production import INTERSECTION_TYPE_MAPPING, INTERSECTION_SUBTYPE_MAPPING
        for _, row in details.iterrows():
            type_name = INTERSECTION_TYPE_MAPPING.get(row['intersection_type'], '未知')
            subtype_name = INTERSECTION_SUBTYPE_MAPPING.get(row['intersection_subtype'], '未知')
            print(f"场景: {row['scene_token']}")
            print(f"  - 路口ID: {row['intersection_id']}")
            print(f"  - 路口类型: {row['intersection_type']}({type_name})")
            print(f"  - 路口子类型: {row['intersection_subtype']}({subtype_name})")
            print(f"  - 创建时间: {row['created_at']}")
            print()
        
    except Exception as e:
        print(f"❌ 详细信息获取失败: {e}")
    
    print("🎉 分析演示完成！")


def demo_performance_comparison():
    """演示缓存vs实时查询的性能对比"""
    
    print("\n⚡ 性能对比演示")
    print("=" * 60)
    
    config = SpatialJoinConfig(enable_cache_table=True)
    spatial_join = ProductionSpatialJoin(config)
    
    city = None  # 不使用城市过滤，避免数据不存在的问题
    num_bbox = 10  # 减少数量，加快测试
    
    # 1. 构建缓存并测量时间
    print("📊 构建缓存...")
    import time
    start_time = time.time()
    
    try:
        cached_count, _ = spatial_join.build_intersection_cache(
            num_bbox=num_bbox,
            city_filter=city,
            force_rebuild=True  # 强制重建以测量时间
        )
        cache_build_time = time.time() - start_time
        print(f"✅ 缓存构建完成: {cache_build_time:.2f}秒")
        
    except Exception as e:
        print(f"❌ 缓存构建失败: {e}")
        return
    
    # 2. 基于缓存的分析（多次测试取平均）
    print("\n📈 基于缓存的分析性能...")
    cache_times = []
    
    for i in range(3):
        start_time = time.time()
        try:
            result = spatial_join.analyze_intersections(
                city_filter=city,
                group_by=["intersection_type"]
            )
            cache_times.append(time.time() - start_time)
        except Exception as e:
            print(f"缓存查询失败: {e}")
            break
    
    if cache_times:
        avg_cache_time = sum(cache_times) / len(cache_times)
        print(f"✅ 平均查询时间: {avg_cache_time:.4f}秒")
    
    # 3. 实时查询对比
    print("\n🔄 实时查询性能...")
    try:
        start_time = time.time()
        result, stats = spatial_join.polygon_intersect(num_bbox, city_filter=city)
        realtime_query_time = time.time() - start_time
        print(f"✅ 实时查询时间: {realtime_query_time:.2f}秒")
        
        # 性能对比
        if cache_times:
            speedup = realtime_query_time / avg_cache_time
            print(f"\n💡 性能提升: 缓存比实时查询快 {speedup:.1f}x")
    
    except Exception as e:
        print(f"❌ 实时查询失败: {e}")


def demo_advanced_analysis():
    """演示高级分析场景"""
    
    print("\n🎓 高级分析场景演示")
    print("=" * 60)
    
    spatial_join = ProductionSpatialJoin()
    city = None  # 不使用城市过滤
    
    # 分析场景1：路口热度分析
    print("🔥 场景1：路口热度分析")
    print("-" * 30)
    
    try:
        # 按路口ID分组，找出被最多bbox相交的路口
        hotspot_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["intersection_id", "intersection_type"]
        )
        
        if not hotspot_analysis.empty:
            # 排序找出热点路口
            hotspots = hotspot_analysis.nlargest(10, 'intersection_count')
            print("十大热点路口:")
            print(hotspots.to_string(index=False))
        
    except Exception as e:
        print(f"❌ 热点分析失败: {e}")
    
    # 分析场景2：场景复杂度分析
    print(f"\n🏙️ 场景2：场景复杂度分析")
    print("-" * 30)
    
    try:
        # 按场景分组，分析每个场景的路口多样性
        complexity_analysis = spatial_join.analyze_intersections(
            city_filter=city,
            group_by=["scene_token"]
        )
        
        if not complexity_analysis.empty:
            # 找出最复杂的场景（相交路口最多）
            complex_scenes = complexity_analysis.nlargest(5, 'intersection_count')
            print("最复杂的5个场景:")
            print(complex_scenes.to_string(index=False))
        
    except Exception as e:
        print(f"❌ 复杂度分析失败: {e}")


if __name__ == "__main__":
    print("🌟 空间连接分析全功能演示")
    print("=" * 80)
    
    try:
        # 首先探索数据格式
        sample_city = explore_data_format()
        
        # 主要工作流程演示
        demo_cache_workflow(sample_city)
        
        # 性能对比演示
        demo_performance_comparison()
        
        # 高级分析演示
        demo_advanced_analysis()
        
        print("\n" + "=" * 80)
        print("✨ 演示完成！现在你可以:")
        print("   1. 使用 build_cache() 构建相交关系缓存")
        print("   2. 使用 analyze_cached_intersections() 进行快速分析")
        print("   3. 使用 get_intersection_details() 获取详细信息")
        print("   4. 根据需要组合各种过滤和分组条件")
        
    except Exception as e:
        print(f"❌ 演示过程中出现错误: {e}")
        logger.exception("详细错误信息:") 