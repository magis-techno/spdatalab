#!/usr/bin/env python3
"""
增强版QGIS可视化指导
支持多层次路口可视化分析
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig,
    export_analysis_to_qgis,
    export_intersection_details_for_qgis,
    get_high_density_scenes,
    get_available_cities,
    get_qgis_connection_info,
    build_cache
)
import pandas as pd
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    print("🎯 增强版QGIS可视化数据准备")
    print("=" * 60)
    
    # 1. 获取可用城市
    print("\n📍 获取可用城市...")
    cities_df = get_available_cities()
    if cities_df.empty or 'city_id' not in cities_df.columns:
        print("❌ 无法获取城市数据")
        return
    
    sample_city = cities_df.iloc[0]['city_id']
    print(f"使用示例城市: {sample_city}")
    print(f"可用城市: {cities_df['city_id'].tolist()[:5]}")
    
    # 2. 确保缓存数据存在
    print(f"\n🔄 确保缓存数据存在...")
    cached_count, _ = build_cache(100, city_filter=sample_city)
    print(f"缓存数据: {cached_count} 条记录")
    
    # 3. 导出统计分析（不会清空表）
    print(f"\n📊 导出统计分析数据...")
    
    # 路口类型分析
    analysis_id1 = export_analysis_to_qgis(
        analysis_type="intersection_type",
        city_filter=sample_city,
        include_geometry=True
    )
    print(f"✅ 路口类型分析: {analysis_id1}")
    
    # 路口子类型分析
    analysis_id2 = export_analysis_to_qgis(
        analysis_type="intersection_subtype",
        city_filter=sample_city,
        include_geometry=True
    )
    print(f"✅ 路口子类型分析: {analysis_id2}")
    
    # 4. 找到高密度场景
    print(f"\n🔍 查找路口密度最高的场景...")
    high_density_scenes = get_high_density_scenes(city_filter=sample_city, top_n=5)
    print("路口密度最高的5个场景:")
    print(high_density_scenes.to_string(index=False))
    
    # 5. 导出具体场景的路口详情
    if not high_density_scenes.empty:
        top_scene = high_density_scenes.iloc[0]['scene_token']
        print(f"\n🎯 导出最高密度场景的路口详情: {top_scene}")
        
        details_id1 = export_intersection_details_for_qgis(
            scene_tokens=[top_scene],
            export_id=f"scene_{top_scene}_details"
        )
        print(f"✅ 场景详情导出: {details_id1}")
        
        # 导出该场景的特定类型路口
        details_id2 = export_intersection_details_for_qgis(
            scene_tokens=[top_scene],
            intersection_types=[1, 4],  # Intersection 和 T-Junction Area
            export_id=f"scene_{top_scene}_main_types"
        )
        print(f"✅ 主要路口类型详情: {details_id2}")
    
    # 6. 抽样导出全城市路口（用于整体分布分析）
    print(f"\n🎲 抽样导出全城市路口分布...")
    sample_id = export_intersection_details_for_qgis(
        city_filter=sample_city,
        sample_size=200,
        export_id=f"city_{sample_city}_sample"
    )
    print(f"✅ 城市抽样路口: {sample_id}")
    
    # 7. 导出特定类型路口的全市分布
    print(f"\n🚦 导出特定路口类型的全市分布...")
    roundabout_id = export_intersection_details_for_qgis(
        city_filter=sample_city,
        intersection_types=[5],  # Roundabout
        export_id=f"city_{sample_city}_roundabouts"
    )
    print(f"✅ 环岛分布: {roundabout_id}")
    
    # 8. 提供QGIS使用指导
    print(f"\n🎯 QGIS可视化指导")
    print("=" * 40)
    
    conn_info = get_qgis_connection_info()
    
    print(f"📋 数据库连接信息:")
    print(f"   主机: {conn_info.get('host')}")
    print(f"   端口: {conn_info.get('port')}")
    print(f"   数据库: {conn_info.get('database')}")
    print(f"   用户名: {conn_info.get('username')}")
    
    print(f"\n📊 可用的可视化图层:")
    print(f"   1. clips_bbox - 场景边界框")
    print(f"   2. spatial_analysis_results - 统计分析结果")
    print(f"   3. intersection_details_view - 路口详细信息")
    
    print(f"\n🎨 推荐的可视化方案:")
    print(f"   方案1 - 整体概览:")
    print(f"     • 加载 clips_bbox 作为底图")
    print(f"     • 加载 spatial_analysis_results 显示统计区域")
    print(f"     • 使用颜色区分不同路口类型的统计结果")
    
    print(f"   方案2 - 详细分析:")
    print(f"     • 加载 clips_bbox 显示场景边界")
    print(f"     • 加载 intersection_details_view 显示具体路口")
    print(f"     • 按 export_id 过滤显示不同的导出结果")
    print(f"     • 按 type_name 分类显示不同路口类型")
    
    print(f"   方案3 - 对比分析:")
    print(f"     • 使用不同颜色显示不同导出批次的数据")
    print(f"     • 对比场景级和城市级的路口分布")
    print(f"     • 分析高密度区域的路口类型构成")
    
    print(f"\n💡 QGIS操作提示:")
    print(f"   • 在图层面板中按 export_id 进行分组")
    print(f"   • 使用属性表筛选特定的场景或路口类型")
    print(f"   • 设置不同的符号样式区分路口类型")
    print(f"   • 使用标注显示路口ID或类型名称")
    
    print(f"\n🔍 数据筛选示例:")
    print(f"   • export_id = 'scene_{top_scene}_details' - 查看特定场景")
    print(f"   • type_name = 'Roundabout' - 查看所有环岛")
    print(f"   • intersection_count > 10 - 查看高密度统计区域")
    
    print(f"\n✅ 数据准备完成！")
    print(f"现在可以在QGIS中连接数据库并加载这些图层进行可视化分析。")

if __name__ == "__main__":
    main() 