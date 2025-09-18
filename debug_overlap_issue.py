#!/usr/bin/env python3
"""
调试重叠分析问题的详细脚本
逐步分析SQL执行过程，找出问题根源
"""

import sys
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    try:
        from src.spdatalab.dataset.bbox import LOCAL_DSN
    except ImportError:
        print("❌ 无法导入 LOCAL_DSN，请检查模块路径")
        sys.exit(1)

def debug_overlap_analysis():
    """详细调试重叠分析过程"""
    print("🔍 开始调试重叠分析问题")
    print("=" * 60)
    
    engine = create_engine(LOCAL_DSN, future=True)
    city_filter = "A263"  # 用户报告问题的城市
    unified_view = "clips_bbox_unified_qgis"
    
    with engine.connect() as conn:
        
        # 步骤1: 检查基础数据
        print("\n📊 步骤1: 检查基础数据")
        print("-" * 40)
        
        basic_check_sql = text(f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(*) FILTER (WHERE city_id = '{city_filter}') as city_count,
                COUNT(*) FILTER (WHERE city_id = '{city_filter}' AND all_good = true) as city_good_count,
                COUNT(DISTINCT city_id) as distinct_cities,
                MIN(ST_Area(geometry)) as min_area,
                MAX(ST_Area(geometry)) as max_area,
                AVG(ST_Area(geometry)) as avg_area
            FROM {unified_view}
            WHERE city_id IS NOT NULL;
        """)
        
        basic_result = conn.execute(basic_check_sql).fetchone()
        print(f"总数据量: {basic_result.total_count:,}")
        print(f"城市{city_filter}数据量: {basic_result.city_count:,}")
        print(f"城市{city_filter}质量良好数据: {basic_result.city_good_count:,}")
        print(f"不同城市数: {basic_result.distinct_cities}")
        print(f"面积范围: {basic_result.min_area:.10f} ~ {basic_result.max_area:.10f}")
        print(f"平均面积: {basic_result.avg_area:.10f}")
        
        # 步骤2: 检查重叠对的生成
        print("\n🔗 步骤2: 检查重叠对生成（前10个样本）")
        print("-" * 40)
        
        sample_overlaps_sql = text(f"""
            WITH overlapping_pairs AS (
                SELECT 
                    a.qgis_id as bbox_a_id,
                    b.qgis_id as bbox_b_id,
                    a.subdataset_name as subdataset_a,
                    b.subdataset_name as subdataset_b,
                    a.scene_token as scene_a,
                    b.scene_token as scene_b,
                    ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area,
                    ST_Area(a.geometry) as area_a,
                    ST_Area(b.geometry) as area_b
                FROM {unified_view} a
                JOIN {unified_view} b ON a.qgis_id < b.qgis_id
                WHERE ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
                AND a.city_id = b.city_id
                AND a.city_id = '{city_filter}'
                AND a.all_good = true
                AND b.all_good = true
                AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0
                ORDER BY overlap_area DESC
                LIMIT 10
            )
            SELECT 
                bbox_a_id,
                bbox_b_id,
                ROUND(overlap_area::numeric, 12) as overlap_area,
                ROUND(area_a::numeric, 12) as area_a,
                ROUND(area_b::numeric, 12) as area_b,
                ROUND((overlap_area/area_a*100)::numeric, 2) as overlap_percent_a,
                ROUND((overlap_area/area_b*100)::numeric, 2) as overlap_percent_b,
                subdataset_a,
                subdataset_b,
                scene_a,
                scene_b
            FROM overlapping_pairs;
        """)
        
        sample_df = pd.read_sql(sample_overlaps_sql, conn)
        if not sample_df.empty:
            print("前10个重叠对样本:")
            print(sample_df.to_string(index=False))
        else:
            print("❌ 没有找到任何重叠对！")
            
        # 步骤3: 统计重叠对总数
        print("\n📈 步骤3: 统计重叠对总数")
        print("-" * 40)
        
        count_overlaps_sql = text(f"""
            SELECT 
                COUNT(*) as total_overlap_pairs,
                COUNT(*) FILTER (WHERE overlap_area > 0.000001) as pairs_over_1e6,
                COUNT(*) FILTER (WHERE overlap_area > 0.0001) as pairs_over_1e4,
                MIN(overlap_area) as min_overlap,
                MAX(overlap_area) as max_overlap,
                AVG(overlap_area) as avg_overlap
            FROM (
                SELECT ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
                FROM {unified_view} a
                JOIN {unified_view} b ON a.qgis_id < b.qgis_id
                WHERE ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
                AND a.city_id = b.city_id
                AND a.city_id = '{city_filter}'
                AND a.all_good = true
                AND b.all_good = true
                AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0
            ) overlap_stats;
        """)
        
        count_result = conn.execute(count_overlaps_sql).fetchone()
        print(f"总重叠对数: {count_result.total_overlap_pairs:,}")
        print(f"面积>1e-6的重叠对: {count_result.pairs_over_1e6:,}")
        print(f"面积>1e-4的重叠对: {count_result.pairs_over_1e4:,}")
        print(f"重叠面积范围: {count_result.min_overlap:.12f} ~ {count_result.max_overlap:.12f}")
        print(f"平均重叠面积: {count_result.avg_overlap:.12f}")
        
        # 步骤4: 测试不同的面积阈值
        print("\n🎯 步骤4: 测试不同面积阈值的影响")
        print("-" * 40)
        
        thresholds = [0, 0.000001, 0.00001, 0.0001, 0.001]
        for threshold in thresholds:
            threshold_sql = text(f"""
                SELECT COUNT(*) as count_at_threshold
                FROM {unified_view} a
                JOIN {unified_view} b ON a.qgis_id < b.qgis_id
                WHERE ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
                AND a.city_id = b.city_id
                AND a.city_id = '{city_filter}'
                AND a.all_good = true
                AND b.all_good = true
                AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {threshold};
            """)
            
            threshold_result = conn.execute(threshold_sql).fetchone()
            print(f"阈值 {threshold:>10}: {threshold_result.count_at_threshold:>8,} 个重叠对")
        
        # 步骤5: 检查现有分析结果表
        print("\n📋 步骤5: 检查现有分析结果")
        print("-" * 40)
        
        check_results_sql = text("""
            SELECT 
                analysis_id,
                COUNT(*) as result_count,
                MIN(hotspot_rank) as min_rank,
                MAX(hotspot_rank) as max_rank,
                MIN(overlap_count) as min_overlap_count,
                MAX(overlap_count) as max_overlap_count,
                MIN(total_overlap_area) as min_area,
                MAX(total_overlap_area) as max_area,
                created_at
            FROM bbox_overlap_analysis_results
            WHERE analysis_id LIKE '%A263%' OR analysis_id LIKE '%docker%'
            GROUP BY analysis_id, created_at
            ORDER BY created_at DESC
            LIMIT 5;
        """)
        
        try:
            results_df = pd.read_sql(check_results_sql, conn)
            if not results_df.empty:
                print("最近5次分析结果:")
                print(results_df.to_string(index=False))
                
                # 详细查看最新一次分析
                latest_analysis = results_df.iloc[0]['analysis_id']
                print(f"\n🔍 详细查看最新分析: {latest_analysis}")
                
                detail_sql = text(f"""
                    SELECT 
                        hotspot_rank,
                        overlap_count,
                        ROUND(total_overlap_area::numeric, 10) as total_overlap_area,
                        subdataset_count,
                        scene_count,
                        involved_subdatasets[1:2] as sample_subdatasets,
                        ST_AsText(ST_Centroid(geometry)) as centroid
                    FROM bbox_overlap_analysis_results
                    WHERE analysis_id = '{latest_analysis}'
                    ORDER BY hotspot_rank
                    LIMIT 10;
                """)
                
                detail_df = pd.read_sql(detail_sql, conn)
                print("前10个热点详情:")
                print(detail_df.to_string(index=False))
                
            else:
                print("❌ 没有找到相关分析结果")
        except Exception as e:
            print(f"⚠️ 查询分析结果时出错: {e}")
        
        # 步骤6: 测试SQL模板
        print("\n🧪 步骤6: 测试当前SQL逻辑")
        print("-" * 40)
        
        test_sql = text(f"""
            WITH overlapping_pairs AS (
                SELECT 
                    a.qgis_id as bbox_a_id,
                    b.qgis_id as bbox_b_id,
                    a.subdataset_name as subdataset_a,
                    b.subdataset_name as subdataset_b,
                    a.scene_token as scene_a,
                    b.scene_token as scene_b,
                    ST_Intersection(a.geometry, b.geometry) as overlap_geometry,
                    ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
                FROM {unified_view} a
                JOIN {unified_view} b ON a.qgis_id < b.qgis_id
                WHERE ST_Intersects(a.geometry, b.geometry)
                AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0
                AND NOT ST_Equals(a.geometry, b.geometry)
                AND a.city_id = b.city_id
                AND a.city_id = '{city_filter}'
                AND a.all_good = true
                AND b.all_good = true
            ),
            individual_overlaps AS (
                SELECT 
                    overlap_geometry as hotspot_geometry,
                    1 as overlap_count,
                    ARRAY[subdataset_a, subdataset_b] as involved_subdatasets,
                    ARRAY[scene_a, scene_b] as involved_scenes,
                    overlap_area as total_overlap_area,
                    CONCAT(bbox_a_id, '_', bbox_b_id) as pair_id
                FROM overlapping_pairs
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY total_overlap_area DESC) as hotspot_rank,
                overlap_count,
                ROUND(total_overlap_area::numeric, 10) as total_overlap_area,
                ARRAY_LENGTH(involved_subdatasets, 1) as subdataset_count,
                ARRAY_LENGTH(involved_scenes, 1) as scene_count,
                involved_subdatasets,
                involved_scenes,
                pair_id
            FROM individual_overlaps
            ORDER BY total_overlap_area DESC
            LIMIT 10;
        """)
        
        test_df = pd.read_sql(test_sql, conn)
        if not test_df.empty:
            print("当前SQL逻辑测试结果:")
            print(test_df.to_string(index=False))
            
            # 分析结果
            ranks = test_df['hotspot_rank'].tolist()
            overlaps = test_df['overlap_count'].tolist()
            scenes = test_df['scene_count'].tolist()
            
            print(f"\n📊 结果分析:")
            print(f"Rank分布: {ranks}")
            print(f"Overlap count分布: {set(overlaps)}")
            print(f"Scene count分布: {set(scenes)}")
            print(f"Rank是否连续: {'是' if ranks == list(range(1, len(ranks)+1)) else '否'}")
            
        else:
            print("❌ 当前SQL逻辑没有返回结果")

if __name__ == "__main__":
    debug_overlap_analysis()
