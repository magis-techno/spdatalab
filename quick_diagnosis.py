#!/usr/bin/env python3
"""
快速诊断用户报告的问题：
- overlap_count 都是1
- scene 都是2  
- hotspot_rank 不连续且很大（4000多、5万多）
"""

import sys
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
        print("❌ 无法导入 LOCAL_DSN")
        sys.exit(1)

def main():
    print("🔍 快速诊断用户问题")
    print("=" * 50)
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    with engine.connect() as conn:
        
        # 1. 检查最近的分析结果
        print("📋 检查最近的分析结果...")
        recent_sql = text("""
            SELECT 
                analysis_id,
                COUNT(*) as total_results,
                MIN(hotspot_rank) as min_rank,
                MAX(hotspot_rank) as max_rank,
                MIN(overlap_count) as min_overlap_count,
                MAX(overlap_count) as max_overlap_count,
                created_at
            FROM bbox_overlap_analysis_results
            WHERE created_at > NOW() - INTERVAL '1 day'
            GROUP BY analysis_id, created_at
            ORDER BY created_at DESC
            LIMIT 5;
        """)
        
        recent_df = pd.read_sql(recent_sql, conn)
        if not recent_df.empty:
            print("最近的分析结果:")
            print(recent_df.to_string(index=False))
            
            # 详细检查最新的分析
            latest_analysis = recent_df.iloc[0]['analysis_id']
            print(f"\n🔍 详细检查最新分析: {latest_analysis}")
            
            detail_sql = text(f"""
                SELECT 
                    hotspot_rank,
                    overlap_count,
                    total_overlap_area,
                    subdataset_count,
                    scene_count,
                    CASE 
                        WHEN hotspot_rank <= 10 THEN 'TOP 10'
                        WHEN hotspot_rank <= 100 THEN 'TOP 100'
                        WHEN hotspot_rank <= 1000 THEN 'TOP 1000'
                        ELSE 'OTHERS'
                    END as rank_category
                FROM bbox_overlap_analysis_results
                WHERE analysis_id = '{latest_analysis}'
                ORDER BY hotspot_rank
                LIMIT 20;
            """)
            
            detail_df = pd.read_sql(detail_sql, conn)
            print("前20个结果:")
            print(detail_df.to_string(index=False))
            
            # 分析异常
            all_ranks = pd.read_sql(text(f"""
                SELECT hotspot_rank FROM bbox_overlap_analysis_results 
                WHERE analysis_id = '{latest_analysis}' 
                ORDER BY hotspot_rank
            """), conn)['hotspot_rank'].tolist()
            
            print(f"\n📊 Rank分析:")
            print(f"总数量: {len(all_ranks)}")
            print(f"Rank范围: {min(all_ranks)} ~ {max(all_ranks)}")
            print(f"是否连续: {'否' if max(all_ranks) > len(all_ranks) else '是'}")
            
            if max(all_ranks) > len(all_ranks):
                print(f"❌ 发现问题！Rank不连续")
                print(f"   应该是1-{len(all_ranks)}，但实际最大值是{max(all_ranks)}")
                
                # 找出缺失的rank
                expected = set(range(1, len(all_ranks) + 1))
                actual = set(all_ranks)
                missing = expected - actual
                if missing:
                    print(f"   缺失的rank: {sorted(list(missing))[:10]}...")
                
        else:
            print("❌ 没有找到最近的分析结果")
        
        # 2. 检查ROW_NUMBER()的实际行为
        print(f"\n🧪 测试ROW_NUMBER()逻辑...")
        test_sql = text("""
            WITH test_data AS (
                SELECT 
                    generate_series(1, 10) as id,
                    random() * 100 as value
            )
            SELECT 
                id,
                value,
                ROW_NUMBER() OVER (ORDER BY value DESC) as row_num_desc,
                ROW_NUMBER() OVER (ORDER BY id) as row_num_id
            FROM test_data
            ORDER BY value DESC;
        """)
        
        test_df = pd.read_sql(test_sql, conn)
        print("ROW_NUMBER()测试:")
        print(test_df.to_string(index=False))
        
        # 3. 检查是否有数据重复或过滤问题
        print(f"\n🔍 检查数据质量...")
        quality_sql = text(f"""
            WITH overlap_analysis AS (
                SELECT 
                    a.qgis_id as bbox_a_id,
                    b.qgis_id as bbox_b_id,
                    ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
                FROM clips_bbox_unified_qgis a
                JOIN clips_bbox_unified_qgis b ON a.qgis_id < b.qgis_id
                WHERE ST_Intersects(a.geometry, b.geometry)
                AND NOT ST_Equals(a.geometry, b.geometry)
                AND a.city_id = b.city_id
                AND a.city_id = 'A263'
                AND a.all_good = true
                AND b.all_good = true
                AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 0
            )
            SELECT 
                COUNT(*) as total_overlaps,
                COUNT(DISTINCT bbox_a_id) as distinct_a,
                COUNT(DISTINCT bbox_b_id) as distinct_b,
                MIN(overlap_area) as min_area,
                MAX(overlap_area) as max_area
            FROM overlap_analysis;
        """)
        
        quality_result = conn.execute(quality_sql).fetchone()
        print(f"数据质量检查:")
        print(f"  总重叠对数: {quality_result.total_overlaps}")
        print(f"  不同bbox_a: {quality_result.distinct_a}")
        print(f"  不同bbox_b: {quality_result.distinct_b}")
        print(f"  面积范围: {quality_result.min_area:.10f} ~ {quality_result.max_area:.10f}")

if __name__ == "__main__":
    main()
