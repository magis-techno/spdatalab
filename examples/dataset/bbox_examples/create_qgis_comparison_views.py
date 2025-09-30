#!/usr/bin/env python3
"""
简化版QGIS对比视图创建器
========================

基于现有分析结果创建QGIS可视化视图，不重新运行分析

使用方法：
    # 1. 先运行现有分析
    python run_overlap_analysis.py --city A263 --top-percent 10
    
    # 2. 创建对比视图
    python create_qgis_comparison_views.py --city A263
"""

import sys
from pathlib import Path
import argparse

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from sqlalchemy import create_engine, text

def create_simple_dbscan_view(engine, city_id):
    """创建简单的DBSCAN对比视图"""
    
    dbscan_view_sql = text(f"""
        CREATE OR REPLACE VIEW qgis_dbscan_hotspots AS
        WITH dbscan_clusters AS (
            SELECT 
                ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() as cluster_id,
                geometry
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        cluster_summary AS (
            SELECT 
                cluster_id,
                COUNT(*) as bbox_count,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_geom
            FROM dbscan_clusters 
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY bbox_count DESC) as id,
            cluster_id,
            bbox_count,
            'DBSCAN' as method_name,
            '#4ECDC4' as method_color,
            CASE 
                WHEN bbox_count >= 50 THEN 'Very High'
                WHEN bbox_count >= 20 THEN 'High'
                WHEN bbox_count >= 10 THEN 'Medium'
                ELSE 'Low'
            END as density_level,
            cluster_geom as geometry
        FROM cluster_summary
        ORDER BY bbox_count DESC;
    """)
    
    with engine.connect() as conn:
        conn.execute(dbscan_view_sql)
        conn.commit()
    
    print("✅ DBSCAN对比视图创建成功")

def create_grid_comparison_view(engine, city_id):
    """基于现有网格分析结果创建对比视图"""
    
    # 检查是否有现有的网格分析结果
    check_sql = text("""
        SELECT COUNT(*) as result_count
        FROM bbox_overlap_analysis_results 
        WHERE analysis_params::json->>'analysis_type' = 'bbox_density'
        AND analysis_params::json->>'city_filter' = :city_id
        ORDER BY created_at DESC
        LIMIT 1;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(check_sql, {'city_id': city_id}).fetchone()
        
        if result.result_count > 0:
            # 基于现有结果创建视图
            grid_view_sql = text(f"""
                CREATE OR REPLACE VIEW qgis_grid_hotspots AS
                SELECT 
                    id,
                    hotspot_rank,
                    overlap_count as bbox_count,
                    'Grid' as method_name,
                    '#FF6B6B' as method_color,
                    CASE 
                        WHEN overlap_count >= 50 THEN 'Very High'
                        WHEN overlap_count >= 20 THEN 'High'
                        WHEN overlap_count >= 10 THEN 'Medium'
                        ELSE 'Low'
                    END as density_level,
                    geometry
                FROM bbox_overlap_analysis_results
                WHERE analysis_params::json->>'analysis_type' = 'bbox_density'
                AND analysis_params::json->>'city_filter' = '{city_id}'
                AND created_at = (
                    SELECT MAX(created_at) 
                    FROM bbox_overlap_analysis_results 
                    WHERE analysis_params::json->>'city_filter' = '{city_id}'
                )
                ORDER BY hotspot_rank;
            """)
            
            conn.execute(grid_view_sql)
            conn.commit()
            print("✅ 网格对比视图创建成功（基于现有结果）")
        else:
            print("⚠️ 未找到现有网格分析结果，请先运行：")
            print(f"   python run_overlap_analysis.py --city {city_id}")

def create_unified_comparison_view(engine):
    """创建统一的对比视图"""
    
    unified_view_sql = text("""
        CREATE OR REPLACE VIEW qgis_method_comparison_unified AS
        
        -- 网格方法结果
        SELECT 
            'grid_' || id::text as unique_id,
            id,
            hotspot_rank as rank,
            overlap_count as bbox_count,
            'Grid' as method_name,
            '#FF6B6B' as method_color,
            CASE 
                WHEN overlap_count >= 50 THEN 'Very High'
                WHEN overlap_count >= 20 THEN 'High'
                WHEN overlap_count >= 10 THEN 'Medium'
                ELSE 'Low'
            END as density_level,
            'Grid_' || hotspot_rank || ' (' || overlap_count || ')' as display_label,
            geometry
        FROM qgis_grid_hotspots
        WHERE method_name = 'Grid'
        
        UNION ALL
        
        -- DBSCAN方法结果  
        SELECT 
            'dbscan_' || id::text as unique_id,
            id,
            id as rank,
            bbox_count,
            'DBSCAN' as method_name,
            '#4ECDC4' as method_color,
            density_level,
            'DBSCAN_' || id || ' (' || bbox_count || ')' as display_label,
            geometry
        FROM qgis_dbscan_hotspots
        WHERE method_name = 'DBSCAN'
        
        ORDER BY method_name, rank;
    """)
    
    with engine.connect() as conn:
        conn.execute(unified_view_sql)
        conn.commit()
    
    print("✅ 统一对比视图创建成功")

def print_qgis_guide(city_id):
    """输出QGIS使用指南"""
    
    print(f"\n🎨 QGIS可视化指南")
    print("=" * 50)
    print(f"📋 数据库连接信息:")
    print(f"   Host: local_pg")
    print(f"   Port: 5432")
    print(f"   Database: postgres")
    print(f"   Username: postgres")
    print(f"")
    print(f"📊 推荐加载的图层:")
    print(f"   1. qgis_method_comparison_unified - 统一对比视图 ⭐")
    print(f"   2. qgis_grid_hotspots - 网格方法结果")
    print(f"   3. qgis_dbscan_hotspots - DBSCAN方法结果")
    print(f"   4. clips_bbox_unified - 原始bbox数据（底图）")
    print(f"")
    print(f"🎨 可视化建议:")
    print(f"   • 主键: unique_id (统一视图) 或 id (单独视图)")
    print(f"   • 几何列: geometry")
    print(f"   • 按 method_name 字段分类显示")
    print(f"   • 按 method_color 字段设置颜色:")
    print(f"     - 网格方法: 红色 (#FF6B6B)")
    print(f"     - DBSCAN: 青色 (#4ECDC4)")
    print(f"   • 显示 display_label 标签")
    print(f"   • 按 density_level 设置符号大小")
    print(f"")
    print(f"🔍 对比分析要点:")
    print(f"   1. 覆盖差异 - 哪些区域只被某种方法识别？")
    print(f"   2. 形状差异 - 网格方形 vs DBSCAN自然形状")
    print(f"   3. 密度分布 - 各方法的热点数量和强度")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='创建QGIS方法对比视图')
    parser.add_argument('--city', default='A263', help='分析城市ID')
    
    args = parser.parse_args()
    
    print("🎯 创建QGIS方法对比视图")
    print("=" * 50)
    print(f"目标城市: {args.city}")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    try:
        # 1. 创建DBSCAN对比视图
        create_simple_dbscan_view(engine, args.city)
        
        # 2. 创建网格对比视图（基于现有结果）
        create_grid_comparison_view(engine, args.city)
        
        # 3. 创建统一对比视图
        create_unified_comparison_view(engine)
        
        # 4. 输出使用指南
        print_qgis_guide(args.city)
        
        print(f"\n✅ 对比视图创建完成！")
        print(f"💡 现在可以在QGIS中加载这些视图进行对比分析")
        
    except Exception as e:
        print(f"\n❌ 创建失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
