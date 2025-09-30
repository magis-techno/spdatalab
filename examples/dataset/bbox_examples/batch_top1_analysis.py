#!/usr/bin/env python3
"""
批量城市Top1热点分析脚本
=======================

遍历所有城市，分析每个城市的top1重叠热点区域

使用方法：
    python examples/dataset/bbox_examples/batch_top1_analysis.py
    python examples/dataset/bbox_examples/batch_top1_analysis.py --min-bbox-count 1000
    python examples/dataset/bbox_examples/batch_top1_analysis.py --output-table city_top1_hotspots
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime
import time

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from sqlalchemy import create_engine, text
import pandas as pd

def get_all_cities(conn, min_bbox_count=500):
    """获取所有有足够bbox数据的城市"""
    
    print(f"🔍 查找有足够数据的城市（最少{min_bbox_count}个bbox）...")
    
    cities_sql = text(f"""
        SELECT 
            city_id,
            COUNT(*) as bbox_count,
            COUNT(*) FILTER (WHERE all_good = true) as good_bbox_count,
            ROUND((COUNT(*) FILTER (WHERE all_good = true) * 100.0 / COUNT(*))::numeric, 1) as quality_rate
        FROM clips_bbox_unified
        WHERE city_id IS NOT NULL 
        GROUP BY city_id
        HAVING COUNT(*) >= {min_bbox_count}
        ORDER BY COUNT(*) DESC;
    """)
    
    cities_df = pd.read_sql(cities_sql, conn)
    
    print(f"📊 找到 {len(cities_df)} 个符合条件的城市:")
    print(cities_df.to_string(index=False))
    
    return cities_df

def analyze_city_top1(conn, city_id, grid_size=0.002, density_threshold=5):
    """分析单个城市的top1热点"""
    
    print(f"\n🎯 分析城市: {city_id}")
    
    # 使用与run_overlap_analysis.py相同的网格分析逻辑
    analysis_sql = text(f"""
        WITH bbox_bounds AS (
            -- 🚀 第1步：提取bbox边界（一次性几何计算）
            SELECT 
                id,
                subdataset_name,
                scene_token,
                ST_XMin(geometry) as xmin,
                ST_XMax(geometry) as xmax,
                ST_YMin(geometry) as ymin,
                ST_YMax(geometry) as ymax
            FROM clips_bbox_unified
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        bbox_grid_coverage AS (
            -- 🎯 第2步：计算每个bbox覆盖的网格范围（纯数学计算）
            SELECT 
                id,
                subdataset_name,
                scene_token,
                floor(xmin / {grid_size})::int as min_grid_x,
                floor(xmax / {grid_size})::int as max_grid_x,
                floor(ymin / {grid_size})::int as min_grid_y,
                floor(ymax / {grid_size})::int as max_grid_y,
                (xmax - xmin) * (ymax - ymin) as bbox_area
            FROM bbox_bounds
        ),
        expanded_grid_coverage AS (
            -- 🔧 第3步：展开每个bbox到它覆盖的所有网格
            SELECT 
                id,
                subdataset_name,
                scene_token,
                bbox_area,
                grid_x,
                grid_y
            FROM bbox_grid_coverage,
            LATERAL generate_series(min_grid_x, max_grid_x) as grid_x,
            LATERAL generate_series(min_grid_y, max_grid_y) as grid_y
        ),
        grid_density_stats AS (
            -- 📊 第4步：统计每个网格的bbox密度
            SELECT 
                grid_x,
                grid_y,
                COUNT(*) as bbox_count_in_grid,
                COUNT(DISTINCT subdataset_name) as subdataset_count,
                COUNT(DISTINCT scene_token) as scene_count,
                ARRAY_AGG(DISTINCT subdataset_name) as involved_subdatasets,
                ARRAY_AGG(DISTINCT scene_token) as involved_scenes,
                SUM(bbox_area) as total_bbox_area,
                -- 🔧 按需生成网格几何
                ST_MakeEnvelope(
                    grid_x * {grid_size}, 
                    grid_y * {grid_size},
                    (grid_x + 1) * {grid_size}, 
                    (grid_y + 1) * {grid_size}, 
                    4326
                ) as grid_geom
            FROM expanded_grid_coverage
            GROUP BY grid_x, grid_y
            HAVING COUNT(*) >= {density_threshold}
        )
        -- 只返回TOP1热点
        SELECT 
            '{city_id}' as city_id,
            grid_x,
            grid_y,
            bbox_count_in_grid,
            subdataset_count,
            scene_count,
            involved_subdatasets,
            involved_scenes,
            total_bbox_area,
            grid_geom,
            '({grid_x},{grid_y})' as grid_coords
        FROM grid_density_stats
        ORDER BY bbox_count_in_grid DESC
        LIMIT 1;
    """)
    
    result = conn.execute(analysis_sql).fetchone()
    
    if result:
        print(f"   ✅ Top1热点: 网格({result.grid_x},{result.grid_y}), 密度={result.bbox_count_in_grid}")
        return result
    else:
        print(f"   ⚠️ 未找到符合条件的热点（密度阈值>={density_threshold}）")
        return None

def create_top1_results_table(conn, table_name):
    """创建top1结果表"""
    
    print(f"📋 创建结果表: {table_name}")
    
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            city_id VARCHAR(50) NOT NULL,
            analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- 网格信息
            grid_x INTEGER,
            grid_y INTEGER,
            grid_coords VARCHAR(50),
            
            -- 密度统计
            bbox_count INTEGER,
            subdataset_count INTEGER,
            scene_count INTEGER,
            total_bbox_area NUMERIC,
            
            -- 详细信息
            involved_subdatasets TEXT[],
            involved_scenes TEXT[],
            
            -- 分析参数
            analysis_params TEXT
        );
        
        -- 添加几何列
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND column_name = 'geometry'
            ) THEN
                PERFORM AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'GEOMETRY', 2);
            END IF;
        END $$;
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_{table_name}_city_id ON {table_name} (city_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_bbox_count ON {table_name} (bbox_count);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geom ON {table_name} USING GIST (geometry);
        
        -- 添加约束（每个城市每天只能有一条记录）
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique_city_date 
        ON {table_name} (city_id, DATE(analysis_time));
    """)
    
    conn.execute(create_sql)
    conn.commit()
    print(f"✅ 表 {table_name} 创建成功")

def save_top1_result(conn, table_name, city_result, analysis_params):
    """保存单个城市的top1结果"""
    
    if not city_result:
        return
    
    insert_sql = text(f"""
        INSERT INTO {table_name} 
        (city_id, grid_x, grid_y, grid_coords, bbox_count, subdataset_count, scene_count, 
         total_bbox_area, involved_subdatasets, involved_scenes, geometry, analysis_params)
        VALUES 
        (:city_id, :grid_x, :grid_y, :grid_coords, :bbox_count, :subdataset_count, :scene_count,
         :total_bbox_area, :involved_subdatasets, :involved_scenes, :geometry, :analysis_params);
    """)
    
    conn.execute(insert_sql, {
        'city_id': city_result.city_id,
        'grid_x': city_result.grid_x,
        'grid_y': city_result.grid_y,
        'grid_coords': city_result.grid_coords,
        'bbox_count': city_result.bbox_count_in_grid,
        'subdataset_count': city_result.subdataset_count,
        'scene_count': city_result.scene_count,
        'total_bbox_area': city_result.total_bbox_area,
        'involved_subdatasets': city_result.involved_subdatasets,
        'involved_scenes': city_result.involved_scenes,
        'geometry': city_result.grid_geom,
        'analysis_params': analysis_params
    })

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='批量城市Top1热点分析')
    parser.add_argument('--min-bbox-count', type=int, default=500, 
                       help='城市最少bbox数量阈值 (默认: 500)')
    parser.add_argument('--grid-size', type=float, default=0.002,
                       help='网格大小 (默认: 0.002度)')
    parser.add_argument('--density-threshold', type=int, default=5,
                       help='密度阈值 (默认: 5)')
    parser.add_argument('--output-table', default='city_top1_hotspots',
                       help='输出表名 (默认: city_top1_hotspots)')
    parser.add_argument('--cities', nargs='+', 
                       help='指定分析的城市列表，如: --cities A263 B001')
    
    args = parser.parse_args()
    
    print("🚀 批量城市Top1热点分析")
    print("=" * 50)
    print(f"参数配置:")
    print(f"  最少bbox数量: {args.min_bbox_count}")
    print(f"  网格大小: {args.grid_size}度")
    print(f"  密度阈值: {args.density_threshold}")
    print(f"  输出表: {args.output_table}")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    try:
        with engine.connect() as conn:
            
            # 创建结果表
            create_top1_results_table(conn, args.output_table)
            
            # 获取城市列表
            if args.cities:
                print(f"\n🎯 指定分析城市: {args.cities}")
                cities_to_analyze = args.cities
            else:
                cities_df = get_all_cities(conn, args.min_bbox_count)
                cities_to_analyze = cities_df['city_id'].tolist()
            
            if not cities_to_analyze:
                print("❌ 没有找到符合条件的城市")
                return 1
            
            # 分析参数
            analysis_params = f'{{"grid_size": {args.grid_size}, "density_threshold": {args.density_threshold}, "analysis_type": "top1_hotspot", "timestamp": "{datetime.now().isoformat()}"}}'
            
            # 批量分析
            print(f"\n🔄 开始批量分析 {len(cities_to_analyze)} 个城市...")
            
            successful_cities = []
            failed_cities = []
            
            start_time = time.time()
            
            for i, city_id in enumerate(cities_to_analyze, 1):
                print(f"\n[{i}/{len(cities_to_analyze)}] 处理城市: {city_id}")
                
                try:
                    # 分析城市top1
                    city_result = analyze_city_top1(
                        conn, city_id, 
                        args.grid_size, 
                        args.density_threshold
                    )
                    
                    if city_result:
                        # 保存结果
                        save_top1_result(conn, args.output_table, city_result, analysis_params)
                        successful_cities.append(city_id)
                        print(f"   ✅ 已保存到 {args.output_table}")
                    else:
                        failed_cities.append(city_id)
                        
                except Exception as e:
                    print(f"   ❌ 分析失败: {str(e)}")
                    failed_cities.append(city_id)
            
            conn.commit()
            
            # 统计结果
            total_time = time.time() - start_time
            
            print(f"\n" + "=" * 60)
            print(f"📊 批量分析完成！")
            print(f"=" * 60)
            print(f"总耗时: {total_time:.2f}秒")
            print(f"成功分析: {len(successful_cities)} 个城市")
            print(f"失败/无结果: {len(failed_cities)} 个城市")
            
            if successful_cities:
                print(f"\n✅ 成功的城市: {', '.join(successful_cities)}")
            
            if failed_cities:
                print(f"\n⚠️ 失败/无结果的城市: {', '.join(failed_cities)}")
            
            # 显示结果概览
            summary_sql = text(f"""
                SELECT 
                    city_id,
                    bbox_count,
                    subdataset_count,
                    scene_count,
                    grid_coords,
                    ROUND(total_bbox_area::numeric, 6) as total_bbox_area
                FROM {args.output_table}
                WHERE analysis_time::date = CURRENT_DATE
                ORDER BY bbox_count DESC;
            """)
            
            results_df = pd.read_sql(summary_sql, conn)
            
            if not results_df.empty:
                print(f"\n📋 今日Top1热点汇总:")
                print(results_df.to_string(index=False))
                
                print(f"\n🎯 QGIS可视化:")
                print(f"   表名: {args.output_table}")
                print(f"   主键: id")
                print(f"   几何列: geometry")
                print(f"   按 bbox_count 设置颜色（密度越高越热）")
                print(f"   显示 city_id 和 bbox_count 标签")
            
    except Exception as e:
        print(f"\n❌ 批量分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
