#!/usr/bin/env python3
"""
空间冗余分析脚本
===============================

功能：
1. 创建city_grid_density基础表
2. 计算城市空间冗余度指标

使用方法：
    # 首次运行：创建表
    python analyze_spatial_redundancy.py --create-table
    
    # 分析冗余度（默认top1%）
    python analyze_spatial_redundancy.py
    
    # 分析top5%
    python analyze_spatial_redundancy.py --top-percent 5
    
    # 指定城市
    python analyze_spatial_redundancy.py --cities A263 B001
    
    # 导出CSV
    python analyze_spatial_redundancy.py --export-csv
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
import pandas as pd

# 常量定义
# Grid 大小：0.002度 × 0.002度
GRID_SIZE_DEGREES = 0.002
# 1度约等于111km（在赤道附近）
KM_PER_DEGREE = 111.0
# 单个 grid 的面积（km²）
SINGLE_GRID_AREA_KM2 = (GRID_SIZE_DEGREES * KM_PER_DEGREE) ** 2  # ≈ 0.049 km²


def create_density_table(conn):
    """创建city_grid_density基础表"""
    
    print("🔨 创建 city_grid_density 表...")
    
    create_sql = text("""
        -- 创建城市网格密度表
        CREATE TABLE IF NOT EXISTS city_grid_density (
            id SERIAL PRIMARY KEY,
            city_id VARCHAR(50) NOT NULL,
            analysis_date DATE DEFAULT CURRENT_DATE,
            grid_x INTEGER NOT NULL,
            grid_y INTEGER NOT NULL,
            grid_size NUMERIC DEFAULT 0.002,
            bbox_count INTEGER NOT NULL,
            subdataset_count INTEGER,
            scene_count INTEGER,
            involved_subdatasets TEXT[],
            involved_scenes TEXT[],
            total_bbox_area NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(city_id, analysis_date, grid_x, grid_y)
        );
        
        -- 添加几何列
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'city_grid_density' AND column_name = 'geometry'
            ) THEN
                PERFORM AddGeometryColumn('public', 'city_grid_density', 'geometry', 4326, 'POLYGON', 2);
            END IF;
        END $$;
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_city_grid_city_date ON city_grid_density (city_id, analysis_date);
        CREATE INDEX IF NOT EXISTS idx_city_grid_bbox_count ON city_grid_density (bbox_count DESC);
        CREATE INDEX IF NOT EXISTS idx_city_grid_scene_count ON city_grid_density (scene_count DESC);
        CREATE INDEX IF NOT EXISTS idx_city_grid_geom ON city_grid_density USING GIST (geometry);
    """)
    
    conn.execute(create_sql)
    conn.commit()
    print("✅ 表创建成功")


def calculate_city_redundancy(conn, city_id: str, top_percent: float = 1.0):
    """计算单个城市的冗余度指标
    
    使用 grid 面积统一计算，避免分子分母不一致的问题。
    """
    
    # 1. 城市总体统计（只需要 scene 和 bbox 数量）
    total_sql = text("""
        SELECT 
            COUNT(DISTINCT scene_token) as total_scenes,
            COUNT(*) as total_bboxes
        FROM clips_bbox_unified
        WHERE city_id = :city_id AND all_good = true
    """)
    
    total = conn.execute(total_sql, {'city_id': city_id}).fetchone()
    
    if not total or total.total_scenes == 0:
        return None
    
    # 2. 获取该城市有数据的 grid 统计
    grid_count_sql = text("""
        SELECT COUNT(*) FROM city_grid_density
        WHERE city_id = :city_id AND analysis_date = CURRENT_DATE
    """)
    grid_count = conn.execute(grid_count_sql, {'city_id': city_id}).scalar()
    
    if not grid_count or grid_count == 0:
        return None
    
    # 计算 top N% 对应的 grid 数量
    top_n = max(1, int(grid_count * top_percent / 100.0))
    
    # 3. 通过空间连接计算 top N% 网格内的实际 scene 数
    hotspot_sql = text("""
        WITH top_grids AS (
            SELECT geometry
            FROM city_grid_density
            WHERE city_id = :city_id AND analysis_date = CURRENT_DATE
            ORDER BY bbox_count DESC
            LIMIT :top_n
        )
        SELECT 
            COUNT(DISTINCT b.scene_token) as hotspot_scenes,
            COUNT(b.*) as hotspot_bboxes
        FROM top_grids tg
        LEFT JOIN clips_bbox_unified b ON ST_Intersects(tg.geometry, b.geometry)
        WHERE b.city_id = :city_id AND b.all_good = true
    """)
    
    hotspot = conn.execute(hotspot_sql, {
        'city_id': city_id,
        'top_n': top_n
    }).fetchone()
    
    if not hotspot:
        return None
    
    # 4. 使用 grid 面积统一计算指标
    # 分母：所有有数据的 grid 的总面积
    total_grid_area_km2 = grid_count * SINGLE_GRID_AREA_KM2
    
    # 分子：top N% grid 的总面积
    hotspot_grid_area_km2 = top_n * SINGLE_GRID_AREA_KM2
    
    # 面积百分比（理论上应该接近 top_percent）
    area_pct = (top_n / grid_count) * 100 if grid_count > 0 else 0
    
    # Scene 百分比
    scene_pct = (hotspot.hotspot_scenes / total.total_scenes) * 100 if total.total_scenes > 0 else 0
    
    # BBox 百分比
    bbox_pct = (hotspot.hotspot_bboxes / total.total_bboxes) * 100 if total.total_bboxes > 0 else 0
    
    # 冗余指数 = scene占比 / 面积占比
    redundancy = scene_pct / area_pct if area_pct > 0 else 0
    
    return {
        'city_id': city_id,
        'total_scenes': int(total.total_scenes),
        'total_bboxes': int(total.total_bboxes),
        'total_grids': grid_count,
        'total_grid_area_km2': round(total_grid_area_km2, 2),
        'top_n_grids': top_n,
        'hotspot_grid_area_km2': round(hotspot_grid_area_km2, 2),
        'hotspot_scenes': int(hotspot.hotspot_scenes),
        'hotspot_bboxes': int(hotspot.hotspot_bboxes),
        'area_percentage': round(area_pct, 2),
        'scene_percentage': round(scene_pct, 2),
        'bbox_percentage': round(bbox_pct, 2),
        'redundancy_index': round(redundancy, 2),
    }


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='空间冗余分析')
    parser.add_argument('--create-table', action='store_true',
                       help='创建city_grid_density表')
    parser.add_argument('--top-percent', type=float, default=1.0,
                       help='分析的top百分比（默认1%%）')
    parser.add_argument('--cities', nargs='+',
                       help='指定分析的城市列表')
    parser.add_argument('--export-csv', action='store_true',
                       help='导出CSV报告')
    
    args = parser.parse_args()
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    try:
        with engine.connect() as conn:
            
            # 如果需要创建表
            if args.create_table:
                create_density_table(conn)
                print("\n💡 表创建完成，现在可以运行分析：")
                print("   1. 先生成grid数据: python batch_grid_analysis.py")
                print("   2. 再分析冗余: python analyze_spatial_redundancy.py")
                return 0
            
            # 否则进行冗余分析
            print(f"🚀 空间冗余分析 (Top {args.top_percent}%)")
            print("=" * 60)
            
            # 获取城市列表
            if args.cities:
                cities = args.cities
                print(f"🎯 分析指定城市: {cities}")
            else:
                # 按 scene 数量从多到少排序
                result = conn.execute(text("""
                    SELECT 
                        cgd.city_id,
                        COUNT(DISTINCT cbu.scene_token) as scene_count
                    FROM city_grid_density cgd
                    LEFT JOIN clips_bbox_unified cbu 
                        ON cgd.city_id = cbu.city_id AND cbu.all_good = true
                    WHERE cgd.analysis_date = CURRENT_DATE
                    GROUP BY cgd.city_id
                    ORDER BY scene_count DESC, cgd.city_id
                """))
                cities = [row.city_id for row in result]
                print(f"📊 分析所有城市: 共 {len(cities)} 个（按scene数量排序）")
            
            if not cities:
                print("\n❌ 没有找到城市数据")
                print("💡 提示:")
                print("   1. 先运行: python analyze_spatial_redundancy.py --create-table")
                print("   2. 再运行: python batch_grid_analysis.py")
                print("   3. 最后运行: python analyze_spatial_redundancy.py")
                return 1
            
            # 逐城市分析
            print(f"\n🔄 计算冗余度指标...\n")
            results = []
            
            for city_id in cities:
                metrics = calculate_city_redundancy(conn, city_id, args.top_percent)
                if metrics:
                    results.append(metrics)
                    print(f"✓ {city_id}: 冗余指数 {metrics['redundancy_index']} "
                          f"({metrics['area_percentage']:.1f}%面积[{metrics['top_n_grids']}/{metrics['total_grids']}grid] "
                          f"→ {metrics['scene_percentage']:.1f}%场景[{metrics['hotspot_scenes']}/{metrics['total_scenes']}])")
                else:
                    print(f"✗ {city_id}: 无数据")
            
            if not results:
                print("\n❌ 没有获取到任何数据")
                return 1
            
            # 汇总统计
            df = pd.DataFrame(results)
            
            print(f"\n" + "=" * 60)
            print(f"📈 汇总统计")
            print(f"=" * 60)
            print(f"分析城市数: {len(df)}")
            print(f"总场景数: {df['total_scenes'].sum():,}")
            print(f"总网格数: {df['total_grids'].sum():,}")
            print(f"平均冗余指数: {df['redundancy_index'].mean():.2f}")
            print(f"中位数: {df['redundancy_index'].median():.2f}")
            print(f"范围: {df['redundancy_index'].min():.2f} ~ {df['redundancy_index'].max():.2f}")
            
            # 冗余度分级
            severe = len(df[df['redundancy_index'] >= 20])
            moderate = len(df[(df['redundancy_index'] >= 10) & (df['redundancy_index'] < 20)])
            normal = len(df[df['redundancy_index'] < 10])
            
            print(f"\n冗余度分级:")
            print(f"  - 严重冗余 (≥20): {severe} 个城市")
            print(f"  - 中度冗余 (10-20): {moderate} 个城市")
            print(f"  - 合理范围 (<10): {normal} 个城市")
            
            # Top 5
            print(f"\n🔝 Top 5 高冗余城市:")
            top5 = df.nlargest(5, 'redundancy_index')
            for i, row in enumerate(top5.itertuples(), 1):
                print(f"  {i}. {row.city_id}: 冗余指数 {row.redundancy_index} "
                      f"({row.area_percentage:.1f}%面积[{row.top_n_grids}grid/{row.total_grid_area_km2:.1f}km²] "
                      f"包含{row.scene_percentage:.1f}%场景[{row.hotspot_scenes}/{row.total_scenes}])")
            
            # 导出CSV
            if args.export_csv:
                output_file = f'redundancy_report_top{int(args.top_percent)}pct.csv'
                df_sorted = df.sort_values('redundancy_index', ascending=False)
                df_sorted.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"\n📄 已导出: {output_file}")
            
            print(f"\n💡 计算方法说明:")
            print(f"   - 面积计算：使用网格(grid)面积统一计算")
            print(f"   - 单个grid：{GRID_SIZE_DEGREES}° × {GRID_SIZE_DEGREES}° ≈ {SINGLE_GRID_AREA_KM2:.3f} km²")
            print(f"   - 冗余指数：scene占比 / 面积占比（越高表示数据越集中）")
            print(f"\n💡 下一步:")
            print(f"   - 在Jupyter Notebook中进行可视化分析")
            print(f"   - 在QGIS中加载 city_grid_density 表查看空间分布")
            print("=" * 60)
            
    except Exception as e:
        print(f"\n❌ 分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

