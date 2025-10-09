#!/usr/bin/env python3
"""
批量城市热点分析脚本
===============================

遍历所有城市，提取每个城市的top热点区域（支持固定数量或百分比）

使用方法：
    # 使用百分比（默认前1%）
    python examples/dataset/bbox_examples/batch_top1_analysis.py --top-percent 1
    
    # 使用固定数量
    python examples/dataset/bbox_examples/batch_top1_analysis.py --top-n 1
    
    # 自定义输出表
    python examples/dataset/bbox_examples/batch_top1_analysis.py --top-percent 5 --output-table city_top5pct_hotspots
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime
import subprocess
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

def get_all_cities(conn):
    """获取所有城市"""
    
    print(f"🔍 查找所有城市...")
    
    cities_sql = text("""
        SELECT 
            city_id,
            COUNT(*) as bbox_count,
            COUNT(*) FILTER (WHERE all_good = true) as good_bbox_count
        FROM clips_bbox_unified
        WHERE city_id IS NOT NULL 
        GROUP BY city_id
        ORDER BY COUNT(*) DESC;
    """)
    
    cities_df = pd.read_sql(cities_sql, conn)
    
    print(f"📊 找到 {len(cities_df)} 个城市:")
    print(cities_df.head(10).to_string(index=False))
    if len(cities_df) > 10:
        print(f"... 还有 {len(cities_df) - 10} 个城市")
    
    return cities_df['city_id'].tolist()

def analyze_city_with_existing_script(city_id, top_n=None, top_percent=None):
    """使用现有脚本分析单个城市的热点
    
    Args:
        city_id: 城市ID
        top_n: 固定数量（与top_percent互斥）
        top_percent: 百分比（与top_n互斥）
    """
    
    print(f"\n🎯 分析城市: {city_id}")
    
    try:
        # 调用现有的run_overlap_analysis.py脚本
        cmd = [
            'python', 
            'run_overlap_analysis.py',
            '--city', city_id,
            '--grid-size', '0.002',
            '--density-threshold', '5'
        ]
        
        # 添加top-n或top-percent参数
        if top_n is not None:
            cmd.extend(['--top-n', str(top_n)])
        elif top_percent is not None:
            cmd.extend(['--top-percent', str(top_percent)])
        else:
            # 默认使用1%
            cmd.extend(['--top-percent', '1'])
        
        print(f"   执行命令: {' '.join(cmd)}")
        
        # 在bbox_examples目录下执行
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        if result.returncode == 0:
            print(f"   ✅ 城市 {city_id} 分析成功")
            return True
        else:
            print(f"   ❌ 城市 {city_id} 分析失败:")
            print(f"   错误输出: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ 城市 {city_id} 分析超时（>5分钟）")
        return False
    except Exception as e:
        print(f"   ❌ 城市 {city_id} 分析异常: {str(e)}")
        return False

def create_top1_summary_table(conn, table_name):
    """创建top1汇总表"""
    
    print(f"📋 创建汇总表: {table_name}")
    
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            city_id VARCHAR(50) NOT NULL,
            analysis_id VARCHAR(100),
            bbox_count INTEGER,
            subdataset_count INTEGER,
            scene_count INTEGER,
            total_overlap_area NUMERIC,
            grid_coords TEXT,
            analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    """)
    
    conn.execute(create_sql)
    conn.commit()
    print(f"✅ 表 {table_name} 创建成功")

def extract_single_city_top1(conn, table_name, city_id):
    """从bbox_overlap_analysis_results中提取单个城市的top1结果"""
    
    # 先删除该城市今天的旧数据
    cleanup_sql = text(f"""
        DELETE FROM {table_name} 
        WHERE city_id = :city_id 
        AND analysis_time::date = CURRENT_DATE;
    """)
    conn.execute(cleanup_sql, {'city_id': city_id})
    
    # 提取该城市的top1热点
    extract_sql = text(f"""
        INSERT INTO {table_name} 
        (city_id, analysis_id, bbox_count, subdataset_count, scene_count, 
         total_overlap_area, geometry, grid_coords)
        SELECT 
            (analysis_params::json->>'city_filter') as city_id,
            analysis_id,
            overlap_count as bbox_count,
            subdataset_count,
            scene_count,
            total_overlap_area,
            geometry,
            (analysis_params::json->>'grid_coords') as grid_coords
        FROM bbox_overlap_analysis_results 
        WHERE analysis_id = (
            SELECT analysis_id
            FROM bbox_overlap_analysis_results
            WHERE analysis_params::json->>'city_filter' = :city_id
            AND analysis_time::date = CURRENT_DATE
            ORDER BY analysis_time DESC
            LIMIT 1
        );
    """)
    
    result = conn.execute(extract_sql, {'city_id': city_id})
    conn.commit()
    
    return result.rowcount

def extract_top1_results(conn, table_name):
    """从bbox_overlap_analysis_results中提取所有城市的top1结果"""
    
    print(f"📊 提取top1结果到 {table_name}...")
    
    # 先清空今天的数据
    cleanup_sql = text(f"""
        DELETE FROM {table_name} 
        WHERE analysis_time::date = CURRENT_DATE;
    """)
    conn.execute(cleanup_sql)
    
    # 提取每个城市的top1热点（hotspot_rank = 1）
    extract_sql = text(f"""
        INSERT INTO {table_name} 
        (city_id, analysis_id, bbox_count, subdataset_count, scene_count, 
         total_overlap_area, geometry, grid_coords)
        SELECT 
            -- 从analysis_params JSON中提取city_id
            (analysis_params::json->>'city_filter') as city_id,
            analysis_id,
            overlap_count as bbox_count,
            subdataset_count,
            scene_count,
            total_overlap_area,
            geometry,
            (analysis_params::json->>'grid_coords') as grid_coords
        FROM bbox_overlap_analysis_results 
        WHERE hotspot_rank = 1  -- 只要每个分析的top1
        AND analysis_time::date = CURRENT_DATE  -- 只要今天的分析
        AND analysis_params::json->>'city_filter' IS NOT NULL;
    """)
    
    result = conn.execute(extract_sql)
    conn.commit()
    
    extracted_count = result.rowcount
    print(f"✅ 提取了 {extracted_count} 个城市的top1热点")
    
    return extracted_count

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='批量城市热点分析')
    parser.add_argument('--output-table', default='city_hotspots',
                       help='输出汇总表名 (默认: city_hotspots)')
    parser.add_argument('--cities', nargs='+', 
                       help='指定分析的城市列表，如: --cities A263 B001')
    parser.add_argument('--max-cities', type=int, default=None,
                       help='最多分析城市数量 (默认: 无限制)')
    
    # 🎯 支持固定数量或百分比两种方式
    result_group = parser.add_mutually_exclusive_group()
    result_group.add_argument('--top-n', type=int, help='返回的热点数量（与--top-percent互斥）')
    result_group.add_argument('--top-percent', type=float, default=1.0, 
                            help='返回最密集的前X%%网格（默认1%%）')
    
    args = parser.parse_args()
    
    print("🚀 批量城市热点分析")
    print("=" * 50)
    print(f"输出表: {args.output_table}")
    if args.top_n is not None:
        print(f"分析参数: 返回前 {args.top_n} 个最密集网格")
    else:
        print(f"分析参数: 返回前 {args.top_percent}% 最密集网格")
    if args.max_cities:
        print(f"最多分析: {args.max_cities} 个城市")
    else:
        print(f"分析所有城市（无限制）")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    try:
        with engine.connect() as conn:
            
            # 创建汇总表
            create_top1_summary_table(conn, args.output_table)
            
            # 获取城市列表
            if args.cities:
                print(f"\n🎯 指定分析城市: {args.cities}")
                cities_to_analyze = args.cities
            else:
                all_cities = get_all_cities(conn)
                if args.max_cities:
                    cities_to_analyze = all_cities[:args.max_cities]
                else:
                    cities_to_analyze = all_cities  # 分析所有城市
            
            if not cities_to_analyze:
                print("❌ 没有找到城市")
                return 1
            
            # 批量分析
            print(f"\n🔄 开始批量分析 {len(cities_to_analyze)} 个城市...")
            if args.top_n is not None:
                print(f"每个城市使用 run_overlap_analysis.py --top-n {args.top_n} 进行分析")
            else:
                print(f"每个城市使用 run_overlap_analysis.py --top-percent {args.top_percent} 进行分析")
            
            successful_cities = []
            failed_cities = []
            
            start_time = time.time()
            
            for i, city_id in enumerate(cities_to_analyze, 1):
                print(f"\n[{i}/{len(cities_to_analyze)}] 处理城市: {city_id}")
                
                success = analyze_city_with_existing_script(city_id, top_n=args.top_n, top_percent=args.top_percent)
                
                if success:
                    successful_cities.append(city_id)
                    # 立即提取该城市的热点结果
                    try:
                        extract_single_city_top1(conn, args.output_table, city_id)
                        print(f"   📊 已提取 {city_id} 的热点结果到汇总表")
                    except Exception as e:
                        print(f"   ⚠️ 提取 {city_id} 结果失败: {str(e)}")
                else:
                    failed_cities.append(city_id)
                
                # 每10个城市休息一下
                if i % 10 == 0:
                    print(f"   💤 已处理 {i} 个城市，休息2秒...")
                    time.sleep(2)
            
            # 检查汇总表中的结果数量
            count_sql = text(f"""
                SELECT COUNT(*) as count 
                FROM {args.output_table} 
                WHERE analysis_time::date = CURRENT_DATE;
            """)
            result = conn.execute(count_sql).fetchone()
            extracted_count = result.count if result else 0
            
            print(f"\n📊 汇总表中共有 {extracted_count} 个城市的top1热点")
            
            # 统计结果
            total_time = time.time() - start_time
            
            print(f"\n" + "=" * 60)
            print(f"📊 批量分析完成！")
            print(f"=" * 60)
            print(f"总耗时: {total_time:.2f}秒")
            print(f"成功分析: {len(successful_cities)} 个城市")
            print(f"失败: {len(failed_cities)} 个城市")
            print(f"提取top1: {extracted_count} 个热点")
            
            if successful_cities:
                print(f"\n✅ 成功的城市: {', '.join(successful_cities[:10])}")
                if len(successful_cities) > 10:
                    print(f"   ... 还有 {len(successful_cities) - 10} 个")
            
            if failed_cities:
                print(f"\n⚠️ 失败的城市: {', '.join(failed_cities[:10])}")
                if len(failed_cities) > 10:
                    print(f"   ... 还有 {len(failed_cities) - 10} 个")
            
            # 显示结果概览
            if extracted_count > 0:
                summary_sql = text(f"""
                    SELECT 
                        city_id,
                        bbox_count,
                        subdataset_count,
                        scene_count,
                        grid_coords,
                        ROUND(total_overlap_area::numeric, 6) as total_overlap_area
                    FROM {args.output_table}
                    WHERE analysis_time::date = CURRENT_DATE
                    ORDER BY bbox_count DESC
                    LIMIT 10;
                """)
                
                results_df = pd.read_sql(summary_sql, conn)
                
                print(f"\n📋 Top10热点城市:")
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