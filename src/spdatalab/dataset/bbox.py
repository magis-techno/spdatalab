from __future__ import annotations
import argparse
import json
import signal
import sys
from pathlib import Path
import geopandas as gpd, pandas as pd
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

# 检查是否有parquet支持
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 全局变量用于优雅退出
interrupted = False

def signal_handler(signum, frame):
    """信号处理函数，用于优雅退出"""
    global interrupted
    print(f"\n接收到中断信号 ({signum})，正在优雅退出...")
    print("等待当前批次处理完成，请稍候...")
    interrupted = True

def setup_signal_handlers():
    """设置信号处理器"""
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号

def create_table_if_not_exists(eng, table_name='clips_bbox'):
    """如果表不存在则创建表"""
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            scene_token VARCHAR(255),
            data_name VARCHAR(255),
            event_id VARCHAR(255),
            city_id VARCHAR(255),
            timestamp BIGINT,
            all_good BOOLEAN,
            geometry GEOMETRY(GEOMETRY, 4326)
        );
        
        -- 创建索引以提高查询性能
        CREATE INDEX IF NOT EXISTS idx_{table_name}_scene_token ON {table_name}(scene_token);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_data_name ON {table_name}(data_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
    """)
    
    try:
        with eng.connect() as conn:
            conn.execute(create_sql)
            conn.commit()
            print(f"确保表 {table_name} 存在并已创建必要索引")
            return True
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        return False

def chunk(lst, n):
    """将列表分块处理"""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def load_scene_ids_from_json(file_path):
    """从JSON格式的数据集文件中加载scene_id列表"""
    with open(file_path, 'r', encoding='utf-8') as f:
        dataset_data = json.load(f)
    
    scene_ids = []
    subdatasets = dataset_data.get('subdatasets', [])
    
    for subdataset in subdatasets:
        scene_ids.extend(subdataset.get('scene_ids', []))
    
    print(f"从JSON文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids_from_parquet(file_path):
    """从Parquet格式的数据集文件中加载scene_id列表"""
    if not PARQUET_AVAILABLE:
        raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式: pip install pandas pyarrow")
    
    df = pd.read_parquet(file_path)
    scene_ids = df['scene_id'].unique().tolist()
    
    print(f"从Parquet文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids_from_text(file_path):
    """从文本文件中加载scene_id列表（兼容原格式）"""
    scene_ids = [t.strip() for t in Path(file_path).read_text().splitlines() if t.strip()]
    print(f"从文本文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids(file_path):
    """智能加载scene_id列表，自动检测文件格式"""
    file_path = Path(file_path)
    
    if file_path.suffix.lower() == '.json':
        return load_scene_ids_from_json(file_path)
    elif file_path.suffix.lower() == '.parquet':
        return load_scene_ids_from_parquet(file_path)
    else:
        # 默认按文本格式处理
        return load_scene_ids_from_text(file_path)

def fetch_meta(tokens):
    """批量获取场景元数据"""
    sql = ("SELECT id AS scene_token,name AS data_name,event_id,city_id,timestamp "
           "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s")
    with hive_cursor() as cur:
        cur.execute(sql, {"tok": tuple(tokens)})
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)

def fetch_bbox_with_geometry(names, eng):
    """批量获取边界框信息并直接在PostGIS中构建几何对象"""
    sql_query = text(f"""
        WITH bbox_data AS (
            SELECT 
                dataset_name,
                ST_XMin(ST_Extent(point_lla)) AS xmin,
                ST_YMin(ST_Extent(point_lla)) AS ymin,
                ST_XMax(ST_Extent(point_lla)) AS xmax,
                ST_YMax(ST_Extent(point_lla)) AS ymax,
                bool_and(workstage = 2) AS all_good
            FROM {POINT_TABLE}
            WHERE dataset_name = ANY(:names_param)
            GROUP BY dataset_name
        )
        SELECT 
            dataset_name,
            all_good,
            CASE 
                WHEN xmin = xmax OR ymin = ymax THEN
                    -- 对于点数据，直接创建点几何对象
                    ST_Point((xmin + xmax) / 2, (ymin + ymax) / 2)
                ELSE
                    -- 对于已有边界框的数据，直接使用边界框
                    ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
            END AS geometry
        FROM bbox_data;""")
    
    return gpd.read_postgis(
        sql_query, 
        eng, 
        params={"names_param": names},
        geom_col='geometry'
    )

def batch_insert_to_postgis(gdf, eng, table_name='clips_bbox', batch_size=1000):
    """批量插入到PostGIS，提高插入效率"""
    total_rows = len(gdf)
    inserted_rows = 0
    
    # 分批插入
    for i in range(0, total_rows, batch_size):
        batch_gdf = gdf.iloc[i:i+batch_size]
        
        try:
            batch_gdf.to_postgis(
                table_name, 
                eng, 
                if_exists='append', 
                index=False
            )
            inserted_rows += len(batch_gdf)
            print(f'[批量插入] 已插入: {inserted_rows}/{total_rows} 行')
            
        except Exception as e:
            print(f'[批量插入错误] 批次 {i//batch_size + 1}: {str(e)}')
            # 如果批量失败，尝试逐行插入
            for _, row in batch_gdf.iterrows():
                try:
                    # 创建单行GeoDataFrame，确保几何列名正确
                    single_gdf = gpd.GeoDataFrame(
                        [row.drop('geometry')], 
                        geometry=[row.geometry], 
                        crs=4326
                    )
                    single_gdf.to_postgis(table_name, eng, if_exists='append', index=False)
                    inserted_rows += 1
                except Exception as row_e:
                    print(f'[逐行插入错误] scene_token: {row.get("scene_token", "unknown")}: {str(row_e)}')
    
    return inserted_rows

def run(input_path, batch=1000, insert_batch=1000, create_table=True):
    """主运行函数
    
    Args:
        input_path: 输入文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小
        create_table: 是否创建表（如果不存在）
    """
    global interrupted
    
    # 设置信号处理器
    setup_signal_handlers()
    
    print(f"开始处理输入文件: {input_path}")
    
    # 智能加载scene_id列表
    try:
        scene_ids = load_scene_ids(input_path)
    except Exception as e:
        print(f"加载输入文件失败: {str(e)}")
        return
    
    if not scene_ids:
        print("没有找到有效的scene_id")
        return
    
    eng = create_engine(LOCAL_DSN, future=True)
    
    # 创建表（如果需要）
    if create_table:
        if not create_table_if_not_exists(eng):
            print("创建表失败，退出")
            return
    
    total_processed = 0
    total_inserted = 0
    
    print(f"开始处理 {len(scene_ids)} 个场景，批次大小: {batch}")
    print("使用原始边界框数据，不进行缓冲区扩展")
    
    try:
        for batch_num, token_batch in enumerate(chunk(scene_ids, batch), 1):
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，已处理 {batch_num-1} 个批次")
                break
                
            print(f"[批次 {batch_num}] 处理 {len(token_batch)} 个场景")
            
            # 获取元数据
            try:
                meta = fetch_meta(token_batch)
                if meta.empty:
                    print(f"[批次 {batch_num}] 没有找到元数据，跳过")
                    continue
                    
                print(f"[批次 {batch_num}] 获取到 {len(meta)} 条元数据")
                
                # 调试：显示前几个data_name
                print(f"[批次 {batch_num}] 元数据中的前5个data_name: {meta.data_name.head().tolist()}")
                
            except Exception as e:
                print(f"[批次 {batch_num}] 获取元数据失败: {str(e)}")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 获取边界框和几何对象（直接从PostGIS获取原始几何）
            try:
                bbox_gdf = fetch_bbox_with_geometry(meta.data_name.tolist(), eng)
                if bbox_gdf.empty:
                    print(f"[批次 {batch_num}] 没有找到边界框数据，跳过")
                    continue
                    
                print(f"[批次 {batch_num}] 获取到 {len(bbox_gdf)} 条边界框数据")
                
                # 调试：显示边界框数据中的dataset_name
                print(f"[批次 {batch_num}] 边界框数据中的前5个dataset_name: {bbox_gdf.dataset_name.head().tolist()}")
                
                # 调试：检查哪些data_name没有匹配到边界框数据
                meta_names = set(meta.data_name.tolist())
                bbox_names = set(bbox_gdf.dataset_name.tolist())
                missing_names = meta_names - bbox_names
                
                if missing_names:
                    print(f"[批次 {batch_num}] 警告：{len(missing_names)} 个data_name在点数据表中未找到")
                    print(f"[批次 {batch_num}] 缺失的前10个data_name: {list(missing_names)[:10]}")
                    
                    # 如果缺失数据过多，进行详细分析
                    if len(missing_names) > len(meta_names) * 0.5:  # 超过50%缺失
                        print(f"[批次 {batch_num}] 缺失数据过多，进行详细分析...")
                        analyze_data_availability(meta.data_name.tolist(), eng)
                    
                    # 进一步调试：检查点数据表中是否真的没有这些数据
                    debug_sql = text(f"""
                        SELECT dataset_name, COUNT(*) as point_count
                        FROM {POINT_TABLE}
                        WHERE dataset_name = ANY(:debug_names)
                        GROUP BY dataset_name
                        LIMIT 10
                    """)
                    
                    debug_names = list(missing_names)[:10]  # 只检查前10个
                    with eng.connect() as conn:
                        debug_result = conn.execute(debug_sql, {"debug_names": debug_names})
                        debug_data = debug_result.fetchall()
                        
                    if debug_data:
                        print(f"[批次 {batch_num}] 点数据表中找到的缺失数据: {debug_data}")
                    else:
                        print(f"[批次 {batch_num}] 点数据表中确实没有这些dataset_name的数据")
                
            except Exception as e:
                print(f"[批次 {batch_num}] 获取边界框失败: {str(e)}")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 合并数据
            try:
                merged = meta.merge(bbox_gdf, left_on='data_name', right_on='dataset_name', how='inner')
                if merged.empty:
                    print(f"[批次 {batch_num}] 合并后数据为空，跳过")
                    continue
                    
                print(f"[批次 {batch_num}] 合并后得到 {len(merged)} 条记录")
                
                # 创建最终的GeoDataFrame
                final_gdf = gpd.GeoDataFrame(
                    merged[['scene_token', 'data_name', 'event_id', 'city_id', 'timestamp', 'all_good']], 
                    geometry=merged['geometry'], 
                    crs=4326
                )
                
            except Exception as e:
                print(f"[批次 {batch_num}] 数据合并失败: {str(e)}")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 批量插入数据库
            try:
                batch_inserted = batch_insert_to_postgis(final_gdf, eng, batch_size=insert_batch)
                total_inserted += batch_inserted
                total_processed += len(final_gdf)
                
                print(f"[批次 {batch_num}] 完成，插入 {batch_inserted} 条记录")
                print(f"[累计进度] 已处理: {total_processed}, 已插入: {total_inserted}")
                
            except Exception as e:
                print(f"[批次 {batch_num}] 插入数据库失败: {str(e)}")
                continue
                
    except KeyboardInterrupt:
        print(f"\n程序被用户中断")
        interrupted = True
    except Exception as e:
        print(f"\n程序遇到未预期的错误: {str(e)}")
    finally:
        if interrupted:
            print(f"程序优雅退出！已处理: {total_processed} 条记录，成功插入: {total_inserted} 条记录")
        else:
            print(f"处理完成！总计处理: {total_processed} 条记录，成功插入: {total_inserted} 条记录")

def analyze_data_availability(data_names, eng, sample_size=20):
    """分析数据可用性，帮助诊断为什么边界框数据少"""
    print(f"\n=== 数据可用性分析 ===")
    
    # 1. 检查总体数据分布
    total_sql = text(f"""
        SELECT 
            COUNT(DISTINCT dataset_name) as unique_datasets,
            COUNT(*) as total_points
        FROM {POINT_TABLE}
    """)
    
    with eng.connect() as conn:
        result = conn.execute(total_sql)
        total_stats = result.fetchone()
        print(f"点数据表总体统计: {total_stats.unique_datasets} 个唯一数据集, {total_stats.total_points} 个点")
    
    # 2. 检查请求的数据名称在表中的存在情况
    sample_names = data_names[:sample_size] if len(data_names) > sample_size else data_names
    
    availability_sql = text(f"""
        SELECT 
            input_name,
            CASE WHEN point_count > 0 THEN 'EXISTS' ELSE 'MISSING' END as status,
            COALESCE(point_count, 0) as point_count
        FROM (
            SELECT unnest(:input_names) as input_name
        ) input_data
        LEFT JOIN (
            SELECT dataset_name, COUNT(*) as point_count
            FROM {POINT_TABLE}
            GROUP BY dataset_name
        ) point_data ON input_data.input_name = point_data.dataset_name
        ORDER BY point_count DESC NULLS LAST
    """)
    
    with eng.connect() as conn:
        result = conn.execute(availability_sql, {"input_names": sample_names})
        availability_data = result.fetchall()
        
        exists_count = sum(1 for row in availability_data if row.status == 'EXISTS')
        missing_count = len(availability_data) - exists_count
        
        print(f"样本分析结果 (前{len(sample_names)}个data_name):")
        print(f"  - 存在: {exists_count} 个")
        print(f"  - 缺失: {missing_count} 个")
        
        # 显示前几个存在的和缺失的
        exists_samples = [row for row in availability_data if row.status == 'EXISTS'][:5]
        missing_samples = [row for row in availability_data if row.status == 'MISSING'][:5]
        
        if exists_samples:
            print(f"存在的数据样本: {[(row.input_name, row.point_count) for row in exists_samples]}")
        if missing_samples:
            print(f"缺失的数据样本: {[row.input_name for row in missing_samples]}")
    
    # 3. 检查数据名称的模式
    pattern_sql = text(f"""
        SELECT 
            dataset_name,
            COUNT(*) as point_count
        FROM {POINT_TABLE}
        WHERE dataset_name LIKE ANY(:patterns)
        GROUP BY dataset_name
        ORDER BY point_count DESC
        LIMIT 10
    """)
    
    # 根据样本数据推测可能的命名模式
    patterns = []
    for name in sample_names[:5]:
        if '_' in name:
            # 尝试截取不同长度的前缀
            parts = name.split('_')
            if len(parts) > 1:
                patterns.append(f"{parts[0]}_%")
    
    if patterns:
        with eng.connect() as conn:
            result = conn.execute(pattern_sql, {"patterns": patterns})
            pattern_data = result.fetchall()
            
            if pattern_data:
                print(f"模式匹配结果: {pattern_data}")
    
    print("=== 分析完成 ===\n")

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='从数据集文件生成边界框数据')
    ap.add_argument('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
    ap.add_argument('--batch', type=int, default=1000, help='处理批次大小')
    ap.add_argument('--insert-batch', type=int, default=1000, help='插入批次大小')
    ap.add_argument('--create-table', action='store_true', default=True, help='是否创建表（如果不存在）')
    
    args = ap.parse_args()
    run(args.input, args.batch, args.insert_batch, args.create_table)