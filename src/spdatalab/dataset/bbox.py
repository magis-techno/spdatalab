from __future__ import annotations
import argparse
import json
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
            END AS geom
        FROM bbox_data;""")
    
    return gpd.read_postgis(
        sql_query, 
        eng, 
        params={"names_param": names},
        geom_col='geom'
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

def run(input_path, batch=1000, insert_batch=1000):
    """主运行函数
    
    Args:
        input_path: 输入文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小
    """
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
    total_processed = 0
    total_inserted = 0
    
    print(f"开始处理 {len(scene_ids)} 个场景，批次大小: {batch}")
    print("使用原始边界框数据，不进行缓冲区扩展")
    
    for batch_num, token_batch in enumerate(chunk(scene_ids, batch), 1):
        print(f"[批次 {batch_num}] 处理 {len(token_batch)} 个场景")
        
        # 获取元数据
        try:
            meta = fetch_meta(token_batch)
            if meta.empty:
                print(f"[批次 {batch_num}] 没有找到元数据，跳过")
                continue
                
            print(f"[批次 {batch_num}] 获取到 {len(meta)} 条元数据")
        except Exception as e:
            print(f"[批次 {batch_num}] 获取元数据失败: {str(e)}")
            continue
        
        # 获取边界框和几何对象（直接从PostGIS获取原始几何）
        try:
            bbox_gdf = fetch_bbox_with_geometry(meta.data_name.tolist(), eng)
            if bbox_gdf.empty:
                print(f"[批次 {batch_num}] 没有找到边界框数据，跳过")
                continue
                
            print(f"[批次 {batch_num}] 获取到 {len(bbox_gdf)} 条边界框数据")
        except Exception as e:
            print(f"[批次 {batch_num}] 获取边界框失败: {str(e)}")
            continue
        
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
                geometry=merged['geom'], 
                crs=4326
            )
            
        except Exception as e:
            print(f"[批次 {batch_num}] 数据合并失败: {str(e)}")
            continue
        
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
    
    print(f"处理完成！总计处理: {total_processed} 条记录，成功插入: {total_inserted} 条记录")

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='从数据集文件生成边界框数据')
    ap.add_argument('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
    ap.add_argument('--batch', type=int, default=1000, help='处理批次大小')
    ap.add_argument('--insert-batch', type=int, default=1000, help='插入批次大小')
    
    args = ap.parse_args()
    run(args.input, args.batch, args.insert_batch)