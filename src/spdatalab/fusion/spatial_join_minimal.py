"""
最简单的polygon相交判断
就是一个ST_Intersects查询，没有任何多余复杂性
"""

import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

def simple_polygon_intersect(num_bbox: int = 5) -> pd.DataFrame:
    """
    最简单的polygon相交判断
    """
    local_engine = create_engine(LOCAL_DSN, future=True, connect_args={"client_encoding": "utf8"})
    remote_engine = create_engine(REMOTE_DSN, future=True, connect_args={"client_encoding": "utf8"})
    
    # 1. 获取本地bbox（作为WKT文本）
    local_sql = text(f"""
        SELECT 
            scene_token,
            ST_AsText(geometry) as bbox_wkt
        FROM clips_bbox 
        ORDER BY scene_token
        LIMIT {num_bbox}
    """)
    
    local_data = pd.read_sql(local_sql, local_engine)
    print(f"获取到{len(local_data)}个bbox")
    
    # 2. 对每个bbox，在远端查询相交的polygon
    results = []
    
    with remote_engine.connect() as conn:
        for idx, row in local_data.iterrows():
            scene_token = str(row['scene_token'])  # 确保是字符串
            bbox_wkt = str(row['bbox_wkt'])        # 确保是字符串
            
            print(f"处理bbox {idx+1}: {scene_token}")
            print(f"WKT: {bbox_wkt[:100]}...")  # 打印前100个字符
            
            # 使用参数化查询避免SQL注入
            remote_sql = text("""
                SELECT 
                    :scene_token as scene_token,
                    COUNT(*) as intersect_count
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText(:bbox_wkt, 4326))
            """)
            
            result = pd.read_sql(remote_sql, conn, params={
                'scene_token': scene_token,
                'bbox_wkt': bbox_wkt
            })
            results.append(result)
    
    # 合并结果
    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    result = simple_polygon_intersect(5)
    print(result) 