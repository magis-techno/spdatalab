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
    local_engine = create_engine(LOCAL_DSN, future=True)
    remote_engine = create_engine(REMOTE_DSN, future=True)
    
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
    
    # 2. 对每个bbox，在远端查询相交的polygon
    results = []
    
    with remote_engine.connect() as conn:
        for _, row in local_data.iterrows():
            scene_token = row['scene_token']
            bbox_wkt = row['bbox_wkt']
            
            # 就是最简单的相交查询
            remote_sql = text(f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    COUNT(*) as intersect_count
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
            """)
            
            result = pd.read_sql(remote_sql, conn)
            results.append(result)
    
    # 合并结果
    return pd.concat(results, ignore_index=True)

if __name__ == "__main__":
    result = simple_polygon_intersect(5)
    print(result) 