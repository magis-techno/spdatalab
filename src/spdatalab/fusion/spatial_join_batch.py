"""
批量优化的polygon相交判断
将多个bbox合并到一个SQL查询中，减少网络往返
"""

import pandas as pd
from sqlalchemy import create_engine, text

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

def batch_polygon_intersect(num_bbox: int = 50) -> pd.DataFrame:
    """
    真正的批量polygon相交判断 - 单个SQL查询处理所有bbox
    """
    local_engine = create_engine(LOCAL_DSN, future=True, connect_args={"client_encoding": "utf8"})
    remote_engine = create_engine(REMOTE_DSN, future=True, connect_args={"client_encoding": "utf8"})
    
    # 1. 获取本地bbox数据
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
    
    if local_data.empty:
        return pd.DataFrame()
    
    # 2. 构建批量查询 - 关键优化点！
    # 将所有bbox组合成一个UNION查询
    bbox_queries = []
    for _, row in local_data.iterrows():
        scene_token = str(row['scene_token'])
        bbox_wkt = str(row['bbox_wkt'])
        
        # 每个bbox的子查询
        subquery = f"""
            SELECT 
                '{scene_token}' as scene_token,
                COUNT(*) as intersect_count
            FROM full_intersection 
            WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
        """
        bbox_queries.append(subquery)
    
    # 合并所有子查询为一个大查询
    batch_sql = text(" UNION ALL ".join(bbox_queries))
    
    # 3. 执行单个批量查询
    print(f"执行批量查询...")
    with remote_engine.connect() as conn:
        result = pd.read_sql(batch_sql, conn)
    
    return result

def chunked_batch_intersect(num_bbox: int = 200, chunk_size: int = 50) -> pd.DataFrame:
    """
    分块批量处理 - 适合更大规模的数据
    """
    local_engine = create_engine(LOCAL_DSN, future=True, connect_args={"client_encoding": "utf8"})
    remote_engine = create_engine(REMOTE_DSN, future=True, connect_args={"client_encoding": "utf8"})
    
    # 获取所有bbox
    local_sql = text(f"""
        SELECT 
            scene_token,
            ST_AsText(geometry) as bbox_wkt
        FROM clips_bbox 
        ORDER BY scene_token
        LIMIT {num_bbox}
    """)
    
    local_data = pd.read_sql(local_sql, local_engine)
    print(f"获取到{len(local_data)}个bbox，将分{len(local_data)//chunk_size + 1}批处理")
    
    if local_data.empty:
        return pd.DataFrame()
    
    all_results = []
    
    # 分块处理
    for i in range(0, len(local_data), chunk_size):
        chunk = local_data.iloc[i:i+chunk_size]
        print(f"处理第{i//chunk_size + 1}批: {len(chunk)}个bbox")
        
        # 构建这一批的查询
        bbox_queries = []
        for _, row in chunk.iterrows():
            scene_token = str(row['scene_token'])
            bbox_wkt = str(row['bbox_wkt'])
            
            subquery = f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    COUNT(*) as intersect_count
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
            """
            bbox_queries.append(subquery)
        
        # 执行这一批查询
        batch_sql = text(" UNION ALL ".join(bbox_queries))
        with remote_engine.connect() as conn:
            chunk_result = pd.read_sql(batch_sql, conn)
            all_results.append(chunk_result)
    
    # 合并所有结果
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

if __name__ == "__main__":
    import time
    
    # 测试批量查询
    print("测试批量查询...")
    start = time.time()
    result1 = batch_polygon_intersect(20)
    print(f"批量查询 20个bbox 耗时: {time.time() - start:.2f}秒")
    print(result1.head())
    
    # 测试分块查询  
    print("\n测试分块查询...")
    start = time.time()
    result2 = chunked_batch_intersect(100, chunk_size=25)
    print(f"分块查询 100个bbox 耗时: {time.time() - start:.2f}秒")
    print(result2.head()) 