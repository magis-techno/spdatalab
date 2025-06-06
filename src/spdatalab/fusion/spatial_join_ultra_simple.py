"""
超简化空间连接模块
直接参照 direct_remote_query_test.py 的高效方式
"""

import logging
import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from typing import List, Optional

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

logger = logging.getLogger(__name__)

class FastSpatialJoin:
    """
    超快速空间连接器 - 直接参照 direct_remote_query_test.py 的方式
    最小化抽象，最大化性能
    """
    
    def __init__(self):
        self.local_engine = create_engine(LOCAL_DSN, future=True)
        self.remote_engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
    
    def spatial_join(
        self,
        num_bbox: int = 10,
        buffer_meters: float = 100,
        city_filter: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        超简化的空间连接 - 直接复制 direct_remote_query_test.py 的成功模式
        
        Args:
            num_bbox: 处理的bbox数量
            buffer_meters: 缓冲区半径（米）
            city_filter: 城市过滤（可选）
            
        Returns:
            空间连接结果
        """
        logger.info(f"开始快速空间连接: {num_bbox}个bbox, {buffer_meters}米缓冲区")
        
        total_start_time = time.time()
        
        # 步骤1: 从本地获取坐标数据（完全复制direct_remote_query_test.py的方式）
        logger.info("步骤1: 获取本地坐标数据")
        start_time = time.time()
        
        # 构建WHERE条件
        where_clause = ""
        if city_filter:
            where_clause = f"WHERE city_id = '{city_filter}'"
        
        local_sql = text(f"""
            SELECT 
                scene_token,
                city_id,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat,
                geometry
            FROM clips_bbox 
            {where_clause}
            ORDER BY scene_token
            LIMIT {num_bbox}
        """)
        
        local_data = gpd.read_postgis(local_sql, self.local_engine, geom_col='geometry')
        local_fetch_time = time.time() - start_time
        
        logger.info(f"本地数据获取: {len(local_data)}条记录，耗时{local_fetch_time:.2f}秒")
        
        if local_data.empty:
            logger.warning("本地数据为空")
            return gpd.GeoDataFrame()
        
        # 步骤2: 构建远端查询（完全复制direct_remote_query_test.py的方式）
        logger.info("步骤2: 构建远端查询")
        start_time = time.time()
        
        # 转换缓冲区到度数
        buffer_degrees = buffer_meters / 111000
        
        point_queries = []
        
        for _, row in local_data.iterrows():
            lon, lat = row['lon'], row['lat']
            scene_token = row['scene_token']
            
            # 直接使用 direct_remote_query_test.py 中证明有效的查询
            point_query = f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    COUNT(*) as intersection_count,
                    MIN(ST_Distance(
                        ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                        wkb_geometry::geography
                    )) as nearest_distance
                FROM full_intersection 
                WHERE wkb_geometry && ST_MakeEnvelope(
                    {lon - buffer_degrees}, 
                    {lat - buffer_degrees},
                    {lon + buffer_degrees}, 
                    {lat + buffer_degrees}, 
                    4326
                )
                AND ST_DWithin(
                    ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                    wkb_geometry::geography,
                    {buffer_meters}
                )
            """
            point_queries.append(point_query)
        
        # 合并所有查询
        remote_sql = text(" UNION ALL ".join(point_queries))
        
        # 在远端执行查询
        with self.remote_engine.connect() as conn:
            remote_results = pd.read_sql(remote_sql, conn)
        
        remote_query_time = time.time() - start_time
        logger.info(f"远端查询耗时: {remote_query_time:.2f}秒")
        logger.info(f"返回{len(remote_results)}条结果")
        
        # 步骤3: 合并结果（完全复制direct_remote_query_test.py的方式）
        logger.info("步骤3: 合并结果")
        start_time = time.time()
        
        # 准备本地数据用于合并（只保留必要字段）
        local_for_merge = local_data[['scene_token', 'city_id', 'geometry']].copy()
        
        # 合并本地数据和远端查询结果
        final_result = local_for_merge.merge(
            remote_results, 
            on='scene_token', 
            how='left'
        )
        
        # 确保是GeoDataFrame
        final_gdf = gpd.GeoDataFrame(final_result, geometry='geometry', crs=4326)
        
        merge_time = time.time() - start_time
        logger.info(f"结果合并耗时: {merge_time:.2f}秒")
        
        # 总耗时
        total_time = time.time() - total_start_time
        logger.info(f"空间连接总耗时: {total_time:.2f}秒")
        
        # 显示结果样例
        logger.info("空间连接结果样例:")
        for _, row in final_gdf.head(3).iterrows():
            count = row['intersection_count'] if pd.notna(row['intersection_count']) else 0
            distance = row['nearest_distance'] if pd.notna(row['nearest_distance']) else 999999
            logger.info(f"  {row['scene_token']}: {count}个相交, 最近距离: {distance:.1f}米")
        
        return final_gdf
    
    def bbox_intersect_features(
        self,
        num_bbox: int = 10,
        distance_meters: float = 100,
        city_filter: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        bbox与要素相交分析 - 简化接口
        """
        return self.spatial_join(
            num_bbox=num_bbox,
            buffer_meters=distance_meters,
            city_filter=city_filter
        )

# 便捷函数
def quick_spatial_join(
    num_bbox: int = 10,
    buffer_meters: float = 100,
    city_filter: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    快速空间连接的便捷函数
    """
    joiner = FastSpatialJoin()
    return joiner.spatial_join(num_bbox, buffer_meters, city_filter) 