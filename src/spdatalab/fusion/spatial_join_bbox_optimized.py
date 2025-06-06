"""
针对bbox优化的空间连接模块
充分利用bbox的边界框特性，比中心点策略更优
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

class BboxOptimizedSpatialJoin:
    """
    bbox优化的空间连接器
    针对bbox特性的专门优化
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
        city_filter: Optional[str] = None,
        use_bbox_boundary: bool = True
    ) -> gpd.GeoDataFrame:
        """
        bbox优化的空间连接
        
        Args:
            num_bbox: 处理的bbox数量
            buffer_meters: 缓冲区半径（米）
            city_filter: 城市过滤（可选）
            use_bbox_boundary: 是否使用bbox边界（而非中心点）
            
        Returns:
            空间连接结果
        """
        logger.info(f"开始bbox优化空间连接: {num_bbox}个bbox, {buffer_meters}米缓冲区")
        logger.info(f"使用{'bbox边界' if use_bbox_boundary else '中心点'}策略")
        
        total_start_time = time.time()
        
        # 步骤1: 获取bbox数据（优化版）
        logger.info("步骤1: 获取bbox边界数据")
        start_time = time.time()
        
        # 构建WHERE条件
        where_clause = ""
        if city_filter:
            where_clause = f"WHERE city_id = '{city_filter}'"
        
        if use_bbox_boundary:
            # 获取bbox的四个边界坐标，比中心点更精确
            local_sql = text(f"""
                SELECT 
                    scene_token,
                    city_id,
                    ST_XMin(geometry) as xmin,
                    ST_XMax(geometry) as xmax,
                    ST_YMin(geometry) as ymin,
                    ST_YMax(geometry) as ymax,
                    ST_X(ST_Centroid(geometry)) as center_lon,
                    ST_Y(ST_Centroid(geometry)) as center_lat,
                    geometry
                FROM clips_bbox 
                {where_clause}
                ORDER BY scene_token
                LIMIT {num_bbox}
            """)
        else:
            # 回退到中心点策略
            local_sql = text(f"""
                SELECT 
                    scene_token,
                    city_id,
                    ST_X(ST_Centroid(geometry)) as center_lon,
                    ST_Y(ST_Centroid(geometry)) as center_lat,
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
        
        # 步骤2: 构建优化的远端查询
        logger.info("步骤2: 构建bbox优化查询")
        start_time = time.time()
        
        point_queries = []
        
        for _, row in local_data.iterrows():
            scene_token = row['scene_token']
            
            if use_bbox_boundary and 'xmin' in row:
                # 策略1: 使用bbox的实际边界+缓冲区
                xmin, xmax = row['xmin'], row['xmax']
                ymin, ymax = row['ymin'], row['ymax']
                
                # 转换缓冲区到度数
                buffer_degrees = buffer_meters / 111000
                
                # 扩展bbox边界
                buffered_xmin = xmin - buffer_degrees
                buffered_xmax = xmax + buffer_degrees
                buffered_ymin = ymin - buffer_degrees
                buffered_ymax = ymax + buffer_degrees
                
                # 使用bbox直接相交查询，比点+缓冲区更准确
                point_query = f"""
                    SELECT 
                        '{scene_token}' as scene_token,
                        COUNT(*) as intersection_count,
                        MIN(ST_Distance(
                            ST_SetSRID(ST_MakePoint({row['center_lon']}, {row['center_lat']}), 4326)::geography,
                            wkb_geometry::geography
                        )) as nearest_distance_to_center,
                        MIN(ST_Distance(
                            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326)::geography,
                            wkb_geometry::geography
                        )) as nearest_distance_to_bbox
                    FROM full_intersection 
                    WHERE wkb_geometry && ST_MakeEnvelope(
                        {buffered_xmin}, {buffered_ymin}, 
                        {buffered_xmax}, {buffered_ymax}, 4326
                    )
                    AND (
                        ST_Intersects(
                            wkb_geometry, 
                            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326)
                        )
                        OR ST_DWithin(
                            wkb_geometry::geography,
                            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326)::geography,
                            {buffer_meters}
                        )
                    )
                """
            else:
                # 策略2: 回退到中心点策略（与ultra_simple一致）
                lon, lat = row['center_lon'], row['center_lat']
                buffer_degrees = buffer_meters / 111000
                
                point_query = f"""
                    SELECT 
                        '{scene_token}' as scene_token,
                        COUNT(*) as intersection_count,
                        MIN(ST_Distance(
                            ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                            wkb_geometry::geography
                        )) as nearest_distance_to_center
                    FROM full_intersection 
                    WHERE wkb_geometry && ST_MakeEnvelope(
                        {lon - buffer_degrees}, {lat - buffer_degrees},
                        {lon + buffer_degrees}, {lat + buffer_degrees}, 4326
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
        
        # 步骤3: 合并结果
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
        logger.info(f"bbox优化空间连接总耗时: {total_time:.2f}秒")
        
        # 显示结果样例
        logger.info("空间连接结果样例:")
        for _, row in final_gdf.head(3).iterrows():
            count = row['intersection_count'] if pd.notna(row['intersection_count']) else 0
            
            # 根据可用字段显示距离信息
            if 'nearest_distance_to_bbox' in row and pd.notna(row['nearest_distance_to_bbox']):
                distance_info = f"中心距离: {row.get('nearest_distance_to_center', 999999):.1f}米, bbox距离: {row['nearest_distance_to_bbox']:.1f}米"
            else:
                distance_info = f"距离: {row.get('nearest_distance_to_center', 999999):.1f}米"
            
            logger.info(f"  {row['scene_token']}: {count}个相交, {distance_info}")
        
        return final_gdf
    
    def bbox_intersect_features(
        self,
        num_bbox: int = 10,
        distance_meters: float = 100,
        city_filter: Optional[str] = None,
        use_bbox_boundary: bool = True
    ) -> gpd.GeoDataFrame:
        """
        bbox与要素相交分析 - bbox优化接口
        """
        return self.spatial_join(
            num_bbox=num_bbox,
            buffer_meters=distance_meters,
            city_filter=city_filter,
            use_bbox_boundary=use_bbox_boundary
        )

# 便捷函数
def quick_bbox_spatial_join(
    num_bbox: int = 10,
    buffer_meters: float = 100,
    city_filter: Optional[str] = None,
    use_bbox_boundary: bool = True
) -> gpd.GeoDataFrame:
    """
    bbox优化空间连接的便捷函数
    """
    joiner = BboxOptimizedSpatialJoin()
    return joiner.spatial_join(num_bbox, buffer_meters, city_filter, use_bbox_boundary) 