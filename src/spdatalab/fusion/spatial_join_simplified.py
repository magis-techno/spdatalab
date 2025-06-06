"""
空间连接模块 - 简化版
基于直接远端查询策略，无需推送数据到远端
"""

import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from enum import Enum
import time

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

logger = logging.getLogger(__name__)

class SpatialRelation(Enum):
    """空间关系枚举"""
    INTERSECTS = "intersects"
    WITHIN = "within"
    CONTAINS = "contains"
    DWITHIN = "dwithin"  # 距离范围内

class SpatialJoinSimplified:
    """
    简化的空间连接分析器
    基于直接远端查询策略，性能优异
    """
    
    def __init__(self, local_engine=None, remote_engine=None):
        """
        初始化空间连接器
        
        Args:
            local_engine: 本地SQLAlchemy引擎
            remote_engine: 远端SQLAlchemy引擎
        """
        if local_engine is None:
            self.local_engine = create_engine(LOCAL_DSN, future=True)
        else:
            self.local_engine = local_engine
        
        if remote_engine is None:
            self.remote_engine = create_engine(
                REMOTE_DSN, 
                future=True,
                connect_args={"client_encoding": "utf8"}
            )
        else:
            self.remote_engine = remote_engine
            
        self.logger = logging.getLogger(__name__)
    
    def spatial_join_remote(
        self,
        left_table: str = "clips_bbox",
        remote_table: str = "full_intersection",
        spatial_relation: Union[SpatialRelation, str] = SpatialRelation.INTERSECTS,
        distance_meters: Optional[float] = 100,
        
        # 批次控制
        batch_size: int = 50,
        city_ids: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        
        # 统计选项
        summarize: bool = True,
        summary_fields: Optional[Dict[str, str]] = None,
        
        # 输出选项
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        直接远端空间连接 - 高性能策略
        
        核心思路：
        1. 从本地获取bbox的坐标点（不传输几何数据）
        2. 直接在远端构建空间查询SQL
        3. 一次性获取所有结果
        
        Args:
            left_table: 本地左表名
            remote_table: 远端右表名
            spatial_relation: 空间关系
            distance_meters: 距离阈值（米）
            
            batch_size: 批次大小（控制单次查询的点数）
            city_ids: 指定要处理的城市ID列表
            where_clause: 本地表的过滤条件
            
            summarize: 是否进行统计汇总
            summary_fields: 统计字段和方法
            
            output_table: 输出表名
            
        Returns:
            空间连接结果的GeoDataFrame
        """
        self.logger.info(f"开始直接远端空间连接: {left_table} -> {remote_table}")
        
        # 转换枚举为字符串
        if isinstance(spatial_relation, SpatialRelation):
            spatial_relation = spatial_relation.value
        
        # 设置默认统计字段
        if summarize and summary_fields is None:
            summary_fields = {
                "intersection_count": "count",
                "nearest_distance": "distance"
            }
        
        try:
            # 步骤1: 从本地获取坐标数据
            start_time = time.time()
            local_data = self._fetch_local_coordinates(
                left_table, city_ids, where_clause
            )
            local_time = time.time() - start_time
            self.logger.info(f"本地数据获取: {len(local_data)}条记录，耗时{local_time:.2f}秒")
            
            if local_data.empty:
                self.logger.warning("本地数据为空")
                return gpd.GeoDataFrame()
            
            # 步骤2: 分批进行远端查询
            all_results = []
            total_batches = (len(local_data) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(local_data))
                batch_data = local_data.iloc[start_idx:end_idx]
                
                self.logger.info(f"处理批次 {batch_num + 1}/{total_batches}: {len(batch_data)}个点")
                
                # 构建并执行远端查询
                batch_start = time.time()
                batch_result = self._execute_batch_remote_query(
                    batch_data, remote_table, spatial_relation, 
                    distance_meters, summarize, summary_fields
                )
                batch_time = time.time() - batch_start
                
                if not batch_result.empty:
                    all_results.append(batch_result)
                    self.logger.info(f"批次{batch_num + 1}完成，返回{len(batch_result)}条结果，耗时{batch_time:.2f}秒")
            
            # 步骤3: 合并结果
            if all_results:
                final_result = pd.concat(all_results, ignore_index=True)
                
                # 与本地数据合并，恢复几何信息
                final_gdf = self._merge_with_local_geometry(local_data, final_result)
                
                self.logger.info(f"空间连接完成，总计{len(final_gdf)}条结果")
                
                # 保存到输出表
                if output_table:
                    self._save_to_database(final_gdf, output_table)
                
                return final_gdf
            else:
                self.logger.info("所有批次均无匹配结果")
                return gpd.GeoDataFrame()
                
        except Exception as e:
            self.logger.error(f"直接远端空间连接失败: {str(e)}")
            raise
    
    def _fetch_local_coordinates(
        self, 
        table_name: str, 
        city_ids: Optional[List[str]] = None,
        where_clause: Optional[str] = None
    ) -> pd.DataFrame:
        """从本地获取坐标数据（不传输几何）"""
        
        # 构建WHERE条件
        where_conditions = []
        if where_clause:
            where_conditions.append(where_clause)
        if city_ids:
            city_list = "', '".join(city_ids)
            where_conditions.append(f"city_id IN ('{city_list}')")
        
        where_sql = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # 只获取必要的坐标信息
        sql = text(f"""
            SELECT 
                scene_token,
                city_id,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat,
                geometry  -- 保留原始几何，用于最终结果
            FROM {table_name} 
            {where_sql}
            ORDER BY scene_token
        """)
        
        return gpd.read_postgis(sql, self.local_engine, geom_col='geometry')
    
    def _execute_batch_remote_query(
        self,
        batch_data: pd.DataFrame,
        remote_table: str,
        spatial_relation: str,
        distance_meters: Optional[float],
        summarize: bool,
        summary_fields: Optional[Dict[str, str]]
    ) -> pd.DataFrame:
        """在远端执行批量空间查询"""
        
        # 构建批量查询SQL（使用UNION ALL）
        point_queries = []
        
        for _, row in batch_data.iterrows():
            lon, lat = row['lon'], row['lat']
            scene_token = row['scene_token']
            
            # 构建单点查询
            point_query = self._build_single_point_query(
                scene_token, lon, lat, remote_table,
                spatial_relation, distance_meters, 
                summarize, summary_fields
            )
            point_queries.append(point_query)
        
        # 合并所有查询
        if not point_queries:
            return pd.DataFrame()
        
        batch_sql = text(" UNION ALL ".join(point_queries))
        
        # 在远端执行查询
        with self.remote_engine.connect() as conn:
            return pd.read_sql(batch_sql, conn)
    
    def _build_single_point_query(
        self,
        scene_token: str,
        lon: float,
        lat: float,
        remote_table: str,
        spatial_relation: str,
        distance_meters: Optional[float],
        summarize: bool,
        summary_fields: Optional[Dict[str, str]]
    ) -> str:
        """构建单点的空间查询SQL"""
        
        # 构建空间条件
        spatial_condition = self._build_spatial_condition(
            spatial_relation, lon, lat, distance_meters
        )
        
        if summarize and summary_fields:
            # 统计模式
            field_selection = self._build_summary_fields_sql(summary_fields, lon, lat)
            
            return f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    {field_selection}
                FROM {remote_table} r
                WHERE {spatial_condition}
            """
        else:
            # 详细模式（获取匹配的要素详情）
            return f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    r.intersection_id,
                    r.road_type,
                    ST_Distance(
                        ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                        r.wkb_geometry::geography
                    ) as distance_meters
                FROM {remote_table} r
                WHERE {spatial_condition}
                ORDER BY distance_meters
                LIMIT 10
            """
    
    def _build_spatial_condition(
        self,
        relation: str,
        lon: float,
        lat: float,
        distance_meters: Optional[float]
    ) -> str:
        """构建空间条件SQL"""
        
        point_geom = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
        
        if relation == "intersects":
            # 使用边界框预过滤 + 精确相交
            buffer_degrees = (distance_meters or 100) / 111000  # 转换为度数
            return f"""
                r.wkb_geometry && ST_MakeEnvelope(
                    {lon - buffer_degrees}, {lat - buffer_degrees},
                    {lon + buffer_degrees}, {lat + buffer_degrees}, 4326
                )
                AND ST_Intersects({point_geom}, r.wkb_geometry)
            """
        elif relation == "dwithin":
            if distance_meters is None:
                distance_meters = 100
            return f"""
                ST_DWithin(
                    {point_geom}::geography,
                    r.wkb_geometry::geography,
                    {distance_meters}
                )
            """
        elif relation == "within":
            return f"ST_Within({point_geom}, r.wkb_geometry)"
        elif relation == "contains":
            return f"ST_Contains({point_geom}, r.wkb_geometry)"
        else:
            # 默认使用距离查询
            distance_meters = distance_meters or 100
            return f"""
                ST_DWithin(
                    {point_geom}::geography,
                    r.wkb_geometry::geography,
                    {distance_meters}
                )
            """
    
    def _build_summary_fields_sql(
        self, 
        summary_fields: Dict[str, str], 
        lon: float, 
        lat: float
    ) -> str:
        """构建统计字段SQL"""
        
        selections = []
        point_geom = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
        
        for field_name, method in summary_fields.items():
            method = method.lower()
            
            if method == "count":
                selections.append(f"COUNT(r.*) AS {field_name}")
            elif method == "distance":
                selections.append(f"""
                    COALESCE(
                        MIN(ST_Distance(
                            {point_geom}::geography,
                            r.wkb_geometry::geography
                        )), 
                        999999
                    ) AS {field_name}
                """)
            elif method in ["sum", "mean", "avg", "max", "min"]:
                # 对于数值统计，需要指定具体字段
                base_field = field_name.replace(f"_{method}", "")
                sql_method = "AVG" if method in ["mean", "avg"] else method.upper()
                selections.append(f"COALESCE({sql_method}(r.{base_field}), 0) AS {field_name}")
            else:
                selections.append(f"COUNT(r.*) AS {field_name}")
        
        return ", ".join(selections) if selections else "COUNT(r.*) AS feature_count"
    
    def _merge_with_local_geometry(
        self, 
        local_data: gpd.GeoDataFrame, 
        remote_results: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """将远端查询结果与本地几何数据合并"""
        
        # 准备本地数据（只保留必要字段）
        local_simple = local_data[['scene_token', 'city_id', 'geometry']].copy()
        
        # 与远端结果合并
        merged = local_simple.merge(
            remote_results, 
            on='scene_token', 
            how='left'
        )
        
        # 转换为GeoDataFrame
        return gpd.GeoDataFrame(merged, geometry='geometry', crs=4326)
    
    def _save_to_database(self, gdf: gpd.GeoDataFrame, table_name: str):
        """保存到数据库"""
        try:
            gdf.to_postgis(
                table_name,
                self.local_engine,
                if_exists='replace',
                index=False
            )
            self.logger.info(f"结果已保存到数据库表: {table_name}")
        except Exception as e:
            self.logger.error(f"保存到数据库失败: {str(e)}")
            raise
    
    # 便捷方法
    def bbox_intersect_features(
        self,
        feature_table: str = "full_intersection",
        distance_meters: float = 100,
        city_ids: Optional[List[str]] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        bbox与要素相交分析的简化接口
        
        Args:
            feature_table: 远端要素表名
            distance_meters: 搜索半径（米）
            city_ids: 指定城市ID列表
            output_table: 输出表名
            
        Returns:
            相交分析结果
        """
        return self.spatial_join_remote(
            remote_table=feature_table,
            spatial_relation=SpatialRelation.DWITHIN,
            distance_meters=distance_meters,
            city_ids=city_ids,
            summarize=True,
            summary_fields={
                "intersection_count": "count",
                "nearest_distance": "distance"
            },
            output_table=output_table
        ) 