"""
轨迹交集分析模块

提供轨迹与各种空间数据的交集分析功能，包括：
1. 轨迹与路口的交集分析
2. 轨迹与道路的交集分析  
3. 轨迹与区域的交集分析
4. 轨迹间的交集分析
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import transform
import pyproj
from functools import partial

from ..common.config import LOCAL_DSN

logger = logging.getLogger(__name__)

class TrajectoryIntersectionAnalyzer:
    """轨迹交集分析器"""
    
    def __init__(self, engine=None):
        """
        初始化分析器
        
        Args:
            engine: SQLAlchemy引擎，如果为None则创建默认引擎
        """
        if engine is None:
            self.engine = create_engine(LOCAL_DSN, future=True)
        else:
            self.engine = engine
            
        self.logger = logging.getLogger(__name__)
    
    def analyze_trajectory_intersection_with_junctions(
        self, 
        trajectory_table: str = "clips_bbox",
        junction_table: str = "intersections", 
        buffer_meters: float = 20.0,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        分析轨迹与路口的交集
        
        Args:
            trajectory_table: 轨迹数据表名
            junction_table: 路口数据表名  
            buffer_meters: 缓冲区半径(米)
            output_table: 输出表名，如果为None则不保存到数据库
            
        Returns:
            包含交集分析结果的GeoDataFrame
        """
        sql = text(f"""
            WITH trajectory_buffered AS (
                SELECT 
                    scene_token,
                    data_name,
                    city_id,
                    ST_Transform(
                        ST_Buffer(
                            ST_Transform(geometry, 3857), 
                            :buffer_meters
                        ), 
                        4326
                    ) AS buffered_geom,
                    geometry AS original_geom
                FROM {trajectory_table}
                WHERE geometry IS NOT NULL
            ),
            intersection_analysis AS (
                SELECT 
                    t.scene_token,
                    t.data_name,
                    t.city_id,
                    j.inter_id,
                    j.inter_type,
                    j.geom AS junction_geom,
                    t.original_geom AS trajectory_geom,
                    ST_Distance(
                        ST_Transform(t.original_geom, 3857),
                        ST_Transform(j.geom, 3857)
                    ) AS distance_meters,
                    ST_Area(
                        ST_Intersection(t.buffered_geom, j.geom)::geography
                    ) AS intersection_area_m2,
                    CASE 
                        WHEN ST_Intersects(t.original_geom, j.geom) THEN 'direct_intersection'
                        WHEN ST_Intersects(t.buffered_geom, j.geom) THEN 'buffer_intersection'
                        ELSE 'no_intersection'
                    END AS intersection_type
                FROM trajectory_buffered t
                JOIN {junction_table} j ON t.city_id = j.city_id
                WHERE ST_DWithin(
                    ST_Transform(t.original_geom, 3857),
                    ST_Transform(j.geom, 3857),
                    :buffer_meters
                )
            )
            SELECT 
                scene_token,
                data_name,
                city_id,
                inter_id,
                inter_type,
                distance_meters,
                intersection_area_m2,
                intersection_type,
                trajectory_geom,
                junction_geom,
                ST_Centroid(
                    ST_Intersection(trajectory_geom, junction_geom)
                ) AS intersection_point
            FROM intersection_analysis
            WHERE intersection_type != 'no_intersection'
            ORDER BY scene_token, distance_meters
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"buffer_meters": buffer_meters},
                geom_col="intersection_point"
            )
            
            # 如果指定了输出表，保存结果
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功分析轨迹与路口交集，共找到 {len(result_gdf)} 个交集点")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"轨迹与路口交集分析失败: {str(e)}")
            raise
    
    def analyze_trajectory_intersection_with_roads(
        self,
        trajectory_table: str = "clips_bbox",
        road_table: str = "roads",
        buffer_meters: float = 10.0,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        分析轨迹与道路的交集
        
        Args:
            trajectory_table: 轨迹数据表名
            road_table: 道路数据表名
            buffer_meters: 缓冲区半径(米)
            output_table: 输出表名
            
        Returns:
            包含交集分析结果的GeoDataFrame
        """
        sql = text(f"""
            WITH trajectory_road_intersections AS (
                SELECT 
                    t.scene_token,
                    t.data_name,
                    t.city_id,
                    r.road_id,
                    r.road_type,
                    r.road_name,
                    t.geometry AS trajectory_geom,
                    r.geom AS road_geom,
                    ST_Length(
                        ST_Intersection(
                            ST_Transform(t.geometry, 3857),
                            ST_Transform(r.geom, 3857)
                        )
                    ) AS intersection_length_meters,
                    ST_Intersection(t.geometry, r.geom) AS intersection_geom,
                    ST_Distance(
                        ST_Transform(t.geometry, 3857),
                        ST_Transform(r.geom, 3857)
                    ) AS min_distance_meters
                FROM {trajectory_table} t
                JOIN {road_table} r ON t.city_id = r.city_id
                WHERE ST_DWithin(
                    ST_Transform(t.geometry, 3857),
                    ST_Transform(r.geom, 3857),
                    :buffer_meters
                )
                AND ST_Intersects(
                    ST_Buffer(ST_Transform(t.geometry, 3857), :buffer_meters),
                    ST_Transform(r.geom, 3857)
                )
            )
            SELECT *,
                CASE 
                    WHEN intersection_length_meters > 0 THEN 'direct_overlap'
                    WHEN min_distance_meters <= :buffer_meters THEN 'proximity_match'
                    ELSE 'no_intersection'
                END AS intersection_type
            FROM trajectory_road_intersections
            WHERE intersection_length_meters > 0 OR min_distance_meters <= :buffer_meters
            ORDER BY scene_token, intersection_length_meters DESC
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"buffer_meters": buffer_meters},
                geom_col="intersection_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功分析轨迹与道路交集，共找到 {len(result_gdf)} 个交集")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"轨迹与道路交集分析失败: {str(e)}")
            raise
    
    def analyze_trajectory_intersection_with_regions(
        self,
        trajectory_table: str = "clips_bbox",
        region_table: str = "regions",
        region_type: Optional[str] = None,
        buffer_meters: float = 0.0,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        分析轨迹与区域的交集
        
        Args:
            trajectory_table: 轨迹数据表名
            region_table: 区域数据表名
            region_type: 区域类型过滤条件
            buffer_meters: 缓冲区半径(米)
            output_table: 输出表名
            
        Returns:
            包含交集分析结果的GeoDataFrame
        """
        region_filter = ""
        if region_type:
            region_filter = f"AND r.region_type = '{region_type}'"
            
        sql = text(f"""
            WITH trajectory_region_analysis AS (
                SELECT 
                    t.scene_token,
                    t.data_name,
                    t.city_id,
                    r.region_id,
                    r.region_type,
                    r.region_name,
                    t.geometry AS trajectory_geom,
                    r.geom AS region_geom,
                    ST_Area(
                        ST_Intersection(t.geometry, r.geom)::geography
                    ) AS intersection_area_m2,
                    ST_Area(t.geometry::geography) AS trajectory_area_m2,
                    ST_Area(r.geom::geography) AS region_area_m2,
                    ST_Intersection(t.geometry, r.geom) AS intersection_geom,
                    CASE 
                        WHEN ST_Within(t.geometry, r.geom) THEN 'completely_within'
                        WHEN ST_Contains(t.geometry, r.geom) THEN 'completely_contains' 
                        WHEN ST_Intersects(t.geometry, r.geom) THEN 'partial_overlap'
                        ELSE 'no_intersection'
                    END AS spatial_relationship
                FROM {trajectory_table} t
                JOIN {region_table} r ON t.city_id = r.city_id
                WHERE ST_Intersects(
                    CASE WHEN :buffer_meters > 0 THEN
                        ST_Buffer(ST_Transform(t.geometry, 3857), :buffer_meters)
                    ELSE 
                        ST_Transform(t.geometry, 3857)
                    END,
                    ST_Transform(r.geom, 3857)
                )
                {region_filter}
            )
            SELECT *,
                ROUND(
                    (intersection_area_m2 / NULLIF(trajectory_area_m2, 0)) * 100, 2
                ) AS trajectory_coverage_percent,
                ROUND(
                    (intersection_area_m2 / NULLIF(region_area_m2, 0)) * 100, 2
                ) AS region_coverage_percent
            FROM trajectory_region_analysis
            WHERE spatial_relationship != 'no_intersection'
            ORDER BY scene_token, intersection_area_m2 DESC
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"buffer_meters": buffer_meters},
                geom_col="intersection_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功分析轨迹与区域交集，共找到 {len(result_gdf)} 个交集")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"轨迹与区域交集分析失败: {str(e)}")
            raise
    
    def analyze_trajectory_to_trajectory_intersection(
        self,
        trajectory_table1: str = "clips_bbox",
        trajectory_table2: Optional[str] = None,
        buffer_meters: float = 5.0,
        time_tolerance_seconds: Optional[int] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        分析轨迹间的交集
        
        Args:
            trajectory_table1: 第一个轨迹数据表名
            trajectory_table2: 第二个轨迹数据表名，如果为None则分析table1内部的交集
            buffer_meters: 缓冲区半径(米)
            time_tolerance_seconds: 时间容差(秒)，仅考虑时间接近的轨迹
            output_table: 输出表名
            
        Returns:
            包含交集分析结果的GeoDataFrame
        """
        if trajectory_table2 is None:
            trajectory_table2 = trajectory_table1
            
        time_filter = ""
        if time_tolerance_seconds:
            time_filter = f"AND ABS(t1.timestamp - t2.timestamp) <= {time_tolerance_seconds}"
            
        sql = text(f"""
            WITH trajectory_intersections AS (
                SELECT 
                    t1.scene_token AS scene_token_1,
                    t1.data_name AS data_name_1,
                    t1.timestamp AS timestamp_1,
                    t2.scene_token AS scene_token_2,
                    t2.data_name AS data_name_2,
                    t2.timestamp AS timestamp_2,
                    t1.city_id,
                    t1.geometry AS trajectory_geom_1,
                    t2.geometry AS trajectory_geom_2,
                    ST_Distance(
                        ST_Transform(t1.geometry, 3857),
                        ST_Transform(t2.geometry, 3857)
                    ) AS min_distance_meters,
                    ST_Area(
                        ST_Intersection(
                            ST_Buffer(ST_Transform(t1.geometry, 3857), :buffer_meters),
                            ST_Buffer(ST_Transform(t2.geometry, 3857), :buffer_meters)
                        )::geography
                    ) AS intersection_area_m2,
                    ST_Intersection(
                        ST_Buffer(ST_Transform(t1.geometry, 3857), :buffer_meters),
                        ST_Buffer(ST_Transform(t2.geometry, 3857), :buffer_meters)
                    ) AS intersection_geom,
                    ABS(t1.timestamp - t2.timestamp) AS time_diff_seconds
                FROM {trajectory_table1} t1
                JOIN {trajectory_table2} t2 ON t1.city_id = t2.city_id
                WHERE t1.scene_token != t2.scene_token  -- 避免自己与自己比较
                AND ST_DWithin(
                    ST_Transform(t1.geometry, 3857),
                    ST_Transform(t2.geometry, 3857),
                    :buffer_meters * 2
                )
                {time_filter}
            )
            SELECT *,
                CASE 
                    WHEN min_distance_meters = 0 THEN 'direct_intersection'
                    WHEN min_distance_meters <= :buffer_meters THEN 'buffer_intersection'
                    ELSE 'distant'
                END AS intersection_type
            FROM trajectory_intersections
            WHERE intersection_area_m2 > 0
            ORDER BY city_id, time_diff_seconds, min_distance_meters
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"buffer_meters": buffer_meters},
                geom_col="intersection_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功分析轨迹间交集，共找到 {len(result_gdf)} 个交集")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"轨迹间交集分析失败: {str(e)}")
            raise
    
    def generate_intersection_summary(
        self,
        intersection_results: List[gpd.GeoDataFrame],
        output_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        生成交集分析汇总报告
        
        Args:
            intersection_results: 交集分析结果列表
            output_path: 输出路径，如果为None则不保存文件
            
        Returns:
            汇总统计的DataFrame
        """
        summary_data = []
        
        for i, result_gdf in enumerate(intersection_results):
            if len(result_gdf) == 0:
                continue
                
            # 基础统计
            stats = {
                'analysis_id': i,
                'total_intersections': len(result_gdf),
                'unique_scenes': result_gdf['scene_token'].nunique() if 'scene_token' in result_gdf.columns else 0,
                'unique_cities': result_gdf['city_id'].nunique() if 'city_id' in result_gdf.columns else 0,
            }
            
            # 特定字段统计
            if 'intersection_area_m2' in result_gdf.columns:
                stats.update({
                    'total_intersection_area_m2': result_gdf['intersection_area_m2'].sum(),
                    'avg_intersection_area_m2': result_gdf['intersection_area_m2'].mean(),
                    'max_intersection_area_m2': result_gdf['intersection_area_m2'].max(),
                })
                
            if 'distance_meters' in result_gdf.columns:
                stats.update({
                    'avg_distance_meters': result_gdf['distance_meters'].mean(),
                    'min_distance_meters': result_gdf['distance_meters'].min(),
                    'max_distance_meters': result_gdf['distance_meters'].max(),
                })
                
            if 'intersection_type' in result_gdf.columns:
                type_counts = result_gdf['intersection_type'].value_counts().to_dict()
                for intersection_type, count in type_counts.items():
                    stats[f'type_{intersection_type}_count'] = count
                    
            summary_data.append(stats)
        
        summary_df = pd.DataFrame(summary_data)
        
        if output_path:
            summary_df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"交集分析汇总已保存到: {output_path}")
            
        return summary_df
    
    def _save_to_database(self, gdf: gpd.GeoDataFrame, table_name: str):
        """保存GeoDataFrame到数据库"""
        try:
            gdf.to_postgis(
                table_name,
                self.engine,
                if_exists='replace',
                index=False
            )
            self.logger.info(f"分析结果已保存到数据库表: {table_name}")
        except Exception as e:
            self.logger.error(f"保存到数据库失败: {str(e)}")
            raise 