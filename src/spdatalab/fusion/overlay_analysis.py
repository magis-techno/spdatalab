"""
通用空间叠置分析模块

提供灵活的空间几何对象叠置分析功能，支持：
1. 点与面的叠置分析
2. 线与面的叠置分析  
3. 面与面的叠置分析
4. 缓冲区分析
5. 统计分析和可视化
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.geometry import Point, LineString, Polygon, MultiPolygon
from shapely.ops import transform, unary_union
import pyproj
from functools import partial

from ..common.config import LOCAL_DSN

logger = logging.getLogger(__name__)

class OverlayAnalyzer:
    """通用空间叠置分析器"""
    
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
    
    def point_in_polygon_analysis(
        self,
        point_table: str,
        polygon_table: str,
        point_geom_col: str = "geometry",
        polygon_geom_col: str = "geom",
        join_condition: Optional[str] = None,
        additional_filters: Optional[str] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        点在面内的叠置分析
        
        Args:
            point_table: 点数据表名
            polygon_table: 面数据表名
            point_geom_col: 点几何列名
            polygon_geom_col: 面几何列名
            join_condition: 额外的连接条件，如 "p.city_id = poly.city_id"
            additional_filters: 额外的过滤条件
            output_table: 输出表名
            
        Returns:
            包含叠置分析结果的GeoDataFrame
        """
        join_clause = ""
        if join_condition:
            join_clause = f"AND {join_condition}"
            
        filter_clause = ""
        if additional_filters:
            filter_clause = f"AND {additional_filters}"
            
        sql = text(f"""
            WITH point_polygon_overlay AS (
                SELECT 
                    p.*,
                    poly.* EXCEPT ({polygon_geom_col}),
                    poly.{polygon_geom_col} AS containing_polygon,
                    ST_Distance(
                        ST_Transform(p.{point_geom_col}, 3857),
                        ST_Transform(poly.{polygon_geom_col}, 3857)
                    ) AS distance_to_boundary_meters,
                    ST_Area(poly.{polygon_geom_col}::geography) AS polygon_area_m2
                FROM {point_table} p
                JOIN {polygon_table} poly ON ST_Within(p.{point_geom_col}, poly.{polygon_geom_col})
                {join_clause}
                WHERE p.{point_geom_col} IS NOT NULL 
                AND poly.{polygon_geom_col} IS NOT NULL
                {filter_clause}
            )
            SELECT 
                *,
                'point_in_polygon' AS overlay_type,
                CURRENT_TIMESTAMP AS analysis_time
            FROM point_polygon_overlay
            ORDER BY polygon_area_m2 DESC
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                geom_col=point_geom_col
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功完成点在面内分析，共找到 {len(result_gdf)} 个叠置结果")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"点在面内分析失败: {str(e)}")
            raise
    
    def line_polygon_intersection_analysis(
        self,
        line_table: str,
        polygon_table: str,
        line_geom_col: str = "geometry",
        polygon_geom_col: str = "geom",
        join_condition: Optional[str] = None,
        intersection_threshold_meters: float = 1.0,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        线与面的交集分析
        
        Args:
            line_table: 线数据表名
            polygon_table: 面数据表名  
            line_geom_col: 线几何列名
            polygon_geom_col: 面几何列名
            join_condition: 连接条件
            intersection_threshold_meters: 最小交集长度阈值(米)
            output_table: 输出表名
            
        Returns:
            包含交集分析结果的GeoDataFrame
        """
        join_clause = ""
        if join_condition:
            join_clause = f"AND {join_condition}"
            
        sql = text(f"""
            WITH line_polygon_intersections AS (
                SELECT 
                    l.*,
                    p.* EXCEPT ({polygon_geom_col}),
                    ST_Intersection(l.{line_geom_col}, p.{polygon_geom_col}) AS intersection_geom,
                    ST_Length(
                        ST_Intersection(
                            ST_Transform(l.{line_geom_col}, 3857),
                            ST_Transform(p.{polygon_geom_col}, 3857)
                        )
                    ) AS intersection_length_meters,
                    ST_Length(ST_Transform(l.{line_geom_col}, 3857)) AS total_line_length_meters,
                    ST_Area(p.{polygon_geom_col}::geography) AS polygon_area_m2,
                    CASE 
                        WHEN ST_Within(l.{line_geom_col}, p.{polygon_geom_col}) THEN 'completely_within'
                        WHEN ST_Intersects(l.{line_geom_col}, p.{polygon_geom_col}) THEN 'partial_intersection'
                        ELSE 'no_intersection'
                    END AS intersection_type
                FROM {line_table} l
                JOIN {polygon_table} p ON ST_Intersects(l.{line_geom_col}, p.{polygon_geom_col})
                {join_clause}
                WHERE l.{line_geom_col} IS NOT NULL 
                AND p.{polygon_geom_col} IS NOT NULL
            )
            SELECT 
                *,
                ROUND(
                    (intersection_length_meters / NULLIF(total_line_length_meters, 0)) * 100, 2
                ) AS intersection_percentage,
                'line_polygon_intersection' AS overlay_type,
                CURRENT_TIMESTAMP AS analysis_time
            FROM line_polygon_intersections
            WHERE intersection_length_meters >= :threshold
            ORDER BY intersection_length_meters DESC
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"threshold": intersection_threshold_meters},
                geom_col="intersection_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功完成线面交集分析，共找到 {len(result_gdf)} 个交集")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"线面交集分析失败: {str(e)}")
            raise
    
    def polygon_polygon_overlay_analysis(
        self,
        polygon_table1: str,
        polygon_table2: str,
        geom_col1: str = "geometry",
        geom_col2: str = "geom",
        join_condition: Optional[str] = None,
        overlay_type: str = "intersection",  # intersection, union, difference, symmetric_difference
        min_area_threshold_m2: float = 1.0,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        面与面的叠置分析
        
        Args:
            polygon_table1: 第一个面数据表名
            polygon_table2: 第二个面数据表名
            geom_col1: 第一个表的几何列名
            geom_col2: 第二个表的几何列名
            join_condition: 连接条件
            overlay_type: 叠置类型 (intersection, union, difference, symmetric_difference)
            min_area_threshold_m2: 最小面积阈值(平方米)
            output_table: 输出表名
            
        Returns:
            包含叠置分析结果的GeoDataFrame
        """
        overlay_func_map = {
            "intersection": "ST_Intersection",
            "union": "ST_Union", 
            "difference": "ST_Difference",
            "symmetric_difference": "ST_SymDifference"
        }
        
        if overlay_type not in overlay_func_map:
            raise ValueError(f"不支持的叠置类型: {overlay_type}")
            
        overlay_func = overlay_func_map[overlay_type]
        
        join_clause = ""
        if join_condition:
            join_clause = f"AND {join_condition}"
            
        sql = text(f"""
            WITH polygon_overlay AS (
                SELECT 
                    p1.*,
                    p2.* EXCEPT ({geom_col2}),
                    {overlay_func}(p1.{geom_col1}, p2.{geom_col2}) AS overlay_geom,
                    ST_Area(p1.{geom_col1}::geography) AS polygon1_area_m2,
                    ST_Area(p2.{geom_col2}::geography) AS polygon2_area_m2,
                    ST_Area(
                        {overlay_func}(p1.{geom_col1}, p2.{geom_col2})::geography
                    ) AS overlay_area_m2,
                    CASE 
                        WHEN ST_Within(p1.{geom_col1}, p2.{geom_col2}) THEN 'p1_within_p2'
                        WHEN ST_Within(p2.{geom_col2}, p1.{geom_col1}) THEN 'p2_within_p1'
                        WHEN ST_Intersects(p1.{geom_col1}, p2.{geom_col2}) THEN 'partial_overlap'
                        ELSE 'no_overlap'
                    END AS spatial_relationship
                FROM {polygon_table1} p1
                JOIN {polygon_table2} p2 ON ST_Intersects(p1.{geom_col1}, p2.{geom_col2})
                {join_clause}
                WHERE p1.{geom_col1} IS NOT NULL 
                AND p2.{geom_col2} IS NOT NULL
            )
            SELECT 
                *,
                ROUND(
                    (overlay_area_m2 / NULLIF(polygon1_area_m2, 0)) * 100, 2
                ) AS polygon1_coverage_percent,
                ROUND(
                    (overlay_area_m2 / NULLIF(polygon2_area_m2, 0)) * 100, 2
                ) AS polygon2_coverage_percent,
                '{overlay_type}' AS overlay_type,
                CURRENT_TIMESTAMP AS analysis_time
            FROM polygon_overlay
            WHERE overlay_area_m2 >= :min_area_threshold
            AND overlay_geom IS NOT NULL
            ORDER BY overlay_area_m2 DESC
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"min_area_threshold": min_area_threshold_m2},
                geom_col="overlay_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功完成面面叠置分析({overlay_type})，共找到 {len(result_gdf)} 个叠置结果")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"面面叠置分析失败: {str(e)}")
            raise
    
    def buffer_analysis(
        self,
        input_table: str,
        buffer_distance_meters: float,
        geom_col: str = "geometry",
        dissolve: bool = False,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        缓冲区分析
        
        Args:
            input_table: 输入数据表名
            buffer_distance_meters: 缓冲区距离(米)
            geom_col: 几何列名
            dissolve: 是否合并重叠的缓冲区
            output_table: 输出表名
            
        Returns:
            包含缓冲区的GeoDataFrame
        """
        dissolve_clause = ""
        if dissolve:
            dissolve_clause = """
                , dissolved_buffers AS (
                    SELECT 
                        ST_Union(buffer_geom) AS dissolved_geom,
                        COUNT(*) AS merged_count,
                        SUM(buffer_area_m2) AS total_area_m2
                    FROM buffered_geometries
                )
            """
            select_clause = """
                SELECT 
                    dissolved_geom AS buffer_geom,
                    merged_count,
                    total_area_m2 AS buffer_area_m2,
                    'dissolved_buffer' AS buffer_type
                FROM dissolved_buffers
            """
        else:
            select_clause = """
                SELECT 
                    *,
                    'individual_buffer' AS buffer_type,
                    CURRENT_TIMESTAMP AS analysis_time
                FROM buffered_geometries
            """
            
        sql = text(f"""
            WITH buffered_geometries AS (
                SELECT 
                    *,
                    ST_Transform(
                        ST_Buffer(
                            ST_Transform({geom_col}, 3857), 
                            :buffer_distance
                        ), 
                        4326
                    ) AS buffer_geom,
                    ST_Area(
                        ST_Buffer(
                            ST_Transform({geom_col}, 3857), 
                            :buffer_distance
                        )::geography
                    ) AS buffer_area_m2
                FROM {input_table}
                WHERE {geom_col} IS NOT NULL
            )
            {dissolve_clause}
            {select_clause}
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"buffer_distance": buffer_distance_meters},
                geom_col="buffer_geom"
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功完成缓冲区分析，生成 {len(result_gdf)} 个缓冲区")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"缓冲区分析失败: {str(e)}")
            raise
    
    def proximity_analysis(
        self,
        target_table: str,
        reference_table: str,
        max_distance_meters: float,
        target_geom_col: str = "geometry",
        reference_geom_col: str = "geom",
        join_condition: Optional[str] = None,
        k_nearest: Optional[int] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        邻近分析
        
        Args:
            target_table: 目标数据表名
            reference_table: 参考数据表名
            max_distance_meters: 最大距离(米)
            target_geom_col: 目标表几何列名
            reference_geom_col: 参考表几何列名
            join_condition: 连接条件
            k_nearest: 每个目标找k个最近的参考点，如果为None则找所有在距离内的
            output_table: 输出表名
            
        Returns:
            包含邻近分析结果的GeoDataFrame
        """
        join_clause = ""
        if join_condition:
            join_clause = f"AND {join_condition}"
            
        k_filter = ""
        if k_nearest:
            k_filter = f"""
                , ranked_distances AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY target_id 
                            ORDER BY distance_meters
                        ) AS distance_rank
                    FROM proximity_results
                )
                SELECT * FROM ranked_distances 
                WHERE distance_rank <= {k_nearest}
            """
        else:
            k_filter = "SELECT * FROM proximity_results"
            
        sql = text(f"""
            WITH proximity_results AS (
                SELECT 
                    t.*,
                    r.* EXCEPT ({reference_geom_col}),
                    r.{reference_geom_col} AS reference_geom,
                    ST_Distance(
                        ST_Transform(t.{target_geom_col}, 3857),
                        ST_Transform(r.{reference_geom_col}, 3857)
                    ) AS distance_meters,
                    ST_Azimuth(
                        ST_Centroid(t.{target_geom_col}),
                        ST_Centroid(r.{reference_geom_col})
                    ) * 180 / PI() AS bearing_degrees,
                    CASE 
                        WHEN ST_Distance(
                            ST_Transform(t.{target_geom_col}, 3857),
                            ST_Transform(r.{reference_geom_col}, 3857)
                        ) <= 50 THEN 'very_close'
                        WHEN ST_Distance(
                            ST_Transform(t.{target_geom_col}, 3857),
                            ST_Transform(r.{reference_geom_col}, 3857)
                        ) <= 200 THEN 'close'
                        WHEN ST_Distance(
                            ST_Transform(t.{target_geom_col}, 3857),
                            ST_Transform(r.{reference_geom_col}, 3857)
                        ) <= 500 THEN 'moderate'
                        ELSE 'distant'
                    END AS proximity_category
                FROM {target_table} t
                CROSS JOIN {reference_table} r
                WHERE ST_DWithin(
                    ST_Transform(t.{target_geom_col}, 3857),
                    ST_Transform(r.{reference_geom_col}, 3857),
                    :max_distance
                )
                {join_clause}
                AND t.{target_geom_col} IS NOT NULL 
                AND r.{reference_geom_col} IS NOT NULL
            )
            {k_filter}
            ORDER BY distance_meters
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                params={"max_distance": max_distance_meters},
                geom_col=target_geom_col
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(f"成功完成邻近分析，共找到 {len(result_gdf)} 个邻近关系")
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"邻近分析失败: {str(e)}")
            raise
    
    def generate_overlay_statistics(
        self,
        overlay_results: gpd.GeoDataFrame,
        group_by_columns: List[str] = None,
        output_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        生成叠置分析统计报告
        
        Args:
            overlay_results: 叠置分析结果
            group_by_columns: 分组统计的列名列表
            output_path: 输出路径
            
        Returns:
            统计结果DataFrame
        """
        if len(overlay_results) == 0:
            self.logger.warning("没有叠置结果可以统计")
            return pd.DataFrame()
            
        stats_data = []
        
        # 基础统计
        basic_stats = {
            'total_records': len(overlay_results),
            'analysis_time': pd.Timestamp.now(),
        }
        
        # 几何统计
        if 'overlay_area_m2' in overlay_results.columns:
            basic_stats.update({
                'total_overlay_area_m2': overlay_results['overlay_area_m2'].sum(),
                'avg_overlay_area_m2': overlay_results['overlay_area_m2'].mean(),
                'max_overlay_area_m2': overlay_results['overlay_area_m2'].max(),
                'min_overlay_area_m2': overlay_results['overlay_area_m2'].min(),
            })
            
        if 'distance_meters' in overlay_results.columns:
            basic_stats.update({
                'avg_distance_meters': overlay_results['distance_meters'].mean(),
                'max_distance_meters': overlay_results['distance_meters'].max(),
                'min_distance_meters': overlay_results['distance_meters'].min(),
            })
            
        stats_data.append(basic_stats)
        
        # 分组统计
        if group_by_columns:
            for col in group_by_columns:
                if col in overlay_results.columns:
                    group_stats = overlay_results.groupby(col).agg({
                        overlay_results.columns[0]: 'count',  # 使用第一列作为计数
                        **({
                            'overlay_area_m2': ['sum', 'mean', 'max']
                        } if 'overlay_area_m2' in overlay_results.columns else {}),
                        **({
                            'distance_meters': ['mean', 'min', 'max']
                        } if 'distance_meters' in overlay_results.columns else {})
                    }).round(2)
                    
                    group_stats.columns = ['_'.join(col_names).strip() for col_names in group_stats.columns]
                    group_stats = group_stats.reset_index()
                    group_stats['group_by_column'] = col
                    
                    for _, row in group_stats.iterrows():
                        stats_data.append(row.to_dict())
        
        stats_df = pd.DataFrame(stats_data)
        
        if output_path:
            stats_df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"叠置分析统计已保存到: {output_path}")
            
        return stats_df
    
    def _save_to_database(self, gdf: gpd.GeoDataFrame, table_name: str):
        """保存GeoDataFrame到数据库"""
        try:
            gdf.to_postgis(
                table_name,
                self.engine,
                if_exists='replace',
                index=False
            )
            self.logger.info(f"叠置分析结果已保存到数据库表: {table_name}")
        except Exception as e:
            self.logger.error(f"保存到数据库失败: {str(e)}")
            raise 