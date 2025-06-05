"""
空间连接模块 - 类似QGIS的"join attributes by location"

提供简洁的空间连接功能，支持bbox与各种要素的空间叠置分析
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from enum import Enum

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

logger = logging.getLogger(__name__)

class SpatialRelation(Enum):
    """空间关系枚举"""
    INTERSECTS = "intersects"
    WITHIN = "within"
    CONTAINS = "contains"
    TOUCHES = "touches"
    CROSSES = "crosses"
    OVERLAPS = "overlaps"
    DWITHIN = "dwithin"  # 距离范围内

class JoinType(Enum):
    """连接类型枚举"""
    LEFT = "left"         # 保留所有左表记录
    INNER = "inner"       # 只保留有匹配的记录
    RIGHT = "right"       # 保留所有右表记录（少用）

class SummaryMethod(Enum):
    """汇总方法枚举"""
    FIRST = "first"       # 第一个匹配
    COUNT = "count"       # 计数
    SUM = "sum"           # 求和
    MEAN = "mean"         # 平均值
    MAX = "max"           # 最大值
    MIN = "min"           # 最小值
    COLLECT = "collect"   # 收集所有值（数组）

class SpatialJoin:
    """空间连接分析器 - 类似QGIS的join attributes by location"""
    
    def __init__(self, engine=None):
        """
        初始化空间连接器
        
        Args:
            engine: SQLAlchemy引擎，如果为None则创建默认引擎
        """
        if engine is None:
            self.engine = create_engine(LOCAL_DSN, future=True)
        else:
            self.engine = engine
            
        self.logger = logging.getLogger(__name__)
    
    def join_attributes_by_location(
        self,
        left_table: str,
        right_table: str,
        spatial_relation: Union[SpatialRelation, str] = SpatialRelation.INTERSECTS,
        join_type: Union[JoinType, str] = JoinType.LEFT,
        left_geom_col: str = "geometry",
        right_geom_col: str = "geometry",
        distance_meters: Optional[float] = None,
        select_fields: Optional[Dict[str, Union[str, SummaryMethod]]] = None,
        where_clause: Optional[str] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        根据位置连接属性 - 类似QGIS的join attributes by location
        
        Args:
            left_table: 左表名（如clips_bbox）
            right_table: 右表名（如intersections）
            spatial_relation: 空间关系
            join_type: 连接类型
            left_geom_col: 左表几何列名
            right_geom_col: 右表几何列名
            distance_meters: 距离阈值（仅用于DWITHIN关系）
            select_fields: 要选择的字段及汇总方式，格式: {"new_field_name": "source_field" 或 SummaryMethod}
            where_clause: 额外的WHERE条件
            output_table: 输出表名
            
        Returns:
            连接结果的GeoDataFrame
        """
        # 转换枚举为字符串
        if isinstance(spatial_relation, SpatialRelation):
            spatial_relation = spatial_relation.value
        if isinstance(join_type, JoinType):
            join_type = join_type.value
            
        # 构建空间条件
        spatial_condition = self._build_spatial_condition(
            spatial_relation, f"l.{left_geom_col}", f"r.{right_geom_col}", distance_meters
        )
        
        # 构建连接类型
        join_clause = self._build_join_clause(join_type)
        
        # 构建字段选择
        field_selection = self._build_field_selection(select_fields)
        
        # 构建WHERE条件
        where_condition = ""
        if where_clause:
            where_condition = f"WHERE {where_clause}"
        
        sql = text(f"""
            SELECT 
                l.*,
                {field_selection}
            FROM {left_table} l
            {join_clause} {right_table} r ON {spatial_condition}
            {where_condition}
            ORDER BY l.scene_token
        """)
        
        try:
            result_gdf = gpd.read_postgis(
                sql,
                self.engine,
                geom_col=left_geom_col
            )
            
            if output_table:
                self._save_to_database(result_gdf, output_table)
                
            self.logger.info(
                f"空间连接完成: {left_table} {join_type} {right_table} "
                f"({spatial_relation}), 结果: {len(result_gdf)} 条记录"
            )
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"空间连接失败: {str(e)}")
            raise
    
    def bbox_intersect_features(
        self,
        feature_table: str,
        feature_type: str = "intersections",
        buffer_meters: float = 0.0,
        summary_fields: Optional[Dict[str, str]] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        bbox与要素相交分析的简化接口
        
        Args:
            feature_table: 要素表名（应使用标准化的表名，如 intersections）
            feature_type: 要素类型描述（用于日志）
            buffer_meters: 缓冲区半径
            summary_fields: 要汇总的字段，格式: {"new_name": "field|method"}
            output_table: 输出表名
            
        Returns:
            相交分析结果
        """
        # 默认汇总字段
        if summary_fields is None:
            summary_fields = {
                f"{feature_type}_count": "count",
                f"nearest_{feature_type}_distance": "min_distance"
            }
        
        # 构建字段选择
        select_fields = {}
        for new_name, field_method in summary_fields.items():
            if field_method == "count":
                select_fields[new_name] = SummaryMethod.COUNT
            elif field_method == "min_distance":
                select_fields[new_name] = "distance_meters|min"
            else:
                select_fields[new_name] = field_method
        
        # 如果有缓冲区，使用DWITHIN关系
        spatial_relation = SpatialRelation.DWITHIN if buffer_meters > 0 else SpatialRelation.INTERSECTS
        
        return self.join_attributes_by_location(
            left_table="clips_bbox",
            right_table=feature_table,
            spatial_relation=spatial_relation,
            distance_meters=buffer_meters if buffer_meters > 0 else None,
            select_fields=select_fields,
            output_table=output_table
        )
    
    def spatial_summary(
        self,
        target_table: str,
        summary_table: str,
        group_by_field: str,
        spatial_relation: Union[SpatialRelation, str] = SpatialRelation.INTERSECTS,
        summary_fields: Optional[Dict[str, SummaryMethod]] = None,
        distance_meters: Optional[float] = None
    ) -> pd.DataFrame:
        """
        空间汇总分析
        
        Args:
            target_table: 目标表
            summary_table: 汇总表
            group_by_field: 分组字段
            spatial_relation: 空间关系
            summary_fields: 汇总字段和方法
            distance_meters: 距离阈值
            
        Returns:
            汇总结果DataFrame
        """
        if isinstance(spatial_relation, SpatialRelation):
            spatial_relation = spatial_relation.value
            
        if summary_fields is None:
            summary_fields = {"count": SummaryMethod.COUNT}
        
        # 构建空间条件
        spatial_condition = self._build_spatial_condition(
            spatial_relation, "t.geometry", "s.geom", distance_meters
        )
        
        # 构建汇总字段
        summary_expressions = []
        for field_name, method in summary_fields.items():
            if method == SummaryMethod.COUNT:
                summary_expressions.append("COUNT(*) AS count")
            elif method == SummaryMethod.SUM:
                summary_expressions.append(f"SUM({field_name}) AS sum_{field_name}")
            elif method == SummaryMethod.MEAN:
                summary_expressions.append(f"AVG({field_name}) AS avg_{field_name}")
            # 可以添加更多汇总方法
        
        sql = text(f"""
            SELECT 
                s.{group_by_field},
                {', '.join(summary_expressions)}
            FROM {summary_table} s
            LEFT JOIN {target_table} t ON {spatial_condition}
            GROUP BY s.{group_by_field}
            ORDER BY s.{group_by_field}
        """)
        
        return pd.read_sql(sql, self.engine)
    
    def _build_spatial_condition(
        self, 
        relation: str, 
        left_geom: str, 
        right_geom: str, 
        distance: Optional[float]
    ) -> str:
        """构建空间条件SQL"""
        if relation == "intersects":
            return f"ST_Intersects({left_geom}, {right_geom})"
        elif relation == "within":
            return f"ST_Within({left_geom}, {right_geom})"
        elif relation == "contains":
            return f"ST_Contains({left_geom}, {right_geom})"
        elif relation == "touches":
            return f"ST_Touches({left_geom}, {right_geom})"
        elif relation == "crosses":
            return f"ST_Crosses({left_geom}, {right_geom})"
        elif relation == "overlaps":
            return f"ST_Overlaps({left_geom}, {right_geom})"
        elif relation == "dwithin":
            if distance is None:
                raise ValueError("distance_meters is required for DWITHIN relation")
            # 使用geography类型直接在WGS84坐标系进行米单位的距离计算
            # 避免坐标系转换，更准确且高效
            return f"""ST_DWithin(
                {left_geom}::geography,
                {right_geom}::geography,
                {distance}
            )"""
        else:
            raise ValueError(f"Unsupported spatial relation: {relation}")
    
    def _build_join_clause(self, join_type: str) -> str:
        """构建连接子句"""
        if join_type == "left":
            return "LEFT JOIN"
        elif join_type == "inner":
            return "JOIN"
        elif join_type == "right":
            return "RIGHT JOIN"
        else:
            raise ValueError(f"Unsupported join type: {join_type}")
    
    def _build_field_selection(self, select_fields: Optional[Dict]) -> str:
        """构建字段选择SQL"""
        if not select_fields:
            return "r.*"
        
        selections = []
        for new_name, field_spec in select_fields.items():
            if isinstance(field_spec, SummaryMethod):
                if field_spec == SummaryMethod.COUNT:
                    selections.append(f"COUNT(r.*) AS {new_name}")
                elif field_spec == SummaryMethod.FIRST:
                    selections.append(f"FIRST(r.*) AS {new_name}")
                # 可以添加更多汇总方法
            elif isinstance(field_spec, str):
                if "|" in field_spec:
                    field, method = field_spec.split("|")
                    if method == "min" and field == "distance_meters":
                        selections.append(f"""
                            MIN(ST_Distance(
                                l.geometry::geography,
                                r.geometry::geography
                            )) AS {new_name}
                        """)
                    else:
                        selections.append(f"{method.upper()}(r.{field}) AS {new_name}")
                else:
                    selections.append(f"r.{field_spec} AS {new_name}")
        
        return ", ".join(selections) if selections else "r.*"
    
    def _save_to_database(self, gdf: gpd.GeoDataFrame, table_name: str):
        """保存到数据库"""
        try:
            gdf.to_postgis(
                table_name,
                self.engine,
                if_exists='replace',
                index=False
            )
            self.logger.info(f"结果已保存到数据库表: {table_name}")
        except Exception as e:
            self.logger.error(f"保存到数据库失败: {str(e)}")
            raise 