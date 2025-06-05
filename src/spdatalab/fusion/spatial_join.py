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
        left_geom_col: str = "geometry",
        right_geom_col: str = "geometry",
        distance_meters: Optional[float] = None,
        
        # QGIS风格的参数
        fields_to_add: Optional[List[str]] = None,  # 要添加的右表字段，None表示添加所有
        discard_nonmatching: bool = False,          # 是否丢弃未匹配的记录 (INNER vs LEFT JOIN)
        
        # 统计选项
        summarize: bool = False,                    # 是否进行统计汇总
        summary_fields: Optional[Dict[str, str]] = None,  # 统计字段和方法 {"field_name": "method"}
        
        # 其他选项  
        where_clause: Optional[str] = None,
        output_table: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        根据位置连接属性 - 类似QGIS的join attributes by location
        
        Args:
            left_table: 左表名（如clips_bbox）
            right_table: 右表名（如intersections）
            spatial_relation: 空间关系
            left_geom_col: 左表几何列名
            right_geom_col: 右表几何列名  
            distance_meters: 距离阈值（仅用于DWITHIN关系）
            
            # QGIS风格参数
            fields_to_add: 要添加的右表字段列表，None表示添加所有字段
            discard_nonmatching: True=INNER JOIN (丢弃未匹配), False=LEFT JOIN (保留所有左表记录)
            
            # 统计选项
            summarize: 是否对连接结果进行统计汇总
            summary_fields: 统计字段和方法，格式: {"field_name": "method"}
                          支持的方法: "count", "sum", "mean", "max", "min", "first"
                          特殊字段: "distance" - 计算到最近要素的距离
            
            where_clause: 额外的WHERE条件
            output_table: 输出表名
            
        Returns:
            连接结果的GeoDataFrame
            
        Examples:
            # 简单连接，添加所有字段
            result = joiner.join_attributes_by_location("clips_bbox", "intersections")
            
            # 只添加指定字段
            result = joiner.join_attributes_by_location(
                "clips_bbox", "intersections", 
                fields_to_add=["road_type", "intersection_id"]
            )
            
            # 统计分析
            result = joiner.join_attributes_by_location(
                "clips_bbox", "intersections",
                spatial_relation="dwithin", distance_meters=50,
                summarize=True,
                summary_fields={
                    "intersection_count": "count",
                    "nearest_distance": "distance",
                    "avg_road_width": "mean"  # 假设intersections表有road_width字段
                }
            )
        """
        # 转换枚举为字符串
        if isinstance(spatial_relation, SpatialRelation):
            spatial_relation = spatial_relation.value
            
        # 构建空间条件
        spatial_condition = self._build_spatial_condition(
            spatial_relation, f"l.{left_geom_col}", f"r.{right_geom_col}", distance_meters
        )
        
        # 构建连接类型
        join_clause = "JOIN" if discard_nonmatching else "LEFT JOIN"
        
        # 构建字段选择
        if summarize and summary_fields:
            # 统计模式
            field_selection = self._build_summary_fields(summary_fields)
            select_left_part = "l.scene_token, l.geometry"
            group_by_clause = "GROUP BY l.scene_token, l.geometry"
        else:
            # 普通连接模式
            field_selection = self._build_field_selection_qgis_style(fields_to_add)
            select_left_part = "l.*"
            group_by_clause = ""
        
        # 构建WHERE条件
        where_condition = ""
        if where_clause:
            where_condition = f"WHERE {where_clause}"
        
        sql = text(f"""
            SELECT 
                {select_left_part},
                {field_selection}
            FROM {left_table} l
            {join_clause} {right_table} r ON {spatial_condition}
            {where_condition}
            {group_by_clause}
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
                
            join_type_desc = "INNER" if discard_nonmatching else "LEFT"
            mode_desc = "统计汇总" if summarize else "字段连接"
            
            self.logger.info(
                f"空间连接完成: {left_table} {join_type_desc} JOIN {right_table} "
                f"({spatial_relation}, {mode_desc}), 结果: {len(result_gdf)} 条记录"
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
            summary_fields: 要汇总的字段，格式: {"new_name": "method"}
            output_table: 输出表名
            
        Returns:
            相交分析结果
        """
        # 默认统计字段
        if summary_fields is None:
            summary_fields = {
                f"{feature_type}_count": "count",
                f"nearest_{feature_type}_distance": "distance"
            }
        
        # 如果有缓冲区，使用DWITHIN关系
        spatial_relation = SpatialRelation.DWITHIN if buffer_meters > 0 else SpatialRelation.INTERSECTS
        
        return self.join_attributes_by_location(
            left_table="clips_bbox",
            right_table=feature_table,
            spatial_relation=spatial_relation,
            distance_meters=buffer_meters if buffer_meters > 0 else None,
            summarize=True,
            summary_fields=summary_fields,
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
    
    def _build_field_selection_qgis_style(self, fields_to_add: Optional[List[str]]) -> str:
        """构建QGIS风格的字段选择SQL"""
        if not fields_to_add:
            # None表示添加所有字段
            return "r.*"
        
        # 指定特定字段
        field_list = [f"r.{field}" for field in fields_to_add]
        return ", ".join(field_list)
    
    def _build_summary_fields(self, summary_fields: Dict[str, str]) -> str:
        """构建统计字段SQL"""
        selections = []
        
        for new_name, method in summary_fields.items():
            method = method.lower()
            
            if method == "count":
                selections.append(f"COUNT(r.*) AS {new_name}")
            elif method == "distance":
                # 特殊处理：计算最近距离
                selections.append(f"""
                    MIN(ST_Distance(
                        l.geometry::geography,
                        r.geometry::geography
                    )) AS {new_name}
                """)
            elif method in ["sum", "mean", "avg", "max", "min"]:
                # 对于数值统计，假设字段名就是要统计的字段
                # 这里需要用户在字段名中指定，如 "road_width_sum": "sum"
                # 我们从new_name中推断字段名
                if "_" in new_name:
                    # 尝试从new_name推断原字段名，如 "road_width_sum" -> "road_width"
                    base_field = "_".join(new_name.split("_")[:-1])
                    sql_method = "AVG" if method in ["mean", "avg"] else method.upper()
                    selections.append(f"{sql_method}(r.{base_field}) AS {new_name}")
                else:
                    # 如果无法推断，使用COUNT作为默认
                    selections.append(f"COUNT(r.*) AS {new_name}")
            elif method == "first":
                # 获取第一个匹配的记录
                selections.append(f"(array_agg(r.* ORDER BY ST_Distance(l.geometry, r.geometry)))[1] AS {new_name}")
            else:
                # 未知方法，默认为COUNT
                selections.append(f"COUNT(r.*) AS {new_name}")
        
        return ", ".join(selections) if selections else "COUNT(r.*) AS feature_count"

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