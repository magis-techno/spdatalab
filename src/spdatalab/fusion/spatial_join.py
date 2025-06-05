"""
空间连接模块 - 类似QGIS的"join attributes by location"

提供简洁的空间连接功能，支持bbox与各种要素的空间叠置分析
支持分批推送到远端计算的高效模式
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from enum import Enum
import uuid
from datetime import datetime

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
# 远端数据库连接配置（rcdatalake_gy1）
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

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
    
    def __init__(self, engine=None, remote_engine=None):
        """
        初始化空间连接器
        
        Args:
            engine: 本地SQLAlchemy引擎，如果为None则创建默认引擎
            remote_engine: 远端SQLAlchemy引擎，如果为None则创建默认引擎
        """
        if engine is None:
            self.engine = create_engine(LOCAL_DSN, future=True)
        else:
            self.engine = engine
        
        if remote_engine is None:
            # 添加编码和连接参数
            self.remote_engine = create_engine(
                REMOTE_DSN, 
                future=True,
                connect_args={
                    "client_encoding": "utf8",
                    "application_name": "spdatalab_spatial_join"
                }
            )
        else:
            self.remote_engine = remote_engine
            
        self.logger = logging.getLogger(__name__)
    
    def batch_spatial_join_with_remote(
        self,
        left_table: str = "clips_bbox",
        remote_table: str = "full_intersection",
        batch_by_city: bool = True,
        spatial_relation: Union[SpatialRelation, str] = SpatialRelation.INTERSECTS,
        distance_meters: Optional[float] = None,
        
        # 字段和统计选项
        fields_to_add: Optional[List[str]] = None,
        summarize: bool = True,
        summary_fields: Optional[Dict[str, str]] = None,
        
        # 分批控制
        limit_batches: Optional[int] = None,  # 限制处理的批次数量（用于测试）
        city_ids: Optional[List[str]] = None,  # 指定要处理的城市ID列表
        where_clause: Optional[str] = None,   # 本地表的过滤条件
        
        # 输出选项
        output_table: Optional[str] = None,
        temp_table_prefix: str = "temp_bbox_batch"
    ) -> gpd.GeoDataFrame:
        """
        分批推送到远端进行空间连接计算
        
        这个方法解决了FDW远程计算效率低的问题，通过以下策略：
        1. 从本地clips_bbox表按城市ID分批获取数据
        2. 将每批数据推送到远端作为临时表
        3. 在远端与full_intersection进行空间连接
        4. 返回结果并合并
        
        Args:
            left_table: 本地左表名（通常是clips_bbox）
            remote_table: 远端右表名（通常是full_intersection）
            batch_by_city: 是否按城市ID分批（推荐，默认True）
            spatial_relation: 空间关系
            distance_meters: 距离阈值（仅用于DWITHIN关系）
            
            fields_to_add: 要添加的远端表字段列表
            summarize: 是否进行统计汇总
            summary_fields: 统计字段和方法
            
            limit_batches: 限制处理的批次数量（用于测试）
            city_ids: 指定要处理的城市ID列表，None表示处理所有城市
            where_clause: 本地表的过滤条件
            
            output_table: 输出表名
            temp_table_prefix: 远端临时表前缀
            
        Returns:
            空间连接结果的GeoDataFrame
        """
        self.logger.info(f"开始分批空间连接: {left_table} -> 远端{remote_table}")
        
        # 检查远端数据库版本
        self._check_remote_db_version()
        
        # 转换枚举为字符串
        if isinstance(spatial_relation, SpatialRelation):
            spatial_relation = spatial_relation.value
        
        # 设置默认统计字段
        if summarize and summary_fields is None:
            summary_fields = {
                "intersection_count": "count",
                "nearest_distance": "distance"
            }
        
        # 获取本地数据的城市ID列表和分批信息
        total_count, city_batches = self._get_city_batch_info(left_table, city_ids, where_clause)
        
        if limit_batches:
            city_batches = city_batches[:limit_batches]
            self.logger.info(f"限制测试：只处理前{limit_batches}个城市")
        
        self.logger.info(f"总计{total_count}条记录，涉及{len(city_batches)}个城市")
        
        # 存储所有批次的结果
        all_results = []
        
        try:
            for batch_num, (city_id, city_count) in enumerate(city_batches, 1):
                self.logger.info(f"处理城市 {batch_num}/{len(city_batches)}: {city_id} ({city_count}条记录)")
                
                # 步骤1：获取本地数据批次（按城市ID）
                local_batch = self._fetch_local_batch_by_city(left_table, city_id, where_clause)
                if local_batch.empty:
                    self.logger.warning(f"城市{city_id}数据为空，跳过")
                    continue
                
                # 步骤2：推送到远端临时表
                temp_table_name = f"{temp_table_prefix}_{city_id}_{uuid.uuid4().hex[:8]}"
                self._push_batch_to_remote(local_batch, temp_table_name)
                
                try:
                    # 步骤3：在远端执行空间连接
                    batch_result = self._execute_remote_spatial_join(
                        temp_table_name, remote_table,
                        spatial_relation, distance_meters,
                        fields_to_add, summarize, summary_fields
                    )
                    
                    if not batch_result.empty:
                        all_results.append(batch_result)
                        self.logger.info(f"城市{city_id}完成，返回{len(batch_result)}条结果")
                    else:
                        self.logger.info(f"城市{city_id}无匹配结果")
                    
                finally:
                    # 步骤4：清理远端临时表
                    self._cleanup_remote_temp_table(temp_table_name)
        
        except Exception as e:
            self.logger.error(f"分批空间连接失败: {str(e)}")
            raise
        
        # 合并所有结果
        if all_results:
            final_result = pd.concat(all_results, ignore_index=True)
            # 转换为GeoDataFrame
            final_gdf = gpd.GeoDataFrame(final_result, geometry='geometry', crs=4326)
            
            self.logger.info(f"分批空间连接完成，总计{len(final_gdf)}条结果")
            
            # 保存到输出表
            if output_table:
                self._save_to_database(final_gdf, output_table)
            
            return final_gdf
        else:
            self.logger.info("所有批次均无匹配结果，返回空结果")
            return gpd.GeoDataFrame()
    
    def _get_city_batch_info(self, table_name: str, city_ids: Optional[List[str]] = None, where_clause: Optional[str] = None) -> Tuple[int, List[Tuple[str, int]]]:
        """获取城市分批信息"""
        # 构建WHERE条件
        where_conditions = []
        if where_clause:
            where_conditions.append(where_clause)
        if city_ids:
            city_list = "', '".join(city_ids)
            where_conditions.append(f"city_id IN ('{city_list}')")
        
        where_sql = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # 获取每个城市的记录数
        sql = text(f"""
            SELECT city_id, COUNT(*) as count
            FROM {table_name} 
            {where_sql}
            GROUP BY city_id
            ORDER BY city_id
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(sql).fetchall()
        
        # 转换为列表格式 [(city_id, count), ...]
        city_batches = [(row[0], row[1]) for row in result]
        total_count = sum(count for _, count in city_batches)
        
        return total_count, city_batches
    
    def _fetch_local_batch_by_city(self, table_name: str, city_id: str, where_clause: Optional[str] = None) -> gpd.GeoDataFrame:
        """按城市ID获取本地数据批次"""
        # 构建WHERE条件
        where_conditions = [f"city_id = '{city_id}'"]
        if where_clause:
            where_conditions.append(where_clause)
        
        where_sql = f"WHERE {' AND '.join(where_conditions)}"
        
        sql = text(f"""
            SELECT scene_token, city_id, 
                   ST_AsText(geometry) as geometry_wkt,
                   geometry
            FROM {table_name} 
            {where_sql}
            ORDER BY scene_token
        """)
        
        try:
            # 使用WKT格式确保几何数据兼容性
            result_gdf = gpd.read_postgis(sql, self.engine, geom_col='geometry')
            
            # 确保几何数据有效
            if not result_gdf.empty:
                # 验证几何数据
                valid_mask = result_gdf['geometry'].notna()
                if valid_mask.sum() < len(result_gdf):
                    self.logger.warning(f"城市{city_id}有{(~valid_mask).sum()}条无效几何记录被过滤")
                    result_gdf = result_gdf[valid_mask]
            
            return result_gdf
            
        except Exception as e:
            self.logger.error(f"获取城市{city_id}数据失败: {str(e)}")
            # 如果上面的方法失败，尝试简单方式
            simple_sql = text(f"""
                SELECT scene_token, city_id, geometry
                FROM {table_name} 
                {where_sql}
                ORDER BY scene_token
            """)
            return gpd.read_postgis(simple_sql, self.engine, geom_col='geometry')
    
    def _push_batch_to_remote(self, batch_gdf: gpd.GeoDataFrame, temp_table_name: str):
        """推送批次数据到远端临时表"""
        try:
            # 确保几何列格式正确
            batch_copy = batch_gdf.copy()
            
            # 确保几何列是正确的格式
            if 'geometry' in batch_copy.columns:
                # 确保几何列是有效的几何对象
                batch_copy['geometry'] = batch_copy['geometry'].apply(
                    lambda geom: geom if geom is not None and hasattr(geom, 'wkt') else None
                )
                # 移除无效几何的行
                batch_copy = batch_copy.dropna(subset=['geometry'])
            
            if batch_copy.empty:
                self.logger.warning(f"批次数据在清理后为空，跳过推送到 {temp_table_name}")
                return
            
            # 推送到远端作为临时表
            batch_copy.to_postgis(
                temp_table_name,
                self.remote_engine,
                if_exists='replace',
                index=False
            )
            
            self.logger.debug(f"已推送{len(batch_copy)}条记录到远端临时表: {temp_table_name}")
            
            # 在单独的事务中创建空间索引（避免事务回滚影响表创建）
            self._create_spatial_index_safe(temp_table_name)
            
        except Exception as e:
            self.logger.error(f"推送数据到远端失败: {str(e)}")
            # 如果表创建失败，尝试清理
            try:
                self._cleanup_remote_temp_table(temp_table_name)
            except:
                pass
            
            # 提供更详细的错误信息
            if "unexpected keyword argument" in str(e):
                self.logger.error("提示：请检查GeoPandas版本，某些参数可能不被支持")
            elif "string pattern on a byte-like object" in str(e):
                self.logger.error("提示：几何数据格式问题，可能是WKB/WKT转换错误")
            elif "encoding" in str(e).lower():
                self.logger.error("提示：字符编码问题，检查数据库连接配置")
            raise
    
    def _execute_remote_spatial_join(
        self,
        temp_table_name: str,
        remote_table: str,
        spatial_relation: str,
        distance_meters: Optional[float],
        fields_to_add: Optional[List[str]],
        summarize: bool,
        summary_fields: Optional[Dict[str, str]]
    ) -> pd.DataFrame:
        """在远端执行空间连接"""
        
        # 构建空间条件
        spatial_condition = self._build_spatial_condition(
            spatial_relation, f"t.geometry", f"r.wkb_geometry", distance_meters
        )
        
        if summarize and summary_fields:
            # 统计模式
            field_selection = self._build_summary_fields_remote(summary_fields)
            select_part = "t.scene_token, t.geometry"
            group_by_clause = "GROUP BY t.scene_token, t.geometry"
        else:
            # 普通连接模式
            field_selection = self._build_field_selection_remote(fields_to_add)
            select_part = "t.*"
            group_by_clause = ""
        
        sql = text(f"""
            SELECT 
                {select_part},
                {field_selection}
            FROM {temp_table_name} t
            LEFT JOIN {remote_table} r ON {spatial_condition}
            {group_by_clause}
            ORDER BY t.scene_token
        """)
        
        try:
            # 在远端执行查询并返回结果
            return pd.read_sql(sql, self.remote_engine)
        except Exception as e:
            self.logger.error(f"远端空间连接执行失败: {str(e)}")
            raise
    
    def _build_field_selection_remote(self, fields_to_add: Optional[List[str]]) -> str:
        """构建远端字段选择SQL"""
        if not fields_to_add:
            # 添加一些默认的有用字段
            return "r.intersection_id, r.road_type"
        
        field_list = [f"r.{field}" for field in fields_to_add]
        return ", ".join(field_list)
    
    def _build_summary_fields_remote(self, summary_fields: Dict[str, str]) -> str:
        """构建远端统计字段SQL"""
        selections = []
        
        for new_name, method in summary_fields.items():
            method = method.lower()
            
            if method == "count":
                selections.append(f"COUNT(r.*) AS {new_name}")
            elif method == "distance":
                # 计算最近距离（使用geography类型获得米单位）
                selections.append(f"""
                    MIN(ST_Distance(
                        t.geometry::geography,
                        r.wkb_geometry::geography
                    )) AS {new_name}
                """)
            elif method in ["sum", "mean", "avg", "max", "min"]:
                # 对于数值统计，从字段名推断原字段
                if "_" in new_name:
                    base_field = "_".join(new_name.split("_")[:-1])
                    sql_method = "AVG" if method in ["mean", "avg"] else method.upper()
                    selections.append(f"{sql_method}(r.{base_field}) AS {new_name}")
                else:
                    selections.append(f"COUNT(r.*) AS {new_name}")
            else:
                selections.append(f"COUNT(r.*) AS {new_name}")
        
        return ", ".join(selections) if selections else "COUNT(r.*) AS feature_count"
    
    def _check_remote_db_version(self):
        """检查远端数据库版本"""
        try:
            with self.remote_engine.connect() as conn:
                result = conn.execute(text("SELECT version()")).scalar()
                version_info = result.split()[1] if result else "unknown"
                self.logger.info(f"远端PostgreSQL版本: {version_info}")
                
                # 检查是否支持现代语法
                version_parts = version_info.split('.')
                if len(version_parts) >= 2:
                    major = int(version_parts[0])
                    minor = int(version_parts[1])
                    if major < 9 or (major == 9 and minor < 5):
                        self.logger.warning(f"远端数据库版本较旧({version_info})，将使用兼容性语法")
        except Exception as e:
            self.logger.warning(f"无法获取远端数据库版本: {str(e)}")

    def _create_spatial_index_safe(self, temp_table_name: str):
        """安全地创建空间索引（兼容不同PostgreSQL版本）"""
        index_name = f"idx_{temp_table_name}_geom"
        
        # 使用独立的连接和事务
        try:
            with self.remote_engine.connect() as conn:
                # 首先检查表是否存在
                table_exists = conn.execute(text(f"""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{temp_table_name.lower()}'
                """)).fetchone()
                
                if not table_exists:
                    self.logger.warning(f"表{temp_table_name}不存在，跳过索引创建")
                    return
                
                try:
                    # 方法1：尝试现代语法 (PostgreSQL 9.5+)
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {temp_table_name} USING GIST (geometry)"))
                    conn.commit()
                    self.logger.debug(f"使用IF NOT EXISTS语法创建索引: {index_name}")
                    
                except Exception as e:
                    # 回滚任何失败的事务
                    conn.rollback()
                    
                    if "syntax error" in str(e) and "IF NOT EXISTS" in str(e):
                        # 方法2：对于旧版PostgreSQL，直接尝试创建索引
                        try:
                            conn.execute(text(f"CREATE INDEX {index_name} ON {temp_table_name} USING GIST (geometry)"))
                            conn.commit()
                            self.logger.debug(f"使用传统语法创建索引: {index_name}")
                            
                        except Exception as e2:
                            conn.rollback()
                            if "already exists" in str(e2):
                                self.logger.debug(f"索引已存在: {index_name}")
                            else:
                                # 索引创建失败，但不影响主流程
                                self.logger.warning(f"创建空间索引失败，继续处理: {str(e2)}")
                    else:
                        # 其他类型的错误，记录警告但不中断流程
                        self.logger.warning(f"创建空间索引失败，继续处理: {str(e)}")
                        
        except Exception as e:
            # 连接级别的错误
            self.logger.warning(f"连接远端数据库创建索引失败: {str(e)}")

    def _cleanup_remote_temp_table(self, temp_table_name: str):
        """清理远端临时表"""
        try:
            with self.remote_engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                conn.commit()
            self.logger.debug(f"已清理远端临时表: {temp_table_name}")
        except Exception as e:
            # 如果远端数据库不支持DROP TABLE IF EXISTS，尝试直接删除
            if "syntax error" in str(e) and "IF EXISTS" in str(e):
                try:
                    with self.remote_engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE {temp_table_name}"))
                        conn.commit()
                    self.logger.debug(f"已清理远端临时表: {temp_table_name}")
                except Exception as e2:
                    if "does not exist" not in str(e2):
                        self.logger.warning(f"清理远端临时表失败: {str(e2)}")
            else:
                self.logger.warning(f"清理远端临时表失败: {str(e)}")

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
        
        注意：此方法基于FDW，对于复杂空间计算效率较低。
        建议使用 batch_spatial_join_with_remote 方法进行高效的远端计算。
        
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
        # 发出弃用警告
        self.logger.warning(
            "join_attributes_by_location 方法基于FDW，性能较低。"
            "建议使用 batch_spatial_join_with_remote 方法进行高效计算。"
        )
        
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