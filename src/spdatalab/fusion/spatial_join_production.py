"""
生产级空间连接模块

基于大量测试验证的高性能polygon相交解决方案：
- 小中规模（≤200个bbox）：批量查询（UNION ALL）- 最快
- 大规模（>200个bbox）：分块批量查询 - 最稳定

支持两种工作模式：
1. 预存模式：将相交关系存储到中间表，支持快速分析
2. 实时模式：实时计算相交关系，适合临时分析

性能数据：
- 100个bbox: 0.97秒 (批量查询)
- 1000个bbox: 2.75秒 (分块查询, 364 bbox/秒)
- 10000个bbox: 19.77秒 (分块查询, 447 bbox/秒)
"""

import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

# 路口类型枚举映射
INTERSECTION_TYPE_MAPPING = {
    1: "Intersection",
    2: "Toll Station", 
    3: "Lane Change Area",
    4: "T-Junction Area",
    5: "Roundabout",
    6: "H-Junction Area",
    7: "Invalid",
    8: "Toll Booth Area"
}

INTERSECTION_SUBTYPE_MAPPING = {
    1: "Regular",
    2: "T-Junction with Through Markings",
    3: "Minor Junction (No Traffic Conflict)", 
    4: "Unmarked Junction",
    5: "Secondary Junction",
    6: "Conservative Through Junction",
    7: "Invalid"
}

@dataclass
class SpatialJoinConfig:
    """空间连接配置"""
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_dsn: str = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"
    batch_threshold: int = 200  # 批量查询vs分块查询的阈值
    chunk_size: int = 50        # 分块大小
    max_timeout_seconds: int = 300  # 5分钟超时
    # 新增：预存表配置
    intersection_table: str = "bbox_intersection_cache"  # 预存相交关系的表名
    enable_cache_table: bool = True  # 是否启用缓存表
    # 分析结果表配置
    analysis_results_table: str = "spatial_analysis_results"  # 分析结果表名
    enable_results_export: bool = True  # 是否启用结果导出

class ProductionSpatialJoin:
    """
    生产级空间连接器
    
    支持两种工作模式：
    1. 预存模式：将相交关系存储到中间表，支持快速分析
    2. 实时模式：实时计算相交关系，适合临时分析
    
    自动选择最优策略：
    - ≤200个bbox: 批量查询 (最快)
    - >200个bbox: 分块查询 (最稳定)
    """
    
    def __init__(self, config: Optional[SpatialJoinConfig] = None):
        self.config = config or SpatialJoinConfig()
        self.local_engine = create_engine(
            self.config.local_dsn, 
            future=True, 
            connect_args={"client_encoding": "utf8"}
        )
        self.remote_engine = create_engine(
            self.config.remote_dsn, 
            future=True, 
            connect_args={"client_encoding": "utf8"}
        )
        
        # 初始化缓存表
        if self.config.enable_cache_table:
            self._init_cache_table()
        
        # 初始化分析结果表
        if self.config.enable_results_export:
            self._init_results_table()
    
    def _init_cache_table(self):
        """初始化缓存表"""
        # 创建主表
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.intersection_table} (
                id SERIAL PRIMARY KEY,
                scene_token VARCHAR(255) NOT NULL,
                city_id VARCHAR(100),
                intersection_id BIGINT NOT NULL,
                intersectiontype INTEGER,
                intersectionsubtype INTEGER,
                intersection_geometry TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_scene_intersection UNIQUE (scene_token, intersection_id)
            )
        """)
        
        # 创建索引的SQL语句
        create_indexes_sql = [
            text(f"CREATE INDEX IF NOT EXISTS idx_scene_token ON {self.config.intersection_table} (scene_token)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_city_id ON {self.config.intersection_table} (city_id)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_intersectiontype ON {self.config.intersection_table} (intersectiontype)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_intersection_id ON {self.config.intersection_table} (intersection_id)")
        ]
        
        try:
            with self.local_engine.connect() as conn:
                # 创建表
                conn.execute(create_table_sql)
                
                # 创建索引
                for index_sql in create_indexes_sql:
                    try:
                        conn.execute(index_sql)
                    except Exception as idx_e:
                        logger.warning(f"索引创建失败: {idx_e}")
                
                conn.commit()
            logger.info(f"缓存表 {self.config.intersection_table} 初始化完成")
        except Exception as e:
            logger.warning(f"缓存表初始化失败: {e}")
            logger.warning("请检查local_pg数据库连接和权限")
    
    def _init_results_table(self):
        """初始化分析结果表，专为QGIS可视化设计"""
        # 创建分析结果表
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.analysis_results_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                analysis_type VARCHAR(50) NOT NULL,
                analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                city_filter VARCHAR(100),
                group_dimension VARCHAR(50),
                group_value VARCHAR(200),
                group_value_name VARCHAR(200),
                intersection_count INTEGER,
                unique_intersections INTEGER,
                unique_scenes INTEGER,
                bbox_count INTEGER,
                analysis_params JSONB,
                geometry GEOMETRY(GEOMETRY, 4326),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建索引
        create_indexes_sql = [
            text(f"CREATE INDEX IF NOT EXISTS idx_analysis_id ON {self.config.analysis_results_table} (analysis_id)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_analysis_type ON {self.config.analysis_results_table} (analysis_type)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_group_dimension ON {self.config.analysis_results_table} (group_dimension)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_city_filter_results ON {self.config.analysis_results_table} (city_filter)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_geometry_results ON {self.config.analysis_results_table} USING GIST (geometry)")
        ]
        
        try:
            with self.local_engine.connect() as conn:
                # 创建表
                conn.execute(create_table_sql)
                
                # 创建索引
                for index_sql in create_indexes_sql:
                    try:
                        conn.execute(index_sql)
                    except Exception as idx_e:
                        logger.warning(f"结果表索引创建失败: {idx_e}")
                
                conn.commit()
            logger.info(f"分析结果表 {self.config.analysis_results_table} 初始化完成")
        except Exception as e:
            logger.warning(f"分析结果表初始化失败: {e}")
            logger.warning("请检查local_pg数据库连接和权限")
    
    def build_intersection_cache(
        self, 
        num_bbox: int,
        city_filter: Optional[str] = None,
        force_rebuild: bool = False
    ) -> Tuple[int, dict]:
        """
        构建相交关系缓存
        
        Args:
            num_bbox: 要处理的bbox数量
            city_filter: 城市过滤条件
            force_rebuild: 是否强制重建缓存
            
        Returns:
            (缓存记录数, 性能统计)
        """
        start_time = time.time()
        stats = {
            'bbox_count': num_bbox,
            'city_filter': city_filter,
            'cached_records': 0,
            'build_time': 0,
            'strategy': 'cache_build'
        }
        
        logger.info(f"开始构建 {num_bbox} 个bbox的相交关系缓存")
        
        # 检查是否需要重建
        if not force_rebuild:
            existing_count = self._get_cached_count(city_filter)
            if existing_count >= num_bbox:
                logger.info(f"缓存已存在 {existing_count} 条记录，无需重建")
                stats['cached_records'] = existing_count
                stats['build_time'] = time.time() - start_time
                return existing_count, stats
        
        # 清理旧缓存（如果需要）
        if force_rebuild:
            self._clear_cache(city_filter)
        
        # 获取bbox数据
        bbox_data = self._fetch_bbox_data(num_bbox, city_filter)
        if bbox_data.empty:
            logger.warning("未找到bbox数据")
            return 0, stats
        
        # 获取详细相交数据并存储
        cached_count = 0
        
        if len(bbox_data) <= self.config.batch_threshold:
            cached_count = self._cache_batch_intersections(bbox_data)
        else:
            cached_count = self._cache_chunked_intersections(bbox_data)
        
        stats['cached_records'] = cached_count
        stats['build_time'] = time.time() - start_time
        
        logger.info(f"缓存构建完成！存储了 {cached_count} 条相交记录，耗时: {stats['build_time']:.2f}秒")
        return cached_count, stats
    
    def _cache_batch_intersections(self, bbox_data: pd.DataFrame) -> int:
        """批量缓存相交关系"""
        logger.info(f"批量缓存 {len(bbox_data)} 个bbox的相交关系")
        
        # 构建获取详细相交信息的查询
        subqueries = []
        for _, row in bbox_data.iterrows():
            scene_token = str(row['scene_token'])
            city_id = str(row['city_id']) if row['city_id'] is not None else 'NULL'
            bbox_wkt = str(row['bbox_wkt'])
            
            subquery = f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    {f"'{city_id}'" if city_id != 'NULL' else 'NULL'} as city_id,
                    id as intersection_id,
                    intersectiontype,
                    intersectionsubtype,
                    ST_AsText(wkb_geometry) as intersection_geometry
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
            """
            subqueries.append(subquery)
        
        batch_sql = text(" UNION ALL ".join(subqueries))
        
        # 执行查询并存储到缓存表
        with self.remote_engine.connect() as remote_conn:
            intersections_df = pd.read_sql(batch_sql, remote_conn)
        
        if not intersections_df.empty:
            with self.local_engine.connect() as local_conn:
                intersections_df.to_sql(
                    self.config.intersection_table,
                    local_conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
        
        return len(intersections_df)
    
    def _cache_chunked_intersections(self, bbox_data: pd.DataFrame) -> int:
        """分块缓存相交关系"""
        logger.info(f"分块缓存 {len(bbox_data)} 个bbox的相交关系")
        
        total_cached = 0
        
        for i in range(0, len(bbox_data), self.config.chunk_size):
            chunk = bbox_data.iloc[i:i+self.config.chunk_size]
            chunk_num = i // self.config.chunk_size + 1
            logger.info(f"缓存第 {chunk_num} 块: {len(chunk)} 个bbox")
            
            chunk_cached = self._cache_batch_intersections(chunk)
            total_cached += chunk_cached
        
        return total_cached
    
    def analyze_intersections(
        self,
        scene_tokens: Optional[List[str]] = None,
        city_filter: Optional[str] = None,
        intersection_types: Optional[List[int]] = None,
        group_by: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        基于缓存数据进行相交分析
        
        Args:
            scene_tokens: 指定场景tokens
            city_filter: 城市过滤
            intersection_types: 路口类型过滤
            group_by: 分组字段 ['city_id', 'intersection_type', 'scene_token']
            
        Returns:
            分析结果DataFrame
        """
        if not self.config.enable_cache_table:
            raise ValueError("缓存表未启用，请使用实时查询模式")
        
        # 列名映射：用户友好名称 -> 数据库实际列名
        column_mapping = {
            'intersection_type': 'intersectiontype',
            'intersection_subtype': 'intersectionsubtype',
            'city_id': 'city_id',
            'scene_token': 'scene_token'
        }
        
        # 构建查询条件
        where_conditions = []
        
        if scene_tokens:
            tokens_str = "', '".join(scene_tokens)
            where_conditions.append(f"scene_token IN ('{tokens_str}')")
        
        if city_filter:
            where_conditions.append(f"city_id = '{city_filter}'")
        
        if intersection_types:
            types_str = ", ".join(str(t) for t in intersection_types)
            where_conditions.append(f"intersectiontype IN ({types_str})")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # 构建分组字段，处理列名映射
        if group_by:
            # 映射用户友好的列名到实际的数据库列名
            mapped_group_fields = []
            display_group_fields = []
            
            for field in group_by:
                db_field = column_mapping.get(field, field)
                mapped_group_fields.append(db_field)
                
                # 为intersection相关字段添加别名以保持用户友好的列名
                if field == 'intersection_type':
                    display_group_fields.append(f"{db_field} as intersection_type")
                elif field == 'intersection_subtype':
                    display_group_fields.append(f"{db_field} as intersection_subtype")
                else:
                    display_group_fields.append(db_field)
            
            group_fields_sql = ", ".join(mapped_group_fields)
            display_fields_sql = ", ".join(display_group_fields)
            select_fields = f"{display_fields_sql}, COUNT(*) as intersection_count, COUNT(DISTINCT intersection_id) as unique_intersections"
            group_clause = f"GROUP BY {group_fields_sql}"
            order_clause = f"ORDER BY {group_fields_sql}"
        else:
            select_fields = "COUNT(*) as total_intersections, COUNT(DISTINCT intersection_id) as unique_intersections, COUNT(DISTINCT scene_token) as unique_scenes"
            group_clause = ""
            order_clause = ""
        
        # 执行分析查询
        analysis_sql = text(f"""
            SELECT {select_fields}
            FROM {self.config.intersection_table}
            WHERE {where_clause}
            {group_clause}
            {order_clause}
        """)
        
        with self.local_engine.connect() as conn:
            return pd.read_sql(analysis_sql, conn)
    
    def save_analysis_to_db(
        self,
        analysis_result: pd.DataFrame,
        analysis_type: str,
        analysis_id: Optional[str] = None,
        city_filter: Optional[str] = None,
        group_dimension: Optional[str] = None,
        analysis_params: Optional[dict] = None,
        include_geometry: bool = True
    ) -> str:
        """
        将分析结果保存到数据库表中，方便QGIS可视化
        
        Args:
            analysis_result: 分析结果DataFrame
            analysis_type: 分析类型 ('intersection_type', 'scene_analysis', 'hotspot', etc.)
            analysis_id: 分析ID（可选，自动生成）
            city_filter: 城市过滤条件
            group_dimension: 分组维度
            analysis_params: 分析参数
            include_geometry: 是否包含几何信息
            
        Returns:
            生成的analysis_id
        """
        if not self.config.enable_results_export:
            raise ValueError("结果导出未启用，请在配置中设置enable_results_export=True")
        
        if analysis_result.empty:
            logger.warning("分析结果为空，跳过保存")
            return ""
        
        # 生成分析ID
        if not analysis_id:
            from datetime import datetime
            analysis_id = f"{analysis_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"保存分析结果到数据库: {analysis_id}")
        
        # 准备保存的数据
        save_records = []
        
        for _, row in analysis_result.iterrows():
            record = {
                'analysis_id': analysis_id,
                'analysis_type': analysis_type,
                'city_filter': city_filter,
                'group_dimension': group_dimension,
                'analysis_params': analysis_params  # 直接传递字典，pandas会自动处理JSONB
            }
            
            # 处理分组值和统计信息
            if group_dimension:
                group_value = row.get(group_dimension, 'unknown')
                record['group_value'] = str(group_value)
                
                # 添加可读名称
                if group_dimension == 'intersection_type':
                    # 确保group_value是整数
                    try:
                        int_value = int(group_value) if group_value != 'unknown' else 0
                        record['group_value_name'] = INTERSECTION_TYPE_MAPPING.get(int_value, 'Unknown')
                    except (ValueError, TypeError):
                        record['group_value_name'] = 'Unknown'
                elif group_dimension == 'intersection_subtype':
                    # 确保group_value是整数
                    try:
                        int_value = int(group_value) if group_value != 'unknown' else 0
                        record['group_value_name'] = INTERSECTION_SUBTYPE_MAPPING.get(int_value, 'Unknown')
                    except (ValueError, TypeError):
                        record['group_value_name'] = 'Unknown'
                else:
                    record['group_value_name'] = str(group_value)
            else:
                record['group_value'] = 'overall'
                record['group_value_name'] = 'Overall Analysis'
            
            # 统计信息（确保是整数类型）
            record['intersection_count'] = int(row.get('intersection_count', 0))
            record['unique_intersections'] = int(row.get('unique_intersections', 0))
            record['unique_scenes'] = int(row.get('unique_scenes', 0))
            record['bbox_count'] = int(analysis_params.get('bbox_count', 0)) if analysis_params else 0
            
            # 几何信息（如果需要）
            if include_geometry and group_dimension:
                geometry_wkt = self._get_analysis_geometry(
                    group_dimension, 
                    record['group_value'], 
                    city_filter
                )
                record['geometry'] = geometry_wkt
            
            save_records.append(record)
        
        # 保存到数据库
        if save_records:
            try:
                # 方法1：使用pandas to_sql，让其自动处理JSONB
                save_df = pd.DataFrame(save_records)
                with self.local_engine.connect() as conn:
                    save_df.to_sql(
                        self.config.analysis_results_table,
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                logger.info(f"成功保存 {len(save_records)} 条分析结果到 {self.config.analysis_results_table}")
                
            except Exception as e:
                # 方法2：如果pandas自动处理失败，使用手动SQL插入
                logger.warning(f"pandas to_sql 失败，尝试手动插入: {e}")
                self._manual_insert_records(save_records)
                logger.info(f"手动插入成功保存 {len(save_records)} 条分析结果到 {self.config.analysis_results_table}")
        
        return analysis_id
    
    def _manual_insert_records(self, save_records: List[dict]):
        """手动插入记录，处理JSONB类型"""
        import json
        
        with self.local_engine.connect() as conn:
            for record in save_records:
                # 序列化analysis_params为JSON字符串，并在SQL中明确转换为JSONB
                analysis_params_json = json.dumps(record['analysis_params']) if record['analysis_params'] else None
                
                insert_sql = text(f"""
                    INSERT INTO {self.config.analysis_results_table} 
                    (analysis_id, analysis_type, city_filter, group_dimension, group_value, 
                     group_value_name, intersection_count, unique_intersections, unique_scenes, 
                     bbox_count, analysis_params, geometry)
                    VALUES (:analysis_id, :analysis_type, :city_filter, :group_dimension, :group_value,
                            :group_value_name, :intersection_count, :unique_intersections, :unique_scenes,
                            :bbox_count, :analysis_params::jsonb, 
                            CASE WHEN :geometry IS NOT NULL THEN ST_GeomFromText(:geometry, 4326) ELSE NULL END)
                """)
                
                conn.execute(insert_sql, {
                    'analysis_id': record['analysis_id'],
                    'analysis_type': record['analysis_type'],
                    'city_filter': record['city_filter'],
                    'group_dimension': record['group_dimension'],
                    'group_value': record['group_value'],
                    'group_value_name': record['group_value_name'],
                    'intersection_count': record['intersection_count'],
                    'unique_intersections': record['unique_intersections'],
                    'unique_scenes': record['unique_scenes'],
                    'bbox_count': record['bbox_count'],
                    'analysis_params': analysis_params_json,
                    'geometry': record.get('geometry')
                })
            
            conn.commit()
    
    def _get_analysis_geometry(
        self, 
        group_dimension: str, 
        group_value: str, 
        city_filter: Optional[str] = None
    ) -> Optional[str]:
        """获取分析组的代表性几何信息"""
        try:
            if group_dimension == 'intersection_type':
                # 获取该类型路口的联合几何
                where_conditions = [f"intersectiontype = {group_value}"]
                if city_filter:
                    where_conditions.append(f"city_id = '{city_filter}'")
                
                where_clause = " AND ".join(where_conditions)
                
                geom_sql = text(f"""
                    SELECT ST_AsText(ST_Union(ST_GeomFromText(intersection_geometry, 4326))) as geometry
                    FROM {self.config.intersection_table}
                    WHERE {where_clause}
                    LIMIT 100
                """)
                
                with self.local_engine.connect() as conn:
                    result = conn.execute(geom_sql).fetchone()
                    return result[0] if result and result[0] else None
            
            elif group_dimension == 'scene_token':
                # 获取场景的bbox几何
                bbox_sql = text(f"""
                    SELECT ST_AsText(geometry) as geometry
                    FROM clips_bbox
                    WHERE scene_token = '{group_value}'
                    LIMIT 1
                """)
                
                with self.local_engine.connect() as conn:
                    result = conn.execute(bbox_sql).fetchone()
                    return result[0] if result and result[0] else None
            
            return None
            
        except Exception as e:
            logger.warning(f"获取几何信息失败: {e}")
            return None
    
    def get_intersection_details(
        self,
        scene_tokens: Optional[List[str]] = None,
        city_filter: Optional[str] = None,
        intersection_types: Optional[List[int]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        获取相交关系的详细信息
        
        Args:
            scene_tokens: 指定场景tokens
            city_filter: 城市过滤
            intersection_types: 路口类型过滤
            limit: 结果数量限制
            
        Returns:
            详细信息DataFrame
        """
        if not self.config.enable_cache_table:
            raise ValueError("缓存表未启用，请使用实时查询模式")
        
        # 构建查询条件
        where_conditions = []
        
        if scene_tokens:
            tokens_str = "', '".join(scene_tokens)
            where_conditions.append(f"scene_token IN ('{tokens_str}')")
        
        if city_filter:
            where_conditions.append(f"city_id = '{city_filter}'")
        
        if intersection_types:
            types_str = ", ".join(str(t) for t in intersection_types)
            where_conditions.append(f"intersectiontype IN ({types_str})")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # 执行详细查询
        detail_sql = text(f"""
            SELECT 
                scene_token,
                city_id,
                intersection_id,
                intersectiontype as intersection_type,
                intersectionsubtype as intersection_subtype,
                intersection_geometry,
                created_at
            FROM {self.config.intersection_table}
            WHERE {where_clause}
            ORDER BY scene_token, intersection_id
            {limit_clause}
        """)
        
        with self.local_engine.connect() as conn:
            return pd.read_sql(detail_sql, conn)
    
    def _get_cached_count(self, city_filter: Optional[str] = None) -> int:
        """获取缓存记录数"""
        where_clause = f"WHERE city_id = '{city_filter}'" if city_filter else ""
        
        count_sql = text(f"""
            SELECT COUNT(DISTINCT scene_token) as count
            FROM {self.config.intersection_table}
            {where_clause}
        """)
        
        try:
            with self.local_engine.connect() as conn:
                result = conn.execute(count_sql).fetchone()
                return result[0] if result else 0
        except Exception:
            return 0
    
    def _clear_cache(self, city_filter: Optional[str] = None):
        """清理缓存"""
        where_clause = f"WHERE city_id = '{city_filter}'" if city_filter else ""
        
        clear_sql = text(f"""
            DELETE FROM {self.config.intersection_table}
            {where_clause}
        """)
        
        with self.local_engine.connect() as conn:
            conn.execute(clear_sql)
            conn.commit()
    
    def polygon_intersect(
        self, 
        num_bbox: int,
        city_filter: Optional[str] = None,
        chunk_size: Optional[int] = None
    ) -> Tuple[pd.DataFrame, dict]:
        """
        高性能polygon相交查询
        
        Args:
            num_bbox: 要处理的bbox数量
            city_filter: 城市过滤条件（可选）
            chunk_size: 自定义分块大小（可选）
            
        Returns:
            (结果DataFrame, 性能统计)
        """
        start_time = time.time()
        
        # 性能统计
        stats = {
            'bbox_count': num_bbox,
            'city_filter': city_filter,
            'strategy': None,
            'chunk_size': None,
            'fetch_time': 0,
            'query_time': 0,
            'total_time': 0,
            'result_count': 0,
            'speed_bbox_per_sec': 0
        }
        
        logger.info(f"开始处理 {num_bbox} 个bbox的空间连接")
        
        # 1. 获取bbox数据
        fetch_start = time.time()
        bbox_data = self._fetch_bbox_data(num_bbox, city_filter)
        stats['fetch_time'] = time.time() - fetch_start
        
        if bbox_data.empty:
            logger.warning("未找到bbox数据")
            return pd.DataFrame(), stats
        
        actual_count = len(bbox_data)
        stats['bbox_count'] = actual_count
        
        # 2. 选择最优策略
        if actual_count <= self.config.batch_threshold:
            stats['strategy'] = 'batch_query'
            result = self._batch_query_strategy(bbox_data)
        else:
            stats['strategy'] = 'chunked_query'
            effective_chunk_size = chunk_size or self.config.chunk_size
            stats['chunk_size'] = effective_chunk_size
            result = self._chunked_query_strategy(bbox_data, effective_chunk_size)
        
        # 3. 计算性能统计
        stats['query_time'] = time.time() - fetch_start - stats['fetch_time']
        stats['total_time'] = time.time() - start_time
        stats['result_count'] = len(result)
        stats['speed_bbox_per_sec'] = actual_count / stats['total_time'] if stats['total_time'] > 0 else 0
        
        logger.info(f"完成！策略: {stats['strategy']}, 耗时: {stats['total_time']:.2f}秒, "
                   f"速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")
        
        return result, stats
    
    def _fetch_bbox_data(self, num_bbox: int, city_filter: Optional[str]) -> pd.DataFrame:
        """获取bbox数据"""
        # 检查是否有city_id字段
        try:
            with self.local_engine.connect() as conn:
                check_column_sql = text("""
                    SELECT EXISTS (
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'clips_bbox' AND column_name = 'city_id'
                    );
                """)
                has_city_id = conn.execute(check_column_sql).fetchone()[0]
        except Exception:
            has_city_id = False
        
        # 构建查询语句
        if has_city_id:
            # 如果有city_id字段
            where_clause = ""
            if city_filter:
                where_clause = f"WHERE city_id = '{city_filter}'"
            
            sql = text(f"""
                SELECT 
                    scene_token,
                    city_id,
                    ST_AsText(geometry) as bbox_wkt
                FROM clips_bbox 
                {where_clause}
                ORDER BY scene_token
                LIMIT {num_bbox}
            """)
        else:
            # 如果没有city_id字段
            if city_filter:
                logger.warning(f"clips_bbox表中没有city_id字段，忽略city_filter参数: {city_filter}")
            
            sql = text(f"""
                SELECT 
                    scene_token,
                    NULL as city_id,
                    ST_AsText(geometry) as bbox_wkt
                FROM clips_bbox 
                ORDER BY scene_token
                LIMIT {num_bbox}
            """)
        
        return pd.read_sql(sql, self.local_engine)
    
    def _batch_query_strategy(self, bbox_data: pd.DataFrame) -> pd.DataFrame:
        """批量查询策略 - 适合≤200个bbox"""
        logger.info(f"使用批量查询策略处理 {len(bbox_data)} 个bbox")
        
        # 构建UNION ALL查询
        subqueries = []
        for _, row in bbox_data.iterrows():
            scene_token = str(row['scene_token'])
            bbox_wkt = str(row['bbox_wkt'])
            
            subquery = f"""
                SELECT 
                    '{scene_token}' as scene_token,
                    COUNT(*) as intersect_count
                FROM full_intersection 
                WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('{bbox_wkt}', 4326))
            """
            subqueries.append(subquery)
        
        batch_sql = text(" UNION ALL ".join(subqueries))
        
        with self.remote_engine.connect() as conn:
            return pd.read_sql(batch_sql, conn)
    
    def _chunked_query_strategy(self, bbox_data: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
        """分块查询策略 - 适合大规模数据"""
        logger.info(f"使用分块查询策略，{len(bbox_data)} 个bbox分为 {len(bbox_data)//chunk_size + 1} 块")
        
        all_results = []
        
        for i in range(0, len(bbox_data), chunk_size):
            chunk = bbox_data.iloc[i:i+chunk_size]
            chunk_num = i // chunk_size + 1
            logger.info(f"处理第 {chunk_num} 块: {len(chunk)} 个bbox")
            
            # 构建当前块的查询
            subqueries = []
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
                subqueries.append(subquery)
            
            batch_sql = text(" UNION ALL ".join(subqueries))
            
            with self.remote_engine.connect() as conn:
                chunk_result = pd.read_sql(batch_sql, conn)
                all_results.append(chunk_result)
        
        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()


def build_cache(
    num_bbox: int,
    city_filter: Optional[str] = None,
    config: Optional[SpatialJoinConfig] = None,
    force_rebuild: bool = False
) -> Tuple[int, dict]:
    """
    构建相交关系缓存的便捷接口
    
    Args:
        num_bbox: bbox数量
        city_filter: 城市过滤
        config: 自定义配置
        force_rebuild: 是否强制重建
        
    Returns:
        (缓存记录数, 性能统计)
    """
    spatial_join = ProductionSpatialJoin(config)
    return spatial_join.build_intersection_cache(num_bbox, city_filter, force_rebuild)


def analyze_cached_intersections(
    scene_tokens: Optional[List[str]] = None,
    city_filter: Optional[str] = None,
    intersection_types: Optional[List[int]] = None,
    group_by: Optional[List[str]] = None,
    config: Optional[SpatialJoinConfig] = None
) -> pd.DataFrame:
    """
    基于缓存数据进行相交分析的便捷接口
    
    Args:
        scene_tokens: 指定场景tokens
        city_filter: 城市过滤
        intersection_types: 路口类型过滤
        group_by: 分组字段
        config: 自定义配置
        
    Returns:
        分析结果DataFrame
    """
    spatial_join = ProductionSpatialJoin(config)
    return spatial_join.analyze_intersections(scene_tokens, city_filter, intersection_types, group_by)


def quick_spatial_join(
    num_bbox: int,
    city_filter: Optional[str] = None,
    config: Optional[SpatialJoinConfig] = None
) -> Tuple[pd.DataFrame, dict]:
    """
    快速空间连接接口
    
    Args:
        num_bbox: bbox数量
        city_filter: 城市过滤（如"A72", "B15"等实际城市代码）
        config: 自定义配置
        
    Returns:
        (结果, 性能统计)
    """
    spatial_join = ProductionSpatialJoin(config)
    return spatial_join.polygon_intersect(num_bbox, city_filter)


def get_available_cities(config: Optional[SpatialJoinConfig] = None) -> pd.DataFrame:
    """
    获取可用的城市列表
    
    Args:
        config: 自定义配置
        
    Returns:
        包含城市信息的DataFrame
    """
    spatial_join = ProductionSpatialJoin(config)
    
    try:
        # 检查是否有city_id字段
        with spatial_join.local_engine.connect() as conn:
            check_column_sql = text("""
                SELECT EXISTS (
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'clips_bbox' AND column_name = 'city_id'
                );
            """)
            has_city_id = conn.execute(check_column_sql).fetchone()[0]
        
        if has_city_id:
            city_sql = text("""
                SELECT 
                    city_id,
                    COUNT(*) as bbox_count
                FROM clips_bbox 
                WHERE city_id IS NOT NULL
                GROUP BY city_id
                ORDER BY bbox_count DESC
            """)
            return pd.read_sql(city_sql, spatial_join.local_engine)
        else:
            logger.warning("clips_bbox表中没有city_id字段")
            return pd.DataFrame({'message': ['clips_bbox表中没有city_id字段']})
    
    except Exception as e:
        logger.error(f"获取城市列表失败: {e}")
        return pd.DataFrame({'error': [str(e)]})


def explain_intersection_types() -> pd.DataFrame:
    """
    获取路口类型和子类型的说明
    
    Returns:
        包含类型说明的DataFrame
    """
    type_df = pd.DataFrame([
        {'id': k, 'type': 'intersectiontype', 'name': v} 
        for k, v in INTERSECTION_TYPE_MAPPING.items()
    ])
    
    subtype_df = pd.DataFrame([
        {'id': k, 'type': 'intersectionsubtype', 'name': v} 
        for k, v in INTERSECTION_SUBTYPE_MAPPING.items()
    ])
    
    return pd.concat([type_df, subtype_df], ignore_index=True)


def get_intersection_types_summary(config: Optional[SpatialJoinConfig] = None) -> pd.DataFrame:
    """
    获取路口类型汇总信息
    
    Args:
        config: 自定义配置
        
    Returns:
        包含路口类型统计的DataFrame
    """
    spatial_join = ProductionSpatialJoin(config)
    
    try:
        type_sql = text("""
            SELECT 
                intersectiontype,
                intersectionsubtype,
                COUNT(*) as intersection_count
            FROM full_intersection
            GROUP BY intersectiontype, intersectionsubtype
            ORDER BY intersection_count DESC
            LIMIT 50
        """)
        result = pd.read_sql(type_sql, spatial_join.remote_engine)
        
        # 添加可读的描述
        if not result.empty:
            result['type_name'] = result['intersectiontype'].map(INTERSECTION_TYPE_MAPPING)
            result['subtype_name'] = result['intersectionsubtype'].map(INTERSECTION_SUBTYPE_MAPPING)
        
        return result
    
    except Exception as e:
        logger.error(f"获取路口类型失败: {e}")
        return pd.DataFrame({'error': [str(e)]})


def export_analysis_to_qgis(
    analysis_type: str = "intersection_type",
    city_filter: Optional[str] = None,
    group_by: Optional[List[str]] = None,
    analysis_id: Optional[str] = None,
    config: Optional[SpatialJoinConfig] = None,
    include_geometry: bool = True
) -> str:
    """
    执行分析并导出结果到数据库，方便QGIS可视化
    
    Args:
        analysis_type: 分析类型
        city_filter: 城市过滤
        group_by: 分组字段
        analysis_id: 自定义分析ID
        config: 配置
        include_geometry: 是否包含几何信息
        
    Returns:
        生成的analysis_id
    """
    spatial_join = ProductionSpatialJoin(config)
    
    # 执行分析
    if not group_by:
        if analysis_type == "intersection_type":
            group_by = ["intersection_type"]
        elif analysis_type == "intersection_subtype":
            group_by = ["intersection_subtype"]
        elif analysis_type == "scene_analysis":
            group_by = ["scene_token"]
        elif analysis_type == "city_analysis":
            group_by = ["city_id"] 
        else:
            group_by = []
    
    result = spatial_join.analyze_intersections(
        city_filter=city_filter,
        group_by=group_by
    )
    
    # 保存到数据库
    analysis_params = {
        'analysis_type': analysis_type,
        'city_filter': city_filter,
        'group_by': group_by,
        'bbox_count': 0  # 从缓存中无法直接获取
    }
    
    group_dimension = group_by[0] if group_by else None
    
    return spatial_join.save_analysis_to_db(
        result,
        analysis_type,
        analysis_id,
        city_filter,
        group_dimension,
        analysis_params,
        include_geometry
    )


def get_qgis_connection_info(config: Optional[SpatialJoinConfig] = None) -> dict:
    """
    获取QGIS连接信息
    
    Returns:
        QGIS连接配置字典
    """
    if not config:
        config = SpatialJoinConfig()
    
    # 解析连接字符串
    import re
    pattern = r'postgresql\+psycopg://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, config.local_dsn)
    
    if match:
        username, password, host, port, database = match.groups()
        return {
            'host': host,
            'port': int(port),
            'database': database,
            'username': username,
            'password': password,
            'results_table': config.analysis_results_table,
            'cache_table': config.intersection_table,
            'connection_string': f"host={host} port={port} dbname={database} user={username} password={password}",
            'qgis_uri': f"host={host} port={port} dbname={database} user={username} password={password} sslmode=disable key='id' srid=4326 type=GEOMETRY table=\"{config.analysis_results_table}\" (geometry) sql="
        }
    else:
        return {
            'error': 'Unable to parse connection string',
            'dsn': config.local_dsn
        }


if __name__ == "__main__":
    # 示例用法
    import sys
    
    # 设置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("=== 空间连接缓存构建示例 ===")
    
    # 先探索可用的数据格式
    print("\n0. 探索数据格式...")
    
    # 显示路口类型说明
    print("路口类型说明:")
    explanation_df = explain_intersection_types()
    print(explanation_df.to_string(index=False))
    
    cities_df = get_available_cities()
    types_df = get_intersection_types_summary()
    
    sample_city = None
    if not cities_df.empty and 'city_id' in cities_df.columns:
        sample_city = cities_df.iloc[0]['city_id']
        print(f"\n可用城市: {cities_df['city_id'].tolist()[:5]}")
        print(f"使用示例城市: {sample_city}")
    
    if not types_df.empty and 'intersectiontype' in types_df.columns:
        print(f"\n路口类型分布:")
        print(types_df.head(10).to_string(index=False))
    
    # 1. 构建缓存
    print("\n1. 构建相交关系缓存...")
    cached_count, build_stats = build_cache(50, city_filter=sample_city)
    print(f"缓存了 {cached_count} 条相交关系")
    
    # 2. 基于缓存进行分析
    print("\n2. 基于缓存进行统计分析...")
    
    # 按路口类型分组统计
    type_analysis = analyze_cached_intersections(
        city_filter=sample_city,
        group_by=["intersection_type"]
    )
    print("按路口类型统计:")
    print(type_analysis.to_string(index=False))
    
    # 按场景和路口类型分组统计
    scene_type_analysis = analyze_cached_intersections(
        city_filter=sample_city,
        group_by=["scene_token", "intersection_type"]
    )
    print(f"\n按场景和路口类型统计 (前10条):")
    print(scene_type_analysis.head(10).to_string(index=False))
    
    # 3. 原有功能测试
    print(f"\n3. 原有功能兼容性测试...")
    result, stats = quick_spatial_join(10, city_filter=sample_city)
    print(f"原有接口结果: {len(result)} 条记录") 