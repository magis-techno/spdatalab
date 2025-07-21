"""基于polygon区域的道路元素分析模块。

与轨迹分析不同，这个模块专注于：
1. 基于polygon区域批量查找roads/intersections/lanes
2. 高效的批量空间查询，避免逐个polygon处理
3. 使用临时表和空间索引优化性能

主要功能：
- 从GeoJSON文件加载多个polygon
- 批量查询polygon内的roads/intersections  
- 从roads获取对应的lanes
- 按polygon组织查询结果并保存到数据库
"""

from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
import logging

import pandas as pd
import numpy as np
from sqlalchemy import text, create_engine
from shapely.geometry import shape, Polygon
from dataclasses import dataclass

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

@dataclass
class PolygonRoadAnalysisConfig:
    """polygon道路分析配置"""
    local_dsn: str = LOCAL_DSN
    remote_catalog: str = "rcdatalake_gy1"
    
    # 远程表名配置（复用trajectory_road_analysis的表名）
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    road_table: str = "full_road"
    
    # 批量处理配置
    polygon_batch_size: int = 50  # 每批处理的polygon数量
    enable_parallel_queries: bool = True  # 启用并行查询
    temp_table_timeout: int = 300  # 临时表超时时间（秒）
    
    # 查询限制配置
    max_polygon_area: float = 10000000  # 最大polygon面积（平方米）
    max_roads_per_polygon: int = 1000  # 单个polygon最大road数量
    max_intersections_per_polygon: int = 200  # 单个polygon最大intersection数量
    max_lanes_from_roads: int = 10000  # 从roads查找lanes的最大数量
    
    # 边界处理配置
    include_boundary_roads: bool = True  # 是否包含边界roads
    boundary_inclusion_threshold: float = 0.1  # 边界road包含阈值（相交长度比）
    
    # 结果表名
    polygon_analysis_table: str = "polygon_road_analysis"
    polygon_roads_table: str = "polygon_roads"
    polygon_intersections_table: str = "polygon_intersections"
    polygon_lanes_table: str = "polygon_lanes"

# 默认配置
DEFAULT_CONFIG = PolygonRoadAnalysisConfig()

logger = logging.getLogger(__name__)

class BatchPolygonRoadAnalyzer:
    """批量polygon道路分析器 - 高效批量查询"""
    
    def __init__(self, config: Optional[PolygonRoadAnalysisConfig] = None):
        """初始化分析器"""
        self.config = config or DEFAULT_CONFIG
        self.engine = create_engine(
            self.config.local_dsn, 
            future=True,
            connect_args={"client_encoding": "utf8"},
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        # 初始化分析表（延迟创建）
        self._init_analysis_tables()
    
    def _init_analysis_tables(self):
        """初始化所有分析表"""
        self._init_main_analysis_table()
        self._init_polygon_roads_table()
        self._init_polygon_intersections_table()
        self._init_polygon_lanes_table()
    
    def _init_main_analysis_table(self):
        """初始化主分析表"""
        table_name = self.config.polygon_analysis_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                analysis_id VARCHAR(100) PRIMARY KEY,
                batch_analysis_id VARCHAR(100),
                total_polygons INTEGER DEFAULT 0,
                total_roads INTEGER DEFAULT 0,
                total_intersections INTEGER DEFAULT 0,
                total_lanes INTEGER DEFAULT 0,
                processing_time_seconds FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.commit()
            
            # 创建索引
            try:
                create_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_batch_id ON {table_name}(batch_analysis_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name}(created_at);
                """)
                conn.execute(create_index_sql)
                conn.commit()
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
    
    def _init_polygon_roads_table(self):
        """初始化polygon roads表"""
        table_name = self.config.polygon_roads_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                polygon_id VARCHAR(100) NOT NULL,
                road_id BIGINT NOT NULL,
                road_type VARCHAR(50),
                road_level VARCHAR(20),
                intersection_type VARCHAR(20), -- WITHIN/INTERSECTS
                intersection_ratio FLOAT, -- 相交长度比例
                road_length FLOAT,
                intersection_length FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            conn.execute(create_table_sql)
            
            # 添加几何列
            try:
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2)
                """)
                conn.execute(add_geometry_sql)
            except Exception:
                # 几何列可能已存在
                pass
            
            conn.commit()
            
            # 创建索引
            try:
                create_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_polygon_id ON {table_name}(polygon_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_road_id ON {table_name}(road_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                """)
                conn.execute(create_index_sql)
                conn.commit()
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
    
    def _init_polygon_intersections_table(self):
        """初始化polygon intersections表"""
        table_name = self.config.polygon_intersections_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                polygon_id VARCHAR(100) NOT NULL,
                intersection_id BIGINT NOT NULL,
                intersection_type VARCHAR(50),
                intersection_level VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            conn.execute(create_table_sql)
            
            # 添加几何列
            try:
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POINT', 2)
                """)
                conn.execute(add_geometry_sql)
            except Exception:
                # 几何列可能已存在
                pass
            
            conn.commit()
            
            # 创建索引
            try:
                create_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_polygon_id ON {table_name}(polygon_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_intersection_id ON {table_name}(intersection_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                """)
                conn.execute(create_index_sql)
                conn.commit()
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
    
    def _init_polygon_lanes_table(self):
        """初始化polygon lanes表"""
        table_name = self.config.polygon_lanes_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                polygon_id VARCHAR(100) NOT NULL,
                lane_id BIGINT NOT NULL,
                road_id BIGINT,
                lane_type VARCHAR(50),
                lane_direction VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            conn.execute(create_table_sql)
            
            # 添加几何列
            try:
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2)
                """)
                conn.execute(add_geometry_sql)
            except Exception:
                # 几何列可能已存在
                pass
            
            conn.commit()
            
            # 创建索引
            try:
                create_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_polygon_id ON {table_name}(polygon_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_lane_id ON {table_name}(lane_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_road_id ON {table_name}(road_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                """)
                conn.execute(create_index_sql)
                conn.commit()
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
    
    def analyze_polygons_from_geojson(self, geojson_file: str, 
                                    batch_analysis_id: Optional[str] = None) -> str:
        """从GeoJSON文件批量分析polygon"""
        if not batch_analysis_id:
            batch_analysis_id = f"polygon_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        analysis_id = f"{batch_analysis_id}_analysis"
        
        logger.info(f"开始批量polygon分析: {analysis_id}")
        logger.info(f"输入文件: {geojson_file}")
        
        start_time = time.time()
        
        try:
            # 1. 加载和验证polygon数据
            polygons = self._load_and_validate_geojson(geojson_file)
            if not polygons:
                raise ValueError("未找到有效的polygon数据")
            
            logger.info(f"加载了 {len(polygons)} 个polygon")
            
            # 2. 批量查询道路元素
            query_results = self._batch_query_all_elements(polygons)
            
            # 3. 保存结果到数据库
            self._save_analysis_results(analysis_id, batch_analysis_id, polygons, query_results)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # 4. 更新主分析记录
            self._update_main_analysis_record(analysis_id, batch_analysis_id, 
                                            polygons, query_results, processing_time)
            
            logger.info(f"批量polygon分析完成: {analysis_id}")
            logger.info(f"处理时间: {processing_time:.2f} 秒")
            logger.info(f"结果统计:")
            logger.info(f"  - Polygons: {len(polygons)}")
            logger.info(f"  - Roads: {len(query_results.get('roads', pd.DataFrame()))}")
            logger.info(f"  - Intersections: {len(query_results.get('intersections', pd.DataFrame()))}")
            logger.info(f"  - Lanes: {len(query_results.get('lanes', pd.DataFrame()))}")
            
            return analysis_id
            
        except Exception as e:
            logger.error(f"批量polygon分析失败: {e}")
            raise
    
    def _load_and_validate_geojson(self, geojson_file: str) -> List[Dict]:
        """加载并验证GeoJSON文件"""
        logger.info(f"加载GeoJSON文件: {geojson_file}")
        
        with open(geojson_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        if geojson_data.get('type') != 'FeatureCollection':
            raise ValueError("GeoJSON必须是FeatureCollection类型")
        
        polygons = []
        for i, feature in enumerate(geojson_data.get('features', [])):
            try:
                # 验证几何类型
                geometry = feature.get('geometry', {})
                if geometry.get('type') != 'Polygon':
                    logger.warning(f"跳过非Polygon几何: feature {i}")
                    continue
                
                # 提取polygon_id
                properties = feature.get('properties', {})
                polygon_id = properties.get('polygon_id') or properties.get('id') or f"polygon_{i}"
                
                # 转换为shapely对象
                polygon_geom = shape(geometry)
                
                # 验证polygon有效性
                if not polygon_geom.is_valid:
                    logger.warning(f"无效的polygon: {polygon_id}")
                    continue
                
                # 计算面积（用于性能预警）
                area_m2 = polygon_geom.area * (111320 ** 2)  # 近似转换为平方米
                if area_m2 > self.config.max_polygon_area:
                    logger.warning(f"Polygon面积过大: {polygon_id} ({area_m2:.0f} m²)")
                
                polygons.append({
                    'polygon_id': polygon_id,
                    'polygon_geom': polygon_geom,
                    'polygon_wkt': polygon_geom.wkt,
                    'properties': properties,
                    'area_m2': area_m2
                })
                
            except Exception as e:
                logger.error(f"处理feature {i}失败: {e}")
                continue
        
        logger.info(f"成功加载 {len(polygons)} 个有效polygon")
        return polygons
    
    def _batch_query_all_elements(self, polygons: List[Dict]) -> Dict[str, pd.DataFrame]:
        """批量查询所有道路元素"""
        logger.info("开始批量查询道路元素")
        
        # 分批处理以避免内存和查询超时问题
        all_results = {'roads': [], 'intersections': [], 'lanes': []}
        
        batch_size = self.config.polygon_batch_size
        total_batches = (len(polygons) + batch_size - 1) // batch_size
        
        for i in range(0, len(polygons), batch_size):
            batch_polygons = polygons[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            logger.info(f"处理批次 {batch_num}/{total_batches}: {len(batch_polygons)} 个polygon")
            
            # 为这批polygon执行查询
            batch_results = self._batch_query_single_batch(batch_polygons)
            
            # 合并结果
            for key in all_results:
                if key in batch_results and not batch_results[key].empty:
                    all_results[key].append(batch_results[key])
        
        # 合并所有批次的结果
        final_results = {}
        for key in all_results:
            if all_results[key]:
                final_results[key] = pd.concat(all_results[key], ignore_index=True)
                logger.info(f"合并{key}结果: {len(final_results[key])} 条记录")
            else:
                final_results[key] = pd.DataFrame()
                logger.info(f"{key}查询无结果")
        
        return final_results
    
    def _batch_query_single_batch(self, polygons: List[Dict]) -> Dict[str, pd.DataFrame]:
        """批量查询单个批次的polygon"""
        # 直接使用内存中的polygon数据，避免临时表问题
        if self.config.enable_parallel_queries:
            results = self._parallel_batch_query(polygons)
        else:
            results = self._sequential_batch_query(polygons)
        
        return results
    

    
    def _parallel_batch_query(self, polygons: List[Dict]) -> Dict[str, pd.DataFrame]:
        """并行批量查询"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        queries = {
            'roads': lambda: self._batch_query_roads(polygons),
            'intersections': lambda: self._batch_query_intersections(polygons),
        }
        
        results = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_query = {executor.submit(query_func): query_name 
                             for query_name, query_func in queries.items()}
            
            for future in as_completed(future_to_query):
                query_name = future_to_query[future]
                try:
                    results[query_name] = future.result()
                    logger.debug(f"完成{query_name}查询: {len(results[query_name])} 条记录")
                except Exception as e:
                    logger.error(f"{query_name}查询失败: {e}")
                    results[query_name] = pd.DataFrame()
        
        # 基于roads结果查询lanes
        if not results['roads'].empty:
            road_ids = results['roads']['road_id'].unique().tolist()
            results['lanes'] = self._batch_query_lanes_from_roads(road_ids)
        else:
            results['lanes'] = pd.DataFrame()
        
        return results
    
    def _sequential_batch_query(self, polygons: List[Dict]) -> Dict[str, pd.DataFrame]:
        """顺序批量查询"""
        results = {}
        
        # 查询roads
        results['roads'] = self._batch_query_roads(polygons)
        
        # 查询intersections
        results['intersections'] = self._batch_query_intersections(polygons)
        
        # 基于roads查询lanes
        if not results['roads'].empty:
            road_ids = results['roads']['road_id'].unique().tolist()
            results['lanes'] = self._batch_query_lanes_from_roads(road_ids)
        else:
            results['lanes'] = pd.DataFrame()
        
        return results
    
    def _batch_query_roads(self, polygons: List[Dict]) -> pd.DataFrame:
        """批量查询roads"""
        logger.debug("批量查询roads")
        
        if not polygons:
            return pd.DataFrame()
        
        # 使用Trino/Hive连接查询远程数据
        from spdatalab.common.io_hive import hive_cursor
        
        all_results = []
        
        try:
            # 修正catalog名称：去掉_gy1后缀，避免重复
            catalog = self.config.remote_catalog
            if catalog.endswith('_gy1'):
                catalog = catalog[:-4]  # 去掉'_gy1'
            
            logger.info(f"连接Hive catalog: {catalog}")
            
            with hive_cursor(catalog) as cur:
                for polygon in polygons:
                    polygon_id = polygon['polygon_id']
                    polygon_wkt = polygon['polygon_wkt']
                    
                    # 使用完整的表名（包含catalog前缀）
                    full_table_name = f"{self.config.remote_catalog}.{self.config.road_table}"
                    
                    sql = f"""
                    SELECT 
                        '{polygon_id}' as polygon_id,
                        r.road_id,
                        r.road_type,
                        r.road_level,
                        ST_AsText(r.wkb_geometry) as road_geom,
                        CASE 
                            WHEN ST_Within(r.wkb_geometry, ST_GeomFromText('{polygon_wkt}')) THEN 'WITHIN'
                            ELSE 'INTERSECTS'
                        END as intersection_type,
                        ST_Length(r.wkb_geometry) as total_length,
                        ST_Length(ST_Intersection(r.wkb_geometry, ST_GeomFromText('{polygon_wkt}'))) as intersection_length
                    FROM {full_table_name} r
                    WHERE ST_Intersects(r.wkb_geometry, ST_GeomFromText('{polygon_wkt}'))
                    AND r.wkb_geometry IS NOT NULL
                    ORDER BY r.road_id
                    LIMIT 1000
                    """
                    
                    logger.debug(f"执行polygon {polygon_id}的road查询")
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        columns = ['polygon_id', 'road_id', 'road_type', 'road_level', 
                                  'road_geom', 'intersection_type', 'total_length', 'intersection_length']
                        polygon_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(polygon_df)
                        
                        logger.info(f"Polygon {polygon_id}: 查询到 {len(polygon_df)} 条road记录")
                    else:
                        logger.info(f"Polygon {polygon_id}: 未找到road记录")
                
                if all_results:
                    df = pd.concat(all_results, ignore_index=True)
                    # 计算intersection_ratio，避免除零
                    df['intersection_ratio'] = df['intersection_length'] / (df['total_length'] + 1e-10)
                    logger.info(f"总计查询到 {len(df)} 条road记录")
                    return df
                else:
                    logger.info("所有polygon都没有找到road记录")
                    return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"批量查询roads失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _batch_query_intersections(self, polygons: List[Dict]) -> pd.DataFrame:
        """批量查询intersections"""
        logger.debug("批量查询intersections")
        
        if not polygons:
            return pd.DataFrame()
        
        from spdatalab.common.io_hive import hive_cursor
        
        all_results = []
        
        try:
            # 修正catalog名称
            catalog = self.config.remote_catalog
            if catalog.endswith('_gy1'):
                catalog = catalog[:-4]
            
            logger.info(f"连接Hive catalog: {catalog}")
            
            with hive_cursor(catalog) as cur:
                for polygon in polygons:
                    polygon_id = polygon['polygon_id']
                    polygon_wkt = polygon['polygon_wkt']
                    
                    # 使用完整的表名
                    full_table_name = f"{self.config.remote_catalog}.{self.config.intersection_table}"
                    
                    sql = f"""
                    SELECT 
                        '{polygon_id}' as polygon_id,
                        i.id as intersection_id,
                        i.intersectiontype as intersection_type,
                        i.intersectionsubtype as intersection_level,
                        ST_AsText(i.wkb_geometry) as intersection_geom
                    FROM {full_table_name} i
                    WHERE ST_Intersects(i.wkb_geometry, ST_GeomFromText('{polygon_wkt}'))
                    AND i.wkb_geometry IS NOT NULL
                    ORDER BY i.id
                    LIMIT 500
                    """
                    
                    logger.debug(f"执行polygon {polygon_id}的intersection查询")
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        columns = ['polygon_id', 'intersection_id', 'intersection_type', 
                                  'intersection_level', 'intersection_geom']
                        polygon_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(polygon_df)
                        
                        logger.info(f"Polygon {polygon_id}: 查询到 {len(polygon_df)} 条intersection记录")
                    else:
                        logger.info(f"Polygon {polygon_id}: 未找到intersection记录")
                
                if all_results:
                    df = pd.concat(all_results, ignore_index=True)
                    logger.info(f"总计查询到 {len(df)} 条intersection记录")
                    return df
                else:
                    logger.info("所有polygon都没有找到intersection记录")
                    return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"批量查询intersections失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _batch_query_lanes_from_roads(self, road_ids: List[int]) -> pd.DataFrame:
        """从road_ids批量查询lanes"""
        if not road_ids:
            return pd.DataFrame()
        
        logger.info(f"从 {len(road_ids)} 个roads查询lanes")
        
        from spdatalab.common.io_hive import hive_cursor
        
        # 分批查询避免IN子句过长
        batch_size = 500
        all_lanes = []
        
        try:
            # 修正catalog名称
            catalog = self.config.remote_catalog
            if catalog.endswith('_gy1'):
                catalog = catalog[:-4]
            
            with hive_cursor(catalog) as cur:
                for i in range(0, len(road_ids), batch_size):
                    batch_road_ids = road_ids[i:i+batch_size]
                    road_ids_str = ','.join(map(str, batch_road_ids))
                    
                    # 使用完整的表名
                    full_table_name = f"{self.config.remote_catalog}.{self.config.lane_table}"
                    
                    sql = f"""
                    SELECT 
                        road_id,
                        id as lane_id,
                        lanetype as lane_type,
                        lanesourcetype as lane_direction,
                        ST_AsText(wkb_geometry) as lane_geom
                    FROM {full_table_name}
                    WHERE road_id IN ({road_ids_str})
                    AND wkb_geometry IS NOT NULL
                    ORDER BY road_id, id
                    """
                    
                    logger.debug(f"查询lanes批次 {i//batch_size + 1}: {len(batch_road_ids)} roads")
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        columns = ['road_id', 'lane_id', 'lane_type', 'lane_direction', 'lane_geom']
                        batch_df = pd.DataFrame(rows, columns=columns)
                        all_lanes.append(batch_df)
                        logger.info(f"批次 {i//batch_size + 1}: 查询到 {len(batch_df)} 条lane记录")
                    else:
                        logger.info(f"批次 {i//batch_size + 1}: 未找到lane记录")
        
        except Exception as e:
            logger.error(f"查询lanes失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
        
        if all_lanes:
            result_df = pd.concat(all_lanes, ignore_index=True)
            logger.info(f"总计查询到 {len(result_df)} 条lane记录")
            return result_df
        else:
            logger.info("没有查询到lane记录")
            return pd.DataFrame()
    
    def _save_analysis_results(self, analysis_id: str, batch_analysis_id: str,
                             polygons: List[Dict], query_results: Dict[str, pd.DataFrame]):
        """保存分析结果到数据库"""
        logger.info("保存分析结果到数据库")
        
        # 保存roads结果
        if not query_results['roads'].empty:
            self._save_roads_results(analysis_id, query_results['roads'])
        
        # 保存intersections结果
        if not query_results['intersections'].empty:
            self._save_intersections_results(analysis_id, query_results['intersections'])
        
        # 保存lanes结果（需要关联polygon_id）
        if not query_results['lanes'].empty and not query_results['roads'].empty:
            self._save_lanes_results(analysis_id, query_results['roads'], query_results['lanes'])
    
    def _save_roads_results(self, analysis_id: str, roads_df: pd.DataFrame):
        """保存roads结果"""
        table_name = self.config.polygon_roads_table
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, polygon_id, road_id, road_type, road_level, 
             intersection_type, intersection_ratio, road_length, intersection_length, geometry)
            VALUES (
                :analysis_id, :polygon_id, :road_id, :road_type, :road_level,
                :intersection_type, :intersection_ratio, :road_length, :intersection_length,
                ST_SetSRID(ST_GeomFromText(:road_geom), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for _, row in roads_df.iterrows():
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'polygon_id': row['polygon_id'],
                    'road_id': int(row['road_id']),
                    'road_type': row.get('road_type', ''),
                    'road_level': row.get('road_level', ''),
                    'intersection_type': row.get('intersection_type', ''),
                    'intersection_ratio': float(row.get('intersection_ratio', 0.0)),
                    'road_length': float(row.get('total_length', 0.0)),
                    'intersection_length': float(row.get('intersection_length', 0.0)),
                    'road_geom': row.get('road_geom', '')
                })
            
            conn.commit()
        
        logger.info(f"保存roads结果: {len(roads_df)} 条记录")
    
    def _save_intersections_results(self, analysis_id: str, intersections_df: pd.DataFrame):
        """保存intersections结果"""
        table_name = self.config.polygon_intersections_table
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, polygon_id, intersection_id, intersection_type, intersection_level, geometry)
            VALUES (
                :analysis_id, :polygon_id, :intersection_id, :intersection_type, :intersection_level,
                ST_SetSRID(ST_GeomFromText(:intersection_geom), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for _, row in intersections_df.iterrows():
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'polygon_id': row['polygon_id'],
                    'intersection_id': int(row['intersection_id']),
                    'intersection_type': row.get('intersection_type', ''),
                    'intersection_level': row.get('intersection_level', ''),
                    'intersection_geom': row.get('intersection_geom', '')
                })
            
            conn.commit()
        
        logger.info(f"保存intersections结果: {len(intersections_df)} 条记录")
    
    def _save_lanes_results(self, analysis_id: str, roads_df: pd.DataFrame, lanes_df: pd.DataFrame):
        """保存lanes结果（通过road_id关联polygon_id）"""
        table_name = self.config.polygon_lanes_table
        
        # 创建road_id到polygon_id的映射
        road_polygon_map = {}
        for _, row in roads_df.iterrows():
            road_id = int(row['road_id'])
            polygon_id = row['polygon_id']
            if road_id not in road_polygon_map:
                road_polygon_map[road_id] = []
            road_polygon_map[road_id].append(polygon_id)
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, polygon_id, lane_id, road_id, lane_type, lane_direction, geometry)
            VALUES (
                :analysis_id, :polygon_id, :lane_id, :road_id, :lane_type, :lane_direction,
                ST_SetSRID(ST_GeomFromText(:lane_geom), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for _, row in lanes_df.iterrows():
                road_id = int(row['road_id'])
                polygon_ids = road_polygon_map.get(road_id, [])
                
                # 为每个polygon_id插入lane记录
                for polygon_id in polygon_ids:
                    conn.execute(insert_sql, {
                        'analysis_id': analysis_id,
                        'polygon_id': polygon_id,
                        'lane_id': int(row['lane_id']),
                        'road_id': road_id,
                        'lane_type': row.get('lane_type', ''),
                        'lane_direction': row.get('lane_direction', ''),
                        'lane_geom': row.get('lane_geom', '')
                    })
            
            conn.commit()
        
        logger.info(f"保存lanes结果: {len(lanes_df)} 条记录")
    
    def _update_main_analysis_record(self, analysis_id: str, batch_analysis_id: str,
                                   polygons: List[Dict], query_results: Dict[str, pd.DataFrame],
                                   processing_time: float):
        """更新主分析记录"""
        table_name = self.config.polygon_analysis_table
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, batch_analysis_id, total_polygons, total_roads, 
             total_intersections, total_lanes, processing_time_seconds)
            VALUES (
                :analysis_id, :batch_analysis_id, :total_polygons, :total_roads,
                :total_intersections, :total_lanes, :processing_time_seconds
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            conn.execute(insert_sql, {
                'analysis_id': analysis_id,
                'batch_analysis_id': batch_analysis_id,
                'total_polygons': len(polygons),
                'total_roads': len(query_results.get('roads', pd.DataFrame())),
                'total_intersections': len(query_results.get('intersections', pd.DataFrame())),
                'total_lanes': len(query_results.get('lanes', pd.DataFrame())),
                'processing_time_seconds': processing_time
            })
            
            conn.commit()
        
        logger.info(f"更新主分析记录: {analysis_id}")
    
    def get_analysis_summary(self, analysis_id: str) -> Dict[str, Any]:
        """获取分析摘要"""
        table_name = self.config.polygon_analysis_table
        
        sql = text(f"""
            SELECT * FROM {table_name}
            WHERE analysis_id = :analysis_id
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(sql, {'analysis_id': analysis_id}).fetchone()
            
            if not result:
                raise ValueError(f"分析记录不存在: {analysis_id}")
            
            # 获取详细统计
            roads_sql = text(f"""
                SELECT polygon_id, COUNT(*) as road_count
                FROM {self.config.polygon_roads_table}
                WHERE analysis_id = :analysis_id
                GROUP BY polygon_id
            """)
            
            roads_stats = conn.execute(roads_sql, {'analysis_id': analysis_id}).fetchall()
            
            summary = {
                'analysis_id': result.analysis_id,
                'batch_analysis_id': result.batch_analysis_id,
                'total_polygons': result.total_polygons,
                'total_roads': result.total_roads,
                'total_intersections': result.total_intersections,
                'total_lanes': result.total_lanes,
                'processing_time_seconds': result.processing_time_seconds,
                'created_at': result.created_at,
                'polygon_road_stats': {row.polygon_id: row.road_count for row in roads_stats}
            }
            
            return summary


def analyze_polygons_from_geojson(
    geojson_file: str,
    config: Optional[PolygonRoadAnalysisConfig] = None,
    batch_analysis_id: Optional[str] = None
) -> Tuple[str, Dict[str, Any]]:
    """分析GeoJSON文件中的polygon道路元素
    
    Args:
        geojson_file: GeoJSON文件路径
        config: 分析配置
        batch_analysis_id: 批量分析ID
        
    Returns:
        (analysis_id, summary)
    """
    analyzer = BatchPolygonRoadAnalyzer(config)
    analysis_id = analyzer.analyze_polygons_from_geojson(geojson_file, batch_analysis_id)
    summary = analyzer.get_analysis_summary(analysis_id)
    
    return analysis_id, summary


def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='基于polygon区域的道路元素分析',
        epilog="""
输入格式:
  GeoJSON文件，包含多个Polygon类型的Feature
  
功能:
  批量查找polygon区域内的roads、intersections、lanes
  高效的批量空间查询，避免逐个polygon处理
  
示例:
  python -m spdatalab.fusion.polygon_road_analysis --input-geojson areas.geojson
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 输入参数
    parser.add_argument('--input-geojson', required=True,
                       help='包含polygon的GeoJSON文件路径')
    parser.add_argument('--batch-analysis-id', 
                       help='批量分析ID（可选，自动生成）')
    
    # 配置参数
    parser.add_argument('--polygon-batch-size', type=int, default=50,
                       help='polygon批处理大小')
    parser.add_argument('--enable-parallel-queries', action='store_true', default=True,
                       help='启用并行查询')
    parser.add_argument('--disable-parallel-queries', dest='enable_parallel_queries', 
                       action='store_false', help='禁用并行查询')
    
    # 输出参数
    parser.add_argument('--output-summary', action='store_true',
                       help='输出详细统计摘要')
    
    # 其他参数
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 构建配置
        config = PolygonRoadAnalysisConfig(
            polygon_batch_size=args.polygon_batch_size,
            enable_parallel_queries=args.enable_parallel_queries
        )
        
        logger.info(f"输入文件: {args.input_geojson}")
        logger.info(f"批处理大小: {config.polygon_batch_size}")
        logger.info(f"并行查询: {'启用' if config.enable_parallel_queries else '禁用'}")
        
        # 执行分析
        analysis_id, summary = analyze_polygons_from_geojson(
            args.input_geojson, config, args.batch_analysis_id
        )
        
        # 输出结果
        logger.info("=== 分析完成 ===")
        logger.info(f"分析ID: {analysis_id}")
        logger.info(f"批量分析ID: {summary['batch_analysis_id']}")
        logger.info(f"Polygons: {summary['total_polygons']}")
        logger.info(f"Roads: {summary['total_roads']}")
        logger.info(f"Intersections: {summary['total_intersections']}")
        logger.info(f"Lanes: {summary['total_lanes']}")
        logger.info(f"处理时间: {summary['processing_time_seconds']:.2f} 秒")
        
        if args.output_summary:
            logger.info("\n=== 详细统计 ===")
            for polygon_id, road_count in summary['polygon_road_stats'].items():
                logger.info(f"Polygon {polygon_id}: {road_count} roads")
        
        return 0
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 