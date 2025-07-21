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
    
    # 远程表名配置（包含关联表）
    road_table: str = "full_road"
    intersection_table: str = "full_intersection"
    intersection_inroad_table: str = "full_intersectiongoinroad"
    intersection_outroad_table: str = "full_intersectiongooutroad"
    roadintersection_table: str = "full_roadinintersection"
    
    # 批量处理配置
    polygon_batch_size: int = 50  # 每批处理的polygon数量
    enable_parallel_queries: bool = True  # 启用并行查询
    
    # 查询限制配置
    max_polygon_area: float = 10000000  # 最大polygon面积（平方米）
    max_roads_per_polygon: int = 1000  # 单个polygon最大road数量
    max_intersections_per_polygon: int = 200  # 单个polygon最大intersection数量
    
    # 两阶段查询配置
    spatial_prefilter_limit: int = 2000  # 空间预筛选限制
    detailed_query_batch_size: int = 100  # 详细查询批次大小
    
    # 边界处理配置
    include_boundary_roads: bool = True  # 是否包含边界roads
    boundary_inclusion_threshold: float = 0.1  # 边界road包含阈值（相交长度比）
    
    # 结果表名（移除lanes）
    polygon_analysis_table: str = "polygon_road_analysis"
    polygon_roads_table: str = "polygon_roads"
    polygon_intersections_table: str = "polygon_intersections"

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
        
        # 不在初始化时创建表，而是在需要时创建
    
    def _init_analysis_tables(self):
        """初始化所有分析表"""
        logger.info("开始初始化分析表...")
        
        try:
            self._init_main_analysis_table()
            logger.info(f"✓ 主分析表创建成功: {self.config.polygon_analysis_table}")
        except Exception as e:
            logger.error(f"✗ 主分析表创建失败: {e}")
            raise
        
        try:
            self._init_polygon_roads_table()
            logger.info(f"✓ Roads表创建成功: {self.config.polygon_roads_table}")
        except Exception as e:
            logger.error(f"✗ Roads表创建失败: {e}")
            raise
        
        try:
            self._init_polygon_intersections_table()
            logger.info(f"✓ Intersections表创建成功: {self.config.polygon_intersections_table}")
        except Exception as e:
            logger.error(f"✗ Intersections表创建失败: {e}")
            raise
        
        logger.info("所有分析表初始化完成")
    
    def _init_main_analysis_table(self):
        """初始化主分析表"""
        table_name = self.config.polygon_analysis_table
        logger.debug(f"创建主分析表: {table_name}")
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                analysis_id VARCHAR(100) PRIMARY KEY,
                batch_analysis_id VARCHAR(100),
                total_polygons INTEGER DEFAULT 0,
                total_roads INTEGER DEFAULT 0,
                total_intersections INTEGER DEFAULT 0,
                processing_time_seconds FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            logger.debug(f"执行SQL: CREATE TABLE IF NOT EXISTS {table_name}...")
            conn.execute(create_table_sql)
            conn.commit()
            logger.debug(f"主分析表创建SQL执行成功: {table_name}")
            
            # 创建索引
            try:
                create_index_sql = text(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_batch_id ON {table_name}(batch_analysis_id);
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name}(created_at);
                """)
                conn.execute(create_index_sql)
                conn.commit()
                logger.debug(f"主分析表索引创建成功: {table_name}")
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
    
    def _init_polygon_roads_table(self):
        """初始化polygon roads表（包含full_road所有字段和boolean关联字段）"""
        table_name = self.config.polygon_roads_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                polygon_id VARCHAR(100) NOT NULL,
                
                -- full_road的所有原始字段
                road_id BIGINT NOT NULL,
                cityid VARCHAR(50),
                patchid VARCHAR(50),
                patchversion VARCHAR(50),
                releaseversion VARCHAR(50),
                citypatchversion VARCHAR(50),
                length INTEGER,
                roadtype INTEGER,
                isbothway INTEGER,
                roadclass INTEGER,
                roadclasssource INTEGER,
                roadtypesource INTEGER,
                turninfo VARCHAR(50),
                turntype INTEGER,
                turntypesource INTEGER,
                roadflag INTEGER,
                roadflagsource INTEGER,
                
                -- 新增的3个boolean字段
                is_intersection_inroad BOOLEAN DEFAULT FALSE,
                is_intersection_outroad BOOLEAN DEFAULT FALSE,
                is_road_intersection BOOLEAN DEFAULT FALSE,
                
                -- 空间相交分析字段
                intersection_type VARCHAR(20), -- WITHIN/INTERSECTS
                intersection_ratio FLOAT, -- 相交长度比例
                road_length FLOAT,
                intersection_length FLOAT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            logger.debug(f"执行SQL: CREATE TABLE IF NOT EXISTS {table_name}...")
            conn.execute(create_table_sql)
            logger.debug(f"Roads表创建SQL执行成功: {table_name}")
            
            # 添加几何列（roads是LINESTRING类型）
            try:
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRINGZ', 2)
                """)
                logger.debug(f"添加几何列: LINESTRINGZ, 2D")
                conn.execute(add_geometry_sql)
                logger.debug(f"Roads表几何列添加成功: LINESTRINGZ, 2D")
            except Exception as e1:
                logger.debug(f"LINESTRINGZ 2D添加失败: {e1}，尝试3D模式")
                # 几何列可能已存在，尝试3D模式
                try:
                    add_geometry_sql_3d = text(f"""
                        SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRINGZ', 3)
                    """)
                    logger.debug(f"添加几何列: LINESTRINGZ, 3D")
                    conn.execute(add_geometry_sql_3d)
                    logger.debug(f"Roads表几何列添加成功: LINESTRINGZ, 3D")
                except Exception as e2:
                    logger.debug(f"LINESTRINGZ 3D添加也失败: {e2}，可能表已存在")
                    # 如果都失败，表可能已存在
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
        """初始化polygon intersections表（包含full_intersection所有字段）"""
        table_name = self.config.polygon_intersections_table
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                polygon_id VARCHAR(100) NOT NULL,
                
                -- full_intersection的所有原始字段
                intersection_id BIGINT NOT NULL,
                cityid VARCHAR(50),
                patchid VARCHAR(50),
                patchversion VARCHAR(50),
                releaseversion VARCHAR(50),
                citypatchversion VARCHAR(50),
                intersectiontype INTEGER,
                intersectionsubtype INTEGER,
                source INTEGER,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        with self.engine.connect() as conn:
            logger.debug(f"执行SQL: CREATE TABLE IF NOT EXISTS {table_name}...")
            conn.execute(create_table_sql)
            logger.debug(f"Intersections表创建SQL执行成功: {table_name}")
            
            # 添加几何列（intersection是POLYGON类型）
            try:
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POLYGONZ', 2)
                """)
                logger.debug(f"添加几何列: POLYGONZ, 2D")
                conn.execute(add_geometry_sql)
                logger.debug(f"Intersections表几何列添加成功: POLYGONZ, 2D")
            except Exception as e1:
                logger.debug(f"POLYGONZ 2D添加失败: {e1}，尝试3D模式")
                # 几何列可能已存在，尝试3D模式
                try:
                    add_geometry_sql_3d = text(f"""
                        SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POLYGONZ', 3)
                    """)
                    logger.debug(f"添加几何列: POLYGONZ, 3D")
                    conn.execute(add_geometry_sql_3d)
                    logger.debug(f"Intersections表几何列添加成功: POLYGONZ, 3D")
                except Exception as e2:
                    logger.debug(f"POLYGONZ 3D添加也失败: {e2}，可能表已存在")
                    # 如果都失败，表可能已存在
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
            # 0. 初始化数据库表
            self._init_analysis_tables()
            logger.info("✓ 数据库表初始化完成")
            
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
        all_results = {'roads': [], 'intersections': []}
        
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
        

        
        return results
    
    def _sequential_batch_query(self, polygons: List[Dict]) -> Dict[str, pd.DataFrame]:
        """顺序批量查询"""
        results = {}
        
        # 查询roads
        results['roads'] = self._batch_query_roads(polygons)
        
        # 查询intersections
        results['intersections'] = self._batch_query_intersections(polygons)
        

        
        return results
    
    def _batch_query_roads(self, polygons: List[Dict]) -> pd.DataFrame:
        """批量查询roads（两阶段查询策略 - 批量优化版）"""
        logger.info("开始批量两阶段roads查询")
        
        if not polygons:
            return pd.DataFrame()
        
        # 阶段1：对所有polygons进行空间预筛选
        all_road_keys = set()
        polygon_road_map = {}  # polygon_id -> [(id, patchid, releaseversion)]
        
        logger.info("阶段1: 对所有polygons进行空间预筛选")
        for polygon in polygons:
            polygon_id = polygon['polygon_id']
            polygon_wkt = polygon['polygon_wkt']
            
            try:
                filtered_road_keys = self._spatial_prefilter_roads(polygon_wkt)
                if filtered_road_keys:
                    polygon_road_map[polygon_id] = filtered_road_keys
                    all_road_keys.update(filtered_road_keys)
                    logger.debug(f"Polygon {polygon_id}: 预筛选到 {len(filtered_road_keys)} 个roads")
                else:
                    logger.debug(f"Polygon {polygon_id}: 空间预筛选未找到roads")
            except Exception as e:
                logger.error(f"预筛选polygon {polygon_id} roads失败: {e}")
                continue
        
        if not all_road_keys:
            logger.info("所有polygon的空间预筛选都没有找到roads")
            return pd.DataFrame()
        
        logger.info(f"预筛选阶段: 找到 {len(all_road_keys)} 个唯一roads")
        
        # 阶段2：批量详细查询所有预筛选的roads
        logger.info("阶段2: 批量详细查询预筛选的roads")
        all_road_details = self._detailed_query_roads_batch(list(all_road_keys))
        
        if all_road_details.empty:
            logger.info("详细查询未找到任何roads")
            return pd.DataFrame()
        
        # 阶段3：为每个road关联到相应的polygons，并计算空间关系
        logger.info("阶段3: 计算roads与polygons的空间关系")
        final_results = []
        
        for _, road_row in all_road_details.iterrows():
            road_key = (road_row['id'], road_row['patchid'], road_row['releaseversion'])
            road_geom = road_row['road_geom']
            
            # 找到包含这个road的所有polygons
            for polygon_id, polygon_road_keys in polygon_road_map.items():
                if road_key in polygon_road_keys:
                    # 获取polygon几何
                    polygon_wkt = next(p['polygon_wkt'] for p in polygons if p['polygon_id'] == polygon_id)
                    
                    # 计算空间关系
                    try:
                        spatial_result = self._calculate_spatial_relationship(road_geom, polygon_wkt)
                        
                        # 创建结果记录
                        result_row = road_row.copy()
                        result_row['polygon_id'] = polygon_id
                        result_row.update(spatial_result)
                        
                        final_results.append(result_row)
                    except Exception as e:
                        logger.error(f"计算road {road_key} 与 polygon {polygon_id} 空间关系失败: {e}")
                        continue
        
        if final_results:
            df = pd.DataFrame(final_results)
            logger.info(f"总计查询到 {len(df)} 条road记录")
            return df
        else:
            logger.info("没有找到任何有效的road记录")
            return pd.DataFrame()
    
    def _spatial_prefilter_roads(self, polygon_wkt: str) -> List[Tuple]:
        """阶段1：空间预筛选roads，返回(id, patchid, releaseversion)列表"""
        from spdatalab.common.io_hive import hive_cursor
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 快速空间查询，只获取复合键
                sql = f"""
                SELECT r.id, r.patchid, r.releaseversion
                FROM {self.config.road_table} r
                WHERE ST_Intersects(ST_SetSRID(r.wkb_geometry, 4326), ST_GeomFromText('{polygon_wkt}', 4326))
                AND r.wkb_geometry IS NOT NULL
                LIMIT {self.config.spatial_prefilter_limit}
                """
                
                cur.execute(sql)
                rows = cur.fetchall()
                
                # 返回复合键列表
                return [(row[0], row[1], row[2]) for row in rows]
                
        except Exception as e:
            logger.error(f"空间预筛选roads失败: {e}")
            return []
    
    def _detailed_query_roads_batch(self, filtered_road_keys: List[Tuple]) -> pd.DataFrame:
        """批量详细查询roads，不包含polygon特定信息"""
        from spdatalab.common.io_hive import hive_cursor
        
        if not filtered_road_keys:
            return pd.DataFrame()
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 分批处理避免IN子句过长
                batch_size = self.config.detailed_query_batch_size
                all_results = []
                
                for i in range(0, len(filtered_road_keys), batch_size):
                    batch_keys = filtered_road_keys[i:i+batch_size]
                    
                    # 构建IN子句
                    keys_condition = ','.join([
                        f"({key[0]}, '{key[1]}', '{key[2]}')" for key in batch_keys
                    ])
                    
                    # 详细查询SQL，包含所有字段和JOIN
                    sql = f"""
                    SELECT 
                        r.*,  -- full_road所有字段
                        ST_AsText(r.wkb_geometry) as road_geom,
                        CASE WHEN gir.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_intersection_inroad,
                        CASE WHEN gor.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_intersection_outroad,
                        CASE WHEN ri.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_road_intersection
                    FROM {self.config.road_table} r
                    LEFT JOIN {self.config.intersection_inroad_table} gir 
                        ON gir.roadid = r.id AND gir.patchid = r.patchid AND gir.releaseversion = r.releaseversion
                    LEFT JOIN {self.config.intersection_outroad_table} gor 
                        ON gor.roadid = r.id AND gor.patchid = r.patchid AND gor.releaseversion = r.releaseversion
                    LEFT JOIN {self.config.roadintersection_table} ri 
                        ON ri.roadid = r.id AND ri.patchid = r.patchid AND ri.releaseversion = r.releaseversion
                    WHERE (r.id, r.patchid, r.releaseversion) IN ({keys_condition})
                    """
                    
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        # 获取列名
                        columns = [d[0] for d in cur.description]
                        batch_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(batch_df)
                
                if all_results:
                    result_df = pd.concat(all_results, ignore_index=True)
                    return result_df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"批量详细查询roads失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _calculate_spatial_relationship(self, road_geom_wkt: str, polygon_wkt: str) -> Dict:
        """计算road与polygon的空间关系"""
        try:
            from shapely import wkt
            
            # 解析几何
            road_geom = wkt.loads(road_geom_wkt)
            polygon_geom = wkt.loads(polygon_wkt)
            
            # 计算空间关系
            is_within = road_geom.within(polygon_geom)
            intersection = road_geom.intersection(polygon_geom)
            
            # 计算长度
            road_length = road_geom.length * 111320.0  # 转换为米
            intersection_length = intersection.length * 111320.0 if hasattr(intersection, 'length') else 0.0
            
            # 计算比例
            intersection_ratio = intersection_length / (road_length + 1e-10)
            
            return {
                'intersection_type': 'WITHIN' if is_within else 'INTERSECTS',
                'road_length': road_length,
                'intersection_length': intersection_length,
                'intersection_ratio': intersection_ratio
            }
            
        except Exception as e:
            logger.error(f"计算空间关系失败: {e}")
            return {
                'intersection_type': 'UNKNOWN',
                'road_length': 0.0,
                'intersection_length': 0.0,
                'intersection_ratio': 0.0
            }
    
    def _detailed_query_roads(self, filtered_road_keys: List[Tuple], polygon_id: str, polygon_wkt: str) -> pd.DataFrame:
        """阶段2：基于预筛选结果进行详细查询，包含JOIN和完整字段"""
        from spdatalab.common.io_hive import hive_cursor
        
        if not filtered_road_keys:
            return pd.DataFrame()
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 分批处理避免IN子句过长
                batch_size = self.config.detailed_query_batch_size
                all_results = []
                
                for i in range(0, len(filtered_road_keys), batch_size):
                    batch_keys = filtered_road_keys[i:i+batch_size]
                    
                    # 构建IN子句
                    keys_condition = ','.join([
                        f"({key[0]}, '{key[1]}', '{key[2]}')" for key in batch_keys
                    ])
                    
                    # 详细查询SQL，包含所有字段和JOIN
                    sql = f"""
                    SELECT 
                        '{polygon_id}' as polygon_id,
                        r.*,  -- full_road所有字段
                        ST_AsText(r.wkb_geometry) as road_geom,
                        CASE WHEN gir.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_intersection_inroad,
                        CASE WHEN gor.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_intersection_outroad,
                        CASE WHEN ri.roadid IS NOT NULL THEN TRUE ELSE FALSE END as is_road_intersection,
                        CASE 
                            WHEN ST_Within(ST_SetSRID(r.wkb_geometry, 4326), ST_GeomFromText('{polygon_wkt}', 4326)) THEN 'WITHIN'
                            ELSE 'INTERSECTS'
                        END as intersection_type,
                        ST_Length(ST_SetSRID(r.wkb_geometry, 4326)) as road_length,
                        ST_Length(ST_Intersection(ST_SetSRID(r.wkb_geometry, 4326), ST_GeomFromText('{polygon_wkt}', 4326))) as intersection_length
                    FROM {self.config.road_table} r
                    LEFT JOIN {self.config.intersection_inroad_table} gir 
                        ON gir.roadid = r.id AND gir.patchid = r.patchid AND gir.releaseversion = r.releaseversion
                    LEFT JOIN {self.config.intersection_outroad_table} gor 
                        ON gor.roadid = r.id AND gor.patchid = r.patchid AND gor.releaseversion = r.releaseversion
                    LEFT JOIN {self.config.roadintersection_table} ri 
                        ON ri.roadid = r.id AND ri.patchid = r.patchid AND ri.releaseversion = r.releaseversion
                    WHERE (r.id, r.patchid, r.releaseversion) IN ({keys_condition})
                    """
                    
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        # 获取列名
                        columns = [d[0] for d in cur.description]
                        batch_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(batch_df)
                
                if all_results:
                    result_df = pd.concat(all_results, ignore_index=True)
                    # 计算intersection_ratio，避免除零
                    result_df['intersection_ratio'] = result_df['intersection_length'] / (result_df['road_length'] + 1e-10)
                    return result_df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"详细查询roads失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _batch_query_intersections(self, polygons: List[Dict]) -> pd.DataFrame:
        """批量查询intersections（两阶段查询策略 - 批量优化版）"""
        logger.info("开始批量两阶段intersections查询")
        
        if not polygons:
            return pd.DataFrame()
        
        # 阶段1：对所有polygons进行空间预筛选
        all_intersection_keys = set()
        polygon_intersection_map = {}  # polygon_id -> [(id, patchid, releaseversion)]
        
        logger.info("阶段1: 对所有polygons进行空间预筛选")
        for polygon in polygons:
            polygon_id = polygon['polygon_id']
            polygon_wkt = polygon['polygon_wkt']
            
            try:
                filtered_intersection_keys = self._spatial_prefilter_intersections(polygon_wkt)
                if filtered_intersection_keys:
                    polygon_intersection_map[polygon_id] = filtered_intersection_keys
                    all_intersection_keys.update(filtered_intersection_keys)
                    logger.debug(f"Polygon {polygon_id}: 预筛选到 {len(filtered_intersection_keys)} 个intersections")
                else:
                    logger.debug(f"Polygon {polygon_id}: 空间预筛选未找到intersections")
            except Exception as e:
                logger.error(f"预筛选polygon {polygon_id} intersections失败: {e}")
                continue
        
        if not all_intersection_keys:
            logger.info("所有polygon的空间预筛选都没有找到intersections")
            return pd.DataFrame()
        
        logger.info(f"预筛选阶段: 找到 {len(all_intersection_keys)} 个唯一intersections")
        
        # 阶段2：批量详细查询所有预筛选的intersections
        logger.info("阶段2: 批量详细查询预筛选的intersections")
        all_intersection_details = self._detailed_query_intersections_batch(list(all_intersection_keys))
        
        if all_intersection_details.empty:
            logger.info("详细查询未找到任何intersections")
            return pd.DataFrame()
        
        # 阶段3：为每个intersection关联到相应的polygons
        logger.info("阶段3: 关联intersections到相应的polygons")
        final_results = []
        
        for _, intersection_row in all_intersection_details.iterrows():
            intersection_key = (intersection_row['id'], intersection_row['patchid'], intersection_row['releaseversion'])
            
            # 找到包含这个intersection的所有polygons
            for polygon_id, polygon_intersection_keys in polygon_intersection_map.items():
                if intersection_key in polygon_intersection_keys:
                    # 创建结果记录
                    result_row = intersection_row.copy()
                    result_row['polygon_id'] = polygon_id
                    
                    final_results.append(result_row)
        
        if final_results:
            df = pd.DataFrame(final_results)
            logger.info(f"总计查询到 {len(df)} 条intersection记录")
            return df
        else:
            logger.info("没有找到任何有效的intersection记录")
            return pd.DataFrame()
    
    def _spatial_prefilter_intersections(self, polygon_wkt: str) -> List[Tuple]:
        """阶段1：空间预筛选intersections，返回(id, patchid, releaseversion)列表"""
        from spdatalab.common.io_hive import hive_cursor
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 快速空间查询，只获取复合键
                sql = f"""
                SELECT i.id, i.patchid, i.releaseversion
                FROM {self.config.intersection_table} i
                WHERE ST_Intersects(ST_SetSRID(i.wkb_geometry, 4326), ST_GeomFromText('{polygon_wkt}', 4326))
                AND i.wkb_geometry IS NOT NULL
                LIMIT {self.config.max_intersections_per_polygon}
                """
                
                cur.execute(sql)
                rows = cur.fetchall()
                
                # 返回复合键列表
                return [(row[0], row[1], row[2]) for row in rows]
                
        except Exception as e:
            logger.error(f"空间预筛选intersections失败: {e}")
            return []
    
    def _detailed_query_intersections_batch(self, filtered_intersection_keys: List[Tuple]) -> pd.DataFrame:
        """批量详细查询intersections，不包含polygon特定信息"""
        from spdatalab.common.io_hive import hive_cursor
        
        if not filtered_intersection_keys:
            return pd.DataFrame()
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 分批处理避免IN子句过长
                batch_size = self.config.detailed_query_batch_size
                all_results = []
                
                for i in range(0, len(filtered_intersection_keys), batch_size):
                    batch_keys = filtered_intersection_keys[i:i+batch_size]
                    
                    # 构建IN子句
                    keys_condition = ','.join([
                        f"({key[0]}, '{key[1]}', '{key[2]}')" for key in batch_keys
                    ])
                    
                    # 详细查询SQL，包含所有字段
                    sql = f"""
                    SELECT 
                        i.*,  -- full_intersection所有字段
                        ST_AsText(i.wkb_geometry) as intersection_geom
                    FROM {self.config.intersection_table} i
                    WHERE (i.id, i.patchid, i.releaseversion) IN ({keys_condition})
                    """
                    
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        # 获取列名
                        columns = [d[0] for d in cur.description]
                        batch_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(batch_df)
                
                if all_results:
                    result_df = pd.concat(all_results, ignore_index=True)
                    return result_df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"批量详细查询intersections失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _detailed_query_intersections(self, filtered_intersection_keys: List[Tuple], polygon_id: str) -> pd.DataFrame:
        """阶段2：基于预筛选结果进行详细查询，包含完整字段"""
        from spdatalab.common.io_hive import hive_cursor
        
        if not filtered_intersection_keys:
            return pd.DataFrame()
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 分批处理避免IN子句过长
                batch_size = self.config.detailed_query_batch_size
                all_results = []
                
                for i in range(0, len(filtered_intersection_keys), batch_size):
                    batch_keys = filtered_intersection_keys[i:i+batch_size]
                    
                    # 构建IN子句
                    keys_condition = ','.join([
                        f"({key[0]}, '{key[1]}', '{key[2]}')" for key in batch_keys
                    ])
                    
                    # 详细查询SQL，包含所有字段
                    sql = f"""
                    SELECT 
                        '{polygon_id}' as polygon_id,
                        i.*,  -- full_intersection所有字段
                        ST_AsText(i.wkb_geometry) as intersection_geom
                    FROM {self.config.intersection_table} i
                    WHERE (i.id, i.patchid, i.releaseversion) IN ({keys_condition})
                    """
                    
                    cur.execute(sql)
                    rows = cur.fetchall()
                    
                    if rows:
                        # 获取列名
                        columns = [d[0] for d in cur.description]
                        batch_df = pd.DataFrame(rows, columns=columns)
                        all_results.append(batch_df)
                
                if all_results:
                    result_df = pd.concat(all_results, ignore_index=True)
                    return result_df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"详细查询intersections失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
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
        

    
    def _save_roads_results(self, analysis_id: str, roads_df: pd.DataFrame):
        """保存roads结果（包含完整字段）"""
        table_name = self.config.polygon_roads_table
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, polygon_id, road_id, cityid, patchid, patchversion, releaseversion, citypatchversion,
             length, roadtype, isbothway, roadclass, roadclasssource, roadtypesource, turninfo, turntype, 
             turntypesource, roadflag, roadflagsource, is_intersection_inroad, is_intersection_outroad, 
             is_road_intersection, intersection_type, intersection_ratio, road_length, intersection_length, geometry)
            VALUES (
                :analysis_id, :polygon_id, :road_id, :cityid, :patchid, :patchversion, :releaseversion, :citypatchversion,
                :length, :roadtype, :isbothway, :roadclass, :roadclasssource, :roadtypesource, :turninfo, :turntype,
                :turntypesource, :roadflag, :roadflagsource, :is_intersection_inroad, :is_intersection_outroad,
                :is_road_intersection, :intersection_type, :intersection_ratio, :road_length, :intersection_length,
                ST_SetSRID(ST_GeomFromText(:road_geom), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for _, row in roads_df.iterrows():
                # 处理boolean字段
                def safe_bool(value):
                    if pd.isna(value) or value is None:
                        return False
                    if isinstance(value, str):
                        return value.lower() in ('true', 't', '1', 'yes')
                    return bool(value)
                
                # 处理整数字段  
                def safe_int(value):
                    if pd.isna(value) or value is None or str(value).strip() == '':
                        return None
                    try:
                        return int(value)
                    except (ValueError, TypeError):
                        return None
                
                # 处理字符串字段
                def safe_str(value):
                    if pd.isna(value) or value is None:
                        return ''
                    return str(value)
                
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'polygon_id': row['polygon_id'],
                    'road_id': int(row['id']),  # 使用原始id字段
                    'cityid': safe_str(row.get('cityid', '')),
                    'patchid': safe_str(row.get('patchid', '')),
                    'patchversion': safe_str(row.get('patchversion', '')),
                    'releaseversion': safe_str(row.get('releaseversion', '')),
                    'citypatchversion': safe_str(row.get('citypatchversion', '')),
                    'length': safe_int(row.get('length')),
                    'roadtype': safe_int(row.get('roadtype')),
                    'isbothway': safe_int(row.get('isbothway')),
                    'roadclass': safe_int(row.get('roadclass')),
                    'roadclasssource': safe_int(row.get('roadclasssource')),
                    'roadtypesource': safe_int(row.get('roadtypesource')),
                    'turninfo': safe_str(row.get('turninfo', '')),
                    'turntype': safe_int(row.get('turntype')),
                    'turntypesource': safe_int(row.get('turntypesource')),
                    'roadflag': safe_int(row.get('roadflag')),
                    'roadflagsource': safe_int(row.get('roadflagsource')),
                    'is_intersection_inroad': safe_bool(row.get('is_intersection_inroad', False)),
                    'is_intersection_outroad': safe_bool(row.get('is_intersection_outroad', False)),
                    'is_road_intersection': safe_bool(row.get('is_road_intersection', False)),
                    'intersection_type': row.get('intersection_type', ''),
                    'intersection_ratio': float(row.get('intersection_ratio', 0.0)),
                    'road_length': float(row.get('road_length', 0.0)),
                    'intersection_length': float(row.get('intersection_length', 0.0)),
                    'road_geom': row.get('road_geom', '')
                })
            
            conn.commit()
        
        logger.info(f"保存roads结果: {len(roads_df)} 条记录")
    
    def _save_intersections_results(self, analysis_id: str, intersections_df: pd.DataFrame):
        """保存intersections结果（包含完整字段）"""
        table_name = self.config.polygon_intersections_table
        
        # 删除现有记录
        delete_sql = text(f"DELETE FROM {table_name} WHERE analysis_id = :analysis_id")
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, polygon_id, intersection_id, cityid, patchid, patchversion, releaseversion, 
             citypatchversion, intersectiontype, intersectionsubtype, source, geometry)
            VALUES (
                :analysis_id, :polygon_id, :intersection_id, :cityid, :patchid, :patchversion, :releaseversion,
                :citypatchversion, :intersectiontype, :intersectionsubtype, :source,
                ST_SetSRID(ST_GeomFromText(:intersection_geom), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for _, row in intersections_df.iterrows():
                # 处理整数字段  
                def safe_int(value):
                    if pd.isna(value) or value is None or str(value).strip() == '':
                        return None
                    try:
                        return int(value)
                    except (ValueError, TypeError):
                        return None
                
                # 处理字符串字段
                def safe_str(value):
                    if pd.isna(value) or value is None:
                        return ''
                    return str(value)
                
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'polygon_id': row['polygon_id'],
                    'intersection_id': int(row['id']),  # 使用原始id字段
                    'cityid': safe_str(row.get('cityid', '')),
                    'patchid': safe_str(row.get('patchid', '')),
                    'patchversion': safe_str(row.get('patchversion', '')),
                    'releaseversion': safe_str(row.get('releaseversion', '')),
                    'citypatchversion': safe_str(row.get('citypatchversion', '')),
                    'intersectiontype': safe_int(row.get('intersectiontype')),
                    'intersectionsubtype': safe_int(row.get('intersectionsubtype')),
                    'source': safe_int(row.get('source')),
                    'intersection_geom': row.get('intersection_geom', '')
                })
            
            conn.commit()
        
        logger.info(f"保存intersections结果: {len(intersections_df)} 条记录")
    

    
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
             total_intersections, processing_time_seconds)
            VALUES (
                :analysis_id, :batch_analysis_id, :total_polygons, :total_roads,
                :total_intersections, :processing_time_seconds
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