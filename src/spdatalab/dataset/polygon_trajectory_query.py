"""高性能Polygon轨迹查询模块

基于spatial_join_production.py的优化策略：
- 小规模（≤50个polygon）：批量查询（UNION ALL）- 最快
- 大规模（>50个polygon）：分块批量查询 - 最稳定
- 高效数据库写入和轨迹构建

功能：
1. 读取GeoJSON文件中的polygon（支持多个）
2. 高效批量查询与polygon相交的轨迹点
3. 智能分组构建轨迹线和统计信息
4. 批量写入数据库，优化性能
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import warnings

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, LineString, Point
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

# 抑制警告
warnings.filterwarnings('ignore', category=UserWarning)

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 日志配置
logger = logging.getLogger(__name__)

@dataclass
class PolygonTrajectoryConfig:
    """Polygon轨迹查询配置"""
    local_dsn: str = LOCAL_DSN
    point_table: str = POINT_TABLE
    
    # 批量查询优化配置
    batch_threshold: int = 50          # 批量查询vs分块查询的阈值
    chunk_size: int = 20               # 分块大小
    limit_per_polygon: int = 10000     # 每个polygon的轨迹点限制
    batch_insert_size: int = 1000      # 批量插入大小
    
    # 查询优化配置
    enable_spatial_index: bool = True  # 启用空间索引优化
    query_timeout: int = 300           # 查询超时时间（秒）
    
    # 轨迹构建配置
    min_points_per_trajectory: int = 2 # 构建轨迹的最小点数
    enable_speed_stats: bool = True    # 启用速度统计
    enable_avp_stats: bool = True      # 启用AVP统计
    
    # 完整轨迹获取配置
    fetch_complete_trajectories: bool = True  # 是否获取完整轨迹（而非仅多边形内的片段）

def load_polygons_from_geojson(file_path: str) -> List[Dict]:
    """从GeoJSON文件加载polygon
    
    Args:
        file_path: GeoJSON文件路径
        
    Returns:
        polygon列表，每个包含geometry和properties
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        polygons = []
        
        if geojson_data.get('type') == 'FeatureCollection':
            # FeatureCollection格式
            for i, feature in enumerate(geojson_data.get('features', [])):
                if feature.get('geometry', {}).get('type') in ['Polygon', 'MultiPolygon']:
                    polygon_info = {
                        'id': feature.get('properties', {}).get('id', f'polygon_{i}'),
                        'geometry': shape(feature['geometry']),
                        'properties': feature.get('properties', {})
                    }
                    polygons.append(polygon_info)
        elif geojson_data.get('type') in ['Polygon', 'MultiPolygon']:
            # 单个几何对象
            polygon_info = {
                'id': 'polygon_0',
                'geometry': shape(geojson_data),
                'properties': {}
            }
            polygons.append(polygon_info)
        else:
            logger.error(f"不支持的GeoJSON格式: {geojson_data.get('type')}")
            return []
        
        logger.info(f"成功加载 {len(polygons)} 个polygon")
        for polygon in polygons:
            logger.debug(f"Polygon {polygon['id']}: {polygon['geometry'].geom_type}")
        
        return polygons
        
    except Exception as e:
        logger.error(f"加载GeoJSON文件失败: {file_path}, 错误: {str(e)}")
        raise

class HighPerformancePolygonTrajectoryQuery:
    """高性能Polygon轨迹查询器"""
    
    def __init__(self, config: Optional[PolygonTrajectoryConfig] = None):
        self.config = config or PolygonTrajectoryConfig()
        self.engine = create_engine(
            self.config.local_dsn, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        # 调试信息：显示类的可用方法
        logger.debug(f"🔧 HighPerformancePolygonTrajectoryQuery 初始化完成")
        logger.debug(f"🔧 可用方法: {[method for method in dir(self) if not method.startswith('_')]}")
        logger.debug(f"🔧 process_complete_workflow 方法存在: {hasattr(self, 'process_complete_workflow')}")
    
    def query_intersecting_trajectory_points(self, polygons: List[Dict]) -> Tuple[pd.DataFrame, Dict]:
        """高效批量查询与polygon相交的轨迹点
        
        Args:
            polygons: polygon列表
            
        Returns:
            (轨迹点DataFrame, 性能统计)
        """
        start_time = time.time()
        
        # 性能统计
        stats = {
            'polygon_count': len(polygons),
            'strategy': None,
            'chunk_size': None,
            'query_time': 0,
            'total_points': 0,
            'unique_datasets': 0,
            'points_per_polygon': 0
        }
        
        if not polygons:
            logger.warning("没有polygon数据")
            return pd.DataFrame(), stats
        
        logger.info(f"开始批量查询 {len(polygons)} 个polygon的轨迹点")
        
        # 选择最优查询策略
        if len(polygons) <= self.config.batch_threshold:
            stats['strategy'] = 'batch_query'
            result_df = self._batch_query_strategy(polygons)
        else:
            stats['strategy'] = 'chunked_query'
            stats['chunk_size'] = self.config.chunk_size
            result_df = self._chunked_query_strategy(polygons)
        
        # 计算统计信息
        stats['query_time'] = time.time() - start_time
        stats['total_points'] = len(result_df)
        
        if not result_df.empty:
            stats['unique_datasets'] = result_df['dataset_name'].nunique()
            stats['points_per_polygon'] = stats['total_points'] / stats['polygon_count']
            
            # 如果启用完整轨迹获取，则获取完整轨迹数据
            if self.config.fetch_complete_trajectories:
                logger.info(f"🔄 获取完整轨迹数据...")
                complete_result_df, complete_stats = self._fetch_complete_trajectories(result_df)
                
                if not complete_result_df.empty:
                    result_df = complete_result_df
                    stats.update(complete_stats)
                    stats['total_points'] = len(result_df)
                    stats['complete_trajectories_fetched'] = True
                    logger.info(f"✅ 完整轨迹获取完成: {stats['total_points']} 个轨迹点")
                else:
                    stats['complete_trajectories_fetched'] = False
                    logger.warning("⚠️ 完整轨迹获取失败，返回原始数据")
            
            logger.info(f"✅ 查询完成: {stats['total_points']} 个轨迹点, "
                       f"{stats['unique_datasets']} 个数据集, "
                       f"策略: {stats['strategy']}, "
                       f"用时: {stats['query_time']:.2f}s")
        else:
            logger.warning("未找到任何相交的轨迹点")
        
        return result_df, stats
    
    def _fetch_complete_trajectories(self, intersection_result_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """获取完整轨迹数据（基于相交结果中的data_name）
        
        Args:
            intersection_result_df: 多边形相交结果DataFrame
            
        Returns:
            (完整轨迹DataFrame, 统计信息)
        """
        start_time = time.time()
        
        # 统计信息
        complete_stats = {
            'complete_query_time': 0,
            'original_datasets': 0,
            'original_points': 0,
            'complete_datasets': 0,
            'complete_points': 0
        }
        
        if intersection_result_df.empty:
            logger.warning("相交结果为空，无法获取完整轨迹")
            return pd.DataFrame(), complete_stats
        
        # 获取所有涉及的data_name
        unique_data_names = intersection_result_df['dataset_name'].unique()
        complete_stats['original_datasets'] = len(unique_data_names)
        complete_stats['original_points'] = len(intersection_result_df)
        
        logger.info(f"📋 需要获取完整轨迹的数据集: {len(unique_data_names)} 个")
        
        try:
            # 构建查询所有完整轨迹的SQL
            data_names_tuple = tuple(unique_data_names)
            
            complete_trajectory_sql = f"""
                SELECT 
                    dataset_name,
                    timestamp,
                    point_lla,
                    twist_linear,
                    avp_flag,
                    workstage,
                    ST_X(point_lla) as longitude,
                    ST_Y(point_lla) as latitude
                FROM {self.config.point_table}
                WHERE dataset_name IN %(data_names)s
                AND point_lla IS NOT NULL
                AND timestamp IS NOT NULL
                ORDER BY dataset_name, timestamp
            """
            
            logger.info(f"🚀 执行完整轨迹查询...")
            
            with hive_cursor("dataset_gy1") as cur:
                cur.execute(complete_trajectory_sql, {"data_names": data_names_tuple})
                
                # 获取列名和数据
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                
                if rows:
                    complete_df = pd.DataFrame(rows, columns=cols)
                    
                    # 为完整轨迹数据添加polygon_id信息
                    # 基于原始相交结果创建data_name到polygon_id的映射
                    dataset_polygon_mapping = {}
                    for _, row in intersection_result_df.iterrows():
                        dataset_name = row['dataset_name']
                        polygon_id = row.get('polygon_id', 'unknown')
                        
                        if dataset_name not in dataset_polygon_mapping:
                            dataset_polygon_mapping[dataset_name] = []
                        if polygon_id not in dataset_polygon_mapping[dataset_name]:
                            dataset_polygon_mapping[dataset_name].append(polygon_id)
                    
                    # 为完整轨迹添加polygon_id信息
                    complete_df['polygon_id'] = complete_df['dataset_name'].map(
                        lambda x: dataset_polygon_mapping.get(x, ['unknown'])[0]
                    )
                    
                    complete_stats['complete_datasets'] = complete_df['dataset_name'].nunique()
                    complete_stats['complete_points'] = len(complete_df)
                    complete_stats['complete_query_time'] = time.time() - start_time
                    
                    logger.info(f"✅ 完整轨迹查询成功: {len(complete_df)} 个点, "
                               f"{complete_df['dataset_name'].nunique()} 个数据集, "
                               f"用时: {complete_stats['complete_query_time']:.2f}s")
                    
                    return complete_df, complete_stats
                else:
                    logger.warning("完整轨迹查询无结果")
                    return pd.DataFrame(), complete_stats
                    
        except Exception as e:
            logger.error(f"获取完整轨迹失败: {str(e)}")
            return pd.DataFrame(), complete_stats

    def _fetch_scene_ids_from_data_names(self, data_names: List[str]) -> pd.DataFrame:
        """根据data_name批量查询对应的scene_id、event_id、event_name（渐进式查询）
        
        采用两阶段查询策略：
        1. 主查询：直接通过origin_name查询
        2. 备选查询：通过data_name->defect_id->origin_source_id查询
        
        Args:
            data_names: 数据名称列表
            
        Returns:
            包含data_name、scene_id、event_id、event_name映射的DataFrame
        """
        if not data_names:
            return pd.DataFrame()
        
        # 第一阶段：主查询（直接通过origin_name查询）
        primary_result_df = self._primary_query_by_origin_name(data_names)
        
        # 检查哪些data_name没有查到
        found_data_names = set(primary_result_df['data_name'].tolist()) if not primary_result_df.empty else set()
        missing_data_names = [name for name in data_names if name not in found_data_names]
        
        logger.info(f"主查询成功: {len(found_data_names)}/{len(data_names)}, 需要备选查询: {len(missing_data_names)}")
        
        # 第二阶段：备选查询（通过defect_id查询）
        if missing_data_names:
            logger.info(f"开始备选查询，处理 {len(missing_data_names)} 个缺失的data_name")
            fallback_result_df = self._fallback_query_by_defect_id(missing_data_names)
            
            # 合并主查询和备选查询的结果
            if not fallback_result_df.empty:
                result_df = pd.concat([primary_result_df, fallback_result_df], ignore_index=True)
                logger.info(f"备选查询成功: {len(fallback_result_df)} 个，总计: {len(result_df)} 个")
            else:
                result_df = primary_result_df
                logger.warning("备选查询未找到任何结果")
        else:
            result_df = primary_result_df
            logger.info("主查询已满足所有需求，无需备选查询")
        
        return result_df
    
    def _primary_query_by_origin_name(self, data_names: List[str]) -> pd.DataFrame:
        """主查询：直接通过origin_name查询scene_id、event_id、event_name"""
        try:
            sql = """
                SELECT origin_name AS data_name, 
                       id AS scene_id,
                       event_id,
                       event_name
                FROM (
                    SELECT origin_name, 
                           id, 
                           event_id,
                           event_name,
                           ROW_NUMBER() OVER (PARTITION BY origin_name ORDER BY updated_at DESC) as rn
                    FROM transform.ods_t_data_fragment_datalake 
                    WHERE origin_name IN %(tok)s
                ) ranked
                WHERE rn = 1
            """
            
            with hive_cursor() as cur:
                cur.execute(sql, {"tok": tuple(data_names)})
                cols = [d[0] for d in cur.description]
                result_df = pd.DataFrame(cur.fetchall(), columns=cols)
                
            logger.debug(f"主查询完成: {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            logger.error(f"主查询失败: {str(e)}")
            return pd.DataFrame()
    
    def _fallback_query_by_defect_id(self, missing_data_names: List[str]) -> pd.DataFrame:
        """备选查询：通过data_name->defect_id->origin_source_id查询"""
        try:
            # 第一步：通过data_name查询defect_id
            defect_mapping = self._query_defect_ids(missing_data_names)
            
            if defect_mapping.empty:
                logger.warning("未查询到任何defect_id")
                return pd.DataFrame()
            
            # 第二步：通过defect_id查询scene_id、event_id、event_name
            defect_ids = defect_mapping['defect_id'].tolist()
            result_df = self._query_by_origin_source_id(defect_ids, defect_mapping)
            
            logger.debug(f"备选查询完成: {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            logger.error(f"备选查询失败: {str(e)}")
            return pd.DataFrame()
    
    def _query_defect_ids(self, data_names: List[str]) -> pd.DataFrame:
        """查询data_name对应的defect_id"""
        try:
            sql = "SELECT id AS data_name, defect_id FROM elasticsearch_ros.ods_ddi_index002_datalake WHERE id IN %(tok)s"
            
            with hive_cursor() as cur:
                cur.execute(sql, {"tok": tuple(data_names)})
                cols = [d[0] for d in cur.description]
                result_df = pd.DataFrame(cur.fetchall(), columns=cols)
                
            logger.debug(f"defect_id查询: {len(result_df)}/{len(data_names)} 成功")
            return result_df
            
        except Exception as e:
            logger.error(f"查询defect_id失败: {str(e)}")
            return pd.DataFrame()
    
    def _query_by_origin_source_id(self, defect_ids: List[str], defect_mapping: pd.DataFrame) -> pd.DataFrame:
        """通过origin_source_id查询scene_id、event_id、event_name"""
        try:
            sql = """
                SELECT origin_source_id AS defect_id,
                       id AS scene_id,
                       event_id,
                       event_name
                FROM (
                    SELECT origin_source_id, 
                           id, 
                           event_id,
                           event_name,
                           ROW_NUMBER() OVER (PARTITION BY origin_source_id ORDER BY updated_at DESC) as rn
                    FROM transform.ods_t_data_fragment_datalake 
                    WHERE origin_source_id IN %(tok)s
                ) ranked
                WHERE rn = 1
            """
            
            with hive_cursor() as cur:
                cur.execute(sql, {"tok": tuple(defect_ids)})
                cols = [d[0] for d in cur.description]
                result_df = pd.DataFrame(cur.fetchall(), columns=cols)
            
            # 合并defect_mapping和查询结果，获得data_name
            final_result = pd.merge(
                defect_mapping, 
                result_df, 
                on='defect_id', 
                how='inner'
            ).drop('defect_id', axis=1)  # 删除中间字段defect_id
            
            logger.debug(f"origin_source_id查询: {len(final_result)} 条最终记录")
            return final_result
            
        except Exception as e:
            logger.error(f"通过origin_source_id查询失败: {str(e)}")
            return pd.DataFrame()

    def _batch_query_strategy(self, polygons: List[Dict]) -> pd.DataFrame:
        """批量查询策略 - 使用hive_cursor连接（性能优化版）"""
        logger.info(f"🔍 使用批量查询策略处理 {len(polygons)} 个polygon")
        logger.info(f"⚡ 每polygon点数限制: {self.config.limit_per_polygon:,}")
        
        # 先测试数据库连接
        logger.info("🔗 测试数据库连接...")
        try:
            with hive_cursor("dataset_gy1") as cur:
                cur.execute("SELECT 1 as test_connection")
                result = cur.fetchone()
                if result and result[0] == 1:
                    logger.info("✅ 数据库连接正常")
                else:
                    logger.error("❌ 数据库连接测试失败")
                    return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return pd.DataFrame()
        
        # 性能优化：检查polygon数量
        if len(polygons) > 10:
            logger.info(f"⚠️ polygon数量较多({len(polygons)})，切换到分块策略")
            return self._chunked_query_strategy(polygons)
        
        # 构建优化的查询
        subqueries = []
        total_estimated_points = len(polygons) * self.config.limit_per_polygon
        
        logger.info(f"📈 预估最大数据量: {total_estimated_points:,} 个点")
        
        for i, polygon in enumerate(polygons, 1):
            polygon_id = polygon['id']
            polygon_wkt = polygon['geometry'].wkt
            
            logger.info(f"🔸 构建查询 {i}/{len(polygons)}: {polygon_id}")
            
            # 去掉ORDER BY，简化子查询
            subquery = f"""
                (SELECT 
                    dataset_name,
                    timestamp,
                    point_lla,
                    twist_linear,
                    avp_flag,
                    workstage,
                    ST_X(point_lla) as longitude,
                    ST_Y(point_lla) as latitude,
                    '{polygon_id}' as polygon_id
                FROM {self.config.point_table}
                WHERE point_lla IS NOT NULL
                AND timestamp IS NOT NULL
                AND dataset_name IS NOT NULL
                AND ST_Intersects(
                    point_lla,
                    ST_SetSRID(ST_GeomFromText('{polygon_wkt}'), 4326)
                )
                LIMIT {self.config.limit_per_polygon})
            """
            subqueries.append(subquery)
        
        # 构建完整的UNION查询
        union_query = " UNION ALL ".join(subqueries)
        batch_sql = f"""
            SELECT * FROM (
                {union_query}
            ) AS combined_results
            ORDER BY dataset_name, timestamp
        """
        
        logger.info("🚀 开始执行批量SQL查询（使用hive_cursor）...")
        logger.debug(f"🔍 SQL查询（前300字符）: {batch_sql[:300]}...")
        
        start_time = time.time()
        
        try:
            with hive_cursor("dataset_gy1") as cur:
                logger.info("📊 正在执行查询，请耐心等待...")
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("=== 执行批量查询SQL (dataset_gy1) ===")
                    logger.debug(batch_sql)
                
                # 执行查询
                cur.execute(batch_sql)
                rows = cur.fetchall()
                
                query_time = time.time() - start_time
                logger.info(f"✅ 查询完成！用时: {query_time:.2f}s, 获得 {len(rows):,} 个数据点")
                
                if not rows:
                    logger.warning("⚠️ 未找到相交的轨迹点")
                    return pd.DataFrame()
                
                # 构建DataFrame
                columns = ['dataset_name', 'timestamp', 'point_lla', 'twist_linear', 
                          'avp_flag', 'workstage', 'longitude', 'latitude', 'polygon_id']
                result_df = pd.DataFrame(rows, columns=columns)
                
                logger.info(f"📊 构建DataFrame完成: {len(result_df)} 行数据")
                return result_df
                
        except Exception as sql_error:
            query_time = time.time() - start_time
            logger.error(f"❌ SQL执行失败 (用时: {query_time:.2f}s): {sql_error}")
            
            if "timeout" in str(sql_error).lower() or "cancelled" in str(sql_error).lower():
                logger.error(f"⏰ 查询超时或被取消！建议：")
                logger.error(f"   1. 减少 limit_per_polygon (当前: {self.config.limit_per_polygon})")
                logger.error(f"   2. 缩小polygon范围或减少polygon数量")
                logger.error(f"   3. 使用分块查询策略")
            
            raise
        
        return pd.DataFrame()
    
    def _chunked_query_strategy(self, polygons: List[Dict]) -> pd.DataFrame:
        """分块查询策略 - 适合大规模polygon"""
        logger.info(f"使用分块查询策略，{len(polygons)} 个polygon分为 {len(polygons)//self.config.chunk_size + 1} 块")
        
        all_results = []
        
        for i in range(0, len(polygons), self.config.chunk_size):
            chunk = polygons[i:i+self.config.chunk_size]
            chunk_num = i // self.config.chunk_size + 1
            logger.info(f"处理第 {chunk_num} 块: {len(chunk)} 个polygon")
            
            # 使用批量策略处理当前块
            chunk_result = self._batch_query_strategy(chunk)
            if not chunk_result.empty:
                all_results.append(chunk_result)
        
        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

    def build_trajectories_from_points(self, points_df: pd.DataFrame) -> Tuple[List[Dict], Dict]:
        """智能构建轨迹线和统计信息
        
        Args:
            points_df: 轨迹点DataFrame
            
        Returns:
            (轨迹列表, 构建统计)
        """
        start_time = time.time()
        
        build_stats = {
            'total_points': len(points_df),
            'total_datasets': 0,
            'valid_trajectories': 0,
            'skipped_trajectories': 0,
            'build_time': 0
        }
        
        if points_df.empty:
            logger.warning("没有轨迹点数据")
            return [], build_stats
        
        trajectories = []
        
        try:
            # 获取所有涉及的data_name，并查询对应的scene_id
            unique_data_names = points_df['dataset_name'].unique()
            logger.info(f"查询 {len(unique_data_names)} 个data_name对应的scene_id...")
            
            # 查询scene_id映射
            scene_id_mappings = self._fetch_scene_ids_from_data_names(unique_data_names.tolist())
            
            # 创建data_name到各字段的映射字典
            data_name_to_scene_id = {}
            data_name_to_event_id = {}
            data_name_to_event_name = {}
            
            if not scene_id_mappings.empty:
                data_name_to_scene_id = dict(zip(scene_id_mappings['data_name'], scene_id_mappings['scene_id']))
                
                # 处理event_id字段（可能为空）
                if 'event_id' in scene_id_mappings.columns:
                    # 确保event_id是整数类型，避免浮点数格式问题
                    event_ids_cleaned = scene_id_mappings['event_id'].apply(
                        lambda x: int(float(x)) if pd.notna(x) and x != '' else None
                    )
                    data_name_to_event_id = dict(zip(scene_id_mappings['data_name'], event_ids_cleaned))
                
                # 处理event_name字段（可能为空）
                if 'event_name' in scene_id_mappings.columns:
                    data_name_to_event_name = dict(zip(scene_id_mappings['data_name'], scene_id_mappings['event_name']))
                
                logger.info(f"成功查询到 {len(data_name_to_scene_id)} 个scene_id映射")
                if data_name_to_event_id:
                    logger.info(f"成功查询到 {len(data_name_to_event_id)} 个event_id映射")
                if data_name_to_event_name:
                    logger.info(f"成功查询到 {len(data_name_to_event_name)} 个event_name映射")
            else:
                logger.warning("未查询到任何scene_id映射，相关字段将为空")
            
            # 按dataset_name分组处理
            grouped = points_df.groupby('dataset_name')
            build_stats['total_datasets'] = len(grouped)
            
            logger.info(f"开始构建轨迹: {build_stats['total_datasets']} 个数据集, {build_stats['total_points']} 个点")
            
            # 移除event_id_counter，改为使用数据库查询的值
            
            for dataset_name, group in grouped:
                # 按时间排序
                group = group.sort_values('timestamp')
                
                # 检查点数量
                if len(group) < self.config.min_points_per_trajectory:
                    build_stats['skipped_trajectories'] += 1
                    logger.debug(f"数据集 {dataset_name} 点数量不足({len(group)})，跳过")
                    continue
                
                # 提取坐标
                coordinates = list(zip(group['longitude'], group['latitude']))
                
                # 构建LineString几何
                trajectory_geom = LineString(coordinates)
                
                # 基础统计信息
                stats = {
                    'dataset_name': dataset_name,
                    'scene_id': data_name_to_scene_id.get(dataset_name, ''),  # 从数据库查询获取scene_id
                    'event_id': data_name_to_event_id.get(dataset_name, None),  # 从数据库查询获取event_id
                    'event_name': data_name_to_event_name.get(dataset_name, ''),  # 从数据库查询获取event_name
                    'start_time': int(group['timestamp'].min()),
                    'end_time': int(group['timestamp'].max()),
                    'duration': int(group['timestamp'].max() - group['timestamp'].min()),
                    'point_count': len(group),
                    'geometry': trajectory_geom,
                    'polygon_ids': list(group['polygon_id'].unique())
                }
                
                # 移除event_id_counter递增，因为现在使用数据库值
                
                # 速度统计（可配置）
                if self.config.enable_speed_stats and 'twist_linear' in group.columns:
                    speed_data = group['twist_linear'].dropna()
                    if len(speed_data) > 0:
                        stats.update({
                            'avg_speed': round(float(speed_data.mean()), 2),
                            'max_speed': round(float(speed_data.max()), 2),
                            'min_speed': round(float(speed_data.min()), 2),
                            'std_speed': round(float(speed_data.std()) if len(speed_data) > 1 else 0.0, 2)
                        })
                
                # AVP统计（可配置）
                if self.config.enable_avp_stats and 'avp_flag' in group.columns:
                    avp_data = group['avp_flag'].dropna()
                    if len(avp_data) > 0:
                        stats.update({
                            'avp_ratio': round(float((avp_data == 1).mean()), 3)
                        })
                
                trajectories.append(stats)
                build_stats['valid_trajectories'] += 1
                
                logger.debug(f"构建轨迹: {dataset_name}, 点数: {stats['point_count']}, "
                           f"时长: {stats['duration']}s, polygon数: {len(stats['polygon_ids'])}")
            
            build_stats['build_time'] = time.time() - start_time
            
            logger.info(f"✅ 轨迹构建完成: {build_stats['valid_trajectories']} 条有效轨迹, "
                       f"{build_stats['skipped_trajectories']} 条跳过, "
                       f"用时: {build_stats['build_time']:.2f}s")
            
            return trajectories, build_stats
            
        except Exception as e:
            logger.error(f"构建轨迹失败: {str(e)}")
            return [], build_stats



    def save_trajectories_to_table(self, trajectories: List[Dict], table_name: str) -> Tuple[int, Dict]:
        """高效批量保存轨迹数据到数据库表
        
        Args:
            trajectories: 轨迹数据列表
            table_name: 目标表名
            
        Returns:
            (保存成功的记录数, 保存统计)
        """
        start_time = time.time()
        
        save_stats = {
            'total_trajectories': len(trajectories),
            'saved_records': 0,
            'save_time': 0,
            'table_created': False,
            'batch_count': 0
        }
        
        if not trajectories:
            logger.warning("没有轨迹数据需要保存")
            return 0, save_stats
        
        try:
            # 创建表
            if not self._create_trajectory_table(table_name):
                logger.error("创建表失败")
                return 0, save_stats
            
            save_stats['table_created'] = True
            
            # 批量插入
            total_saved = 0
            for i in range(0, len(trajectories), self.config.batch_insert_size):
                batch = trajectories[i:i+self.config.batch_insert_size]
                batch_num = i // self.config.batch_insert_size + 1
                
                logger.info(f"保存第 {batch_num} 批: {len(batch)} 条轨迹")
                
                # 准备GeoDataFrame数据
                gdf_data = []
                geometries = []
                
                for traj in batch:
                    # 分离几何和属性数据
                    row = {k: v for k, v in traj.items() if k != 'geometry'}
                    gdf_data.append(row)
                    geometries.append(traj['geometry'])
                
                # 创建GeoDataFrame
                gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
                
                # 强制转换event_id为整数类型，避免浮点数格式问题
                if 'event_id' in gdf.columns:
                    # 处理pandas将整数转换为浮点数的问题
                    valid_mask = gdf['event_id'].notna()
                    new_event_ids = pd.Series([None] * len(gdf), dtype=object)
                    
                    if valid_mask.any():
                        valid_values = gdf.loc[valid_mask, 'event_id']
                        converted_values = valid_values.apply(lambda x: int(x))
                        new_event_ids.loc[valid_mask] = converted_values
                    
                    gdf['event_id'] = new_event_ids
                
                # 转换PostgreSQL数组格式
                if 'polygon_ids' in gdf.columns:
                    # 将Python列表转换为PostgreSQL数组格式 {item1,item2}
                    gdf['polygon_ids'] = gdf['polygon_ids'].apply(
                        lambda x: '{' + ','.join(str(item) for item in x) + '}' if isinstance(x, list) else x
                    )
                
                # 批量插入到数据库
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False
                )
                
                total_saved += len(gdf)
                save_stats['batch_count'] += 1
                
                logger.debug(f"批次 {batch_num} 保存完成: {len(gdf)} 条记录")
            
            save_stats['saved_records'] = total_saved
            save_stats['save_time'] = time.time() - start_time
            
            logger.info(f"✅ 数据库保存完成: {save_stats['saved_records']} 条轨迹记录, "
                       f"{save_stats['batch_count']} 个批次, "
                       f"表: {table_name}, "
                       f"用时: {save_stats['save_time']:.2f}s")
            
            return total_saved, save_stats
            
        except Exception as e:
            logger.error(f"保存轨迹数据失败: {str(e)}")
            return 0, save_stats
    
    def _create_trajectory_table(self, table_name: str) -> bool:
        """创建轨迹结果表（事务修复版）"""
        try:
            # 检查表是否已存在
            check_table_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(check_table_sql)
                table_exists = result.scalar()
                
                if table_exists:
                    logger.info(f"表 {table_name} 已存在，跳过创建")
                    return True
            
            logger.info(f"创建高性能轨迹表: {table_name}")
            
            # 创建表结构（分步执行避免事务冲突）
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    dataset_name text NOT NULL,
                    scene_id text,
                    event_id integer,
                    event_name varchar(765),
                    start_time bigint,
                    end_time bigint,
                    duration bigint,
                    point_count integer,
                    avg_speed numeric(8,2),
                    max_speed numeric(8,2),
                    min_speed numeric(8,2),
                    std_speed numeric(8,2),
                    avp_ratio numeric(5,3),
                    polygon_ids text[],
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 添加几何列
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2);
            """)
            
            # 创建优化索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_dataset_name ON {table_name}(dataset_name);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_event_id ON {table_name}(event_id);
                CREATE INDEX idx_{table_name}_event_name ON {table_name}(event_name);
                CREATE INDEX idx_{table_name}_start_time ON {table_name}(start_time);
                CREATE INDEX idx_{table_name}_point_count ON {table_name}(point_count);
                CREATE INDEX idx_{table_name}_polygon_ids ON {table_name} USING GIN(polygon_ids);
                CREATE INDEX idx_{table_name}_created_at ON {table_name}(created_at);
            """)
            
            # 分步执行SQL（避免事务冲突）
            try:
                # 步骤1：创建表
                with self.engine.connect() as conn:
                    conn.execute(create_sql)
                    conn.commit()
                logger.debug("✅ 表结构创建完成")
                
                # 步骤2：添加几何列
                with self.engine.connect() as conn:
                    conn.execute(add_geom_sql)
                    conn.commit()
                logger.debug("✅ 几何列添加完成")
                
                # 步骤3：创建索引
                with self.engine.connect() as conn:
                    conn.execute(index_sql)
                    conn.commit()
                logger.debug("✅ 索引创建完成")
                
                logger.info(f"✅ 轨迹表创建成功: {table_name}")
                return True
                
            except Exception as e:
                logger.error(f"SQL执行失败: {str(e)}")
                # 尝试清理部分创建的表
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                    logger.info(f"已清理部分创建的表: {table_name}")
                except:
                    pass
                raise e
                
        except Exception as e:
            logger.error(f"创建轨迹表失败: {table_name}, 错误: {str(e)}")
            return False

    def process_complete_workflow(
        self,
        geojson_file: str,
        output_table: Optional[str] = None,
        output_geojson: Optional[str] = None
    ) -> Dict:
        """执行完整的高性能polygon轨迹查询工作流
        
        Args:
            geojson_file: 输入的GeoJSON文件路径
            output_table: 输出数据库表名（可选）
            output_geojson: 输出GeoJSON文件路径（可选）
            
        Returns:
            详细的处理统计信息
        """
        logger.debug("🔧 DEBUG: process_complete_workflow 方法被调用")
        logger.debug(f"🔧 DEBUG: 实例类型: {type(self)}")
        logger.debug(f"🔧 DEBUG: 方法存在性: {hasattr(self, 'process_complete_workflow')}")
        
        workflow_start = time.time()
        
        # 综合统计信息
        complete_stats = {
            'start_time': datetime.now(),
            'geojson_file': geojson_file,
            'output_table': output_table,
            'output_geojson': output_geojson,
            'config': {
                'batch_threshold': self.config.batch_threshold,
                'chunk_size': self.config.chunk_size,
                'limit_per_polygon': self.config.limit_per_polygon,
                'batch_insert_size': self.config.batch_insert_size
            }
        }
        
        try:
            # 阶段1: 加载polygon
            logger.info("=" * 60)
            logger.info("🚀 开始高性能Polygon轨迹查询工作流")
            logger.info("=" * 60)
            
            logger.info(f"📁 阶段1: 加载GeoJSON文件: {geojson_file}")
            polygons = load_polygons_from_geojson(geojson_file)
            complete_stats['polygon_count'] = len(polygons)
            
            if not polygons:
                logger.error("❌ 未加载到任何polygon")
                complete_stats['error'] = "No polygons loaded"
                return complete_stats
            
            logger.info(f"✅ 成功加载 {len(polygons)} 个polygon")
            
            # 阶段2: 高效查询轨迹点
            logger.info(f"🔍 阶段2: 执行高性能轨迹点查询")
            points_df, query_stats = self.query_intersecting_trajectory_points(polygons)
            complete_stats['query_stats'] = query_stats
            
            if points_df.empty:
                logger.warning("⚠️ 未查询到任何轨迹点")
                complete_stats['warning'] = "No trajectory points found"
                return complete_stats
            
            logger.info(f"✅ 查询到 {len(points_df)} 个轨迹点")
            
            # 阶段3: 构建轨迹
            logger.info(f"🔧 阶段3: 构建轨迹线和统计信息")
            trajectories, build_stats = self.build_trajectories_from_points(points_df)
            complete_stats['build_stats'] = build_stats
            
            if not trajectories:
                logger.warning("⚠️ 未构建到任何轨迹")
                complete_stats['warning'] = "No trajectories built"
                return complete_stats
            
            logger.info(f"✅ 构建了 {len(trajectories)} 条轨迹")
            
            # 阶段4: 保存到数据库（可选）
            if output_table:
                logger.info(f"💾 阶段4: 保存到数据库表: {output_table}")
                inserted_count, save_stats = self.save_trajectories_to_table(trajectories, output_table)
                complete_stats['save_stats'] = save_stats
                logger.info(f"✅ 成功保存 {inserted_count} 条轨迹到数据库")
            
            # 阶段5: 导出到GeoJSON（可选）
            if output_geojson:
                logger.info(f"📄 阶段5: 导出到GeoJSON文件: {output_geojson}")
                if export_trajectories_to_geojson(trajectories, output_geojson):
                    complete_stats['geojson_exported'] = True
                    logger.info(f"✅ 成功导出轨迹到GeoJSON文件")
                else:
                    complete_stats['geojson_export_failed'] = True
                    logger.warning("⚠️ GeoJSON导出失败")
            
            # 最终统计
            complete_stats['total_trajectories'] = len(trajectories)
            complete_stats['workflow_duration'] = time.time() - workflow_start
            complete_stats['end_time'] = datetime.now()
            complete_stats['success'] = True
            
            logger.info("=" * 60)
            logger.info("🎉 高性能Polygon轨迹查询工作流完成!")
            logger.info(f"⏱️ 总耗时: {complete_stats['workflow_duration']:.2f}s")
            logger.info(f"📊 输出轨迹: {complete_stats['total_trajectories']} 条")
            logger.info("=" * 60)
            
            return complete_stats
            
        except Exception as e:
            complete_stats['error'] = str(e)
            complete_stats['workflow_duration'] = time.time() - workflow_start
            complete_stats['end_time'] = datetime.now()
            complete_stats['success'] = False
            logger.error(f"❌ 工作流执行失败: {str(e)}")
            return complete_stats

def export_trajectories_to_geojson(trajectories: List[Dict], output_file: str) -> bool:
    """导出轨迹数据到GeoJSON文件
    
    Args:
        trajectories: 轨迹数据列表
        output_file: 输出文件路径
        
    Returns:
        导出是否成功
    """
    if not trajectories:
        logger.warning("没有轨迹数据需要导出")
        return False
    
    try:
        # 准备GeoDataFrame数据
        gdf_data = []
        geometries = []
        
        for traj in trajectories:
            # 分离几何和属性数据
            row = {k: v for k, v in traj.items() if k != 'geometry'}
            # 转换polygon_ids为字符串
            if 'polygon_ids' in row:
                row['polygon_ids'] = ','.join(row['polygon_ids'])
            gdf_data.append(row)
            geometries.append(traj['geometry'])
        
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
        
        # 导出到GeoJSON
        gdf.to_file(output_file, driver='GeoJSON', encoding='utf-8')
        
        logger.info(f"成功导出 {len(gdf)} 条轨迹到文件: {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"导出轨迹数据失败: {str(e)}")
        return False

# 便捷函数（保持向后兼容）
def process_polygon_trajectory_query(
    geojson_file: str,
    output_table: Optional[str] = None,
    output_geojson: Optional[str] = None,
    config: Optional[PolygonTrajectoryConfig] = None
) -> Dict:
    """高性能polygon轨迹查询完整流程
    
    Args:
        geojson_file: 输入的GeoJSON文件路径
        output_table: 输出数据库表名（可选）
        output_geojson: 输出GeoJSON文件路径（可选）
        config: 自定义配置（可选）
        
    Returns:
        详细的处理统计信息
    """
    # 使用高性能查询器
    query_config = config or PolygonTrajectoryConfig()
    processor = HighPerformancePolygonTrajectoryQuery(query_config)
    
    # 调试信息：验证实例和方法
    logger.debug(f"🔧 DEBUG: 创建的处理器类型: {type(processor)}")
    logger.debug(f"🔧 DEBUG: 处理器可用方法: {[method for method in dir(processor) if not method.startswith('_')]}")
    logger.debug(f"🔧 DEBUG: process_complete_workflow 方法是否存在: {hasattr(processor, 'process_complete_workflow')}")
    
    if not hasattr(processor, 'process_complete_workflow'):
        logger.error("❌ CRITICAL: process_complete_workflow 方法不存在!")
        logger.error(f"❌ 可用方法: {[method for method in dir(processor) if callable(getattr(processor, method)) and not method.startswith('_')]}")
        raise AttributeError("HighPerformancePolygonTrajectoryQuery object has no attribute 'process_complete_workflow'")
    
    logger.debug("🔧 DEBUG: 即将调用 process_complete_workflow 方法")
    
    return processor.process_complete_workflow(
        geojson_file=geojson_file,
        output_table=output_table,
        output_geojson=output_geojson
    )

def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='高性能Polygon轨迹查询模块 - 批量查找与polygon相交的轨迹数据',
        epilog="""
高性能特性:
  • 智能批量查询策略：≤50个polygon使用UNION ALL，>50个polygon使用分块查询
  • 优化的数据库写入：批量插入，事务保护，多重索引
  • 详细的性能统计：查询时间、构建时间、处理速度等

示例:
  # 基础用法：查询轨迹并保存到数据库表
  python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories
  
  # 高性能模式：自定义批量查询参数
  python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories \\
    --batch-threshold 30 --chunk-size 15 --batch-insert 500
  
  # 同时保存到数据库和文件
  python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson \\
    --table my_trajectories --output trajectories.geojson
  
  # 调整轨迹点限制和统计选项
  python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson \\
    --table my_trajectories --limit 20000 --no-speed-stats --verbose
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 基本参数
    parser.add_argument('--input', required=True, help='输入GeoJSON文件路径')
    parser.add_argument('--table', help='输出数据库表名（可选）')
    parser.add_argument('--output', help='输出GeoJSON文件路径（可选）')
    
    # 性能优化参数
    parser.add_argument('--batch-threshold', type=int, default=50, 
                       help='批量查询vs分块查询的阈值 (默认: 50)')
    parser.add_argument('--chunk-size', type=int, default=20,
                       help='分块查询的块大小 (默认: 20)')
    parser.add_argument('--limit', type=int, default=10000, 
                       help='每个polygon的轨迹点限制数量 (默认: 10000)')
    parser.add_argument('--batch-insert', type=int, default=1000,
                       help='批量插入数据库的批次大小 (默认: 1000)')
    parser.add_argument('--timeout', type=int, default=300,
                       help='查询超时时间（秒）(默认: 300)')
    
    # 功能选项
    parser.add_argument('--min-points', type=int, default=2,
                       help='构建轨迹的最小点数 (默认: 2)')
    parser.add_argument('--no-speed-stats', action='store_true',
                       help='禁用速度统计计算')
    parser.add_argument('--no-avp-stats', action='store_true',
                       help='禁用AVP统计计算')
    
    # 其他参数
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 验证参数
    if not args.table and not args.output:
        parser.error("必须指定 --table 或 --output 中的至少一个")
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 检查输入文件
        if not Path(args.input).exists():
            logger.error(f"输入文件不存在: {args.input}")
            return 1
        
        # 构建配置
        config = PolygonTrajectoryConfig(
            batch_threshold=args.batch_threshold,
            chunk_size=args.chunk_size,
            limit_per_polygon=args.limit,
            batch_insert_size=args.batch_insert,
            min_points_per_trajectory=args.min_points,
            enable_speed_stats=not args.no_speed_stats,
            enable_avp_stats=not args.no_avp_stats
        )
        
        # 输出配置信息
        logger.info("🔧 配置参数:")
        logger.info(f"   • 批量查询阈值: {config.batch_threshold}")
        logger.info(f"   • 分块大小: {config.chunk_size}")
        logger.info(f"   • 每polygon轨迹点限制: {config.limit_per_polygon:,}")
        logger.info(f"   • 批量插入大小: {config.batch_insert_size}")
        logger.info(f"   • 最小轨迹点数: {config.min_points_per_trajectory}")
        logger.info(f"   • 速度统计: {'启用' if config.enable_speed_stats else '禁用'}")
        logger.info(f"   • AVP统计: {'启用' if config.enable_avp_stats else '禁用'}")
        
        # 执行处理
        stats = process_polygon_trajectory_query(
            geojson_file=args.input,
            output_table=args.table,
            output_geojson=args.output,
            config=config
        )
        
        # 检查处理结果
        if 'error' in stats:
            logger.error(f"❌ 处理错误: {stats['error']}")
            return 1
        
        if not stats.get('success', False):
            logger.error("❌ 处理未成功完成")
            return 1
        
        # 成功完成
        logger.info("🎉 所有处理成功完成！")
        
        # 确定返回代码
        query_stats = stats.get('query_stats', {})
        build_stats = stats.get('build_stats', {})
        
        has_results = (
            query_stats.get('total_points', 0) > 0 and 
            build_stats.get('valid_trajectories', 0) > 0
        )
        
        return 0 if has_results else 1
        
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 