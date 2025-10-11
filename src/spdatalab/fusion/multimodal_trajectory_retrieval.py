"""多模态轨迹检索系统 - 主融合分析模块

核心功能：
1. MultimodalTrajectoryWorkflow - 轻量化协调器，智能聚合策略
2. ResultAggregator - 智能聚合器（dataset_name + 时间窗口聚合）
3. PolygonMerger - Polygon合并优化器（重叠合并）
4. 轻量化工作流：返回轨迹点而非完整轨迹线
5. 映射保持：保留polygon到源数据的对应关系

复用现有模块的80%功能，专注于融合分析和智能优化。
"""

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import warnings

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon, shape
from shapely.ops import unary_union

# 导入现有模块（复用现有功能）
from spdatalab.dataset.polygon_trajectory_query import HighPerformancePolygonTrajectoryQuery

# 导入基础数据处理组件
from spdatalab.dataset.multimodal_data_retriever import (
    APIConfig,
    MultimodalRetriever,
    TrajectoryToPolygonConverter
)

# 导入现有高性能查询引擎（复用80%功能）
from spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery,
    PolygonTrajectoryConfig
)

# 抑制警告
warnings.filterwarnings('ignore', category=UserWarning)

# 日志配置
logger = logging.getLogger(__name__)


@dataclass
class MultimodalConfig:
    """轻量化研发工具配置"""
    
    # API配置
    api_config: APIConfig
    
    # 核心参数（研发分析优化）
    max_search_results: int = 5             # 适合研发分析的默认值（与API示例一致）
    time_window_days: int = 30              # 30天时间窗口
    buffer_distance: float = 10.0           # 10米精确缓冲
    
    # API限制（硬约束）
    max_single_query: int = 10000           # 单次查询上限
    max_total_query: int = 100000           # 累计查询上限
    
    # 聚合优化参数
    overlap_threshold: float = 0.7          # Polygon重叠度阈值
    time_window_hours: int = 24             # 时间窗口聚合（小时）
    
    # 复用现有配置（简化）
    polygon_config: Optional[PolygonTrajectoryConfig] = None
    
    # 输出配置
    output_table: Optional[str] = None
    output_geojson: Optional[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.polygon_config is None:
            # 使用默认的高性能配置
            self.polygon_config = PolygonTrajectoryConfig(
                batch_threshold=50,
                chunk_size=20,
                limit_per_polygon=15000
            )


class ResultAggregator:
    """多模态查询结果聚合器
    
    功能：
    - 按dataset_name聚合，避免重复查询
    - 按时间窗口聚合，合并相近时间的查询
    """
    
    def __init__(self, time_window_hours: int = 24):
        self.time_window_hours = time_window_hours
    
    def aggregate_by_dataset(self, search_results: List[Dict]) -> Dict[str, List[Dict]]:
        """按dataset_name聚合，避免重复查询
        
        Args:
            search_results: 多模态检索结果
            
        Returns:
            按dataset_name分组的结果
        """
        if not search_results:
            return {}
        
        dataset_groups = defaultdict(list)
        for result in search_results:
            dataset_name = result.get('dataset_name', 'unknown')
            dataset_groups[dataset_name].append(result)
        
        logger.info(f"📊 Dataset聚合: {len(search_results)}条结果 → {len(dataset_groups)}个数据集")
        
        # 在调试模式下显示详细的数据集信息
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("📋 聚合的数据集详情:")
            for dataset_name, items in dataset_groups.items():
                logger.debug(f"   📁 {dataset_name}: {len(items)}条结果")
                for i, item in enumerate(items[:3]):  # 只显示前3条
                    similarity = item.get('similarity', 0)
                    timestamp = item.get('timestamp', 0)
                    logger.debug(f"      {i+1}. 相似度={similarity:.3f}, 时间戳={timestamp}")
                if len(items) > 3:
                    logger.debug(f"      ... 还有{len(items)-3}条结果")
        
        return dict(dataset_groups)
    
    def aggregate_by_timewindow(self, dataset_groups: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """按时间窗口聚合，合并相近时间的查询
        
        Args:
            dataset_groups: 按dataset分组的结果
            
        Returns:
            聚合后的时间范围查询参数
        """
        aggregated_queries = {}
        
        for dataset_name, results in dataset_groups.items():
            if not results:
                continue
            
            # 提取时间戳
            timestamps = [r.get('timestamp', 0) for r in results if r.get('timestamp')]
            if not timestamps:
                # 如果没有时间戳，使用当前时间
                current_ts = int(datetime.now().timestamp() * 1000)
                timestamps = [current_ts]
            
            # 计算时间窗口
            min_timestamp = min(timestamps)
            max_timestamp = max(timestamps)
            
            # 扩展时间窗口（默认前后各延伸12小时）
            window_ms = self.time_window_hours * 60 * 60 * 1000 // 2  # 单边窗口
            start_time = min_timestamp - window_ms
            end_time = max_timestamp + window_ms
            
            aggregated_queries[dataset_name] = {
                'start_time': start_time,
                'end_time': end_time,
                'original_timestamps': timestamps,
                'result_count': len(results)
            }
        
        logger.info(f"⏰ 时间窗口聚合: 生成{len(aggregated_queries)}个时间范围查询")
        return aggregated_queries


class PolygonMerger:
    """Polygon合并优化器
    
    功能：
    - 合并重叠度高的polygon，保留源数据映射
    - 优化查询效率，减少数据库访问
    """
    
    def __init__(self, overlap_threshold: float = 0.7):
        self.overlap_threshold = overlap_threshold  # 重叠度阈值
    
    def merge_overlapping_polygons(self, polygons_with_source: List[Dict]) -> List[Dict]:
        """合并重叠度高的polygon，保留源数据映射
        
        Args:
            polygons_with_source: polygon列表，每项包含geometry和properties
            
        Returns:
            合并后的polygon列表，保持source映射
        """
        if not polygons_with_source:
            return []
        
        logger.info(f"🔄 开始Polygon合并优化: {len(polygons_with_source)}个原始Polygon")
        
        # 转换为标准格式
        polygons = []
        for i, item in enumerate(polygons_with_source):
            geom = item.get('geometry')
            if isinstance(geom, Polygon) and geom.is_valid:
                polygons.append({
                    'id': item.get('id', f'polygon_{i}'),
                    'geometry': geom,
                    'sources': [item.get('properties', {})],  # 初始source列表
                    'merged': False
                })
        
        if not polygons:
            return []
        
        # 使用简单的贪心合并算法
        merged_polygons = []
        processed = set()
        
        for i, poly1 in enumerate(polygons):
            if i in processed:
                continue
            
            # 初始化合并组
            merge_group = [poly1]
            current_geom = poly1['geometry']
            processed.add(i)
            
            # 查找可合并的polygon
            for j, poly2 in enumerate(polygons[i+1:], i+1):
                if j in processed:
                    continue
                
                overlap_ratio = self.calculate_overlap_ratio(current_geom, poly2['geometry'])
                if overlap_ratio >= self.overlap_threshold:
                    merge_group.append(poly2)
                    processed.add(j)
                    # 更新合并几何（简单union）
                    try:
                        current_geom = current_geom.union(poly2['geometry'])
                    except Exception as e:
                        logger.warning(f"Polygon合并失败: {e}")
            
            # 创建合并结果
            if len(merge_group) > 1:
                # 多个polygon合并
                all_sources = []
                for poly in merge_group:
                    all_sources.extend(poly['sources'])
                
                merged_polygons.append({
                    'id': f"merged_polygon_{len(merged_polygons)}",
                    'geometry': current_geom,
                    'properties': {
                        'merged_count': len(merge_group),
                        'sources': all_sources,
                        'merge_type': 'overlapping'
                    }
                })
            else:
                # 单个polygon保持不变
                merged_polygons.append({
                    'id': poly1['id'],
                    'geometry': poly1['geometry'],
                    'properties': {
                        'merged_count': 1,
                        'sources': poly1['sources'],
                        'merge_type': 'original'
                    }
                })
        
        optimization_ratio = f"{len(polygons_with_source)} → {len(merged_polygons)}"
        logger.info(f"✅ Polygon合并完成: {optimization_ratio} (压缩率: {1 - len(merged_polygons)/len(polygons_with_source):.1%})")
        
        return merged_polygons
    
    def calculate_overlap_ratio(self, poly1: Polygon, poly2: Polygon) -> float:
        """计算两个polygon的重叠比例
        
        Args:
            poly1, poly2: 待比较的polygon
            
        Returns:
            重叠比例 (0.0 - 1.0)
        """
        try:
            if not poly1.intersects(poly2):
                return 0.0
            
            intersection_area = poly1.intersection(poly2).area
            union_area = poly1.union(poly2).area
            
            return intersection_area / union_area if union_area > 0 else 0.0
        except Exception:
            return 0.0


class MultimodalTrajectoryWorkflow:
    """多模态轨迹检索工作流
    
    轻量化协调器：
    - 智能聚合：dataset_name和时间窗口聚合
    - Polygon优化：重叠polygon合并
    - 轻量输出：返回轨迹点而非完整轨迹  
    - 映射保持：保留polygon到源数据的对应关系
    """
    
    def __init__(self, config: MultimodalConfig):
        # 组合现有和新增组件
        self.config = config
        self.retriever = MultimodalRetriever(config.api_config)
        self.converter = TrajectoryToPolygonConverter(config.buffer_distance)
        self.aggregator = ResultAggregator(config.time_window_hours)         # 新增：结果聚合器
        self.polygon_merger = PolygonMerger(config.overlap_threshold)        # 新增：polygon合并器
        self.polygon_processor = HighPerformancePolygonTrajectoryQuery(config.polygon_config)
    
    def process_text_query(self, text: str, collection: str, count: Optional[int] = None, 
                          start: int = 0, start_time: Optional[int] = None, 
                          end_time: Optional[int] = None, **kwargs) -> Dict:
        """优化的文本查询流程
        
        Args:
            text: 查询文本
            collection: 相机collection
            count: 查询数量（默认使用配置值）
            start: 起始偏移量，默认0
            start_time: 事件开始时间，13位时间戳（可选）
            end_time: 事件结束时间，13位时间戳（可选）
            
        Returns:
            轻量化查询结果
        """
        if count is None:
            count = self.config.max_search_results
        
        return self._execute_optimized_workflow(
            retrieval_func=lambda: self.retriever.retrieve_by_text(
                text=text,
                collection=collection,
                count=count,
                start=start,
                start_time=start_time,
                end_time=end_time
            ),
            query_type="text",
            query_content=text,
            collection=collection,
            **kwargs
        )
    
    def process_image_query(self, images: List[str], collection: str, count: Optional[int] = None,
                           start: int = 0, start_time: Optional[int] = None,
                           end_time: Optional[int] = None, **kwargs) -> Dict:
        """优化的图片查询流程
        
        Args:
            images: 图片base64编码后的字符串列表
            collection: 相机collection
            count: 查询数量（默认使用配置值）
            start: 起始偏移量，默认0
            start_time: 事件开始时间，13位时间戳（可选）
            end_time: 事件结束时间，13位时间戳（可选）
            
        Returns:
            轻量化查询结果
        """
        if count is None:
            count = self.config.max_search_results
        
        return self._execute_optimized_workflow(
            retrieval_func=lambda: self.retriever.retrieve_by_images(
                images=images,
                collection=collection,
                count=count,
                start=start,
                start_time=start_time,
                end_time=end_time
            ),
            query_type="image",
            query_content=f"{len(images)}张图片",
            collection=collection,
            **kwargs
        )
    
    def _execute_optimized_workflow(self, retrieval_func, query_type: str, query_content: str, 
                                   collection: str, **kwargs) -> Dict:
        """优化的工作流引擎，包含智能聚合策略"""
        workflow_start = time.time()
        stats = {
            'query_type': query_type,
            'query_content': query_content,
            'collection': collection,
            'start_time': datetime.now(),
            'config': {
                'buffer_distance': self.config.buffer_distance,
                'time_window_days': self.config.time_window_days,
                'overlap_threshold': self.config.overlap_threshold
            }
        }
        
        try:
            # Stage 1: 多模态检索
            logger.info("🔍 Stage 1: 执行多模态检索...")
            search_results = retrieval_func()
            stats['search_results_count'] = len(search_results)
            
            if not search_results:
                return self._handle_no_results(stats)
            
            # Stage 2: 智能聚合 (新增优化！)
            aggregation_start = time.time()
            logger.info(f"📊 Stage 2: 智能聚合 {len(search_results)} 个检索结果...")
            aggregated_datasets = self.aggregator.aggregate_by_dataset(search_results)
            aggregated_queries = self.aggregator.aggregate_by_timewindow(aggregated_datasets)
            
            # 增强统计信息收集
            aggregation_time = time.time() - aggregation_start
            stats.update({
                'aggregated_datasets': len(aggregated_datasets),
                'aggregated_queries': len(aggregated_queries),
                'aggregation_time': aggregation_time,
                'aggregation_efficiency': {
                    'original_results': len(search_results),
                    'aggregated_datasets': len(aggregated_datasets),
                    'aggregated_queries': len(aggregated_queries),
                    'query_reduction_ratio': (len(search_results) - len(aggregated_queries)) / len(search_results) if len(search_results) > 0 else 0
                }
            })
            
            # 添加数据集详情用于verbose模式显示
            dataset_details = {}
            similarity_stats = {'min': 1.0, 'max': 0.0, 'avg': 0.0}
            timestamps = []
            
            for dataset_name, results in aggregated_datasets.items():
                dataset_details[dataset_name] = len(results)
                # 收集相似度和时间戳统计
                for result in results:
                    similarity = result.get('similarity', 0)
                    similarity_stats['min'] = min(similarity_stats['min'], similarity)
                    similarity_stats['max'] = max(similarity_stats['max'], similarity)
                    timestamps.append(result.get('timestamp', 0))
            
            if search_results:
                similarities = [r.get('similarity', 0) for r in search_results]
                similarity_stats['avg'] = sum(similarities) / len(similarities)
            
            stats.update({
                'dataset_details': dataset_details,
                'similarity_stats': similarity_stats,
                'time_range_stats': {
                    'earliest': min(timestamps) if timestamps else 0,
                    'latest': max(timestamps) if timestamps else 0,
                    'span_hours': (max(timestamps) - min(timestamps)) / (1000 * 3600) if len(timestamps) > 1 else 0
                }
            })
            
            # Stage 3: 轨迹数据获取 (优化后，减少重复查询)
            logger.info(f"🚀 Stage 3: 批量获取 {len(aggregated_datasets)} 个数据集轨迹...")
            trajectory_data = self._fetch_aggregated_trajectories(aggregated_queries)
            stats['trajectory_data_count'] = len(trajectory_data)
            
            if not trajectory_data:
                return self._handle_no_trajectories(stats)
            
            # Stage 4: Polygon转换和合并 (新增合并优化！)
            polygon_start = time.time()
            logger.info(f"🔄 Stage 4: 转换轨迹为Polygon并智能合并...")
            raw_polygons = self.converter.batch_convert(trajectory_data)
            merged_polygons = self.polygon_merger.merge_overlapping_polygons(raw_polygons)
            
            # 增强polygon处理统计
            polygon_time = time.time() - polygon_start
            compression_ratio = ((len(raw_polygons) - len(merged_polygons)) / len(raw_polygons) * 100) if len(raw_polygons) > 0 else 0
            
            stats.update({
                'raw_polygon_count': len(raw_polygons),
                'merged_polygon_count': len(merged_polygons),
                'polygon_processing_time': polygon_time,
                'polygon_optimization': {
                    'compression_ratio': compression_ratio,
                    'polygons_eliminated': len(raw_polygons) - len(merged_polygons),
                    'efficiency_gain': compression_ratio / 100 if compression_ratio > 0 else 0
                }
            })
            
            if not merged_polygons:
                return self._handle_no_polygons(stats)
            
            # Stage 5: 轻量化Polygon查询 (仅返回轨迹点！)
            query_start = time.time()
            logger.info(f"⚡ Stage 5: 基于 {len(merged_polygons)} 个Polygon查询轨迹点...")
            trajectory_points = self._execute_lightweight_polygon_query(merged_polygons)
            
            # 增强查询结果统计
            query_time = time.time() - query_start
            points_count = len(trajectory_points) if trajectory_points is not None else 0
            
            stats.update({
                'discovered_points_count': points_count,
                'trajectory_query_time': query_time,
                'query_performance': {
                    'points_per_polygon': points_count / len(merged_polygons) if len(merged_polygons) > 0 else 0,
                    'points_per_second': points_count / query_time if query_time > 0 else 0,
                    'unique_datasets_discovered': trajectory_points['dataset_name'].nunique() if trajectory_points is not None and not trajectory_points.empty else 0
                }
            })
            
            # Stage 6: 轻量化结果输出
            logger.info("💾 Stage 6: 轻量化结果输出...")
            final_results = self._finalize_lightweight_results(trajectory_points, merged_polygons, stats)
            
            stats['success'] = True
            stats['total_duration'] = time.time() - workflow_start
            
            return final_results
            
        except Exception as e:
            stats['error'] = str(e)
            stats['success'] = False
            stats['total_duration'] = time.time() - workflow_start
            logger.error(f"❌ 优化工作流执行失败: {e}")
            return stats
    
    def _handle_no_results(self, stats: Dict) -> Dict:
        """处理无检索结果的情况"""
        logger.warning("⚠️ 多模态检索未返回任何结果")
        stats['message'] = "多模态检索未返回任何结果，请尝试调整查询条件"
        stats['success'] = False
        return stats
    
    def _handle_no_trajectories(self, stats: Dict) -> Dict:
        """处理无轨迹数据的情况"""
        logger.warning("⚠️ 未找到相应的轨迹数据")
        stats['message'] = "根据检索结果未找到相应的轨迹数据"
        stats['success'] = False
        return stats
    
    def _handle_no_polygons(self, stats: Dict) -> Dict:
        """处理无有效Polygon的情况"""
        logger.warning("⚠️ 轨迹转换为Polygon失败")
        stats['message'] = "轨迹数据转换为Polygon失败，请检查数据质量"
        stats['success'] = False
        return stats
    
    def _fetch_aggregated_trajectories(self, aggregated_queries: Dict[str, Dict]) -> List[Dict]:
        """基于聚合结果获取轨迹数据 - 复用现有的完整轨迹查询功能
        
        复用HighPerformancePolygonTrajectoryQuery._fetch_complete_trajectories方法
        确保80%+代码复用原则
        """
        if not aggregated_queries:
            logger.warning("⚠️ 没有聚合查询数据")
            return []
        
        logger.info(f"🚀 开始获取轨迹数据: {len(aggregated_queries)} 个数据集")
        
        try:
            # 构建模拟的intersection_result_df，让现有方法能处理
            dataset_names = list(aggregated_queries.keys())
            
            # 创建简单的DataFrame来触发现有的轨迹查询功能
            import pandas as pd
            intersection_result_df = pd.DataFrame({
                'dataset_name': dataset_names,
                'timestamp': [time_range.get('start_time', 0) for time_range in aggregated_queries.values()]
            })
            
            logger.info(f"📋 复用现有轨迹查询方法获取 {len(dataset_names)} 个数据集轨迹...")
            
            # 复用现有的完整轨迹查询功能 - 80%复用原则
            complete_trajectory_df, complete_stats = self.polygon_processor._fetch_complete_trajectories(intersection_result_df)
            
            if complete_trajectory_df.empty:
                logger.warning("⚠️ 未获取到任何轨迹数据")
                return []
            
            logger.info(f"✅ 轨迹数据获取成功: {len(complete_trajectory_df)} 个轨迹点")
            logger.info(f"📊 获取统计: 数据集数={complete_stats.get('complete_datasets', 0)}, "
                       f"轨迹点数={complete_stats.get('complete_points', 0)}, "
                       f"用时={complete_stats.get('complete_query_time', 0):.2f}s")
            
            # 将DataFrame转换为LineString列表
            all_trajectory_data = self._convert_dataframe_to_linestrings(complete_trajectory_df, aggregated_queries)
            
            return all_trajectory_data
            
        except Exception as e:
            logger.error(f"❌ 轨迹数据获取失败: {e}")
            return []
    
    def _convert_dataframe_to_linestrings(self, trajectory_df: pd.DataFrame, 
                                        aggregated_queries: Dict[str, Dict]) -> List[Dict]:
        """将轨迹DataFrame转换为LineString列表
        
        Args:
            trajectory_df: 从数据库查询到的轨迹点DataFrame
            aggregated_queries: 聚合查询参数
            
        Returns:
            包含LineString几何的轨迹数据列表
        """
        if trajectory_df.empty:
            return []
        
        logger.info(f"🔄 开始转换 {len(trajectory_df)} 个轨迹点为LineString...")
        
        try:
            from shapely.geometry import LineString
            
            all_trajectory_data = []
            
            # 按dataset_name分组处理
            grouped = trajectory_df.groupby('dataset_name')
            
            for dataset_name, group in grouped:
                if len(group) < 2:
                    logger.debug(f"⚠️ 数据集 {dataset_name} 点数量不足({len(group)})，跳过LineString构建")
                    continue
                
                # 按时间排序
                group = group.sort_values('timestamp')
                
                # 提取坐标点
                coordinates = list(zip(group['longitude'], group['latitude']))
                
                # 创建LineString
                trajectory_linestring = LineString(coordinates)
                
                # 获取时间范围信息
                time_range = aggregated_queries.get(dataset_name, {})
                
                # 构建轨迹数据
                trajectory_data = {
                    'dataset_name': dataset_name,
                    'linestring': trajectory_linestring,
                    'time_range': time_range,
                    'point_count': len(group),
                    'start_time': int(group['timestamp'].min()),
                    'end_time': int(group['timestamp'].max()),
                    'duration': int(group['timestamp'].max() - group['timestamp'].min())
                }
                
                all_trajectory_data.append(trajectory_data)
                
                logger.debug(f"✅ 转换轨迹: {dataset_name}, 点数: {len(group)}, "
                           f"时长: {trajectory_data['duration']//1000:.1f}s")
            
            logger.info(f"✅ LineString转换完成: {len(all_trajectory_data)} 条轨迹")
            
            return all_trajectory_data
            
        except Exception as e:
            logger.error(f"❌ LineString转换失败: {e}")
            return []
    
    def _execute_lightweight_polygon_query(self, merged_polygons: List[Dict]) -> Optional[pd.DataFrame]:
        """轻量化Polygon查询 - 复用现有高性能查询引擎
        
        复用HighPerformancePolygonTrajectoryQuery.query_intersecting_trajectory_points方法
        确保80%+代码复用原则
        """
        if not merged_polygons:
            logger.warning("⚠️ 没有polygon数据需要查询")
            return None
        
        logger.info(f"⚡ 开始轻量化Polygon查询: {len(merged_polygons)} 个polygon")
        
        try:
            # 复用现有的高性能查询引擎 - 80%复用原则
            points_df, query_stats = self.polygon_processor.query_intersecting_trajectory_points(merged_polygons)
            
            if points_df.empty:
                logger.warning("⚠️ 未查询到相交的轨迹点")
                return None
            
            logger.info(f"✅ 轻量化查询成功: {len(points_df)} 个轨迹点")
            logger.info(f"📊 查询统计: 策略={query_stats.get('strategy', 'unknown')}, "
                       f"用时={query_stats.get('query_time', 0):.2f}s, "
                       f"数据集数={query_stats.get('unique_datasets', 0)}")
            
            # 添加源polygon映射信息
            points_df = self._add_polygon_mapping(points_df, merged_polygons)
            
            return points_df
            
        except Exception as e:
            logger.error(f"❌ 轻量化Polygon查询失败: {e}")
            # 如果查询失败，返回None而不是mock数据
            return None
    
    def _add_polygon_mapping(self, points_df: pd.DataFrame, merged_polygons: List[Dict]) -> pd.DataFrame:
        """为轨迹点添加源polygon映射信息
        
        Args:
            points_df: 查询到的轨迹点DataFrame
            merged_polygons: 合并后的polygon列表
            
        Returns:
            添加了source_polygons字段的DataFrame
        """
        if points_df.empty or not merged_polygons:
            return points_df
        
        logger.info(f"🔗 开始计算轨迹点到polygon的映射关系...")
        
        try:
            from shapely.geometry import Point
            
            # 为每个轨迹点创建Point几何
            points_geometry = [Point(row['longitude'], row['latitude']) 
                             for _, row in points_df.iterrows()]
            
            # 初始化source_polygons列
            source_polygons = []
            
            for i, point_geom in enumerate(points_geometry):
                matched_sources = []
                
                # 检查与哪些polygon相交
                for polygon_data in merged_polygons:
                    polygon_geom = polygon_data['geometry']
                    
                    # 空间相交检查
                    if point_geom.within(polygon_geom) or point_geom.intersects(polygon_geom):
                        # 获取源数据信息
                        sources = polygon_data.get('sources', [])
                        for source in sources:
                            dataset_name = source.get('dataset_name', 'unknown')
                            timestamp = source.get('timestamp', 0)
                            matched_sources.append(f"{dataset_name}:{timestamp}")
                
                # 格式化映射信息
                if matched_sources:
                    source_polygons.append(','.join(matched_sources))
                else:
                    # 如果没有精确匹配，使用polygon ID作为后备
                    polygon_id = merged_polygons[0].get('id', 'unknown_polygon')
                    source_polygons.append(f"polygon_{polygon_id}")
            
            # 添加到DataFrame
            points_df = points_df.copy()
            points_df['source_polygons'] = source_polygons
            
            logger.info(f"✅ 映射关系计算完成: {len(points_df)} 个轨迹点已添加polygon映射信息")
            
            return points_df
            
        except Exception as e:
            logger.error(f"❌ 添加polygon映射失败: {e}")
            # 添加默认映射信息
            points_df = points_df.copy()
            points_df['source_polygons'] = 'mapping_failed'
            return points_df
    
    def _finalize_lightweight_results(self, trajectory_points: Optional[pd.DataFrame], 
                                     merged_polygons: List[Dict], stats: Dict) -> Dict:
        """轻量化结果处理 - 返回轨迹点和polygon映射"""
        if trajectory_points is None or trajectory_points.empty:
            stats['final_results'] = "无发现轨迹点"
            return stats
        
        # 轻量化输出格式
        results = {
            'trajectory_points': trajectory_points.to_dict('records') if not trajectory_points.empty else [],
            'source_polygons': [
                {
                    'id': poly['id'],
                    'properties': poly['properties'],
                    'geometry_wkt': poly['geometry'].wkt if poly.get('geometry') else None
                }
                for poly in merged_polygons
            ],
            'summary': {
                'total_points': len(trajectory_points),
                'unique_datasets': trajectory_points['dataset_name'].nunique() if 'dataset_name' in trajectory_points.columns else 0,
                'polygon_sources': len(merged_polygons),
                'optimization_ratio': f"{stats.get('raw_polygon_count', 0)} → {stats.get('merged_polygon_count', 0)}"
            },
            'stats': stats
        }
        
        # 可选的数据库保存（复用现有功能）
        if self.config.output_table:
            logger.info(f"💾 保存结果到数据库表: {self.config.output_table}")
            try:
                # 将轨迹点转换为现有保存方法期望的格式
                trajectory_data = self._convert_points_to_trajectory_format(trajectory_points, stats)
                if trajectory_data:
                    # 复用现有的高性能保存方法
                    inserted_count, save_stats = self.polygon_processor.save_trajectories_to_table(
                        trajectory_data, self.config.output_table
                    )
                    stats['saved_to_database'] = inserted_count
                    stats['save_stats'] = save_stats
                    logger.info(f"✅ 数据库保存成功: {inserted_count} 条轨迹")
                else:
                    logger.warning("⚠️ 没有轨迹数据需要保存")
                    stats['saved_to_database'] = 0
            except Exception as e:
                logger.error(f"❌ 数据库保存失败: {e}")
                stats['database_save_error'] = str(e)
        
        # 可选的GeoJSON保存
        if self.config.output_geojson:
            logger.info(f"💾 导出GeoJSON文件: {self.config.output_geojson}")
            # TODO: 实现GeoJSON导出功能
        
        return results
    
    def _convert_points_to_trajectory_format(self, trajectory_points: pd.DataFrame, stats: Dict) -> List[Dict]:
        """将轨迹点转换为现有保存方法期望的格式（复用现有架构）
        
        Args:
            trajectory_points: 发现的轨迹点DataFrame
            stats: 统计信息
            
        Returns:
            现有保存方法期望的轨迹数据格式
        """
        if trajectory_points.empty:
            logger.warning("📊 没有轨迹点数据需要转换")
            return []
        
        trajectories = []
        
        try:
            # 按dataset_name分组处理（轻量化聚合）
            grouped = trajectory_points.groupby('dataset_name')
            logger.info(f"🔄 转换 {len(grouped)} 个数据集的轨迹点为标准格式...")
            
            for dataset_name, group in grouped:
                # 跳过点数过少的数据集
                if len(group) < 2:
                    logger.debug(f"⚠️ 数据集 {dataset_name} 点数量不足({len(group)})，跳过")
                    continue
                
                # 按时间排序
                group = group.sort_values('timestamp')
                
                # 提取坐标构建LineString几何
                coordinates = list(zip(group['longitude'], group['latitude']))
                from shapely.geometry import LineString
                trajectory_geom = LineString(coordinates)
                
                # 构建符合现有格式的轨迹数据
                trajectory_data = {
                    'dataset_name': dataset_name,
                    'scene_id': group.get('scene_id', pd.Series([''])).iloc[0] if 'scene_id' in group.columns else '',
                    'event_id': group.get('event_id', pd.Series([None])).iloc[0] if 'event_id' in group.columns else None,
                    'event_name': group.get('event_name', pd.Series([''])).iloc[0] if 'event_name' in group.columns else '',
                    'start_time': int(group['timestamp'].min()),
                    'end_time': int(group['timestamp'].max()),
                    'duration': int(group['timestamp'].max() - group['timestamp'].min()),
                    'point_count': len(group),
                    'geometry': trajectory_geom,
                    
                    # 多模态特有字段
                    'query_type': stats.get('query_type', 'text'),
                    'query_content': stats.get('query_content', ''),
                    'collection': stats.get('collection', ''),
                    'source_polygons': group.get('source_polygons', pd.Series([''])).iloc[0] if 'source_polygons' in group.columns else '',
                    'optimization_ratio': f"{stats.get('raw_polygon_count', 0)}→{stats.get('merged_polygon_count', 0)}",
                    
                    # 可选的统计字段
                    'polygon_ids': list(group.get('polygon_id', pd.Series(['unknown'])).unique()) if 'polygon_id' in group.columns else ['multimodal_discovery']
                }
                
                # 添加速度信息（如果有）
                if 'velocity' in group.columns:
                    velocity_data = group['velocity'].dropna()
                    if len(velocity_data) > 0:
                        trajectory_data.update({
                            'avg_speed': round(float(velocity_data.mean()), 2),
                            'max_speed': round(float(velocity_data.max()), 2),
                            'min_speed': round(float(velocity_data.min()), 2)
                        })
                
                trajectories.append(trajectory_data)
                
                logger.debug(f"✅ 转换轨迹: {dataset_name}, 点数: {len(group)}, "
                           f"时长: {trajectory_data['duration']//1000:.1f}s")
            
            logger.info(f"✅ 转换完成: {len(trajectories)} 条轨迹，基于 {len(trajectory_points)} 个轨迹点")
            return trajectories
            
        except Exception as e:
            logger.error(f"❌ 轨迹格式转换失败: {e}")
            raise


# 导出主要类
__all__ = [
    'MultimodalConfig',
    'MultimodalTrajectoryWorkflow',
    'ResultAggregator',
    'PolygonMerger'
]


# CLI支持
if __name__ == '__main__':
    from spdatalab.fusion.cli.multimodal import main

    raise SystemExit(main())
