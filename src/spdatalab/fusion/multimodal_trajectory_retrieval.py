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
    max_search_results: int = 5000          # 适合研发分析的默认值
    similarity_threshold: float = 0.3
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
                          start_time: Optional[int] = None, end_time: Optional[int] = None,
                          **kwargs) -> Dict:
        """优化的文本查询流程
        
        Args:
            text: 查询文本
            collection: 相机collection
            count: 查询数量（默认使用配置值）
            start_time: 开始时间戳
            end_time: 结束时间戳
            
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
                start_time=start_time,
                end_time=end_time,
                similarity_threshold=self.config.similarity_threshold
            ),
            query_type="text",
            query_content=text,
            collection=collection,
            **kwargs
        )
    
    def process_image_query(self, image_paths: List[str], collection: str, **kwargs) -> Dict:
        """图片查询接口（预留）"""
        raise NotImplementedError("图片检索功能预留，暂不开发")
    
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
            logger.info(f"📊 Stage 2: 智能聚合 {len(search_results)} 个检索结果...")
            aggregated_datasets = self.aggregator.aggregate_by_dataset(search_results)
            aggregated_queries = self.aggregator.aggregate_by_timewindow(aggregated_datasets)
            stats['aggregated_datasets'] = len(aggregated_datasets)
            stats['aggregated_queries'] = len(aggregated_queries)
            
            # Stage 3: 轨迹数据获取 (优化后，减少重复查询)
            logger.info(f"🚀 Stage 3: 批量获取 {len(aggregated_datasets)} 个数据集轨迹...")
            trajectory_data = self._fetch_aggregated_trajectories(aggregated_queries)
            stats['trajectory_data_count'] = len(trajectory_data)
            
            if not trajectory_data:
                return self._handle_no_trajectories(stats)
            
            # Stage 4: Polygon转换和合并 (新增合并优化！)
            logger.info(f"🔄 Stage 4: 转换轨迹为Polygon并智能合并...")
            raw_polygons = self.converter.batch_convert(trajectory_data)
            merged_polygons = self.polygon_merger.merge_overlapping_polygons(raw_polygons)
            stats['raw_polygon_count'] = len(raw_polygons)
            stats['merged_polygon_count'] = len(merged_polygons)
            
            if not merged_polygons:
                return self._handle_no_polygons(stats)
            
            # Stage 5: 轻量化Polygon查询 (仅返回轨迹点！)
            logger.info(f"⚡ Stage 5: 基于 {len(merged_polygons)} 个Polygon查询轨迹点...")
            trajectory_points = self._execute_lightweight_polygon_query(merged_polygons)
            stats['discovered_points_count'] = len(trajectory_points) if trajectory_points is not None else 0
            
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
        """基于聚合结果获取轨迹数据 - 减少重复查询
        
        注意：这里应该调用现有的轨迹查询方法，暂时返回模拟数据
        """
        # TODO: 集成现有的轨迹查询功能
        logger.info("🔧 轨迹数据获取功能待集成...")
        
        # 模拟返回数据结构
        all_trajectory_data = []
        for dataset_name, time_range in aggregated_queries.items():
            # 这里应该调用现有的dataset查询方法
            # 暂时创建模拟的LineString
            from shapely.geometry import LineString
            
            # 模拟轨迹点（实际应该从数据库查询）
            mock_coords = [
                (116.3, 39.9), (116.31, 39.91), (116.32, 39.92)  # 北京附近坐标
            ]
            trajectory_linestring = LineString(mock_coords)
            
            all_trajectory_data.append({
                'dataset_name': dataset_name,
                'linestring': trajectory_linestring,
                'time_range': time_range,
                'point_count': len(mock_coords)
            })
        
        return all_trajectory_data
    
    def _execute_lightweight_polygon_query(self, merged_polygons: List[Dict]) -> Optional[pd.DataFrame]:
        """轻量化Polygon查询 - 仅返回轨迹点，不构建完整轨迹
        
        注意：这里应该调用现有的高性能查询引擎，暂时返回模拟数据
        """
        if not merged_polygons:
            return None
        
        logger.info("🔧 轻量化Polygon查询功能待集成...")
        
        # TODO: 调用现有的HighPerformancePolygonTrajectoryQuery
        # points_df, query_stats = self.polygon_processor.query_intersecting_trajectory_points(merged_polygons)
        
        # 模拟返回轨迹点数据
        mock_data = {
            'dataset_name': ['dataset_1', 'dataset_2'],
            'timestamp': [1739958971349, 1739958971350],
            'longitude': [116.3, 116.31],
            'latitude': [39.9, 39.91],
            'source_polygon_id': ['merged_polygon_0', 'merged_polygon_0']
        }
        
        return pd.DataFrame(mock_data)
    
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
        
        # 可选的数据库保存
        if self.config.output_table:
            logger.info(f"💾 保存结果到数据库表: {self.config.output_table}")
            # TODO: 实现数据库保存功能
        
        # 可选的GeoJSON保存
        if self.config.output_geojson:
            logger.info(f"💾 导出GeoJSON文件: {self.config.output_geojson}")
            # TODO: 实现GeoJSON导出功能
        
        return results


# 导出主要类
__all__ = [
    'MultimodalConfig',
    'MultimodalTrajectoryWorkflow',
    'ResultAggregator',
    'PolygonMerger'
]


# CLI支持
if __name__ == '__main__':
    from .multimodal_cli import main
    main()
