"""轨迹与车道空间关系分析模块。

二阶段分析的第二阶段：基于trajectory_road_analysis模块的输出结果，
对预筛选的候选车道进行精细的空间关系分析。

依赖关系：
1. 必须先运行 trajectory_road_analysis 获得候选车道
2. 基于 trajectory_road_lanes 表中的候选车道进行分析
3. 通过 road_analysis_id 关联两个阶段的分析结果

主要功能：
- 基于候选车道进行轨迹分段、采样和缓冲区分析
- 智能质量检查和轨迹重构
- 识别轨迹在不同车道上的行驶特征
"""

from __future__ import annotations
import argparse
import json
import signal
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Iterator, Tuple, Union, Any
import logging

import geopandas as gpd
import pandas as pd
import numpy as np
from sqlalchemy import text, create_engine
from shapely.geometry import LineString, Point
from shapely.ops import transform
import pyproj
from functools import partial

# 导入相关模块
from spdatalab.common.io_hive import hive_cursor
from spdatalab.dataset.trajectory import (
    load_scene_data_mappings,
    fetch_data_names_from_scene_ids,
    fetch_trajectory_points,
    setup_signal_handlers
)

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 默认配置
DEFAULT_CONFIG = {
    # 输入配置
    'input_format': 'scene_id_list',
    'polyline_output': True,
    
    # 采样配置
    'sampling_strategy': 'distance',  # 'distance', 'time', 'uniform'
    'distance_interval': 10.0,        # 米
    'time_interval': 5.0,             # 秒
    'uniform_step': 50,               # 点数
    
    # 滑窗配置
    'window_size': 20,                # 采样点数
    'window_overlap': 0.5,            # 重叠率
    
    # 车道分析配置（基于trajectory_road_analysis结果）
    'road_analysis_lanes_table': 'trajectory_road_lanes',  # 来自trajectory_road_analysis的lane结果
    'buffer_radius': 15.0,            # 米
    'max_lane_distance': 50.0,        # 米
    
    # 轨迹质量检查配置
    'min_points_single_lane': 5,      # 单车道最少点数
    'enable_multi_lane_filter': True, # 启用多车道过滤
    
    # 简化配置
    'simplify_tolerance': 2.0,        # 米
    'enable_simplification': True,
    
    # 性能配置
    'batch_size': 100,
    'enable_parallel': True,
    'max_workers': 4,
    
    # 数据库表名
    'trajectory_segments_table': 'trajectory_lane_segments',
    'trajectory_buffer_table': 'trajectory_lane_buffer',
    'quality_check_table': 'trajectory_quality_check'
}

# 全局变量
interrupted = False
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """信号处理函数，用于优雅退出"""
    global interrupted
    print(f"\n接收到中断信号 ({signum})，正在优雅退出...")
    print("等待当前处理完成，请稍候...")
    interrupted = True

class TrajectoryLaneAnalyzer:
    """轨迹车道分析器主类"""
    
    def __init__(self, config: Dict[str, Any] = None, road_analysis_id: str = None):
        """初始化分析器
        
        Args:
            config: 配置参数字典
            road_analysis_id: trajectory_road_analysis的分析ID，用于获取候选车道
        """
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        
        self.road_analysis_id = road_analysis_id
        self.engine = create_engine(LOCAL_DSN, future=True)
        self.stats = {
            'total_scenes': 0,
            'processed_scenes': 0,
            'successful_trajectories': 0,
            'failed_scenes': 0,
            'empty_scenes': 0,
            'missing_data_names': 0,
            'quality_passed': 0,
            'quality_failed': 0,
            'total_reconstructed': 0,
            'start_time': None,
            'end_time': None
        }
        
    def build_trajectory_polyline(self, scene_id: str, data_name: str, points_df: pd.DataFrame) -> Optional[Dict]:
        """构建polyline格式的轨迹数据
        
        Args:
            scene_id: 场景ID
            data_name: 数据名称
            points_df: 轨迹点DataFrame
            
        Returns:
            轨迹polyline数据
        """
        if points_df.empty:
            logger.warning(f"轨迹点数据为空: {data_name}")
            return None
        
        try:
            # 确保数据按时间排序
            points_df = points_df.sort_values('timestamp')
            
            # 提取坐标点序列
            coordinates = []
            timestamps = []
            speeds = []
            avp_flags = []
            workstages = []
            
            for _, row in points_df.iterrows():
                if pd.notna(row['longitude']) and pd.notna(row['latitude']):
                    coordinates.append((float(row['longitude']), float(row['latitude'])))
                    timestamps.append(int(row['timestamp']))
                    
                    # 提取速度信息
                    if 'twist_linear' in row and pd.notna(row['twist_linear']):
                        speeds.append(float(row['twist_linear']))
                    else:
                        speeds.append(0.0)
                    
                    # 提取AVP标志
                    if 'avp_flag' in row and pd.notna(row['avp_flag']):
                        avp_flags.append(int(row['avp_flag']))
                    else:
                        avp_flags.append(0)
                    
                    # 提取工作阶段
                    if 'workstage' in row and pd.notna(row['workstage']):
                        workstages.append(int(row['workstage']))
                    else:
                        workstages.append(0)
            
            if len(coordinates) < 2:
                logger.warning(f"有效轨迹点数量不足，无法构建轨迹线: {len(coordinates)}")
                return None
            
            # 构建LineString几何（用于后续空间分析）
            trajectory_geom = LineString(coordinates)
            
            # 计算基本统计信息
            speed_data = [s for s in speeds if s > 0]  # 过滤无效速度
            avp_data = [a for a in avp_flags if a in [0, 1]]  # 过滤无效AVP
            
            polyline_data = {
                'scene_id': scene_id,
                'data_name': data_name,
                'polyline': coordinates,  # 坐标序列 [(lng, lat), ...]
                'timestamps': timestamps,  # 时间戳序列
                'speeds': speeds,  # 速度序列
                'avp_flags': avp_flags,  # AVP标志序列
                'workstages': workstages,  # 工作阶段序列
                'geometry': trajectory_geom,  # 几何对象（用于空间分析）
                
                # 统计信息
                'start_time': min(timestamps),
                'end_time': max(timestamps),
                'duration': max(timestamps) - min(timestamps),
                'total_points': len(coordinates),
                'avg_speed': round(sum(speed_data) / len(speed_data), 2) if speed_data else 0.0,
                'max_speed': round(max(speed_data), 2) if speed_data else 0.0,
                'min_speed': round(min(speed_data), 2) if speed_data else 0.0,
                'avp_ratio': round(sum(avp_data) / len(avp_data), 3) if avp_data else 0.0,
                'trajectory_length': trajectory_geom.length  # 轨迹长度（度）
            }
            
            logger.debug(f"构建polyline轨迹: {scene_id} ({data_name}), 点数: {len(coordinates)}")
            return polyline_data
            
        except Exception as e:
            logger.error(f"构建polyline轨迹失败: {scene_id} ({data_name}), 错误: {str(e)}")
            return None
        
    def sample_trajectory(self, polyline_data: Dict) -> List[Dict]:
        """对轨迹进行采样
        
        Args:
            polyline_data: polyline格式的轨迹数据
            
        Returns:
            采样后的轨迹点列表
        """
        if not polyline_data or not polyline_data.get('polyline'):
            logger.warning("轨迹数据为空，无法采样")
            return []
        
        strategy = self.config['sampling_strategy']
        
        try:
            if strategy == 'distance':
                return self._distance_based_sampling(polyline_data)
            elif strategy == 'time':
                return self._time_based_sampling(polyline_data)
            elif strategy == 'uniform':
                return self._uniform_sampling(polyline_data)
            else:
                logger.error(f"未知的采样策略: {strategy}")
                return []
                
        except Exception as e:
            logger.error(f"轨迹采样失败: {str(e)}")
            return []
    
    def _distance_based_sampling(self, polyline_data: Dict) -> List[Dict]:
        """基于距离的采样
        
        Args:
            polyline_data: polyline格式的轨迹数据
            
        Returns:
            采样后的轨迹点列表
        """
        coordinates = polyline_data['polyline']
        timestamps = polyline_data['timestamps']
        speeds = polyline_data['speeds']
        avp_flags = polyline_data['avp_flags']
        workstages = polyline_data['workstages']
        
        interval = self.config['distance_interval']  # 米
        sampled_points = []
        
        # 将距离间隔从米转换为度（粗略转换，1度约等于111320米）
        interval_degrees = interval / 111320.0
        
        accumulated_distance = 0.0
        last_coord = coordinates[0]
        
        # 添加第一个点
        sampled_points.append({
            'coordinate': coordinates[0],
            'timestamp': timestamps[0],
            'speed': speeds[0],
            'avp_flag': avp_flags[0],
            'workstage': workstages[0],
            'original_index': 0
        })
        
        for i in range(1, len(coordinates)):
            current_coord = coordinates[i]
            
            # 计算相邻点距离（简化的欧几里得距离）
            dist = ((current_coord[0] - last_coord[0])**2 + 
                   (current_coord[1] - last_coord[1])**2)**0.5
            
            accumulated_distance += dist
            
            # 当累计距离达到采样间隔时，采样
            if accumulated_distance >= interval_degrees:
                sampled_points.append({
                    'coordinate': current_coord,
                    'timestamp': timestamps[i],
                    'speed': speeds[i],
                    'avp_flag': avp_flags[i],
                    'workstage': workstages[i],
                    'original_index': i
                })
                accumulated_distance = 0.0
                last_coord = current_coord
        
        # 确保包含最后一个点
        if len(sampled_points) == 0 or sampled_points[-1]['original_index'] != len(coordinates) - 1:
            sampled_points.append({
                'coordinate': coordinates[-1],
                'timestamp': timestamps[-1],
                'speed': speeds[-1],
                'avp_flag': avp_flags[-1],
                'workstage': workstages[-1],
                'original_index': len(coordinates) - 1
            })
        
        logger.debug(f"距离采样: {len(coordinates)} -> {len(sampled_points)} 点")
        return sampled_points
    
    def _time_based_sampling(self, polyline_data: Dict) -> List[Dict]:
        """基于时间的采样
        
        Args:
            polyline_data: polyline格式的轨迹数据
            
        Returns:
            采样后的轨迹点列表
        """
        coordinates = polyline_data['polyline']
        timestamps = polyline_data['timestamps']
        speeds = polyline_data['speeds']
        avp_flags = polyline_data['avp_flags']
        workstages = polyline_data['workstages']
        
        interval = self.config['time_interval']  # 秒
        sampled_points = []
        
        # 转换为毫秒
        interval_ms = interval * 1000
        
        # 添加第一个点
        sampled_points.append({
            'coordinate': coordinates[0],
            'timestamp': timestamps[0],
            'speed': speeds[0],
            'avp_flag': avp_flags[0],
            'workstage': workstages[0],
            'original_index': 0
        })
        
        last_timestamp = timestamps[0]
        
        for i in range(1, len(coordinates)):
            current_timestamp = timestamps[i]
            
            # 检查时间间隔
            if current_timestamp - last_timestamp >= interval_ms:
                sampled_points.append({
                    'coordinate': coordinates[i],
                    'timestamp': current_timestamp,
                    'speed': speeds[i],
                    'avp_flag': avp_flags[i],
                    'workstage': workstages[i],
                    'original_index': i
                })
                last_timestamp = current_timestamp
        
        # 确保包含最后一个点
        if len(sampled_points) == 0 or sampled_points[-1]['original_index'] != len(coordinates) - 1:
            sampled_points.append({
                'coordinate': coordinates[-1],
                'timestamp': timestamps[-1],
                'speed': speeds[-1],
                'avp_flag': avp_flags[-1],
                'workstage': workstages[-1],
                'original_index': len(coordinates) - 1
            })
        
        logger.debug(f"时间采样: {len(coordinates)} -> {len(sampled_points)} 点")
        return sampled_points
    
    def _uniform_sampling(self, polyline_data: Dict) -> List[Dict]:
        """均匀采样
        
        Args:
            polyline_data: polyline格式的轨迹数据
            
        Returns:
            采样后的轨迹点列表
        """
        coordinates = polyline_data['polyline']
        timestamps = polyline_data['timestamps']
        speeds = polyline_data['speeds']
        avp_flags = polyline_data['avp_flags']
        workstages = polyline_data['workstages']
        
        step = self.config['uniform_step']  # 点数间隔
        sampled_points = []
        
        # 均匀采样
        for i in range(0, len(coordinates), step):
            sampled_points.append({
                'coordinate': coordinates[i],
                'timestamp': timestamps[i],
                'speed': speeds[i],
                'avp_flag': avp_flags[i],
                'workstage': workstages[i],
                'original_index': i
            })
        
        # 确保包含最后一个点
        if len(sampled_points) == 0 or sampled_points[-1]['original_index'] != len(coordinates) - 1:
            sampled_points.append({
                'coordinate': coordinates[-1],
                'timestamp': timestamps[-1],
                'speed': speeds[-1],
                'avp_flag': avp_flags[-1],
                'workstage': workstages[-1],
                'original_index': len(coordinates) - 1
            })
        
        logger.debug(f"均匀采样: {len(coordinates)} -> {len(sampled_points)} 点")
        return sampled_points
        
    def sliding_window_analysis(self, sampled_points: List[Dict]) -> List[Dict]:
        """滑窗分析，识别车道变化
        
        Args:
            sampled_points: 采样后的轨迹点
            
        Returns:
            轨迹分段列表
        """
        if not sampled_points:
            logger.warning("采样点为空，无法进行滑窗分析")
            return []
        
        window_size = self.config['window_size']
        window_overlap = self.config['window_overlap']
        
        # 计算滑窗步长
        step_size = max(1, int(window_size * (1 - window_overlap)))
        
        segments = []
        current_lane = None
        segment_start = 0
        
        logger.debug(f"开始滑窗分析，点数: {len(sampled_points)}, 窗口大小: {window_size}, 步长: {step_size}")
        
        try:
            for i in range(0, len(sampled_points), step_size):
                # 获取当前窗口的点
                window_end = min(i + window_size, len(sampled_points))
                window_points = sampled_points[i:window_end]
                
                if not window_points:
                    continue
                
                # 计算窗口中心点
                center_point = self._calculate_window_center(window_points)
                
                # 查找最近车道
                nearest_lane = self.find_nearest_lane(center_point)
                
                # 检查车道是否变化
                if nearest_lane != current_lane:
                    # 结束当前segment（如果存在）
                    if current_lane is not None and segment_start < i:
                        segment_end = i
                        segment_points = sampled_points[segment_start:segment_end]
                        
                        segments.append({
                            'lane_id': current_lane,
                            'start_index': segment_start,
                            'end_index': segment_end,
                            'points': segment_points,
                            'points_count': len(segment_points)
                        })
                        
                        logger.debug(f"车道分段: {current_lane}, 点数: {len(segment_points)}")
                    
                    # 开始新segment
                    current_lane = nearest_lane
                    segment_start = i
                    
                    if nearest_lane:
                        logger.debug(f"检测到车道变化: {nearest_lane}")
                
                # 如果到达最后一个窗口，结束当前segment
                if window_end >= len(sampled_points):
                    if current_lane is not None:
                        segment_points = sampled_points[segment_start:]
                        
                        segments.append({
                            'lane_id': current_lane,
                            'start_index': segment_start,
                            'end_index': len(sampled_points),
                            'points': segment_points,
                            'points_count': len(segment_points)
                        })
                        
                        logger.debug(f"最终车道分段: {current_lane}, 点数: {len(segment_points)}")
                    break
            
            logger.info(f"滑窗分析完成，生成 {len(segments)} 个车道分段")
            return segments
            
        except Exception as e:
            logger.error(f"滑窗分析失败: {str(e)}")
            return []
    
    def _calculate_window_center(self, window_points: List[Dict]) -> Dict:
        """计算窗口中心点
        
        Args:
            window_points: 窗口内的轨迹点
            
        Returns:
            窗口中心点
        """
        if not window_points:
            return {}
        
        # 计算坐标中心
        total_lng = sum(point['coordinate'][0] for point in window_points)
        total_lat = sum(point['coordinate'][1] for point in window_points)
        
        center_lng = total_lng / len(window_points)
        center_lat = total_lat / len(window_points)
        
        # 计算时间中心
        total_time = sum(point['timestamp'] for point in window_points)
        center_time = total_time / len(window_points)
        
        # 计算速度平均值
        total_speed = sum(point['speed'] for point in window_points)
        avg_speed = total_speed / len(window_points)
        
        return {
            'coordinate': (center_lng, center_lat),
            'timestamp': int(center_time),
            'speed': avg_speed,
            'window_size': len(window_points)
        }
        
    def find_nearest_lane(self, point: Dict) -> Optional[str]:
        """查找最近的车道（基于trajectory_road_analysis结果）
        
        Args:
            point: 轨迹点
            
        Returns:
            最近车道的ID
        """
        if not point or 'coordinate' not in point:
            logger.warning("轨迹点数据无效")
            return None
        
        if not self.road_analysis_id:
            logger.error("未指定road_analysis_id，无法查找候选车道")
            return None
        
        coordinate = point['coordinate']
        lng, lat = coordinate[0], coordinate[1]
        
        road_lanes_table = self.config['road_analysis_lanes_table']
        max_distance = self.config['max_lane_distance']
        
        # 将距离从米转换为度（粗略转换）
        max_distance_degrees = max_distance / 111320.0
        
        try:
            # 从trajectory_road_analysis的结果中查找最近的车道
            sql = text(f"""
                SELECT 
                    lane_id, 
                    ST_Distance(geometry, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)) as distance,
                    lane_type,
                    road_id
                FROM {road_lanes_table}
                WHERE analysis_id = :road_analysis_id
                AND geometry IS NOT NULL
                AND ST_DWithin(geometry, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326), :max_distance)
                ORDER BY distance
                LIMIT 1
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(sql, {
                    'lng': lng,
                    'lat': lat,
                    'road_analysis_id': self.road_analysis_id,
                    'max_distance': max_distance_degrees
                })
                
                row = result.fetchone()
                
                if row:
                    lane_id = row[0]
                    distance = row[1]
                    lane_type = row[2]
                    road_id = row[3]
                    logger.debug(f"找到最近车道: {lane_id} (type: {lane_type}, road: {road_id}), 距离: {distance:.6f}度")
                    return str(lane_id)
                else:
                    logger.debug(f"未找到最大距离内的候选车道: ({lng:.6f}, {lat:.6f})")
                    return None
                    
        except Exception as e:
            logger.error(f"车道查询失败: {str(e)}")
            return None
        
    def create_lane_buffer(self, lane_id: str) -> Any:
        """为车道创建缓冲区（基于trajectory_road_analysis结果）
        
        Args:
            lane_id: 车道ID
            
        Returns:
            缓冲区几何
        """
        if not lane_id:
            logger.warning("车道ID为空")
            return None
        
        if not self.road_analysis_id:
            logger.error("未指定road_analysis_id，无法查找候选车道")
            return None
        
        road_lanes_table = self.config['road_analysis_lanes_table']
        buffer_radius = self.config['buffer_radius']
        
        # 将缓冲区半径从米转换为度（粗略转换）
        buffer_radius_degrees = buffer_radius / 111320.0
        
        try:
            # 从trajectory_road_analysis结果中查找车道几何并创建缓冲区
            sql = text(f"""
                SELECT ST_AsText(ST_Buffer(geometry, :buffer_radius)) as buffer_geom
                FROM {road_lanes_table}
                WHERE analysis_id = :road_analysis_id
                AND lane_id = :lane_id
                AND geometry IS NOT NULL
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(sql, {
                    'road_analysis_id': self.road_analysis_id,
                    'lane_id': int(lane_id),
                    'buffer_radius': buffer_radius_degrees
                })
                
                row = result.fetchone()
                
                if row:
                    buffer_wkt = row[0]
                    # 将WKT转换为Shapely几何对象
                    from shapely import wkt
                    buffer_geom = wkt.loads(buffer_wkt)
                    
                    logger.debug(f"创建车道缓冲区: {lane_id}, 半径: {buffer_radius}米")
                    return buffer_geom
                else:
                    logger.warning(f"未在候选车道中找到: {lane_id}")
                    return None
                    
        except Exception as e:
            logger.error(f"创建车道缓冲区失败: {lane_id}, 错误: {str(e)}")
            return None
        
    def filter_trajectory_by_buffer(self, trajectory_points: List[Dict], buffer_geom: Any) -> List[Dict]:
        """使用缓冲区过滤轨迹点
        
        Args:
            trajectory_points: 轨迹点列表
            buffer_geom: 缓冲区几何
            
        Returns:
            过滤后的轨迹点
        """
        if not trajectory_points or not buffer_geom:
            logger.warning("轨迹点或缓冲区几何为空")
            return []
        
        filtered_points = []
        
        try:
            for point in trajectory_points:
                if 'coordinate' not in point:
                    continue
                
                lng, lat = point['coordinate']
                
                # 创建点几何
                point_geom = Point(lng, lat)
                
                # 检查点是否在缓冲区内
                if buffer_geom.contains(point_geom) or buffer_geom.intersects(point_geom):
                    filtered_points.append(point)
            
            logger.debug(f"缓冲区过滤: {len(trajectory_points)} -> {len(filtered_points)} 点")
            return filtered_points
            
        except Exception as e:
            logger.error(f"缓冲区过滤失败: {str(e)}")
            return []
        
    def check_trajectory_quality(self, dataset_name: str, buffer_results: List[Dict]) -> Dict:
        """检查轨迹质量
        
        Args:
            dataset_name: 数据集名称
            buffer_results: 缓冲区分析结果
            
        Returns:
            质量检查结果
        """
        if not buffer_results:
            return {
                'dataset_name': dataset_name,
                'status': 'failed',
                'reason': '无缓冲区分析结果',
                'lanes_covered': [],
                'total_points': 0,
                'total_lanes': 0
            }
        
        try:
            # 1. 统计涉及的车道数量
            lanes_covered = set()
            total_points = 0
            
            for result in buffer_results:
                if 'lane_id' in result and result['lane_id']:
                    lanes_covered.add(result['lane_id'])
                
                if 'points_count' in result:
                    total_points += result['points_count']
            
            total_lanes = len(lanes_covered)
            min_points_threshold = self.config['min_points_single_lane']
            
            # 2. 应用过滤规则
            if total_lanes > 1:
                # 多车道情况：保留（无论点数多少）
                status = 'passed'
                reason = f'多车道轨迹，涉及{total_lanes}个车道'
            elif total_lanes == 1 and total_points >= min_points_threshold:
                # 单车道但点数足够：保留
                status = 'passed'
                reason = f'单车道轨迹但点数充足({total_points}点 >= {min_points_threshold}点)'
            elif total_lanes == 1:
                # 单车道且点数不足：丢弃
                status = 'failed'
                reason = f'单车道轨迹点数不足({total_points}点 < {min_points_threshold}点)'
            else:
                # 无车道覆盖：丢弃
                status = 'failed'
                reason = '无车道覆盖'
            
            result = {
                'dataset_name': dataset_name,
                'status': status,
                'reason': reason,
                'lanes_covered': list(lanes_covered),
                'total_points': total_points,
                'total_lanes': total_lanes
            }
            
            logger.debug(f"质量检查结果: {dataset_name} - {status} ({reason})")
            return result
            
        except Exception as e:
            logger.error(f"轨迹质量检查失败: {dataset_name}, 错误: {str(e)}")
            return {
                'dataset_name': dataset_name,
                'status': 'failed',
                'reason': f'质量检查异常: {str(e)}',
                'lanes_covered': [],
                'total_points': 0,
                'total_lanes': 0
            }
        
    def reconstruct_trajectory(self, dataset_name: str, quality_result: Dict) -> Optional[Dict]:
        """重构完整轨迹
        
        Args:
            dataset_name: 数据集名称
            quality_result: 质量检查结果
            
        Returns:
            重构后的轨迹数据
        """
        if not quality_result or quality_result.get('status') != 'passed':
            logger.debug(f"质量检查未通过，跳过轨迹重构: {dataset_name}")
            return None
        
        try:
            # 1. 查询该dataset_name的所有原始轨迹点
            original_points = fetch_trajectory_points(dataset_name)
            
            if original_points.empty:
                logger.warning(f"无法获取原始轨迹点: {dataset_name}")
                return None
            
            # 2. 按timestamp排序
            original_points = original_points.sort_values('timestamp')
            
            # 3. 构建完整轨迹线
            coordinates = []
            for _, row in original_points.iterrows():
                if pd.notna(row['longitude']) and pd.notna(row['latitude']):
                    coordinates.append((float(row['longitude']), float(row['latitude'])))
            
            if len(coordinates) < 2:
                logger.warning(f"有效坐标点不足，无法构建轨迹: {dataset_name}")
                return None
            
            # 4. 创建LineString几何
            trajectory_geom = LineString(coordinates)
            
            # 5. 计算轨迹统计
            speed_data = original_points['twist_linear'].dropna()
            avp_data = original_points['avp_flag'].dropna()
            
            reconstructed_data = {
                'dataset_name': dataset_name,
                'geometry': trajectory_geom,
                'start_time': int(original_points['timestamp'].min()),
                'end_time': int(original_points['timestamp'].max()),
                'duration': int(original_points['timestamp'].max() - original_points['timestamp'].min()),
                'total_points': len(original_points),
                'valid_coordinates': len(coordinates),
                'lanes_covered': quality_result['lanes_covered'],
                'total_lanes': quality_result['total_lanes'],
                'trajectory_length': trajectory_geom.length,  # 轨迹长度（度）
                
                # 速度统计
                'avg_speed': round(float(speed_data.mean()), 2) if len(speed_data) > 0 else 0.0,
                'max_speed': round(float(speed_data.max()), 2) if len(speed_data) > 0 else 0.0,
                'min_speed': round(float(speed_data.min()), 2) if len(speed_data) > 0 else 0.0,
                'std_speed': round(float(speed_data.std()), 2) if len(speed_data) > 1 else 0.0,
                
                # AVP统计
                'avp_ratio': round(float((avp_data == 1).mean()), 3) if len(avp_data) > 0 else 0.0,
                
                # 质量信息
                'quality_status': quality_result['status'],
                'quality_reason': quality_result['reason']
            }
            
            logger.debug(f"重构轨迹: {dataset_name}, 点数: {len(coordinates)}, 车道数: {quality_result['total_lanes']}")
            return reconstructed_data
            
        except Exception as e:
            logger.error(f"轨迹重构失败: {dataset_name}, 错误: {str(e)}")
            return None
        
    def simplify_trajectory(self, trajectory_geom: LineString) -> LineString:
        """简化轨迹几何
        
        Args:
            trajectory_geom: 轨迹几何
            
        Returns:
            简化后的轨迹几何
        """
        if not trajectory_geom or trajectory_geom.is_empty:
            logger.warning("轨迹几何为空，无法简化")
            return trajectory_geom
        
        tolerance = self.config['simplify_tolerance']
        
        # 将容差从米转换为度（粗略转换）
        tolerance_degrees = tolerance / 111320.0
        
        try:
            # 使用Douglas-Peucker算法简化轨迹
            simplified_geom = trajectory_geom.simplify(tolerance_degrees, preserve_topology=True)
            
            # 记录简化效果
            original_coords = len(trajectory_geom.coords)
            simplified_coords = len(simplified_geom.coords)
            
            logger.debug(f"轨迹简化: {original_coords} -> {simplified_coords} 点 "
                        f"(减少 {original_coords - simplified_coords} 点)")
            
            return simplified_geom
            
        except Exception as e:
            logger.error(f"轨迹简化失败: {str(e)}")
            return trajectory_geom  # 返回原始几何
        
    def create_database_tables(self) -> bool:
        """创建数据库表结构
        
        Returns:
            创建是否成功
        """
        try:
            with self.engine.connect() as conn:
                # 创建轨迹-车道分段表
                if not self._create_trajectory_segments_table(conn):
                    return False
                
                # 创建缓冲区分析结果表
                if not self._create_trajectory_buffer_table(conn):
                    return False
                
                # 创建轨迹质量检查表
                if not self._create_quality_check_table(conn):
                    return False
                
                logger.info("成功创建所有数据库表")
                return True
                
        except Exception as e:
            logger.error(f"创建数据库表失败: {str(e)}")
            return False
    
    def _create_trajectory_segments_table(self, conn) -> bool:
        """创建轨迹-车道分段表"""
        table_name = self.config['trajectory_segments_table']
        
        try:
            # 检查表是否已存在
            check_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            
            result = conn.execute(check_sql)
            if result.scalar():
                logger.info(f"轨迹分段表 {table_name} 已存在，跳过创建")
                return True
            
            logger.info(f"创建轨迹分段表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    scene_id TEXT NOT NULL,
                    data_name TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    segment_index INTEGER,
                    start_time BIGINT,
                    end_time BIGINT,
                    start_point_index INTEGER,
                    end_point_index INTEGER,
                    avg_speed NUMERIC(8,2),
                    max_speed NUMERIC(8,2),
                    min_speed NUMERIC(8,2),
                    segment_length NUMERIC(10,2),
                    original_points_count INTEGER,
                    simplified_points_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(create_sql)
            conn.commit()
            
            # 添加几何列
            geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2);
            """)
            
            conn.execute(geom_sql)
            conn.commit()
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_lane_id ON {table_name}(lane_id);
                CREATE INDEX idx_{table_name}_start_time ON {table_name}(start_time);
            """)
            
            conn.execute(index_sql)
            conn.commit()
            
            logger.info(f"成功创建轨迹分段表: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建轨迹分段表失败: {table_name}, 错误: {str(e)}")
            return False
    
    def _create_trajectory_buffer_table(self, conn) -> bool:
        """创建缓冲区分析结果表"""
        table_name = self.config['trajectory_buffer_table']
        
        try:
            # 检查表是否已存在
            check_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            
            result = conn.execute(check_sql)
            if result.scalar():
                logger.info(f"缓冲区分析表 {table_name} 已存在，跳过创建")
                return True
            
            logger.info(f"创建缓冲区分析表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    scene_id TEXT NOT NULL,
                    data_name TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    buffer_radius NUMERIC(6,2),
                    points_in_buffer INTEGER,
                    total_points INTEGER,
                    coverage_ratio NUMERIC(5,3),
                    trajectory_length NUMERIC(10,2),
                    avg_distance_to_lane NUMERIC(8,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(create_sql)
            conn.commit()
            
            # 添加几何列
            geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'filtered_trajectory', 4326, 'LINESTRING', 2);
            """)
            
            conn.execute(geom_sql)
            conn.commit()
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(filtered_trajectory);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_lane_id ON {table_name}(lane_id);
            """)
            
            conn.execute(index_sql)
            conn.commit()
            
            logger.info(f"成功创建缓冲区分析表: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建缓冲区分析表失败: {table_name}, 错误: {str(e)}")
            return False
    
    def _create_quality_check_table(self, conn) -> bool:
        """创建轨迹质量检查表"""
        table_name = self.config['quality_check_table']
        
        try:
            # 检查表是否已存在
            check_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            
            result = conn.execute(check_sql)
            if result.scalar():
                logger.info(f"质量检查表 {table_name} 已存在，跳过创建")
                return True
            
            logger.info(f"创建质量检查表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    scene_id TEXT NOT NULL,
                    data_name TEXT NOT NULL,
                    total_lanes_covered INTEGER,
                    total_points_in_buffer INTEGER,
                    quality_status TEXT NOT NULL,
                    failure_reason TEXT,
                    lanes_list TEXT[],
                    avg_speed NUMERIC(8,2),
                    max_speed NUMERIC(8,2),
                    min_speed NUMERIC(8,2),
                    avp_ratio NUMERIC(5,3),
                    trajectory_length NUMERIC(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(create_sql)
            conn.commit()
            
            # 添加几何列
            geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'reconstructed_trajectory', 4326, 'LINESTRING', 2);
            """)
            
            conn.execute(geom_sql)
            conn.commit()
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(reconstructed_trajectory);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_quality_status ON {table_name}(quality_status);
            """)
            
            conn.execute(index_sql)
            conn.commit()
            
            logger.info(f"成功创建质量检查表: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建质量检查表失败: {table_name}, 错误: {str(e)}")
            return False
        
    def save_results(self, results: List[Dict]) -> bool:
        """保存分析结果
        
        Args:
            results: 分析结果列表
            
        Returns:
            保存是否成功
        """
        if not results:
            logger.warning("没有结果可保存")
            return True
        
        try:
            # 保存到质量检查表
            saved_count = self._save_quality_results(results)
            
            if saved_count > 0:
                logger.info(f"成功保存 {saved_count} 条分析结果")
                return True
            else:
                logger.warning("没有有效结果被保存")
                return False
                
        except Exception as e:
            logger.error(f"保存分析结果失败: {str(e)}")
            return False
    
    def _save_quality_results(self, results: List[Dict]) -> int:
        """保存质量检查结果到数据库
        
        Args:
            results: 重构后的轨迹结果列表
            
        Returns:
            保存成功的记录数
        """
        if not results:
            return 0
        
        table_name = self.config['quality_check_table']
        
        try:
            # 准备GeoDataFrame数据
            gdf_data = []
            geometries = []
            
            for result in results:
                if 'geometry' not in result or not result['geometry']:
                    continue
                
                # 提取属性数据
                row = {
                    'scene_id': result.get('scene_id', ''),
                    'data_name': result.get('dataset_name', ''),
                    'total_lanes_covered': result.get('total_lanes', 0),
                    'total_points_in_buffer': result.get('total_points', 0),
                    'quality_status': result.get('quality_status', 'unknown'),
                    'failure_reason': result.get('quality_reason', ''),
                    'lanes_list': result.get('lanes_covered', []),
                    'avg_speed': result.get('avg_speed', 0.0),
                    'max_speed': result.get('max_speed', 0.0),
                    'min_speed': result.get('min_speed', 0.0),
                    'avp_ratio': result.get('avp_ratio', 0.0),
                    'trajectory_length': result.get('trajectory_length', 0.0)
                }
                
                gdf_data.append(row)
                geometries.append(result['geometry'])
            
            if not gdf_data:
                logger.warning("没有有效的几何数据可保存")
                return 0
            
            # 创建GeoDataFrame
            gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
            
            # 保存到数据库
            gdf.to_postgis(table_name, self.engine, if_exists='append', index=False)
            
            logger.info(f"成功保存 {len(gdf)} 条质量检查结果到 {table_name}")
            return len(gdf)
            
        except Exception as e:
            logger.error(f"保存质量检查结果失败: {str(e)}")
            return 0
        
    def process_scene_mappings(self, mappings_df: pd.DataFrame) -> Dict:
        """处理场景映射，执行完整的分析流程
        
        Args:
            mappings_df: 场景映射DataFrame
            
        Returns:
            处理统计信息
        """
        setup_signal_handlers()
        
        self.stats['total_scenes'] = len(mappings_df)
        self.stats['start_time'] = datetime.now()
        
        logger.info(f"开始处理 {len(mappings_df)} 个场景的轨迹车道分析")
        
        # 创建数据库表
        if not self.create_database_tables():
            logger.error("创建数据库表失败，退出处理")
            return self.stats
            
        # 查询缺失的data_name
        missing_data_names = mappings_df[mappings_df['data_name'].isna()]
        if len(missing_data_names) > 0:
            logger.info(f"需要查询 {len(missing_data_names)} 个scene_id对应的data_name")
            
            scene_ids_to_query = missing_data_names['scene_id'].tolist()
            db_mappings = fetch_data_names_from_scene_ids(scene_ids_to_query)
            
            if not db_mappings.empty:
                for idx, row in db_mappings.iterrows():
                    scene_id = row['scene_id']
                    data_name = row['data_name']
                    mask = (mappings_df['scene_id'] == scene_id) & (mappings_df['data_name'].isna())
                    mappings_df.loc[mask, 'data_name'] = data_name
                    
        # 统计仍然缺失data_name的记录
        still_missing = mappings_df[mappings_df['data_name'].isna()]
        if len(still_missing) > 0:
            self.stats['missing_data_names'] = len(still_missing)
            logger.warning(f"仍有 {len(still_missing)} 个scene_id无法获取data_name，将跳过处理")
            
        # 过滤出有效的映射
        valid_mappings = mappings_df[mappings_df['data_name'].notna()]
        
        # 主处理循环
        batch_results = []
        batch_size = self.config['batch_size']
        
        for i, (idx, row) in enumerate(valid_mappings.iterrows()):
            if interrupted:
                logger.info("处理被中断")
                break
                
            scene_id = row['scene_id']
            data_name = row['data_name']
            
            logger.info(f"处理场景 [{i+1}/{len(valid_mappings)}]: {scene_id} ({data_name})")
            
            try:
                # 1. 查询轨迹点
                points_df = fetch_trajectory_points(data_name)
                
                if points_df.empty:
                    self.stats['empty_scenes'] += 1
                    logger.warning(f"data_name无轨迹数据: {data_name}")
                    continue
                
                # 2. 构建polyline
                polyline_data = self.build_trajectory_polyline(scene_id, data_name, points_df)
                
                if not polyline_data:
                    self.stats['failed_scenes'] += 1
                    logger.warning(f"polyline构建失败: {scene_id} ({data_name})")
                    continue
                
                # 3. 轨迹采样
                sampled_points = self.sample_trajectory(polyline_data)
                
                # 4. 滑窗分析
                segments = self.sliding_window_analysis(sampled_points)
                
                # 5. 缓冲区分析
                buffer_results = []
                for segment in segments:
                    lane_id = segment['lane_id']
                    buffer_geom = self.create_lane_buffer(lane_id)
                    filtered_points = self.filter_trajectory_by_buffer(segment['points'], buffer_geom)
                    
                    if filtered_points:
                        buffer_results.append({
                            'lane_id': lane_id,
                            'points': filtered_points,
                            'points_count': len(filtered_points)
                        })
                
                # 6. 轨迹质量检查
                quality_result = self.check_trajectory_quality(data_name, buffer_results)
                
                if quality_result['status'] == 'passed':
                    self.stats['quality_passed'] += 1
                    
                    # 7. 轨迹重构
                    reconstructed = self.reconstruct_trajectory(data_name, quality_result)
                    
                    if reconstructed:
                        self.stats['total_reconstructed'] += 1
                        
                        # 8. 轨迹简化
                        if self.config['enable_simplification']:
                            reconstructed['geometry'] = self.simplify_trajectory(reconstructed['geometry'])
                        
                        # 9. 添加到批处理结果
                        reconstructed['scene_id'] = scene_id  # 确保包含scene_id
                        batch_results.append(reconstructed)
                        
                        self.stats['successful_trajectories'] += 1
                    else:
                        self.stats['failed_scenes'] += 1
                else:
                    self.stats['quality_failed'] += 1
                    logger.debug(f"质量检查未通过: {data_name}, 原因: {quality_result['reason']}")
                    
            except Exception as e:
                self.stats['failed_scenes'] += 1
                logger.error(f"处理场景失败: {scene_id} ({data_name}), 错误: {str(e)}")
                
            self.stats['processed_scenes'] += 1
            
            # 批量保存结果
            if len(batch_results) >= batch_size:
                if self.save_results(batch_results):
                    logger.info(f"批量保存完成，已处理 {self.stats['processed_scenes']} 个场景")
                batch_results = []
        
        # 保存剩余结果
        if batch_results:
            if self.save_results(batch_results):
                logger.info(f"最终批量保存完成")
            
        self.stats['end_time'] = datetime.now()
        self.stats['duration'] = self.stats['end_time'] - self.stats['start_time']
        
        return self.stats

def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='轨迹车道分析模块 - 轨迹与车道空间关系分析',
        epilog="""
前提条件:
  必须先运行 trajectory_road_analysis 获得道路分析结果，本模块基于其输出的候选车道进行精细分析
  
支持的输入格式:
  1. 文本文件 (.txt): 每行一个scene_id
  2. Dataset文件 (.json/.parquet): 包含scene_id和data_name映射的数据集文件
  
分析流程:
  trajectory_road_analysis结果 → 候选车道 → polyline → 采样 → 滑窗分析 → 车道分段 → buffer分析 → 质量检查 → 轨迹重构 → 结果输出
  
示例:
  # 第一步：运行道路分析（获得road_analysis_id）
  python -m spdatalab.fusion.trajectory_road_analysis --trajectory-id my_traj --trajectory-geom "LINESTRING(...)"
  
  # 第二步：基于道路分析结果进行车道分析
  python -m spdatalab.fusion.trajectory_lane_analysis --input scenes.txt --road-analysis-id trajectory_road_20241201_123456 --output-prefix my_analysis
  python -m spdatalab.fusion.trajectory_lane_analysis --input dataset.json --road-analysis-id trajectory_road_20241201_123456 --buffer-radius 20
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 输入参数
    parser.add_argument('--input', required=True, 
                       help='输入文件：scene_id列表(.txt)或dataset文件(.json/.parquet)')
    parser.add_argument('--output-prefix', default='trajectory_lane_analysis',
                       help='输出表名前缀')
    
    # 采样参数
    parser.add_argument('--sampling-strategy', choices=['distance', 'time', 'uniform'], 
                       default='distance', help='采样策略')
    parser.add_argument('--distance-interval', type=float, default=10.0,
                       help='距离采样间隔（米）')
    parser.add_argument('--time-interval', type=float, default=5.0,
                       help='时间采样间隔（秒）')
    parser.add_argument('--uniform-step', type=int, default=50,
                       help='均匀采样步长')
    
    # 滑窗参数
    parser.add_argument('--window-size', type=int, default=20,
                       help='滑窗大小（采样点数）')
    parser.add_argument('--window-overlap', type=float, default=0.5,
                       help='滑窗重叠率')
    
    # 车道分析参数
    parser.add_argument('--road-analysis-id', required=True,
                       help='trajectory_road_analysis的分析ID（必需，用于获取候选车道）')
    parser.add_argument('--road-lanes-table', default='trajectory_road_lanes',
                       help='道路分析结果车道表名')
    parser.add_argument('--buffer-radius', type=float, default=15.0,
                       help='车道缓冲区半径（米）')
    parser.add_argument('--max-lane-distance', type=float, default=50.0,
                       help='最大车道搜索距离（米）')
    
    # 质量检查参数
    parser.add_argument('--min-points-single-lane', type=int, default=5,
                       help='单车道最少点数')
    parser.add_argument('--disable-multi-lane-filter', action='store_true',
                       help='禁用多车道过滤')
    
    # 简化参数
    parser.add_argument('--simplify-tolerance', type=float, default=2.0,
                       help='轨迹简化容差（米）')
    parser.add_argument('--disable-simplification', action='store_true',
                       help='禁用轨迹简化')
    
    # 性能参数
    parser.add_argument('--batch-size', type=int, default=100,
                       help='批处理大小')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='最大并行工作线程数')
    
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
        config = {
            'sampling_strategy': args.sampling_strategy,
            'distance_interval': args.distance_interval,
            'time_interval': args.time_interval,
            'uniform_step': args.uniform_step,
            'window_size': args.window_size,
            'window_overlap': args.window_overlap,
            'road_analysis_lanes_table': args.road_lanes_table,
            'buffer_radius': args.buffer_radius,
            'max_lane_distance': args.max_lane_distance,
            'min_points_single_lane': args.min_points_single_lane,
            'enable_multi_lane_filter': not args.disable_multi_lane_filter,
            'simplify_tolerance': args.simplify_tolerance,
            'enable_simplification': not args.disable_simplification,
            'batch_size': args.batch_size,
            'max_workers': args.max_workers,
            'trajectory_segments_table': f"{args.output_prefix}_segments",
            'trajectory_buffer_table': f"{args.output_prefix}_buffer",
            'quality_check_table': f"{args.output_prefix}_quality"
        }
        
        # 加载scene_id和data_name映射
        logger.info(f"加载输入文件: {args.input}")
        mappings_df = load_scene_data_mappings(args.input)
        
        if mappings_df.empty:
            logger.error("未加载到任何scene_id映射")
            return 1
            
        # 输出配置信息
        logger.info(f"输出表前缀: {args.output_prefix}")
        logger.info(f"道路分析ID: {args.road_analysis_id}")
        logger.info(f"采样策略: {config['sampling_strategy']}")
        logger.info(f"候选车道表: {config['road_analysis_lanes_table']}")
        logger.info(f"缓冲区半径: {config['buffer_radius']}米")
        logger.info(f"单车道最少点数: {config['min_points_single_lane']}")
        
        # 创建分析器并执行分析
        analyzer = TrajectoryLaneAnalyzer(config, road_analysis_id=args.road_analysis_id)
        stats = analyzer.process_scene_mappings(mappings_df)
        
        # 输出统计信息
        logger.info("=== 处理完成 ===")
        logger.info(f"总场景数: {stats['total_scenes']}")
        logger.info(f"处理场景数: {stats['processed_scenes']}")
        logger.info(f"成功轨迹数: {stats['successful_trajectories']}")
        logger.info(f"失败场景数: {stats['failed_scenes']}")
        logger.info(f"空场景数: {stats['empty_scenes']}")
        logger.info(f"质量检查通过: {stats['quality_passed']}")
        logger.info(f"质量检查失败: {stats['quality_failed']}")
        logger.info(f"重构轨迹数: {stats['total_reconstructed']}")
        
        if stats.get('missing_data_names', 0) > 0:
            logger.info(f"缺失data_name数: {stats['missing_data_names']}")
            
        logger.info(f"处理时间: {stats['duration']}")
        
        return 0 if stats['successful_trajectories'] > 0 else 1
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 