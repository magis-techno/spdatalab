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
import time
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
POINT_TABLE = "ddi_data_points"  # Hive中的轨迹点表名（不带schema前缀）

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
    'points_limit_per_lane': 1000,    # 每个lane最多查询的轨迹点数
    'enable_time_filter': True,       # 启用时间过滤
    'recent_days': 30,                # 只查询最近N天的数据
    
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
    'quality_check_table': 'trajectory_quality_check',
    
    # 车道分析结果表名
    'lane_analysis_main_table': 'trajectory_lane_analysis',
    'lane_hits_table': 'trajectory_lane_hits', 
    'lane_trajectories_table': 'trajectory_lane_complete_trajectories'
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
    """轨迹车道分析器主类
    
    功能：基于输入轨迹查询其他相关轨迹
    1. 对输入轨迹执行road_analysis获得候选lanes
    2. 基于输入轨迹分段找到邻近的候选lanes
    3. 为lanes创建buffer，在轨迹点数据库中查询其他轨迹点
    4. 根据过滤规则保留符合条件的完整轨迹
    """
    
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
        
        # **新增验证**：如果提供了road_analysis_id，验证配置和数据一致性
        if self.road_analysis_id:
            self._validate_road_analysis_connection()
        
        # 不在初始化时创建表，在保存时动态创建
        
        self.stats = {
            'input_trajectories': 0,
            'processed_trajectories': 0,
            'candidate_lanes_found': 0,
            'buffer_queries_executed': 0,
            'trajectory_points_found': 0,
            'unique_data_names_found': 0,
            'trajectories_passed_filter': 0,
            'trajectories_multi_lane': 0,
            'trajectories_sufficient_points': 0,
            'start_time': None,
            'end_time': None
        }
    
    def _generate_dynamic_table_names(self, analysis_id: str) -> Dict[str, str]:
        """根据analysis_id生成动态表名
        
        Args:
            analysis_id: 分析ID
            
        Returns:
            包含各种表名的字典
        """
        # 简化表名生成逻辑，使用时间戳而不是完整的analysis_id
        # 避免PostgreSQL表名长度限制（63字符）
        
        # 提取时间戳部分
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 如果road_analysis_id存在，从中提取基础标识
        if self.road_analysis_id:
            # 从road_analysis_id中提取前缀部分
            # 例如：integrated_20250714_133413_road_f8f65ca59e094aa89f3121fa2510c506
            # 提取：integrated_20250714_133413
            if 'integrated_' in self.road_analysis_id:
                # 提取 integrated_YYYYMMDD_HHMMSS 部分
                parts = self.road_analysis_id.split('_')
                if len(parts) >= 3 and parts[0] == 'integrated':
                    base_name = f"{parts[0]}_{parts[1]}_{parts[2]}"
                else:
                    base_name = f"integrated_{timestamp}"
            else:
                base_name = f"integrated_{timestamp}"
            
            logger.info(f"基于road_analysis_id生成表名，基础名: {base_name}")
        else:
            # 如果没有road_analysis_id，使用当前时间戳
            base_name = f"integrated_{timestamp}"
            logger.info(f"基于当前时间生成表名，基础名: {base_name}")
        
        # 生成简化的表名
        table_names = {
            'lane_analysis_main_table': f"{base_name}_lanes",
            'lane_hits_table': f"{base_name}_lane_hits", 
            'lane_trajectories_table': f"{base_name}_lane_trajectories"
        }
        
        # 检查表名长度，PostgreSQL限制为63字符
        for table_type, table_name in table_names.items():
            if len(table_name) > 63:
                logger.warning(f"表名过长 ({len(table_name)} > 63): {table_name}")
                # 截断到安全长度
                table_names[table_type] = table_name[:63]
                logger.warning(f"截断后: {table_names[table_type]}")
        
        logger.info(f"生成的车道分析表名:")
        logger.info(f"  - 主表: {table_names['lane_analysis_main_table']} ({len(table_names['lane_analysis_main_table'])} chars)")
        logger.info(f"  - 命中表: {table_names['lane_hits_table']} ({len(table_names['lane_hits_table'])} chars)")
        logger.info(f"  - 轨迹表: {table_names['lane_trajectories_table']} ({len(table_names['lane_trajectories_table'])} chars)")
        
        return table_names
    
    def _validate_road_analysis_connection(self):
        """验证与道路分析结果的连接"""
        road_lanes_table = self.config['road_analysis_lanes_table']
        
        logger.info(f"验证道路分析连接: road_analysis_id={self.road_analysis_id}")
        logger.info(f"预期的道路分析结果表: {road_lanes_table}")
        
        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                table_check_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{road_lanes_table}'
                    );
                """)
                
                table_exists = conn.execute(table_check_sql).scalar()
                
                if not table_exists:
                    logger.error(f"❌ 道路分析结果表不存在: {road_lanes_table}")
                    logger.error("可能的原因:")
                    logger.error("1. 道路分析尚未完成")
                    logger.error("2. 表名配置不正确")
                    logger.error("3. 数据库连接问题")
                    return False
                
                logger.info(f"✓ 道路分析结果表存在: {road_lanes_table}")
                
                # 检查指定analysis_id的数据
                count_sql = text(f"""
                    SELECT COUNT(*) FROM {road_lanes_table}
                    WHERE analysis_id = :road_analysis_id
                """)
                
                lane_count = conn.execute(count_sql, {'road_analysis_id': self.road_analysis_id}).scalar()
                
                if lane_count == 0:
                    logger.error(f"❌ 表中没有找到analysis_id={self.road_analysis_id}的数据")
                    
                    # 查看表中实际有哪些analysis_id
                    available_ids_sql = text(f"""
                        SELECT DISTINCT analysis_id, COUNT(*) as lane_count
                        FROM {road_lanes_table}
                        GROUP BY analysis_id
                        ORDER BY lane_count DESC
                        LIMIT 5
                    """)
                    
                    available_results = conn.execute(available_ids_sql).fetchall()
                    if available_results:
                        logger.error("表中可用的analysis_id:")
                        for row in available_results:
                            logger.error(f"  - {row[0]} ({row[1]} lanes)")
                    else:
                        logger.error("表中没有任何数据")
                    
                    return False
                
                logger.info(f"✓ 找到 {lane_count} 个候选lanes (analysis_id={self.road_analysis_id})")
                
                # 获取一些统计信息
                stats_sql = text(f"""
                    SELECT 
                        lane_type,
                        COUNT(*) as count
                    FROM {road_lanes_table}
                    WHERE analysis_id = :road_analysis_id
                    GROUP BY lane_type
                    ORDER BY count DESC
                """)
                
                type_stats = conn.execute(stats_sql, {'road_analysis_id': self.road_analysis_id}).fetchall()
                if type_stats:
                    logger.info("候选lanes类型分布:")
                    for row in type_stats:
                        logger.info(f"  - {row[0]}: {row[1]} lanes")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ 验证道路分析连接失败: {e}")
            return False
    

    
    def analyze_trajectory_neighbors(self, input_trajectory_id: str, input_trajectory_geom: str) -> Dict[str, Any]:
        """分析输入轨迹的邻近轨迹
        
        Args:
            input_trajectory_id: 输入轨迹ID
            input_trajectory_geom: 输入轨迹几何WKT字符串
            
        Returns:
            分析结果字典
        """
        logger.info(f"开始轨迹邻近性分析: {input_trajectory_id}")
        
        self.stats['input_trajectories'] += 1
        self.stats['start_time'] = datetime.now()
        
        try:
            # 1. 对输入轨迹进行分段采样
            trajectory_segments = self._segment_input_trajectory(input_trajectory_geom)
            if not trajectory_segments:
                logger.warning(f"轨迹分段失败: {input_trajectory_id}")
                return {'error': '轨迹分段失败'}
            
            logger.info(f"输入轨迹分为 {len(trajectory_segments)} 段")
            
            # 2. 为每段找到邻近的候选lanes
            nearby_lanes = self._find_nearby_candidate_lanes(trajectory_segments)
            if not nearby_lanes:
                logger.warning(f"未找到邻近的候选lanes: {input_trajectory_id}")
                return {'error': '未找到邻近的候选lanes'}
            
            self.stats['candidate_lanes_found'] = len(nearby_lanes)
            logger.info(f"找到 {len(nearby_lanes)} 个邻近候选lanes")
            
            # 3. 为lanes创建buffer，查询轨迹点数据库
            trajectory_hits = self._query_trajectory_points_in_buffers(nearby_lanes)
            
            self.stats['buffer_queries_executed'] = len(nearby_lanes)
            self.stats['trajectory_points_found'] = sum(len(hits['points']) for hits in trajectory_hits.values())
            self.stats['unique_data_names_found'] = len(trajectory_hits)
            
            logger.info(f"在buffer中找到 {self.stats['trajectory_points_found']} 个轨迹点")
            logger.info(f"涉及 {self.stats['unique_data_names_found']} 个不同的data_name")
            
            # 4. 应用过滤规则
            filtered_trajectories = self._apply_filtering_rules(trajectory_hits)
            
            self.stats['trajectories_passed_filter'] = len(filtered_trajectories)
            
            # 5. 提取符合条件的完整轨迹
            complete_trajectories = self._extract_complete_trajectories(filtered_trajectories)
            
            self.stats['processed_trajectories'] += 1
            self.stats['end_time'] = datetime.now()
            
            logger.info(f"轨迹邻近性分析完成: {input_trajectory_id}")
            logger.info(f"符合条件的轨迹数: {len(complete_trajectories)}")
            
            # 生成分析ID
            analysis_id = f"lane_analysis_{input_trajectory_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 获取动态表名
            dynamic_table_names = self._generate_dynamic_table_names(analysis_id)
            
            # 保存分析结果到数据库
            self._save_lane_analysis_results(analysis_id, input_trajectory_id, trajectory_hits, complete_trajectories, dynamic_table_names)
            
            return {
                'input_trajectory_id': input_trajectory_id,
                'analysis_id': analysis_id,
                'candidate_lanes': nearby_lanes,
                'trajectory_hits': trajectory_hits,
                'filtered_trajectories': filtered_trajectories,
                'complete_trajectories': complete_trajectories,
                'stats': self.stats.copy()
            }
            
        except Exception as e:
            logger.error(f"轨迹邻近性分析失败: {input_trajectory_id}, 错误: {e}")
            return {'error': str(e)}
    
    def _segment_input_trajectory(self, trajectory_geom: str) -> List[Dict]:
        """对输入轨迹进行分段
        
        Args:
            trajectory_geom: 轨迹几何WKT字符串
            
        Returns:
            轨迹分段列表
        """
        try:
            from shapely import wkt
            trajectory = wkt.loads(trajectory_geom)
            
            if trajectory.geom_type != 'LineString':
                logger.error(f"轨迹几何类型错误: {trajectory.geom_type}，期望: LineString")
                return []
            
            # 获取轨迹坐标
            coords = list(trajectory.coords)
            if len(coords) < 2:
                logger.error(f"轨迹坐标点不足: {len(coords)}")
                return []
            
            # 按距离分段（每段约50米）
            segment_distance = 50.0  # 米
            segment_distance_degrees = segment_distance / 111320.0  # 转换为度
            
            segments = []
            current_start = 0
            accumulated_distance = 0.0
            
            for i in range(1, len(coords)):
                # 计算距离
                prev_coord = coords[i-1]
                curr_coord = coords[i]
                dist = ((curr_coord[0] - prev_coord[0])**2 + (curr_coord[1] - prev_coord[1])**2)**0.5
                accumulated_distance += dist
                
                # 当距离达到分段阈值时创建分段
                if accumulated_distance >= segment_distance_degrees or i == len(coords) - 1:
                    segment_coords = coords[current_start:i+1]
                    if len(segment_coords) >= 2:
                        segment_geom = LineString(segment_coords)
                        
                        # 计算分段中心点
                        center_coord = segment_geom.interpolate(0.5, normalized=True)
                        
                        segments.append({
                            'segment_id': len(segments),
                            'start_index': current_start,
                            'end_index': i,
                            'geometry': segment_geom,
                            'center_point': (center_coord.x, center_coord.y),
                            'length': accumulated_distance
                        })
                    
                    current_start = i
                    accumulated_distance = 0.0
            
            logger.debug(f"轨迹分段完成: {len(coords)} 个坐标点 → {len(segments)} 个分段")
            return segments
            
        except Exception as e:
            logger.error(f"轨迹分段失败: {e}")
            return []
    
    def _find_nearby_candidate_lanes(self, trajectory_segments: List[Dict]) -> List[Dict]:
        """为轨迹分段找到邻近的候选lanes
        
        Args:
            trajectory_segments: 轨迹分段列表
            
        Returns:
            邻近候选lanes列表
        """
        if not self.road_analysis_id:
            logger.error("未指定road_analysis_id，无法查找候选车道")
            return []
        
        road_lanes_table = self.config['road_analysis_lanes_table']
        max_distance = self.config['max_lane_distance']
        max_distance_degrees = max_distance / 111320.0
        
        nearby_lanes = []
        
        try:
            with self.engine.connect() as conn:
                for segment in trajectory_segments:
                    center_lng, center_lat = segment['center_point']
                    
                    # 查询该分段附近的候选lanes
                    sql = text(f"""
                        SELECT 
                            lane_id, 
                            ST_Distance(geometry, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)) as distance,
                            lane_type,
                            road_id,
                            ST_AsText(geometry) as geometry_wkt
                        FROM {road_lanes_table}
                        WHERE analysis_id = :road_analysis_id
                        AND geometry IS NOT NULL
                        AND ST_DWithin(geometry, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326), :max_distance)
                        ORDER BY distance
                        LIMIT 5
                    """)
                    
                    result = conn.execute(sql, {
                        'lng': center_lng,
                        'lat': center_lat,
                        'road_analysis_id': self.road_analysis_id,
                        'max_distance': max_distance_degrees
                    })
                    
                    segment_lanes = []
                    for row in result:
                        lane_info = {
                            'lane_id': row[0],
                            'distance': row[1],
                            'lane_type': row[2],
                            'road_id': row[3],
                            'geometry_wkt': row[4],
                            'segment_id': segment['segment_id']
                        }
                        segment_lanes.append(lane_info)
                    
                    if segment_lanes:
                        logger.debug(f"分段 {segment['segment_id']} 找到 {len(segment_lanes)} 个邻近lanes")
                        nearby_lanes.extend(segment_lanes)
            
            # 去重（同一个lane可能被多个分段找到）
            unique_lanes = {}
            for lane in nearby_lanes:
                lane_id = lane['lane_id']
                if lane_id not in unique_lanes or lane['distance'] < unique_lanes[lane_id]['distance']:
                    unique_lanes[lane_id] = lane
            
            result_lanes = list(unique_lanes.values())
            logger.info(f"去重后的邻近候选lanes: {len(result_lanes)} 个")
            
            return result_lanes
            
        except Exception as e:
            logger.error(f"查找邻近候选lanes失败: {e}")
            return []
    
    def _query_trajectory_points_in_buffers(self, nearby_lanes: List[Dict]) -> Dict[str, Dict]:
        """在lanes的buffer中查询轨迹点数据库
        
        Args:
            nearby_lanes: 邻近候选lanes列表
            
        Returns:
            按data_name分组的轨迹点命中结果
        """
        buffer_radius = self.config['buffer_radius']
        trajectory_hits = {}
        
        # 配置参数
        points_limit = self.config.get('points_limit_per_lane', 1000)
        enable_time_filter = self.config.get('enable_time_filter', True)
        recent_days = self.config.get('recent_days', 30)  # 只查询最近30天的数据
        
        logger.info(f"开始查询{len(nearby_lanes)}个lanes的buffer内轨迹点")
        logger.info(f"配置: buffer_radius={buffer_radius}m, points_limit={points_limit}, recent_days={recent_days}")
        
        try:
            # 使用hive_cursor连接dataset_gy1轨迹数据库
            with hive_cursor("dataset_gy1") as cur:
                for i, lane in enumerate(nearby_lanes):
                    logger.info(f"查询lane [{i+1}/{len(nearby_lanes)}]: {lane['lane_id']} (type: {lane['lane_type']})")
                    
                    # 构建时间过滤条件
                    time_filter = ""
                    if enable_time_filter:
                        # 最近N天的时间戳过滤（假设timestamp是Unix时间戳）
                        recent_timestamp = int(time.time()) - (recent_days * 24 * 3600)
                        time_filter = f"AND p.timestamp >= {recent_timestamp}"
                    
                    # 为lane创建buffer并查询轨迹点 (使用Hive/Trino兼容的空间函数)
                    sql = f"""
                        SELECT 
                            p.dataset_name,
                            p.timestamp,
                            ST_X(p.point_lla) as longitude,
                            ST_Y(p.point_lla) as latitude,
                            p.twist_linear,
                            p.avp_flag,
                            p.workstage
                        FROM {POINT_TABLE} p
                        WHERE p.point_lla IS NOT NULL
                        AND ST_DWithin(
                            p.point_lla,
                            ST_Buffer(
                                ST_SetSRID(ST_GeomFromText('{lane['geometry_wkt']}'), 4326),
                                {buffer_radius / 111320.0}
                            ),
                            0
                        )
                        {time_filter}
                        ORDER BY p.timestamp DESC
                        LIMIT {points_limit}
                    """
                    
                    # 在verbose模式下输出DataGrip可执行的SQL
                    if logger.isEnabledFor(logging.DEBUG):
                        datagrip_sql = f"""
-- 查询Lane {lane['lane_id']} buffer内的轨迹点 (dataset_gy1)
SELECT 
    p.dataset_name,
    p.timestamp,
    ST_X(p.point_lla) as longitude,
    ST_Y(p.point_lla) as latitude,
    p.twist_linear,
    p.avp_flag,
    p.workstage
FROM {POINT_TABLE} p
WHERE p.point_lla IS NOT NULL
AND ST_DWithin(
    p.point_lla,
    ST_Buffer(
        ST_SetSRID(ST_GeomFromText('{lane['geometry_wkt']}'), 4326),
        {buffer_radius / 111320.0}
    ),
    0
)
{time_filter}
ORDER BY p.timestamp DESC
LIMIT {points_limit};
"""
                        logger.debug(f"=== DataGrip可执行SQL (Lane {lane['lane_id']}) - dataset_gy1 ===")
                        logger.debug(datagrip_sql)
                    
                    # 执行查询
                    start_time = time.time()
                    cur.execute(sql)
                    result = cur.fetchall()
                    columns = [d[0] for d in cur.description]
                    query_time = time.time() - start_time
                    
                    points_found = 0
                    unique_data_names = set()
                    
                    # 处理Hive查询结果
                    for row in result:
                        # row是tuple，按照SQL select顺序解析
                        data_name = row[0]
                        timestamp = row[1]
                        longitude = row[2]
                        latitude = row[3]
                        twist_linear = row[4]
                        avp_flag = row[5]
                        workstage = row[6]
                        
                        unique_data_names.add(data_name)
                        
                        point_info = {
                            'timestamp': timestamp,
                            'longitude': longitude,
                            'latitude': latitude,
                            'twist_linear': twist_linear,
                            'avp_flag': avp_flag,
                            'workstage': workstage,
                            'lane_id': lane['lane_id'],
                            'lane_type': lane['lane_type']
                        }
                        
                        if data_name not in trajectory_hits:
                            trajectory_hits[data_name] = {
                                'data_name': data_name,
                                'points': [],
                                'lanes_touched': set(),
                                'lane_details': []
                            }
                        
                        trajectory_hits[data_name]['points'].append(point_info)
                        trajectory_hits[data_name]['lanes_touched'].add(lane['lane_id'])
                        
                        points_found += 1
                    
                    # 输出查询结果统计
                    if points_found > 0:
                        logger.info(f"✓ Lane {lane['lane_id']} buffer内找到 {points_found} 个轨迹点")
                        logger.info(f"  - 涉及 {len(unique_data_names)} 个不同data_name")
                        logger.info(f"  - 查询耗时: {query_time:.2f}秒")
                        
                        if points_found >= points_limit:
                            logger.warning(f"  - ⚠️ 达到限制({points_limit})，可能有更多数据未查询")
                    else:
                        logger.info(f"✗ Lane {lane['lane_id']} buffer内未找到轨迹点")
                        logger.info(f"  - 查询耗时: {query_time:.2f}秒")
            
            # 转换set为list以便序列化
            for data_name in trajectory_hits:
                trajectory_hits[data_name]['lanes_touched'] = list(trajectory_hits[data_name]['lanes_touched'])
                trajectory_hits[data_name]['total_points'] = len(trajectory_hits[data_name]['points'])
                trajectory_hits[data_name]['total_lanes'] = len(trajectory_hits[data_name]['lanes_touched'])
            
            logger.info(f"🎯 buffer查询完成，找到 {len(trajectory_hits)} 个data_name的轨迹点")
            
            # 输出详细统计
            if trajectory_hits:
                total_points = sum(hit['total_points'] for hit in trajectory_hits.values())
                multi_lane_count = sum(1 for hit in trajectory_hits.values() if hit['total_lanes'] > 1)
                
                logger.info(f"📊 查询统计:")
                logger.info(f"  - 总轨迹点数: {total_points}")
                logger.info(f"  - 多车道轨迹数: {multi_lane_count}")
                logger.info(f"  - 单车道轨迹数: {len(trajectory_hits) - multi_lane_count}")
                
                # 输出前几个data_name示例
                logger.info(f"  - 前5个data_name示例:")
                for i, (data_name, hit) in enumerate(list(trajectory_hits.items())[:5]):
                    logger.info(f"    {i+1}. {data_name}: {hit['total_points']}点, {hit['total_lanes']}lanes")
            
            return trajectory_hits
            
        except Exception as e:
            logger.error(f"查询buffer内轨迹点失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return {}
    
    def _apply_filtering_rules(self, trajectory_hits: Dict[str, Dict]) -> Dict[str, Dict]:
        """应用过滤规则
        
        Args:
            trajectory_hits: 轨迹点命中结果
            
        Returns:
            符合条件的轨迹
        """
        min_points_threshold = self.config.get('min_points_single_lane', 5)
        filtered = {}
        
        for data_name, hit_info in trajectory_hits.items():
            total_lanes = hit_info['total_lanes']
            total_points = hit_info['total_points']
            
            # 过滤规则1：与2个及以上lane相邻
            if total_lanes >= 2:
                filtered[data_name] = hit_info.copy()
                filtered[data_name]['filter_reason'] = f'multi_lane_{total_lanes}_lanes'
                self.stats['trajectories_multi_lane'] += 1
                logger.debug(f"保留轨迹 {data_name}: 多车道 ({total_lanes} lanes)")
                continue
            
            # 过滤规则2：命中点数超过阈值
            if total_points >= min_points_threshold:
                filtered[data_name] = hit_info.copy()
                filtered[data_name]['filter_reason'] = f'sufficient_points_{total_points}_points'
                self.stats['trajectories_sufficient_points'] += 1
                logger.debug(f"保留轨迹 {data_name}: 足够点数 ({total_points} points)")
                continue
            
            logger.debug(f"过滤掉轨迹 {data_name}: 单车道({total_lanes})且点数不足({total_points}<{min_points_threshold})")
        
        logger.info(f"过滤规则应用完成: {len(trajectory_hits)} → {len(filtered)}")
        logger.info(f"  - 多车道保留: {self.stats['trajectories_multi_lane']}")
        logger.info(f"  - 足够点数保留: {self.stats['trajectories_sufficient_points']}")
        
        return filtered
    
    def _fetch_complete_trajectory_from_hive(self, data_name: str) -> pd.DataFrame:
        """从Hive数据库获取完整轨迹数据
        
        Args:
            data_name: 数据名称
            
        Returns:
            包含轨迹点信息的DataFrame
        """
        try:
            with hive_cursor("dataset_gy1") as cur:
                # 查询完整轨迹数据
                sql = f"""
                    SELECT 
                        dataset_name,
                        timestamp,
                        ST_X(point_lla) as longitude,
                        ST_Y(point_lla) as latitude,
                        twist_linear,
                        avp_flag,
                        workstage
                    FROM {POINT_TABLE}
                    WHERE dataset_name = '{data_name}'
                    AND point_lla IS NOT NULL
                    ORDER BY timestamp ASC
                """
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"=== 获取完整轨迹SQL (dataset_gy1) ===")
                    logger.debug(f"data_name: {data_name}")
                    logger.debug(sql)
                
                cur.execute(sql)
                rows = cur.fetchall()
                
                if not rows:
                    logger.warning(f"未找到data_name的轨迹数据: {data_name}")
                    return pd.DataFrame()
                
                # 构建DataFrame
                columns = ['dataset_name', 'timestamp', 'longitude', 'latitude', 
                          'twist_linear', 'avp_flag', 'workstage']
                df = pd.DataFrame(rows, columns=columns)
                
                logger.debug(f"查询到 {len(df)} 个轨迹点: {data_name}")
                return df
                
        except Exception as e:
            logger.error(f"查询完整轨迹失败: {data_name}, 错误: {str(e)}")
            return pd.DataFrame()
    
    def _extract_complete_trajectories(self, filtered_trajectories: Dict[str, Dict]) -> Dict[str, Dict]:
        """提取符合条件的完整轨迹
        
        Args:
            filtered_trajectories: 符合条件的轨迹信息
            
        Returns:
            完整轨迹数据
        """
        complete_trajectories = {}
        
        for data_name, trajectory_info in filtered_trajectories.items():
            try:
                # 查询完整轨迹数据 (使用Hive连接)
                points_df = self._fetch_complete_trajectory_from_hive(data_name)
                
                if points_df.empty:
                    logger.warning(f"无法获取完整轨迹数据: {data_name}")
                    continue
                
                # 构建完整轨迹
                points_df = points_df.sort_values('timestamp')
                coordinates = []
                for _, row in points_df.iterrows():
                    if pd.notna(row['longitude']) and pd.notna(row['latitude']):
                        coordinates.append((float(row['longitude']), float(row['latitude'])))
                
                if len(coordinates) < 2:
                    logger.warning(f"轨迹坐标点不足: {data_name}")
                    continue
                
                trajectory_geom = LineString(coordinates)
                
                # 计算统计信息
                speed_data = points_df['twist_linear'].dropna()
                avp_data = points_df['avp_flag'].dropna()
                
                complete_trajectory = {
                    'data_name': data_name,
                    'geometry': trajectory_geom,
                    'geometry_wkt': trajectory_geom.wkt,
                    'start_time': int(points_df['timestamp'].min()),
                    'end_time': int(points_df['timestamp'].max()),
                    'duration': int(points_df['timestamp'].max() - points_df['timestamp'].min()),
                    'total_points': len(points_df),
                    'valid_coordinates': len(coordinates),
                    'trajectory_length': trajectory_geom.length,
                    
                    # 从过滤信息继承
                    'lanes_touched': trajectory_info['lanes_touched'],
                    'total_lanes': trajectory_info['total_lanes'],
                    'hit_points_count': trajectory_info['total_points'],
                    'filter_reason': trajectory_info['filter_reason'],
                    
                    # 速度统计
                    'avg_speed': round(float(speed_data.mean()), 2) if len(speed_data) > 0 else 0.0,
                    'max_speed': round(float(speed_data.max()), 2) if len(speed_data) > 0 else 0.0,
                    'min_speed': round(float(speed_data.min()), 2) if len(speed_data) > 0 else 0.0,
                    
                    # AVP统计
                    'avp_ratio': round(float((avp_data == 1).mean()), 3) if len(avp_data) > 0 else 0.0
                }
                
                complete_trajectories[data_name] = complete_trajectory
                logger.debug(f"提取完整轨迹: {data_name}, 点数: {len(coordinates)}")
                
            except Exception as e:
                logger.error(f"提取完整轨迹失败: {data_name}, 错误: {e}")
                continue
        
        logger.info(f"提取完整轨迹完成: {len(complete_trajectories)} 个")
        return complete_trajectories

    def _save_lane_analysis_results(self, analysis_id: str, input_trajectory_id: str, 
                                   trajectory_hits: Dict, complete_trajectories: Dict,
                                   dynamic_table_names: Dict[str, str]):
        """保存车道分析结果到数据库"""
        try:
            # 1. 保存主分析记录
            self._save_main_analysis_record(analysis_id, input_trajectory_id, complete_trajectories, dynamic_table_names)
            
            # 2. 保存轨迹命中记录
            self._save_trajectory_hits_records(analysis_id, trajectory_hits, dynamic_table_names)
            
            # 3. 保存完整轨迹记录
            self._save_complete_trajectories_records(analysis_id, complete_trajectories, dynamic_table_names)
            
            logger.info(f"✓ 车道分析结果已保存到数据库: {analysis_id}")
            
        except Exception as e:
            logger.error(f"保存车道分析结果失败: {analysis_id}, 错误: {e}")

    def _save_main_analysis_record(self, analysis_id: str, input_trajectory_id: str, complete_trajectories: Dict, dynamic_table_names: Dict[str, str]):
        """保存主分析记录"""
        table_name = dynamic_table_names['lane_analysis_main_table']
        
        # 检查表是否存在且结构正确
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        # 检查关键字段是否存在
        check_columns_sql = text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name IN ('analysis_id', 'input_trajectory_id', 'road_analysis_id', 'candidate_lanes_found')
        """)
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(check_table_sql).scalar()
            
            if table_exists:
                # 检查列是否存在
                existing_columns = conn.execute(check_columns_sql).fetchall()
                existing_column_names = {row[0] for row in existing_columns}
                required_columns = {'analysis_id', 'input_trajectory_id', 'road_analysis_id', 'candidate_lanes_found'}
                
                if not required_columns.issubset(existing_column_names):
                    logger.warning(f"表 {table_name} 结构不正确，缺少字段: {required_columns - existing_column_names}")
                    logger.warning(f"删除并重新创建表: {table_name}")
                    
                    # 删除旧表
                    drop_table_sql = text(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute(drop_table_sql)
                    conn.commit()
                    table_exists = False
            
            if not table_exists:
                # 创建新表
                create_table_sql = text(f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        analysis_id VARCHAR(100) NOT NULL,
                        input_trajectory_id VARCHAR(100) NOT NULL,
                        road_analysis_id VARCHAR(100),
                        candidate_lanes_found INTEGER DEFAULT 0,
                        trajectory_points_found INTEGER DEFAULT 0,
                        unique_data_names_found INTEGER DEFAULT 0,
                        trajectories_passed_filter INTEGER DEFAULT 0,
                        trajectories_multi_lane INTEGER DEFAULT 0,
                        trajectories_sufficient_points INTEGER DEFAULT 0,
                        complete_trajectories_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute(create_table_sql)
                
                # 创建索引
                create_indexes_sql = text(f"""
                    CREATE INDEX idx_{table_name.replace('-', '_')}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX idx_{table_name.replace('-', '_')}_input_trajectory_id ON {table_name}(input_trajectory_id);
                """)
                
                conn.execute(create_indexes_sql)
                conn.commit()
                
                logger.info(f"✓ 创建表: {table_name}")
        
        # 先删除可能存在的记录（避免重复）
        delete_sql = text(f"""
            DELETE FROM {table_name} 
            WHERE analysis_id = :analysis_id AND input_trajectory_id = :input_trajectory_id
        """)
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, input_trajectory_id, road_analysis_id, candidate_lanes_found,
             trajectory_points_found, unique_data_names_found, trajectories_passed_filter,
             trajectories_multi_lane, trajectories_sufficient_points, complete_trajectories_count)
            VALUES (
                :analysis_id, :input_trajectory_id, :road_analysis_id, :candidate_lanes_found,
                :trajectory_points_found, :unique_data_names_found, :trajectories_passed_filter,
                :trajectories_multi_lane, :trajectories_sufficient_points, :complete_trajectories_count
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {
                'analysis_id': analysis_id,
                'input_trajectory_id': input_trajectory_id
            })
            
            # 插入新记录
            conn.execute(insert_sql, {
                'analysis_id': analysis_id,
                'input_trajectory_id': input_trajectory_id,
                'road_analysis_id': self.road_analysis_id,
                'candidate_lanes_found': self.stats.get('candidate_lanes_found', 0),
                'trajectory_points_found': self.stats.get('trajectory_points_found', 0),
                'unique_data_names_found': self.stats.get('unique_data_names_found', 0),
                'trajectories_passed_filter': self.stats.get('trajectories_passed_filter', 0),
                'trajectories_multi_lane': self.stats.get('trajectories_multi_lane', 0),
                'trajectories_sufficient_points': self.stats.get('trajectories_sufficient_points', 0),
                'complete_trajectories_count': len(complete_trajectories)
            })
            conn.commit()
            
        logger.info(f"✓ 主分析记录已保存到表: {table_name}")

    def _save_trajectory_hits_records(self, analysis_id: str, trajectory_hits: Dict, dynamic_table_names: Dict[str, str]):
        """保存轨迹命中记录"""
        table_name = dynamic_table_names['lane_hits_table']
        
        if not trajectory_hits:
            return
        
        # 检查表是否存在且结构正确
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        # 检查表结构是否正确
        check_columns_sql = text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name IN ('data_name', 'total_points', 'total_lanes', 'filter_reason')
        """)
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(check_table_sql).scalar()
            
            if table_exists:
                # 检查列是否存在
                existing_columns = conn.execute(check_columns_sql).fetchall()
                existing_column_names = {row[0] for row in existing_columns}
                required_columns = {'data_name', 'total_points', 'total_lanes', 'filter_reason'}
                
                if not required_columns.issubset(existing_column_names):
                    logger.warning(f"表 {table_name} 结构不正确，缺少字段: {required_columns - existing_column_names}")
                    logger.warning(f"删除并重新创建表: {table_name}")
                    
                    # 删除旧表
                    drop_table_sql = text(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute(drop_table_sql)
                    conn.commit()
                    table_exists = False
            
            if not table_exists:
                # 创建新表
                create_table_sql = text(f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        analysis_id VARCHAR(100) NOT NULL,
                        data_name VARCHAR(255) NOT NULL,
                        total_points INTEGER DEFAULT 0,
                        total_lanes INTEGER DEFAULT 0,
                        filter_reason VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_{table_name.replace('-', '_')}_analysis_id (analysis_id),
                        INDEX idx_{table_name.replace('-', '_')}_data_name (data_name)
                    )
                """)
                
                # 修正索引语法为PostgreSQL兼容
                create_table_sql = text(f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        analysis_id VARCHAR(100) NOT NULL,
                        data_name VARCHAR(255) NOT NULL,
                        total_points INTEGER DEFAULT 0,
                        total_lanes INTEGER DEFAULT 0,
                        filter_reason VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute(create_table_sql)
                
                # 创建索引
                create_indexes_sql = text(f"""
                    CREATE INDEX idx_{table_name.replace('-', '_')}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX idx_{table_name.replace('-', '_')}_data_name ON {table_name}(data_name);
                """)
                
                conn.execute(create_indexes_sql)
                conn.commit()
                
                logger.info(f"✓ 创建表: {table_name}")
        
        # 先删除可能存在的记录（避免重复）
        delete_sql = text(f"""
            DELETE FROM {table_name} 
            WHERE analysis_id = :analysis_id
        """)
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, data_name, total_points, total_lanes, filter_reason)
            VALUES (:analysis_id, :data_name, :total_points, :total_lanes, :filter_reason)
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for data_name, hit_info in trajectory_hits.items():
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'data_name': data_name,
                    'total_points': hit_info.get('total_points', 0),
                    'total_lanes': hit_info.get('total_lanes', 0),
                    'filter_reason': hit_info.get('filter_reason', 'hit_found')
                })
            conn.commit()
            
        logger.info(f"✓ 轨迹命中记录已保存到表: {table_name} ({len(trajectory_hits)} 条记录)")

    def _save_complete_trajectories_records(self, analysis_id: str, complete_trajectories: Dict, dynamic_table_names: Dict[str, str]):
        """保存完整轨迹记录"""
        table_name = dynamic_table_names['lane_trajectories_table']
        
        if not complete_trajectories:
            return
        
        # 检查表是否存在且结构正确
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        # 检查关键字段是否存在
        check_columns_sql = text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name IN ('analysis_id', 'data_name', 'filter_reason', 'lanes_touched_count')
        """)
        
        # 检查几何列是否存在
        check_geometry_sql = text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
            AND column_name = 'geometry'
        """)
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(check_table_sql).scalar()
            
            if table_exists:
                # 检查列是否存在
                existing_columns = conn.execute(check_columns_sql).fetchall()
                existing_column_names = {row[0] for row in existing_columns}
                required_columns = {'analysis_id', 'data_name', 'filter_reason', 'lanes_touched_count'}
                
                # 检查几何列
                geometry_columns = conn.execute(check_geometry_sql).fetchall()
                has_geometry = len(geometry_columns) > 0
                
                if not required_columns.issubset(existing_column_names) or not has_geometry:
                    logger.warning(f"表 {table_name} 结构不正确")
                    logger.warning(f"缺少字段: {required_columns - existing_column_names}")
                    logger.warning(f"几何列存在: {has_geometry}")
                    logger.warning(f"删除并重新创建表: {table_name}")
                    
                    # 删除旧表
                    drop_table_sql = text(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute(drop_table_sql)
                    conn.commit()
                    table_exists = False
            
            if not table_exists:
                # 创建新表
                create_table_sql = text(f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        analysis_id VARCHAR(100) NOT NULL,
                        data_name VARCHAR(255) NOT NULL,
                        filter_reason VARCHAR(100),
                        lanes_touched_count INTEGER DEFAULT 0,
                        hit_points_count INTEGER DEFAULT 0,
                        total_points INTEGER DEFAULT 0,
                        valid_coordinates INTEGER DEFAULT 0,
                        trajectory_length FLOAT,
                        avg_speed FLOAT,
                        max_speed FLOAT,
                        min_speed FLOAT,
                        avp_ratio FLOAT,
                        start_time BIGINT,
                        end_time BIGINT,
                        duration BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute(create_table_sql)
                
                # 添加几何列
                add_geometry_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2)
                """)
                
                try:
                    conn.execute(add_geometry_sql)
                    logger.info(f"✓ 添加几何列到表: {table_name}")
                except Exception as e:
                    logger.warning(f"添加几何列失败: {e}")
                
                # 创建索引
                create_indexes_sql = text(f"""
                    CREATE INDEX idx_{table_name.replace('-', '_')}_analysis_id ON {table_name}(analysis_id);
                    CREATE INDEX idx_{table_name.replace('-', '_')}_data_name ON {table_name}(data_name);
                """)
                
                try:
                    conn.execute(create_indexes_sql)
                    # 创建几何索引
                    create_geom_index_sql = text(f"""
                        CREATE INDEX idx_{table_name.replace('-', '_')}_geometry ON {table_name} USING GIST(geometry);
                    """)
                    conn.execute(create_geom_index_sql)
                    logger.info(f"✓ 创建索引: {table_name}")
                except Exception as e:
                    logger.warning(f"创建索引失败: {e}")
                
                conn.commit()
                logger.info(f"✓ 创建表: {table_name}")
        
        # 先删除可能存在的记录（避免重复）
        delete_sql = text(f"""
            DELETE FROM {table_name} 
            WHERE analysis_id = :analysis_id
        """)
        
        insert_sql = text(f"""
            INSERT INTO {table_name} 
            (analysis_id, data_name, filter_reason, lanes_touched_count, hit_points_count,
             total_points, valid_coordinates, trajectory_length, avg_speed, max_speed,
             min_speed, avp_ratio, start_time, end_time, duration, geometry)
            VALUES (
                :analysis_id, :data_name, :filter_reason, :lanes_touched_count, :hit_points_count,
                :total_points, :valid_coordinates, :trajectory_length, :avg_speed, :max_speed,
                :min_speed, :avp_ratio, :start_time, :end_time, :duration,
                ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
            )
        """)
        
        with self.engine.connect() as conn:
            # 删除现有记录
            conn.execute(delete_sql, {'analysis_id': analysis_id})
            
            # 插入新记录
            for data_name, trajectory in complete_trajectories.items():
                conn.execute(insert_sql, {
                    'analysis_id': analysis_id,
                    'data_name': data_name,
                    'filter_reason': trajectory.get('filter_reason', ''),
                    'lanes_touched_count': len(trajectory.get('lanes_touched', [])),
                    'hit_points_count': trajectory.get('hit_points_count', 0),
                    'total_points': trajectory.get('total_points', 0),
                    'valid_coordinates': trajectory.get('valid_coordinates', 0),
                    'trajectory_length': trajectory.get('trajectory_length', 0.0),
                    'avg_speed': trajectory.get('avg_speed', 0.0),
                    'max_speed': trajectory.get('max_speed', 0.0),
                    'min_speed': trajectory.get('min_speed', 0.0),
                    'avp_ratio': trajectory.get('avp_ratio', 0.0),
                    'start_time': trajectory.get('start_time', 0),
                    'end_time': trajectory.get('end_time', 0),
                    'duration': trajectory.get('duration', 0),
                    'geometry_wkt': trajectory.get('geometry_wkt', '')
                })
            conn.commit()
            
        logger.info(f"✓ 完整轨迹记录已保存到表: {table_name} ({len(complete_trajectories)} 条记录)")


def batch_analyze_lanes_from_road_results(
    road_analysis_results: List[Tuple[str, str, Dict[str, Any]]],
    batch_analysis_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    基于道路分析结果批量进行车道分析（轨迹邻近性查询）
    
    Args:
        road_analysis_results: 道路分析结果列表 [(trajectory_id, road_analysis_id, summary), ...]
        batch_analysis_id: 批量分析ID（可选，自动生成）
        config: 轨迹车道分析配置
        
    Returns:
        车道分析结果列表 [(trajectory_id, lane_analysis_id, summary), ...]
    """
    if not batch_analysis_id:
        batch_analysis_id = f"batch_lane_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"开始批量轨迹邻近性分析: {batch_analysis_id}")
    logger.info(f"道路分析结果数: {len(road_analysis_results)}")
    
    # 过滤出成功的道路分析结果
    successful_road_results = [r for r in road_analysis_results if r[1] is not None]
    
    if not successful_road_results:
        logger.warning("没有成功的道路分析结果，跳过车道分析")
        return []
    
    logger.info(f"有效的道路分析结果: {len(successful_road_results)}")
    
    # 批量车道分析
    lane_results = []
    
    for i, (trajectory_id, road_analysis_id, road_summary) in enumerate(successful_road_results):
        try:
            logger.info(f"分析轨迹邻近性 [{i+1}/{len(successful_road_results)}]: {trajectory_id}")
            
            # 获取轨迹几何信息
            input_trajectory_geom = road_summary.get('input_trajectory_geom')
            if not input_trajectory_geom:
                logger.error(f"道路分析结果中缺少轨迹几何信息: {trajectory_id}")
                lane_results.append((trajectory_id, None, {
                    'error': '道路分析结果中缺少轨迹几何信息',
                    'road_analysis_id': road_analysis_id
                }))
                continue
            
            # **关键修复**：确保配置中包含正确的道路分析结果表名
            analyzer_config = (config or {}).copy()
            # 从road_analysis_id中提取批量分析ID，构造正确的表名
            # road_analysis_id格式: integrated_YYYYMMDD_HHMMSS_road_trajectory_id
            # 道路分析结果表名格式: integrated_YYYYMMDD_HHMMSS_road_lanes
            if '_road_' in road_analysis_id:
                batch_part = road_analysis_id.split('_road_')[0]
                lanes_table_name = f"{batch_part}_road_lanes"
            else:
                # 兜底逻辑：直接使用road_analysis_id构造表名
                lanes_table_name = f"{road_analysis_id}_lanes"
            
            analyzer_config['road_analysis_lanes_table'] = lanes_table_name
            
            logger.info(f"车道分析器配置: road_analysis_id={road_analysis_id}, lanes_table={lanes_table_name}")
            
            # 创建车道分析器
            analyzer = TrajectoryLaneAnalyzer(config=analyzer_config, road_analysis_id=road_analysis_id)
            
            # 执行邻近性分析
            analysis_result = analyzer.analyze_trajectory_neighbors(trajectory_id, input_trajectory_geom)
            
            if 'error' in analysis_result:
                lane_results.append((trajectory_id, None, {
                    'error': analysis_result['error'],
                    'road_analysis_id': road_analysis_id
                }))
                continue
            
            # 生成车道分析ID
            lane_analysis_id = f"{batch_analysis_id}_{trajectory_id}"
            
            # 获取动态表名
            dynamic_table_names = analyzer._generate_dynamic_table_names(lane_analysis_id)
            
            # 构建车道分析汇总
            lane_summary = {
                'lane_analysis_id': lane_analysis_id,
                'road_analysis_id': road_analysis_id,
                'trajectory_id': trajectory_id,
                'input_trajectory_geom': input_trajectory_geom,
                'candidate_lanes_found': analysis_result['stats']['candidate_lanes_found'],
                'trajectory_points_found': analysis_result['stats']['trajectory_points_found'],
                'unique_data_names_found': analysis_result['stats']['unique_data_names_found'],
                'trajectories_passed_filter': analysis_result['stats']['trajectories_passed_filter'],
                'trajectories_multi_lane': analysis_result['stats']['trajectories_multi_lane'],
                'trajectories_sufficient_points': analysis_result['stats']['trajectories_sufficient_points'],
                'complete_trajectories_count': len(analysis_result['complete_trajectories']),
                'complete_trajectories': analysis_result['complete_trajectories'],
                'duration': analysis_result['stats'].get('end_time', datetime.now()) - analysis_result['stats'].get('start_time', datetime.now()),
                'properties': road_summary.get('properties', {})
            }
            
            lane_results.append((trajectory_id, lane_analysis_id, lane_summary))
            
            logger.info(f"✓ 完成轨迹邻近性分析: {trajectory_id}")
            logger.info(f"  - 找到 {lane_summary['candidate_lanes_found']} 个候选lanes")
            logger.info(f"  - 找到 {lane_summary['trajectory_points_found']} 个轨迹点")
            logger.info(f"  - 符合条件的轨迹: {lane_summary['complete_trajectories_count']} 个")
            
        except Exception as e:
            logger.error(f"分析轨迹邻近性失败: {trajectory_id}, 错误: {e}")
            lane_results.append((trajectory_id, None, {
                'error': str(e),
                'road_analysis_id': road_analysis_id,
                'properties': road_summary.get('properties', {})
            }))
    
    # 统计结果
    successful_count = len([r for r in lane_results if r[1] is not None])
    failed_count = len(lane_results) - successful_count
    
    logger.info(f"批量轨迹邻近性分析完成: {batch_analysis_id}")
    logger.info(f"  - 成功: {successful_count}")
    logger.info(f"  - 失败: {failed_count}")
    logger.info(f"  - 总计: {len(lane_results)}")
    
    return lane_results


def batch_analyze_lanes_from_trajectory_records(
    trajectory_records: List,
    road_analysis_results: List[Tuple[str, str, Dict[str, Any]]],
    batch_analysis_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    基于轨迹记录和道路分析结果批量进行车道分析（轨迹邻近性查询）
    
    Args:
        trajectory_records: 轨迹记录列表 (TrajectoryRecord对象)
        road_analysis_results: 道路分析结果列表 [(trajectory_id, road_analysis_id, summary), ...]
        batch_analysis_id: 批量分析ID（可选，自动生成）
        config: 轨迹车道分析配置
        
    Returns:
        车道分析结果列表 [(trajectory_id, lane_analysis_id, summary), ...]
    """
    if not batch_analysis_id:
        batch_analysis_id = f"batch_lane_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"开始批量轨迹邻近性分析: {batch_analysis_id}")
    logger.info(f"轨迹记录数: {len(trajectory_records)}")
    logger.info(f"道路分析结果数: {len(road_analysis_results)}")
    
    # 创建trajectory_id到road_analysis结果的映射
    road_analysis_map = {r[0]: (r[1], r[2]) for r in road_analysis_results if r[1] is not None}
    
    # 创建trajectory_id到轨迹几何的映射
    trajectory_geom_map = {t.scene_id: t.geometry_wkt for t in trajectory_records}
    
    # 过滤出有对应道路分析结果的轨迹记录
    valid_trajectories = [t for t in trajectory_records if t.scene_id in road_analysis_map]
    
    if not valid_trajectories:
        logger.warning("没有找到有效的轨迹记录与道路分析结果的匹配")
        return []
    
    logger.info(f"有效的轨迹记录: {len(valid_trajectories)}")
    
    # 批量车道分析
    lane_results = []
    
    for i, trajectory in enumerate(valid_trajectories):
        try:
            trajectory_id = trajectory.scene_id
            road_analysis_id, road_summary = road_analysis_map[trajectory_id]
            input_trajectory_geom = trajectory_geom_map[trajectory_id]
            
            logger.info(f"分析轨迹邻近性 [{i+1}/{len(valid_trajectories)}]: {trajectory_id}")
            
            # **关键修复**：确保配置中包含正确的道路分析结果表名
            analyzer_config = (config or {}).copy()
            # 从road_analysis_id中提取批量分析ID，构造正确的表名
            # road_analysis_id格式: integrated_YYYYMMDD_HHMMSS_road_trajectory_id
            # 道路分析结果表名格式: integrated_YYYYMMDD_HHMMSS_road_lanes
            if '_road_' in road_analysis_id:
                batch_part = road_analysis_id.split('_road_')[0]
                lanes_table_name = f"{batch_part}_road_lanes"
            else:
                # 兜底逻辑：直接使用road_analysis_id构造表名
                lanes_table_name = f"{road_analysis_id}_lanes"
            
            analyzer_config['road_analysis_lanes_table'] = lanes_table_name
            
            logger.info(f"车道分析器配置: road_analysis_id={road_analysis_id}, lanes_table={lanes_table_name}")
            
            # 创建车道分析器
            analyzer = TrajectoryLaneAnalyzer(config=analyzer_config, road_analysis_id=road_analysis_id)
            
            # 执行邻近性分析
            analysis_result = analyzer.analyze_trajectory_neighbors(trajectory_id, input_trajectory_geom)
            
            if 'error' in analysis_result:
                lane_results.append((trajectory_id, None, {
                    'error': analysis_result['error'],
                    'road_analysis_id': road_analysis_id,
                    'data_name': trajectory.data_name
                }))
                continue
            
            # 生成车道分析ID
            lane_analysis_id = f"{batch_analysis_id}_{trajectory_id}"
            
            # 获取动态表名
            dynamic_table_names = analyzer._generate_dynamic_table_names(lane_analysis_id)
            
            # 构建车道分析汇总
            lane_summary = {
                'lane_analysis_id': lane_analysis_id,
                'road_analysis_id': road_analysis_id,
                'trajectory_id': trajectory_id,
                'data_name': trajectory.data_name,
                'input_trajectory_geom': input_trajectory_geom,
                'candidate_lanes_found': analysis_result['stats']['candidate_lanes_found'],
                'trajectory_points_found': analysis_result['stats']['trajectory_points_found'],
                'unique_data_names_found': analysis_result['stats']['unique_data_names_found'],
                'trajectories_passed_filter': analysis_result['stats']['trajectories_passed_filter'],
                'trajectories_multi_lane': analysis_result['stats']['trajectories_multi_lane'],
                'trajectories_sufficient_points': analysis_result['stats']['trajectories_sufficient_points'],
                'complete_trajectories_count': len(analysis_result['complete_trajectories']),
                'complete_trajectories': analysis_result['complete_trajectories'],
                'duration': analysis_result['stats'].get('end_time', datetime.now()) - analysis_result['stats'].get('start_time', datetime.now()),
                'properties': trajectory.properties
            }
            
            lane_results.append((trajectory_id, lane_analysis_id, lane_summary))
            
            logger.info(f"✓ 完成轨迹邻近性分析: {trajectory_id}")
            logger.info(f"  - 找到 {lane_summary['candidate_lanes_found']} 个候选lanes")
            logger.info(f"  - 找到 {lane_summary['trajectory_points_found']} 个轨迹点")
            logger.info(f"  - 符合条件的轨迹: {lane_summary['complete_trajectories_count']} 个")
            
        except Exception as e:
            logger.error(f"分析轨迹邻近性失败: {trajectory_id}, 错误: {e}")
            lane_results.append((trajectory_id, None, {
                'error': str(e),
                'road_analysis_id': road_analysis_map.get(trajectory_id, (None, {}))[0],
                'data_name': trajectory.data_name,
                'properties': trajectory.properties
            }))
    
    # 统计结果
    successful_count = len([r for r in lane_results if r[1] is not None])
    failed_count = len(lane_results) - successful_count
    
    logger.info(f"批量轨迹邻近性分析完成: {batch_analysis_id}")
    logger.info(f"  - 成功: {successful_count}")
    logger.info(f"  - 失败: {failed_count}")
    logger.info(f"  - 总计: {len(lane_results)}")
    
    return lane_results


def create_batch_lane_analysis_report(
    lane_results: List[Tuple[str, str, Dict[str, Any]]],
    batch_analysis_id: str
) -> str:
    """
    创建批量轨迹邻近性分析报告
    
    Args:
        lane_results: 批量邻近性分析结果
        batch_analysis_id: 批量分析ID
        
    Returns:
        报告文本
    """
    successful_results = [r for r in lane_results if r[1] is not None]
    failed_results = [r for r in lane_results if r[1] is None]
    
    report_lines = [
        f"# 批量轨迹邻近性分析报告",
        f"",
        f"**批量分析ID**: {batch_analysis_id}",
        f"**分析时间**: {datetime.now().isoformat()}",
        f"",
        f"## 总体统计",
        f"",
        f"- **总轨迹数**: {len(lane_results)}",
        f"- **成功分析**: {len(successful_results)}",
        f"- **失败分析**: {len(failed_results)}",
        f"- **成功率**: {len(successful_results)/len(lane_results)*100:.1f}%",
        f"",
    ]
    
    if successful_results:
        # 成功分析统计
        total_candidate_lanes = sum(r[2].get('candidate_lanes_found', 0) for r in successful_results)
        total_trajectory_points = sum(r[2].get('trajectory_points_found', 0) for r in successful_results)
        total_unique_data_names = sum(r[2].get('unique_data_names_found', 0) for r in successful_results)
        total_complete_trajectories = sum(r[2].get('complete_trajectories_count', 0) for r in successful_results)
        
        report_lines.extend([
            f"## 成功分析汇总",
            f"",
            f"- **总候选Lane数**: {total_candidate_lanes}",
            f"- **总轨迹点数**: {total_trajectory_points}",
            f"- **总data_name数**: {total_unique_data_names}",
            f"- **总符合条件轨迹数**: {total_complete_trajectories}",
            f"- **平均候选Lane数/输入轨迹**: {total_candidate_lanes/len(successful_results):.1f}",
            f"- **平均符合条件轨迹数/输入轨迹**: {total_complete_trajectories/len(successful_results):.1f}",
            f"",
        ])
        
        # 成功分析详情
        report_lines.extend([
            f"## 成功分析详情",
            f"",
            f"| 轨迹ID | 分析ID | 候选Lanes | 轨迹点数 | 符合条件轨迹数 |",
            f"|--------|--------|-----------|----------|---------------|",
        ])
        
        for trajectory_id, lane_analysis_id, summary in successful_results[:10]:  # 只显示前10个
            candidate_lanes = summary.get('candidate_lanes_found', 0)
            trajectory_points = summary.get('trajectory_points_found', 0)
            complete_trajectories = summary.get('complete_trajectories_count', 0)
            report_lines.append(f"| {trajectory_id} | {lane_analysis_id} | {candidate_lanes} | {trajectory_points} | {complete_trajectories} |")
        
        if len(successful_results) > 10:
            report_lines.append(f"| ... | ... | ... | ... | ... |")
            report_lines.append(f"| (共{len(successful_results)}个成功分析) | | | | |")
    
    if failed_results:
        # 失败分析详情
        report_lines.extend([
            f"",
            f"## 失败分析详情",
            f"",
        ])
        
        for trajectory_id, _, summary in failed_results:
            error_msg = summary.get('error', '未知错误')
            report_lines.append(f"- **{trajectory_id}**: {error_msg}")
    
    return "\n".join(report_lines)


def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='轨迹邻近性分析模块 - 基于输入轨迹查询相关轨迹',
        epilog="""
前提条件:
  必须先运行 trajectory_road_analysis 获得道路分析结果，本模块基于其输出的候选车道进行邻近性查询
  
支持的输入格式:
  需要提供输入轨迹的几何信息（WKT格式）
  
分析流程:
  输入轨迹 → 分段 → 找邻近候选lanes → buffer查询轨迹点数据库 → 过滤规则 → 提取符合条件的完整轨迹
  
示例:
  # 需要先运行道路分析（获得road_analysis_id）
  python -m spdatalab.fusion.trajectory_road_analysis --trajectory-id my_traj --trajectory-geom "LINESTRING(...)"
  
  # 然后运行邻近性分析
  python -m spdatalab.fusion.trajectory_lane_analysis --trajectory-id my_traj --trajectory-geom "LINESTRING(...)" --road-analysis-id trajectory_road_20241201_123456
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 输入参数
    parser.add_argument('--trajectory-id', required=True, 
                       help='输入轨迹ID')
    parser.add_argument('--trajectory-geom', required=True,
                       help='输入轨迹几何（WKT格式）')
    parser.add_argument('--road-analysis-id', required=True,
                       help='trajectory_road_analysis的分析ID（必需，用于获取候选车道）')
    
    # 车道分析参数
    parser.add_argument('--road-lanes-table', default='trajectory_road_lanes',
                       help='道路分析结果车道表名')
    parser.add_argument('--buffer-radius', type=float, default=15.0,
                       help='车道缓冲区半径（米）')
    parser.add_argument('--max-lane-distance', type=float, default=50.0,
                       help='最大车道搜索距离（米）')
    
    # 过滤参数
    parser.add_argument('--min-points-single-lane', type=int, default=5,
                       help='单车道最少点数阈值')
    
    # 输出参数
    parser.add_argument('--output-format', choices=['summary', 'detailed', 'geojson'], 
                       default='summary', help='输出格式')
    parser.add_argument('--output-file', help='输出文件路径（可选）')
    
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
            'road_analysis_lanes_table': args.road_lanes_table,
            'buffer_radius': args.buffer_radius,
            'max_lane_distance': args.max_lane_distance,
            'min_points_single_lane': args.min_points_single_lane
        }
        
        # 输出配置信息
        logger.info(f"输入轨迹ID: {args.trajectory_id}")
        logger.info(f"道路分析ID: {args.road_analysis_id}")
        logger.info(f"候选车道表: {config['road_analysis_lanes_table']}")
        logger.info(f"缓冲区半径: {config['buffer_radius']}米")
        logger.info(f"最大车道距离: {config['max_lane_distance']}米")
        logger.info(f"单车道最少点数: {config['min_points_single_lane']}")
        
        # 创建分析器并执行分析
        analyzer = TrajectoryLaneAnalyzer(config, road_analysis_id=args.road_analysis_id)
        analysis_result = analyzer.analyze_trajectory_neighbors(args.trajectory_id, args.trajectory_geom)
        
        if 'error' in analysis_result:
            logger.error(f"分析失败: {analysis_result['error']}")
            return 1
        
        # 输出结果
        logger.info("=== 分析完成 ===")
        stats = analysis_result['stats']
        logger.info(f"候选lanes: {stats['candidate_lanes_found']} 个")
        logger.info(f"轨迹点数: {stats['trajectory_points_found']} 个")
        logger.info(f"data_name数: {stats['unique_data_names_found']} 个")
        logger.info(f"符合条件的轨迹: {stats['trajectories_passed_filter']} 个")
        logger.info(f"  - 多车道: {stats['trajectories_multi_lane']} 个")
        logger.info(f"  - 足够点数: {stats['trajectories_sufficient_points']} 个")
        logger.info(f"完整轨迹数: {len(analysis_result['complete_trajectories'])} 个")
        
        # 输出详细结果
        if args.output_format == 'detailed' or args.verbose:
            logger.info("\n=== 符合条件的轨迹详情 ===")
            for data_name, trajectory in analysis_result['complete_trajectories'].items():
                logger.info(f"轨迹: {data_name}")
                logger.info(f"  - 过滤原因: {trajectory['filter_reason']}")
                logger.info(f"  - 涉及lanes: {len(trajectory['lanes_touched'])} 个")
                logger.info(f"  - 命中点数: {trajectory['hit_points_count']} 个")
                logger.info(f"  - 总点数: {trajectory['total_points']} 个")
                logger.info(f"  - 轨迹长度: {trajectory['trajectory_length']:.6f} 度")
                logger.info(f"  - 平均速度: {trajectory['avg_speed']} km/h")
        
        # 保存输出文件
        if args.output_file:
            output_data = {
                'input_trajectory_id': args.trajectory_id,
                'road_analysis_id': args.road_analysis_id,
                'analysis_result': analysis_result,
                'config': config,
                'timestamp': datetime.now().isoformat()
            }
            
            if args.output_format == 'geojson':
                # 输出GeoJSON格式
                geojson_features = []
                for data_name, trajectory in analysis_result['complete_trajectories'].items():
                    from shapely.geometry import mapping
                    feature = {
                        'type': 'Feature',
                        'properties': {
                            'data_name': data_name,
                            'filter_reason': trajectory['filter_reason'],
                            'lanes_touched_count': len(trajectory['lanes_touched']),
                            'lanes_touched': trajectory['lanes_touched'],
                            'hit_points_count': trajectory['hit_points_count'],
                            'total_points': trajectory['total_points'],
                            'avg_speed': trajectory['avg_speed'],
                            'max_speed': trajectory['max_speed'],
                            'avp_ratio': trajectory['avp_ratio']
                        },
                        'geometry': mapping(trajectory['geometry'])
                    }
                    geojson_features.append(feature)
                
                geojson_output = {
                    'type': 'FeatureCollection',
                    'features': geojson_features,
                    'metadata': {
                        'input_trajectory_id': args.trajectory_id,
                        'road_analysis_id': args.road_analysis_id,
                        'total_trajectories': len(geojson_features),
                        'analysis_stats': stats
                    }
                }
                
                with open(args.output_file, 'w', encoding='utf-8') as f:
                    json.dump(geojson_output, f, indent=2, ensure_ascii=False)
                
                logger.info(f"GeoJSON结果已保存到: {args.output_file}")
            else:
                # 输出JSON格式
                with open(args.output_file, 'w', encoding='utf-8') as f:
                    # 序列化几何对象
                    def serialize_geometry(obj):
                        if hasattr(obj, 'wkt'):
                            return obj.wkt
                        elif hasattr(obj, 'isoformat'):
                            return obj.isoformat()
                        return str(obj)
                    
                    json.dump(output_data, f, indent=2, ensure_ascii=False, default=serialize_geometry)
                
                logger.info(f"分析结果已保存到: {args.output_file}")
        
        return 0 if len(analysis_result['complete_trajectories']) > 0 else 1
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 