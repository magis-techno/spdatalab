"""Grid轨迹聚类模块

基于city_hotspots表的热点grid，对每个200m×200m区域内的高质量轨迹进行聚类分析。

核心功能：
1. 从city_hotspots表加载热点grid
2. 查询grid内的高质量轨迹点（workstage=2）
3. 按距离优先+时长上限策略切分轨迹段
4. 提取10维特征向量（速度、加速度、航向、形态）
5. DBSCAN聚类分析
6. 保存结果到数据库

使用示例：
    clusterer = GridTrajectoryClusterer()
    results = clusterer.process_all_grids(city_id='A72', max_grids=5)
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Point
from shapely import wkt
from sqlalchemy import create_engine, text
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# 导入高性能polygon查询器
from spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery,
    PolygonTrajectoryConfig
)

# 抑制警告
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """聚类配置参数"""
    # 数据库配置
    local_dsn: str = LOCAL_DSN
    point_table: str = POINT_TABLE
    
    # 轨迹查询配置
    query_limit: int = 50000          # 每个grid的轨迹点查询限制
    
    # 轨迹切分配置（距离优先+时长上限）
    min_distance: float = 50.0        # 主切分：50米/段
    max_duration: float = 15.0        # 强制切分：15秒上限
    min_points: int = 5               # 最少点数
    time_gap_threshold: float = 3.0   # 时间间隔>3秒断开
    
    # 质量过滤配置
    min_movement: float = 10.0        # 最小移动距离（米）
    max_jump: float = 100.0           # 最大点间距（米）
    max_speed: float = 30.0           # 最大合理速度（m/s）
    
    # 聚类配置
    eps: float = 0.4                  # DBSCAN距离阈值
    min_samples: int = 3              # DBSCAN最小样本数
    
    # 性能配置
    batch_size: int = 100             # 批量保存大小


@dataclass
class TrajectorySegment:
    """轨迹段数据结构"""
    dataset_name: str
    segment_index: int
    points: pd.DataFrame              # 包含lon, lat, timestamp, twist_linear, yaw等
    
    # 基本统计
    start_time: int
    end_time: int
    duration: float
    point_count: int
    
    # 特征（稍后计算）
    features: Optional[np.ndarray] = None
    quality_flag: str = 'unknown'
    
    # 几何
    geometry: Optional[LineString] = None


class GridTrajectoryClusterer:
    """Grid轨迹聚类器
    
    核心流程：
    1. 加载热点grids
    2. 查询轨迹点
    3. 切分轨迹段
    4. 特征提取
    5. DBSCAN聚类
    6. 保存结果
    """
    
    def __init__(self, config: Optional[ClusterConfig] = None):
        """初始化聚类器
        
        Args:
            config: 聚类配置参数
        """
        self.config = config or ClusterConfig()
        self.engine = create_engine(
            self.config.local_dsn,
            future=True,
            pool_pre_ping=True
        )
        self.scaler = StandardScaler()
        
        # 初始化高性能查询器
        query_config = PolygonTrajectoryConfig(
            limit_per_polygon=self.config.query_limit,
            fetch_complete_trajectories=False,  # 只查询相交的点
            batch_threshold=1,  # 单个grid使用批量策略
            enable_speed_stats=False,
            enable_avp_stats=False
        )
        self.trajectory_query = HighPerformancePolygonTrajectoryQuery(query_config)
        
        logger.info("🚀 GridTrajectoryClusterer 初始化完成")
        logger.info(f"   查询限制: {self.config.query_limit}点/grid")
        logger.info(f"   切分策略: {self.config.min_distance}米/{self.config.max_duration}秒")
        logger.info(f"   聚类参数: eps={self.config.eps}, min_samples={self.config.min_samples}")
    
    def load_hotspot_grids(
        self, 
        city_id: Optional[str] = None, 
        limit: Optional[int] = None,
        grid_ids: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """从city_hotspots表加载热点grid
        
        Args:
            city_id: 城市ID过滤
            limit: 限制数量
            grid_ids: 指定grid ID列表
            
        Returns:
            包含grid信息的DataFrame
        """
        logger.info("📊 加载热点grids...")
        
        # 构建查询条件
        where_conditions = []
        params = {}
        
        if city_id:
            where_conditions.append("city_id = :city_id")
            params['city_id'] = city_id
        
        if grid_ids:
            where_conditions.append("id = ANY(:grid_ids)")
            params['grid_ids'] = grid_ids
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        sql = text(f"""
            SELECT 
                id as grid_id,
                city_id,
                analysis_id,
                bbox_count,
                subdataset_count,
                scene_count,
                grid_coords,
                ST_AsText(geometry) as geometry_wkt
            FROM city_hotspots
            WHERE {where_clause}
            ORDER BY bbox_count DESC
            {limit_clause};
        """)
        
        with self.engine.connect() as conn:
            grids_df = pd.read_sql(sql, conn, params=params)
        
        if grids_df.empty:
            logger.warning("⚠️ 未找到符合条件的grid")
            return grids_df
        
        # 转换几何
        grids_df['geometry'] = grids_df['geometry_wkt'].apply(wkt.loads)
        
        logger.info(f"✅ 加载了 {len(grids_df)} 个热点grid")
        if city_id:
            logger.info(f"   城市: {city_id}")
        logger.info(f"   bbox数量范围: {grids_df['bbox_count'].min()}-{grids_df['bbox_count'].max()}")
        
        return grids_df
    
    def query_trajectory_points(self, grid_geometry, grid_id: int = 0) -> pd.DataFrame:
        """查询grid内的高质量轨迹点（使用高性能查询器）
        
        Args:
            grid_geometry: Grid的几何对象（Polygon）
            grid_id: Grid ID（用于标识）
            
        Returns:
            轨迹点DataFrame
        """
        logger.debug("🔍 查询grid内的轨迹点（使用高性能查询器）...")
        
        # 将grid包装成polygon格式
        polygon_data = [{
            'id': f'grid_{grid_id}',
            'geometry': grid_geometry,
            'properties': {'grid_id': grid_id}
        }]
        
        try:
            # 使用高性能查询器（复用所有优化策略）
            points_df, stats = self.trajectory_query.query_intersecting_trajectory_points(polygon_data)
            
            if not points_df.empty:
                # 重命名列以匹配后续处理
                points_df = points_df.rename(columns={
                    'longitude': 'lon',
                    'latitude': 'lat',
                    'twist_linear': 'twist_linear'  # 保持原名
                })
                
                # 添加vehicle_id列（如果不存在）
                if 'vehicle_id' not in points_df.columns:
                    points_df['vehicle_id'] = None
                
                logger.debug(f"   查询到 {len(points_df)} 个高质量轨迹点")
                logger.debug(f"   涉及轨迹数: {points_df['dataset_name'].nunique()}")
                logger.debug(f"   查询用时: {stats['query_time']:.2f}s")
            else:
                logger.debug("   未找到轨迹点")
            
            return points_df
            
        except Exception as e:
            logger.error(f"❌ 查询轨迹点失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def segment_trajectories(
        self, 
        points_df: pd.DataFrame
    ) -> List[TrajectorySegment]:
        """按距离优先+时长上限策略切分轨迹段
        
        Args:
            points_df: 轨迹点DataFrame
            
        Returns:
            轨迹段列表
        """
        if points_df.empty:
            return []
        
        logger.debug("✂️ 切分轨迹段...")
        
        all_segments = []
        
        # 按dataset_name分组
        for dataset_name, group in points_df.groupby('dataset_name'):
            # 按timestamp排序
            group = group.sort_values('timestamp').reset_index(drop=True)
            
            if len(group) < self.config.min_points:
                continue
            
            # 切分该轨迹
            segments = self._segment_single_trajectory(dataset_name, group)
            all_segments.extend(segments)
        
        logger.debug(f"   切分得到 {len(all_segments)} 个轨迹段")
        
        return all_segments
    
    def _segment_single_trajectory(
        self, 
        dataset_name: str, 
        points: pd.DataFrame
    ) -> List[TrajectorySegment]:
        """切分单条轨迹
        
        实现距离优先+时长上限策略：
        1. 累计距离达到min_distance → 切分
        2. 时长超过max_duration → 强制切分
        3. 时间间隔>time_gap_threshold → 断开
        """
        segments = []
        current_segment_points = []
        cumulative_distance = 0.0
        segment_index = 0
        
        for i in range(len(points)):
            current_point = points.iloc[i]
            current_segment_points.append(current_point)
            
            if i == 0:
                segment_start_time = current_point['timestamp']
                continue
            
            prev_point = points.iloc[i - 1]
            
            # 计算与上一点的距离
            dist = self._haversine_distance(
                prev_point['lat'], prev_point['lon'],
                current_point['lat'], current_point['lon']
            )
            cumulative_distance += dist
            
            # 计算时长
            duration = current_point['timestamp'] - segment_start_time
            
            # 计算时间间隔
            time_gap = current_point['timestamp'] - prev_point['timestamp']
            
            # 判断切分条件
            should_split = False
            
            # 条件1：时间间隔过大（轨迹断开）
            if time_gap > self.config.time_gap_threshold:
                should_split = True
                logger.debug(f"   时间间隔断开: {time_gap:.1f}秒")
            
            # 条件2：距离达标（主切分）
            elif cumulative_distance >= self.config.min_distance:
                should_split = True
                logger.debug(f"   距离达标切分: {cumulative_distance:.1f}米")
            
            # 条件3：时长超限（强制切分）
            elif duration >= self.config.max_duration:
                should_split = True
                logger.debug(f"   时长超限切分: {duration:.1f}秒")
            
            if should_split:
                # 保存当前段（不包括触发点）
                segment_points = current_segment_points[:-1]
                if len(segment_points) >= self.config.min_points:
                    segment = self._create_segment(
                        dataset_name, 
                        segment_index, 
                        segment_points
                    )
                    segments.append(segment)
                    segment_index += 1
                
                # 开始新段
                current_segment_points = [current_point]
                cumulative_distance = 0.0
                segment_start_time = current_point['timestamp']
        
        # 处理最后一段
        if len(current_segment_points) >= self.config.min_points:
            segment = self._create_segment(
                dataset_name,
                segment_index,
                current_segment_points
            )
            segments.append(segment)
        
        return segments
    
    def _create_segment(
        self, 
        dataset_name: str, 
        segment_index: int, 
        points_list: List
    ) -> TrajectorySegment:
        """创建轨迹段对象"""
        points_df = pd.DataFrame(points_list)
        
        start_time = int(points_df['timestamp'].min())
        end_time = int(points_df['timestamp'].max())
        duration = (end_time - start_time) / 1.0  # 秒
        
        # 创建LineString几何
        coords = list(zip(points_df['lon'], points_df['lat']))
        geometry = LineString(coords) if len(coords) >= 2 else None
        
        return TrajectorySegment(
            dataset_name=dataset_name,
            segment_index=segment_index,
            points=points_df,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            point_count=len(points_df),
            geometry=geometry
        )
    
    def filter_segment_quality(
        self, 
        segment: TrajectorySegment
    ) -> Tuple[bool, str]:
        """质量过滤：原地不动、GPS跳点、速度异常
        
        Returns:
            (is_valid, reason)
        """
        points = segment.points
        
        # 1. 最少点数检查
        if len(points) < self.config.min_points:
            return False, "insufficient_points"
        
        # 2. 移动距离检查（过滤原地不动）
        total_distance = self._calculate_total_distance(points)
        if total_distance < self.config.min_movement:
            return False, "stationary"
        
        # 3. GPS跳点检查
        max_jump = self._calculate_max_consecutive_distance(points)
        if max_jump > self.config.max_jump:
            return False, "gps_jump"
        
        # 4. 速度合理性检查
        if 'twist_linear' in points.columns:
            avg_speed = points['twist_linear'].mean()
            if avg_speed > self.config.max_speed:
                return False, "excessive_speed"
        
        return True, "valid"
    
    def extract_features(self, segment: TrajectorySegment) -> np.ndarray:
        """提取10维特征向量
        
        特征包括：
        - 速度特征（4维）：avg, std, max, min
        - 加速度特征（2维）：avg, std
        - 航向角特征（2维）：change_rate, std
        - 形态特征（2维）：direction_cos, direction_sin
        
        Returns:
            10维特征向量
        """
        points = segment.points
        
        # 速度特征
        speeds = points['twist_linear'].values
        avg_speed = np.mean(speeds)
        std_speed = np.std(speeds)
        max_speed = np.max(speeds)
        min_speed = np.min(speeds)
        
        # 加速度特征（差分计算）
        if len(speeds) > 1:
            time_diffs = np.diff(points['timestamp'].values)
            time_diffs = np.maximum(time_diffs, 0.1)  # 避免除零
            accelerations = np.diff(speeds) / time_diffs
            avg_acceleration = np.mean(accelerations)
            std_acceleration = np.std(accelerations)
        else:
            avg_acceleration = 0.0
            std_acceleration = 0.0
        
        # 航向角特征
        if 'yaw' in points.columns and len(points) > 1:
            yaws = points['yaw'].values
            time_diffs = np.diff(points['timestamp'].values)
            time_diffs = np.maximum(time_diffs, 0.1)
            yaw_changes = np.diff(yaws) / time_diffs
            yaw_change_rate = np.mean(np.abs(yaw_changes))
            std_yaw = np.std(yaws)
        else:
            yaw_change_rate = 0.0
            std_yaw = 0.0
        
        # 轨迹形态特征
        start_point = points.iloc[0]
        end_point = points.iloc[-1]
        
        delta_lng = end_point['lon'] - start_point['lon']
        delta_lat = end_point['lat'] - start_point['lat']
        
        # 起终点方向向量
        angle = np.arctan2(delta_lat, delta_lng)
        direction_cos = np.cos(angle)
        direction_sin = np.sin(angle)
        
        # 组装特征向量（10维）
        features = np.array([
            avg_speed,
            std_speed,
            max_speed,
            min_speed,
            avg_acceleration,
            std_acceleration,
            yaw_change_rate,
            std_yaw,
            direction_cos,
            direction_sin
        ])
        
        return features
    
    def perform_clustering(
        self, 
        segments: List[TrajectorySegment]
    ) -> np.ndarray:
        """DBSCAN聚类
        
        Args:
            segments: 轨迹段列表（已提取特征）
            
        Returns:
            聚类标签数组
        """
        if not segments:
            return np.array([])
        
        logger.info("🔬 执行DBSCAN聚类...")
        
        # 提取特征矩阵
        features_list = [seg.features for seg in segments if seg.features is not None]
        
        if not features_list:
            logger.warning("⚠️ 没有有效特征，跳过聚类")
            return np.array([-1] * len(segments))
        
        features_matrix = np.vstack(features_list)
        
        logger.info(f"   特征矩阵: {features_matrix.shape}")
        
        # 标准化特征
        features_scaled = self.scaler.fit_transform(features_matrix)
        
        # DBSCAN聚类
        dbscan = DBSCAN(
            eps=self.config.eps,
            min_samples=self.config.min_samples,
            metric='euclidean'
        )
        
        labels = dbscan.fit_predict(features_scaled)
        
        # 统计聚类结果
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)
        
        logger.info(f"✅ 聚类完成:")
        logger.info(f"   聚类数量: {n_clusters}")
        logger.info(f"   噪声点: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
        
        return labels
    
    def generate_behavior_labels(
        self, 
        segments: List[TrajectorySegment],
        labels: np.ndarray
    ) -> Dict[int, Dict]:
        """根据聚类中心特征生成行为标签
        
        Returns:
            {cluster_label: {centroid, speed_range, behavior_label}}
        """
        cluster_info = {}
        
        unique_labels = set(labels)
        
        for label in unique_labels:
            # 获取该聚类的所有段
            cluster_segments = [seg for seg, l in zip(segments, labels) if l == label]
            
            if not cluster_segments:
                continue
            
            # 计算聚类中心（特征平均值）
            features = np.vstack([seg.features for seg in cluster_segments])
            centroid = np.mean(features, axis=0)
            
            # 提取关键特征
            avg_speed = centroid[0]
            avg_accel = centroid[4]
            yaw_change = centroid[6]
            direction_cos = centroid[8]
            direction_sin = centroid[9]
            
            # 生成速度范围标签
            if avg_speed < 2:
                speed_range = "极低速(0-2m/s)"
            elif avg_speed < 5:
                speed_range = "低速(2-5m/s)"
            elif avg_speed < 10:
                speed_range = "中速(5-10m/s)"
            elif avg_speed < 15:
                speed_range = "较快(10-15m/s)"
            else:
                speed_range = f"快速({avg_speed:.1f}m/s)"
            
            # 生成行为标签
            if label == -1:
                behavior_label = "噪声/异常"
            elif yaw_change > 0.3:
                behavior_label = "转弯/变道"
            elif avg_accel < -1:
                behavior_label = "减速/刹车"
            elif avg_accel > 1:
                behavior_label = "加速"
            elif avg_speed < 3:
                behavior_label = "缓慢移动"
            else:
                behavior_label = "直行通过"
            
            cluster_info[int(label)] = {
                'centroid': centroid,
                'speed_range': speed_range,
                'behavior_label': behavior_label,
                'segment_count': len(cluster_segments),
                'centroid_avg_speed': float(avg_speed),
                'centroid_avg_acceleration': float(avg_accel),
                'centroid_yaw_change_rate': float(yaw_change),
                'centroid_direction_cos': float(direction_cos),
                'centroid_direction_sin': float(direction_sin)
            }
        
        return cluster_info
    
    def save_results(
        self,
        grid_id: int,
        city_id: str,
        analysis_id: str,
        segments: List[TrajectorySegment],
        labels: np.ndarray,
        cluster_info: Dict
    ):
        """保存结果到数据库
        
        保存到两个表：
        1. grid_trajectory_segments - 每个轨迹段
        2. grid_clustering_summary - 聚类统计
        """
        logger.info("💾 保存结果到数据库...")
        
        if not segments:
            logger.warning("⚠️ 没有轨迹段可保存")
            return
        
        # 准备轨迹段数据
        segments_data = []
        
        for segment, label in zip(segments, labels):
            # 计算额外的形态特征
            total_distance = self._calculate_total_distance(segment.points)
            straight_distance = self._haversine_distance(
                segment.points.iloc[0]['lat'], segment.points.iloc[0]['lon'],
                segment.points.iloc[-1]['lat'], segment.points.iloc[-1]['lon']
            )
            curvature = total_distance / max(straight_distance, 0.001)
            
            # 提取特征值
            features = segment.features
            
            segments_data.append({
                'grid_id': grid_id,
                'city_id': city_id,
                'analysis_id': analysis_id,
                'dataset_name': segment.dataset_name,
                'segment_index': segment.segment_index,
                'start_time': segment.start_time,
                'end_time': segment.end_time,
                'duration': segment.duration,
                'point_count': segment.point_count,
                'avg_speed': float(features[0]) if features is not None else None,
                'std_speed': float(features[1]) if features is not None else None,
                'max_speed': float(features[2]) if features is not None else None,
                'min_speed': float(features[3]) if features is not None else None,
                'avg_acceleration': float(features[4]) if features is not None else None,
                'std_acceleration': float(features[5]) if features is not None else None,
                'avg_yaw': float(segment.points['yaw'].mean()) if 'yaw' in segment.points.columns else None,
                'yaw_change_rate': float(features[6]) if features is not None else None,
                'std_yaw': float(features[7]) if features is not None else None,
                'trajectory_length_m': total_distance,
                'direction_cos': float(features[8]) if features is not None else None,
                'direction_sin': float(features[9]) if features is not None else None,
                'curvature': curvature,
                'quality_flag': segment.quality_flag,
                'cluster_label': int(label),
                'geometry': segment.geometry.wkt if segment.geometry else None
            })
        
        # 批量保存轨迹段
        self._bulk_insert_segments(segments_data)
        
        # 保存聚类统计
        self._save_cluster_summary(grid_id, city_id, analysis_id, cluster_info)
        
        logger.info(f"✅ 保存完成: {len(segments_data)} 个轨迹段, {len(cluster_info)} 个聚类")
    
    def _bulk_insert_segments(self, segments_data: List[Dict]):
        """批量插入轨迹段"""
        if not segments_data:
            return
        
        sql = text("""
            INSERT INTO grid_trajectory_segments (
                grid_id, city_id, analysis_id, dataset_name, segment_index,
                start_time, end_time, duration, point_count,
                avg_speed, std_speed, max_speed, min_speed,
                avg_acceleration, std_acceleration,
                avg_yaw, yaw_change_rate, std_yaw,
                trajectory_length_m, direction_cos, direction_sin, curvature,
                quality_flag, cluster_label,
                geometry
            ) VALUES (
                :grid_id, :city_id, :analysis_id, :dataset_name, :segment_index,
                :start_time, :end_time, :duration, :point_count,
                :avg_speed, :std_speed, :max_speed, :min_speed,
                :avg_acceleration, :std_acceleration,
                :avg_yaw, :yaw_change_rate, :std_yaw,
                :trajectory_length_m, :direction_cos, :direction_sin, :curvature,
                :quality_flag, :cluster_label,
                ST_GeomFromText(:geometry, 4326)
            );
        """)
        
        with self.engine.connect() as conn:
            conn.execute(sql, segments_data)
            conn.commit()
    
    def _save_cluster_summary(
        self, 
        grid_id: int, 
        city_id: str, 
        analysis_id: str,
        cluster_info: Dict
    ):
        """保存聚类统计"""
        if not cluster_info:
            return
        
        summary_data = []
        for label, info in cluster_info.items():
            summary_data.append({
                'grid_id': grid_id,
                'city_id': city_id,
                'analysis_id': analysis_id,
                'cluster_label': label,
                'segment_count': info['segment_count'],
                'centroid_avg_speed': info['centroid_avg_speed'],
                'centroid_avg_acceleration': info['centroid_avg_acceleration'],
                'centroid_yaw_change_rate': info['centroid_yaw_change_rate'],
                'centroid_direction_cos': info['centroid_direction_cos'],
                'centroid_direction_sin': info['centroid_direction_sin'],
                'speed_range': info['speed_range'],
                'behavior_label': info['behavior_label']
            })
        
        sql = text("""
            INSERT INTO grid_clustering_summary (
                grid_id, city_id, analysis_id, cluster_label, segment_count,
                centroid_avg_speed, centroid_avg_acceleration, centroid_yaw_change_rate,
                centroid_direction_cos, centroid_direction_sin,
                speed_range, behavior_label
            ) VALUES (
                :grid_id, :city_id, :analysis_id, :cluster_label, :segment_count,
                :centroid_avg_speed, :centroid_avg_acceleration, :centroid_yaw_change_rate,
                :centroid_direction_cos, :centroid_direction_sin,
                :speed_range, :behavior_label
            );
        """)
        
        with self.engine.connect() as conn:
            conn.execute(sql, summary_data)
            conn.commit()
    
    def process_single_grid(self, grid_row: pd.Series) -> Dict:
        """处理单个grid的完整流程
        
        Returns:
            处理统计信息
        """
        grid_id = grid_row['grid_id']
        city_id = grid_row['city_id']
        analysis_id = grid_row['analysis_id']
        geometry = grid_row['geometry']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 处理Grid #{grid_id} (城市: {city_id})")
        logger.info(f"{'='*60}")
        
        start_time = time.time()
        stats = {
            'grid_id': grid_id,
            'city_id': city_id,
            'success': False,
            'error': None
        }
        
        try:
            # 1. 查询轨迹点（使用高性能查询器）
            points_df = self.query_trajectory_points(geometry, grid_id)
            stats['total_points'] = len(points_df)
            stats['trajectory_count'] = points_df['dataset_name'].nunique() if not points_df.empty else 0
            
            if points_df.empty:
                logger.warning("⚠️ 没有轨迹点，跳过")
                stats['error'] = 'no_points'
                return stats
            
            logger.info(f"📊 轨迹点统计: {len(points_df)} 个点, {stats['trajectory_count']} 条轨迹")
            
            # 2. 切分轨迹段
            segments = self.segment_trajectories(points_df)
            stats['total_segments'] = len(segments)
            
            if not segments:
                logger.warning("⚠️ 没有有效轨迹段")
                stats['error'] = 'no_segments'
                return stats
            
            logger.info(f"✂️ 切分结果: {len(segments)} 个轨迹段")
            
            # 3. 质量过滤和特征提取
            valid_segments = []
            quality_stats = {}
            
            for segment in segments:
                is_valid, reason = self.filter_segment_quality(segment)
                segment.quality_flag = reason
                
                quality_stats[reason] = quality_stats.get(reason, 0) + 1
                
                if is_valid:
                    # 提取特征
                    segment.features = self.extract_features(segment)
                    valid_segments.append(segment)
            
            stats['valid_segments'] = len(valid_segments)
            stats['quality_stats'] = quality_stats
            
            logger.info(f"✅ 有效轨迹段: {len(valid_segments)} ({len(valid_segments)/len(segments)*100:.1f}%)")
            logger.info(f"📋 质量统计: {quality_stats}")
            
            if not valid_segments:
                logger.warning("⚠️ 没有通过质量过滤的轨迹段")
                stats['error'] = 'no_valid_segments'
                return stats
            
            # 4. 聚类
            labels = self.perform_clustering(valid_segments)
            stats['cluster_labels'] = labels.tolist()
            
            # 5. 生成行为标签
            cluster_info = self.generate_behavior_labels(valid_segments, labels)
            stats['cluster_info'] = cluster_info
            
            # 打印聚类详情
            logger.info(f"\n📊 聚类详情:")
            for label in sorted(cluster_info.keys()):
                info = cluster_info[label]
                logger.info(f"   簇{label}: {info['segment_count']}段 | "
                          f"{info['behavior_label']} | {info['speed_range']}")
            
            # 6. 保存结果
            self.save_results(
                grid_id, city_id, analysis_id,
                valid_segments, labels, cluster_info
            )
            
            stats['success'] = True
            stats['elapsed_time'] = time.time() - start_time
            
            logger.info(f"\n✅ Grid #{grid_id} 处理完成 (耗时: {stats['elapsed_time']:.2f}秒)")
            
        except Exception as e:
            logger.error(f"❌ Grid #{grid_id} 处理失败: {e}")
            import traceback
            traceback.print_exc()
            stats['error'] = str(e)
        
        return stats
    
    def process_all_grids(
        self,
        city_id: Optional[str] = None,
        max_grids: Optional[int] = None,
        grid_ids: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """批量处理多个grid
        
        Returns:
            处理统计DataFrame
        """
        logger.info("\n" + "="*70)
        logger.info("🚀 批量Grid轨迹聚类分析")
        logger.info("="*70)
        
        # 加载grids
        grids_df = self.load_hotspot_grids(city_id, max_grids, grid_ids)
        
        if grids_df.empty:
            logger.error("❌ 没有可处理的grid")
            return pd.DataFrame()
        
        logger.info(f"\n📋 准备处理 {len(grids_df)} 个grid")
        
        # 批量处理
        all_stats = []
        start_time = time.time()
        
        for idx, grid_row in grids_df.iterrows():
            logger.info(f"\n[{idx+1}/{len(grids_df)}] 开始处理...")
            
            stats = self.process_single_grid(grid_row)
            all_stats.append(stats)
        
        # 汇总统计
        total_time = time.time() - start_time
        stats_df = pd.DataFrame(all_stats)
        
        success_count = stats_df['success'].sum()
        
        logger.info("\n" + "="*70)
        logger.info("📊 批量处理完成")
        logger.info("="*70)
        logger.info(f"总耗时: {total_time:.2f}秒")
        logger.info(f"成功: {success_count}/{len(stats_df)}")
        logger.info(f"失败: {len(stats_df) - success_count}")
        
        if success_count > 0:
            successful_stats = stats_df[stats_df['success'] == True]
            logger.info(f"\n成功grid统计:")
            logger.info(f"  平均轨迹点数: {successful_stats['total_points'].mean():.0f}")
            logger.info(f"  平均轨迹段数: {successful_stats['total_segments'].mean():.0f}")
            logger.info(f"  平均有效段数: {successful_stats['valid_segments'].mean():.0f}")
        
        return stats_df
    
    # ==================== 辅助函数 ====================
    
    @staticmethod
    def _haversine_distance(lat1, lon1, lat2, lon2) -> float:
        """计算两点间的Haversine距离（米）"""
        R = 6371000  # 地球半径（米）
        
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        return R * c
    
    def _calculate_total_distance(self, points: pd.DataFrame) -> float:
        """计算轨迹总距离（米）"""
        if len(points) < 2:
            return 0.0
        
        total_dist = 0.0
        for i in range(1, len(points)):
            dist = self._haversine_distance(
                points.iloc[i-1]['lat'], points.iloc[i-1]['lon'],
                points.iloc[i]['lat'], points.iloc[i]['lon']
            )
            total_dist += dist
        
        return total_dist
    
    def _calculate_max_consecutive_distance(self, points: pd.DataFrame) -> float:
        """计算最大连续点间距（米）"""
        if len(points) < 2:
            return 0.0
        
        max_dist = 0.0
        for i in range(1, len(points)):
            dist = self._haversine_distance(
                points.iloc[i-1]['lat'], points.iloc[i-1]['lon'],
                points.iloc[i]['lat'], points.iloc[i]['lon']
            )
            max_dist = max(max_dist, dist)
        
        return max_dist







