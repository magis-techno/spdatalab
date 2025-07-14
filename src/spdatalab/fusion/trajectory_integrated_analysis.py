"""
轨迹综合分析模块 - 统一入口

整合轨迹道路分析和轨迹车道分析的一体化处理系统，
支持从GeoJSON输入到完整分析结果的端到端流程。

功能：
1. 从GeoJSON文件加载轨迹数据
2. 自动执行两阶段分析：道路分析 → 车道分析
3. 统一结果管理和导出
4. 批量处理和进度跟踪
"""

import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from .trajectory_road_analysis import TrajectoryRoadAnalysisConfig, TrajectoryRoadAnalyzer
from .trajectory_lane_analysis import TrajectoryLaneAnalysisConfig, TrajectoryLaneAnalyzer

logger = logging.getLogger(__name__)

@dataclass
class TrajectoryIntegratedAnalysisConfig:
    """轨迹综合分析配置"""
    
    # === 基础配置 ===
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_catalog: str = "rcdatalake_gy1"
    
    # === 道路分析配置 ===
    # 远程表名配置
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    road_table: str = "full_road"
    roadnextroad_table: str = "roadnextroad"
    intersection_inroad_table: str = "full_intersectiongoinroad"
    intersection_outroad_table: str = "full_intersectiongooutroad"
    
    # 道路分析参数
    buffer_distance: float = 3.0  # 轨迹膨胀距离(m)
    forward_chain_limit: float = 500.0  # 前向链路扩展限制(m)
    backward_chain_limit: float = 100.0  # 后向链路扩展限制(m)
    max_recursion_depth: int = 10  # 最大递归深度
    
    # 查询限制参数
    max_roads_per_query: int = 100
    max_intersections_per_query: int = 50
    max_forward_road_chains: int = 200
    max_backward_road_chains: int = 100
    max_lanes_from_roads: int = 5000
    query_timeout: int = 120
    recursive_query_timeout: int = 180
    
    # === 车道分析配置 ===
    # 采样策略配置
    sampling_strategy: str = "distance"  # "distance", "time", "uniform"
    sampling_distance: float = 10.0  # 距离采样间隔(m)
    sampling_time: float = 1.0  # 时间采样间隔(s)
    uniform_points: int = 100  # 均匀采样点数
    
    # 滑窗分析配置
    window_size: int = 5  # 滑窗大小
    lane_change_threshold: float = 0.5  # 车道变化阈值
    
    # 车道查询配置
    lane_search_radius: float = 50.0  # 车道搜索半径(m)
    max_candidate_lanes: int = 10  # 最大候选车道数
    
    # 缓冲区分析配置
    lane_buffer_distance: float = 2.0  # 车道缓冲距离(m)
    buffer_analysis_batch_size: int = 1000  # 缓冲分析批量大小
    
    # 质量检查配置
    quality_min_points_single_lane: int = 5  # 单车道最少点数
    quality_min_coverage_ratio: float = 0.3  # 最小覆盖率
    
    # 轨迹重构配置
    trajectory_reconstruction_tolerance: float = 1.0  # 重构容差(m)
    douglas_peucker_tolerance: float = 0.5  # 简化容差(m)
    
    # === 批处理配置 ===
    batch_size: int = 10  # 批处理大小
    max_workers: int = 4  # 最大并行工作数
    progress_report_interval: int = 5  # 进度报告间隔
    
    # === 结果表名配置 ===
    # 道路分析结果表
    road_analysis_table: str = "trajectory_road_analysis"
    road_lanes_table: str = "trajectory_road_lanes"
    road_intersections_table: str = "trajectory_road_intersections"
    road_roads_table: str = "trajectory_road_roads"
    
    # 车道分析结果表
    lane_analysis_table: str = "trajectory_lane_analysis"
    lane_segments_table: str = "trajectory_lane_segments"
    lane_buffer_table: str = "trajectory_lane_buffer"
    lane_quality_table: str = "trajectory_quality_check"
    
    # 综合分析结果表
    integrated_analysis_table: str = "trajectory_integrated_analysis"
    integrated_summary_table: str = "trajectory_integrated_summary"
    
    def to_road_analysis_config(self) -> TrajectoryRoadAnalysisConfig:
        """转换为道路分析配置"""
        return TrajectoryRoadAnalysisConfig(
            local_dsn=self.local_dsn,
            remote_catalog=self.remote_catalog,
            lane_table=self.lane_table,
            intersection_table=self.intersection_table,
            road_table=self.road_table,
            roadnextroad_table=self.roadnextroad_table,
            intersection_inroad_table=self.intersection_inroad_table,
            intersection_outroad_table=self.intersection_outroad_table,
            buffer_distance=self.buffer_distance,
            forward_chain_limit=self.forward_chain_limit,
            backward_chain_limit=self.backward_chain_limit,
            max_recursion_depth=self.max_recursion_depth,
            max_roads_per_query=self.max_roads_per_query,
            max_intersections_per_query=self.max_intersections_per_query,
            max_forward_road_chains=self.max_forward_road_chains,
            max_backward_road_chains=self.max_backward_road_chains,
            max_lanes_from_roads=self.max_lanes_from_roads,
            query_timeout=self.query_timeout,
            recursive_query_timeout=self.recursive_query_timeout,
            analysis_table=self.road_analysis_table,
            lanes_table=self.road_lanes_table,
            intersections_table=self.road_intersections_table,
            roads_table=self.road_roads_table
        )
    
    def to_lane_analysis_config(self) -> TrajectoryLaneAnalysisConfig:
        """转换为车道分析配置"""
        return TrajectoryLaneAnalysisConfig(
            local_dsn=self.local_dsn,
            remote_catalog=self.remote_catalog,
            road_analysis_lanes_table=self.road_lanes_table,
            sampling_strategy=self.sampling_strategy,
            sampling_distance=self.sampling_distance,
            sampling_time=self.sampling_time,
            uniform_points=self.uniform_points,
            window_size=self.window_size,
            lane_change_threshold=self.lane_change_threshold,
            lane_search_radius=self.lane_search_radius,
            max_candidate_lanes=self.max_candidate_lanes,
            lane_buffer_distance=self.lane_buffer_distance,
            buffer_analysis_batch_size=self.buffer_analysis_batch_size,
            quality_min_points_single_lane=self.quality_min_points_single_lane,
            quality_min_coverage_ratio=self.quality_min_coverage_ratio,
            trajectory_reconstruction_tolerance=self.trajectory_reconstruction_tolerance,
            douglas_peucker_tolerance=self.douglas_peucker_tolerance,
            analysis_table=self.lane_analysis_table,
            segments_table=self.lane_segments_table,
            buffer_table=self.lane_buffer_table,
            quality_table=self.lane_quality_table
        )

@dataclass
class TrajectoryInfo:
    """轨迹信息数据结构"""
    scene_id: str
    data_name: str
    geometry: LineString
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    avg_speed: Optional[float] = None
    max_speed: Optional[float] = None
    min_speed: Optional[float] = None
    std_speed: Optional[float] = None
    avp_ratio: Optional[float] = None
    duration: Optional[int] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'scene_id': self.scene_id,
            'data_name': self.data_name,
            'geometry_wkt': self.geometry.wkt,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'avg_speed': self.avg_speed,
            'max_speed': self.max_speed,
            'min_speed': self.min_speed,
            'std_speed': self.std_speed,
            'avp_ratio': self.avp_ratio,
            'duration': self.duration,
            **self.properties
        }

@dataclass
class AnalysisResult:
    """分析结果数据结构"""
    analysis_id: str
    trajectory_info: TrajectoryInfo
    road_analysis_id: Optional[str] = None
    lane_analysis_id: Optional[str] = None
    road_analysis_summary: Optional[Dict[str, Any]] = None
    lane_analysis_summary: Optional[Dict[str, Any]] = None
    status: str = "pending"  # "pending", "road_completed", "lane_completed", "completed", "failed"
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'analysis_id': self.analysis_id,
            'trajectory_info': self.trajectory_info.to_dict(),
            'road_analysis_id': self.road_analysis_id,
            'lane_analysis_id': self.lane_analysis_id,
            'road_analysis_summary': self.road_analysis_summary,
            'lane_analysis_summary': self.lane_analysis_summary,
            'status': self.status,
            'error_message': self.error_message,
            'created_at': self.created_at.isoformat()
        }

class TrajectoryIntegratedAnalyzer:
    """
    轨迹综合分析器
    
    统一管理轨迹道路分析和轨迹车道分析的完整流程，
    提供从GeoJSON输入到完整分析结果的一体化处理。
    """
    
    def __init__(self, config: Optional[TrajectoryIntegratedAnalysisConfig] = None):
        self.config = config or TrajectoryIntegratedAnalysisConfig()
        
        # 创建子分析器
        self.road_analyzer = TrajectoryRoadAnalyzer(self.config.to_road_analysis_config())
        self.lane_analyzer = TrajectoryLaneAnalyzer(self.config.to_lane_analysis_config())
        
        # 初始化日志
        self._setup_logging()
        
        # 初始化分析表
        self._init_integrated_tables()
        
        logger.info(f"轨迹综合分析器初始化完成")
        logger.info(f"配置: buffer_distance={self.config.buffer_distance}m, "
                   f"sampling_strategy={self.config.sampling_strategy}, "
                   f"batch_size={self.config.batch_size}")
    
    def _setup_logging(self):
        """设置日志系统"""
        # 为综合分析器设置专用日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [INTEGRATED] %(message)s'
        )
        
        # 获取当前logger的handler
        if logger.handlers:
            for handler in logger.handlers:
                handler.setFormatter(formatter)
        else:
            # 如果没有handler，添加控制台handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)
    
    def _init_integrated_tables(self):
        """初始化综合分析表"""
        # 创建主分析表
        self._init_integrated_analysis_table()
        
        # 创建汇总表
        self._init_integrated_summary_table()
    
    def _init_integrated_analysis_table(self):
        """初始化综合分析表"""
        from sqlalchemy import text
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.integrated_analysis_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL UNIQUE,
                trajectory_scene_id VARCHAR(100) NOT NULL,
                trajectory_data_name VARCHAR(100) NOT NULL,
                road_analysis_id VARCHAR(100),
                lane_analysis_id VARCHAR(100),
                status VARCHAR(20) DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 轨迹基本信息
                trajectory_geometry GEOMETRY(LINESTRING, 4326),
                trajectory_start_time BIGINT,
                trajectory_end_time BIGINT,
                trajectory_duration BIGINT,
                trajectory_avg_speed NUMERIC(8,2),
                trajectory_max_speed NUMERIC(8,2),
                trajectory_min_speed NUMERIC(8,2),
                trajectory_std_speed NUMERIC(8,2),
                trajectory_avp_ratio NUMERIC(5,3),
                
                -- 道路分析结果汇总
                road_analysis_total_lanes INTEGER,
                road_analysis_total_intersections INTEGER,
                road_analysis_total_roads INTEGER,
                road_analysis_buffer_geometry GEOMETRY(POLYGON, 4326),
                
                -- 车道分析结果汇总
                lane_analysis_total_segments INTEGER,
                lane_analysis_total_candidate_lanes INTEGER,
                lane_analysis_quality_score NUMERIC(5,3),
                lane_analysis_reconstructed_geometry GEOMETRY(LINESTRING, 4326),
                
                -- 综合分析统计
                processing_time_seconds NUMERIC(10,3),
                total_analysis_points INTEGER,
                analysis_success_rate NUMERIC(5,3)
            )
        """)
        
        # 创建索引
        create_indexes_sql = text(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_analysis_id 
                ON {self.config.integrated_analysis_table}(analysis_id);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_scene_id 
                ON {self.config.integrated_analysis_table}(trajectory_scene_id);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_status 
                ON {self.config.integrated_analysis_table}(status);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_created_at 
                ON {self.config.integrated_analysis_table}(created_at);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_traj_geom 
                ON {self.config.integrated_analysis_table} USING GIST(trajectory_geometry);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_analysis_table}_buffer_geom 
                ON {self.config.integrated_analysis_table} USING GIST(road_analysis_buffer_geometry);
        """)
        
        try:
            with self.road_analyzer.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.execute(create_indexes_sql)
                conn.commit()
            logger.info(f"综合分析表 {self.config.integrated_analysis_table} 初始化完成")
        except Exception as e:
            logger.warning(f"综合分析表初始化失败: {e}")
    
    def _init_integrated_summary_table(self):
        """初始化综合汇总表"""
        from sqlalchemy import text
        
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.integrated_summary_table} (
                id SERIAL PRIMARY KEY,
                batch_id VARCHAR(100) NOT NULL,
                batch_name VARCHAR(200),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 批次基本信息
                total_trajectories INTEGER,
                successful_trajectories INTEGER,
                failed_trajectories INTEGER,
                success_rate NUMERIC(5,3),
                
                -- 处理时间统计
                total_processing_time_seconds NUMERIC(10,3),
                avg_processing_time_seconds NUMERIC(10,3),
                min_processing_time_seconds NUMERIC(10,3),
                max_processing_time_seconds NUMERIC(10,3),
                
                -- 道路分析统计
                total_lanes_found INTEGER,
                total_intersections_found INTEGER,
                total_roads_found INTEGER,
                avg_lanes_per_trajectory NUMERIC(8,2),
                avg_intersections_per_trajectory NUMERIC(8,2),
                avg_roads_per_trajectory NUMERIC(8,2),
                
                -- 车道分析统计
                total_lane_segments INTEGER,
                total_candidate_lanes INTEGER,
                avg_quality_score NUMERIC(5,3),
                quality_score_distribution JSONB,
                
                -- 轨迹特征统计
                avg_trajectory_length_meters NUMERIC(10,2),
                avg_trajectory_duration_seconds NUMERIC(10,2),
                avg_trajectory_speed NUMERIC(8,2),
                speed_distribution JSONB,
                
                -- 空间分布统计
                spatial_bounds GEOMETRY(POLYGON, 4326),
                spatial_density NUMERIC(10,6),
                
                -- 详细统计信息(JSON格式)
                detailed_statistics JSONB,
                error_summary JSONB
            )
        """)
        
        # 创建索引
        create_indexes_sql = text(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_summary_table}_batch_id 
                ON {self.config.integrated_summary_table}(batch_id);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_summary_table}_created_at 
                ON {self.config.integrated_summary_table}(created_at);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_summary_table}_success_rate 
                ON {self.config.integrated_summary_table}(success_rate);
            CREATE INDEX IF NOT EXISTS idx_{self.config.integrated_summary_table}_spatial_bounds 
                ON {self.config.integrated_summary_table} USING GIST(spatial_bounds);
        """)
        
        try:
            with self.road_analyzer.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.execute(create_indexes_sql)
                conn.commit()
            logger.info(f"综合汇总表 {self.config.integrated_summary_table} 初始化完成")
        except Exception as e:
            logger.warning(f"综合汇总表初始化失败: {e}")
    
    def analyze_trajectory_batch(
        self,
        trajectories: List[TrajectoryInfo],
        output_prefix: str = "integrated_analysis"
    ) -> List[AnalysisResult]:
        """
        批量分析轨迹
        
        Args:
            trajectories: 轨迹信息列表
            output_prefix: 输出前缀
            
        Returns:
            分析结果列表
        """
        logger.info(f"开始批量分析 {len(trajectories)} 个轨迹")
        
        results = []
        
        # 按批次处理轨迹
        for i in range(0, len(trajectories), self.config.batch_size):
            batch = trajectories[i:i + self.config.batch_size]
            batch_num = i // self.config.batch_size + 1
            
            logger.info(f"处理批次 {batch_num}: 轨迹 {i+1}-{min(i+len(batch), len(trajectories))}")
            
            # 处理当前批次
            batch_results = self._process_trajectory_batch(batch, output_prefix, batch_num)
            results.extend(batch_results)
            
            # 报告进度
            if batch_num % self.config.progress_report_interval == 0:
                completed = len(results)
                success_count = len([r for r in results if r.status == "completed"])
                logger.info(f"进度报告: 已处理 {completed}/{len(trajectories)} 个轨迹，"
                           f"成功 {success_count} 个")
        
        logger.info(f"批量分析完成: 总计 {len(results)} 个轨迹")
        
        # 保存所有分析结果
        for result in results:
            self._save_analysis_result(result)
        
        # 创建批次汇总
        batch_id = f"{output_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        batch_summary = self.create_batch_summary(batch_id, results)
        
        # 输出批次汇总
        logger.info(f"=== 批次汇总 ({batch_id}) ===")
        logger.info(f"总轨迹数: {batch_summary['total_trajectories']}")
        logger.info(f"成功轨迹数: {batch_summary['successful_trajectories']}")
        logger.info(f"失败轨迹数: {batch_summary['failed_trajectories']}")
        logger.info(f"成功率: {batch_summary['success_rate']:.2%}")
        logger.info(f"平均处理时间: {batch_summary['avg_processing_time_seconds']:.2f}秒")
        logger.info(f"总发现车道数: {batch_summary['total_lanes_found']}")
        logger.info(f"总发现路口数: {batch_summary['total_intersections_found']}")
        logger.info(f"总发现道路数: {batch_summary['total_roads_found']}")
        logger.info(f"平均质量分数: {batch_summary['avg_quality_score']:.3f}")
        
        return results
    
    def _process_trajectory_batch(
        self,
        trajectories: List[TrajectoryInfo],
        output_prefix: str,
        batch_num: int
    ) -> List[AnalysisResult]:
        """
        处理轨迹批次
        
        Args:
            trajectories: 轨迹信息列表
            output_prefix: 输出前缀
            batch_num: 批次号
            
        Returns:
            分析结果列表
        """
        results = []
        
        for trajectory in trajectories:
            try:
                # 生成分析ID
                analysis_id = f"{output_prefix}_{trajectory.scene_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # 创建分析结果记录
                result = AnalysisResult(
                    analysis_id=analysis_id,
                    trajectory_info=trajectory
                )
                
                # 执行两阶段分析
                result = self._execute_two_stage_analysis(result)
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"处理轨迹失败: {trajectory.scene_id}, 错误: {str(e)}")
                
                # 创建失败结果记录
                error_result = AnalysisResult(
                    analysis_id=f"error_{trajectory.scene_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    trajectory_info=trajectory,
                    status="failed",
                    error_message=str(e)
                )
                results.append(error_result)
        
        return results
    
    def _execute_two_stage_analysis(self, result: AnalysisResult) -> AnalysisResult:
        """
        执行两阶段分析
        
        Args:
            result: 分析结果对象
            
        Returns:
            更新后的分析结果对象
        """
        try:
            # 第一阶段：道路分析
            logger.info(f"开始道路分析: {result.trajectory_info.scene_id}")
            
            road_analysis_id = self.road_analyzer.analyze_trajectory_roads(
                trajectory_id=result.trajectory_info.scene_id,
                trajectory_geom=result.trajectory_info.geometry.wkt,
                analysis_id=f"road_{result.analysis_id}"
            )
            
            result.road_analysis_id = road_analysis_id
            result.road_analysis_summary = self.road_analyzer.get_analysis_summary(road_analysis_id)
            result.status = "road_completed"
            
            logger.info(f"道路分析完成: {result.trajectory_info.scene_id}")
            
            # 第二阶段：车道分析
            logger.info(f"开始车道分析: {result.trajectory_info.scene_id}")
            
            lane_analysis_id = self.lane_analyzer.analyze_trajectory_lanes(
                trajectory_info=result.trajectory_info,
                road_analysis_id=road_analysis_id,
                analysis_id=f"lane_{result.analysis_id}"
            )
            
            result.lane_analysis_id = lane_analysis_id
            result.lane_analysis_summary = self.lane_analyzer.get_analysis_summary(lane_analysis_id)
            result.status = "completed"
            
            logger.info(f"车道分析完成: {result.trajectory_info.scene_id}")
            
        except Exception as e:
            logger.error(f"两阶段分析失败: {result.trajectory_info.scene_id}, 错误: {str(e)}")
            result.status = "failed"
            result.error_message = str(e)
        
        return result
    
    def _save_analysis_result(self, result: AnalysisResult):
        """
        保存分析结果到数据库
        
        Args:
            result: 分析结果对象
        """
        from sqlalchemy import text
        
        # 计算处理时间
        processing_time = (datetime.now() - result.created_at).total_seconds()
        
        # 准备数据
        data = {
            'analysis_id': result.analysis_id,
            'trajectory_scene_id': result.trajectory_info.scene_id,
            'trajectory_data_name': result.trajectory_info.data_name,
            'road_analysis_id': result.road_analysis_id,
            'lane_analysis_id': result.lane_analysis_id,
            'status': result.status,
            'error_message': result.error_message,
            'trajectory_geometry': result.trajectory_info.geometry.wkt,
            'trajectory_start_time': result.trajectory_info.start_time,
            'trajectory_end_time': result.trajectory_info.end_time,
            'trajectory_duration': result.trajectory_info.duration,
            'trajectory_avg_speed': result.trajectory_info.avg_speed,
            'trajectory_max_speed': result.trajectory_info.max_speed,
            'trajectory_min_speed': result.trajectory_info.min_speed,
            'trajectory_std_speed': result.trajectory_info.std_speed,
            'trajectory_avp_ratio': result.trajectory_info.avp_ratio,
            'processing_time_seconds': processing_time
        }
        
        # 添加道路分析结果
        if result.road_analysis_summary:
            data.update({
                'road_analysis_total_lanes': result.road_analysis_summary.get('total_lanes', 0),
                'road_analysis_total_intersections': result.road_analysis_summary.get('total_intersections', 0),
                'road_analysis_total_roads': result.road_analysis_summary.get('total_roads', 0)
            })
        
        # 添加车道分析结果
        if result.lane_analysis_summary:
            data.update({
                'lane_analysis_total_segments': result.lane_analysis_summary.get('total_segments', 0),
                'lane_analysis_total_candidate_lanes': result.lane_analysis_summary.get('total_candidate_lanes', 0),
                'lane_analysis_quality_score': result.lane_analysis_summary.get('quality_score', 0)
            })
        
        # 构建SQL
        columns = ', '.join(data.keys())
        placeholders = ', '.join([f':{key}' for key in data.keys()])
        
        insert_sql = text(f"""
            INSERT INTO {self.config.integrated_analysis_table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (analysis_id) DO UPDATE SET
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                road_analysis_id = EXCLUDED.road_analysis_id,
                lane_analysis_id = EXCLUDED.lane_analysis_id,
                road_analysis_total_lanes = EXCLUDED.road_analysis_total_lanes,
                road_analysis_total_intersections = EXCLUDED.road_analysis_total_intersections,
                road_analysis_total_roads = EXCLUDED.road_analysis_total_roads,
                lane_analysis_total_segments = EXCLUDED.lane_analysis_total_segments,
                lane_analysis_total_candidate_lanes = EXCLUDED.lane_analysis_total_candidate_lanes,
                lane_analysis_quality_score = EXCLUDED.lane_analysis_quality_score,
                processing_time_seconds = EXCLUDED.processing_time_seconds,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        # 执行插入
        try:
            with self.road_analyzer.local_engine.connect() as conn:
                # 先设置几何
                geometry_data = {'analysis_id': result.analysis_id, 'geometry_wkt': result.trajectory_info.geometry.wkt}
                geometry_sql = text(f"""
                    UPDATE {self.config.integrated_analysis_table}
                    SET trajectory_geometry = ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
                    WHERE analysis_id = :analysis_id
                """)
                
                conn.execute(insert_sql, data)
                conn.execute(geometry_sql, geometry_data)
                conn.commit()
                
                logger.debug(f"保存分析结果: {result.analysis_id}")
        except Exception as e:
            logger.error(f"保存分析结果失败: {result.analysis_id}, 错误: {str(e)}")
    
    def get_analysis_summary(self, analysis_ids: List[str]) -> Dict[str, Any]:
        """
        获取综合分析汇总
        
        Args:
            analysis_ids: 分析ID列表
            
        Returns:
            综合分析汇总字典
        """
        from sqlalchemy import text
        
        if not analysis_ids:
            return {'total_analyses': 0, 'timestamp': datetime.now().isoformat()}
        
        # 构建查询SQL
        analysis_ids_str = "','".join(analysis_ids)
        query_sql = text(f"""
            SELECT 
                COUNT(*) as total_analyses,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_analyses,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_analyses,
                AVG(processing_time_seconds) as avg_processing_time,
                SUM(road_analysis_total_lanes) as total_lanes,
                SUM(road_analysis_total_intersections) as total_intersections,
                SUM(road_analysis_total_roads) as total_roads,
                SUM(lane_analysis_total_segments) as total_segments,
                AVG(lane_analysis_quality_score) as avg_quality_score,
                AVG(trajectory_avg_speed) as avg_trajectory_speed,
                MIN(created_at) as first_analysis_time,
                MAX(created_at) as last_analysis_time
            FROM {self.config.integrated_analysis_table}
            WHERE analysis_id IN ('{analysis_ids_str}')
        """)
        
        try:
            with self.road_analyzer.local_engine.connect() as conn:
                result = conn.execute(query_sql).fetchone()
                
                if result:
                    return {
                        'total_analyses': result[0] or 0,
                        'successful_analyses': result[1] or 0,
                        'failed_analyses': result[2] or 0,
                        'success_rate': (result[1] or 0) / (result[0] or 1),
                        'avg_processing_time_seconds': float(result[3]) if result[3] else 0,
                        'total_lanes': result[4] or 0,
                        'total_intersections': result[5] or 0,
                        'total_roads': result[6] or 0,
                        'total_segments': result[7] or 0,
                        'avg_quality_score': float(result[8]) if result[8] else 0,
                        'avg_trajectory_speed': float(result[9]) if result[9] else 0,
                        'analysis_time_span': {
                            'start': result[10].isoformat() if result[10] else None,
                            'end': result[11].isoformat() if result[11] else None
                        },
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    return {'total_analyses': 0, 'timestamp': datetime.now().isoformat()}
                    
        except Exception as e:
            logger.error(f"获取分析汇总失败: {str(e)}")
            return {
                'total_analyses': len(analysis_ids),
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def create_batch_summary(self, batch_id: str, results: List[AnalysisResult]) -> Dict[str, Any]:
        """
        创建批次汇总
        
        Args:
            batch_id: 批次ID
            results: 分析结果列表
            
        Returns:
            批次汇总字典
        """
        from sqlalchemy import text
        from shapely.geometry import MultiPoint
        from shapely.ops import unary_union
        
        if not results:
            return {'batch_id': batch_id, 'total_trajectories': 0}
        
        # 计算基本统计
        total_trajectories = len(results)
        successful_trajectories = len([r for r in results if r.status == 'completed'])
        failed_trajectories = len([r for r in results if r.status == 'failed'])
        success_rate = successful_trajectories / total_trajectories if total_trajectories > 0 else 0
        
        # 计算处理时间统计
        processing_times = []
        for result in results:
            if result.created_at:
                processing_time = (datetime.now() - result.created_at).total_seconds()
                processing_times.append(processing_time)
        
        # 计算道路分析统计
        road_stats = {
            'total_lanes': 0,
            'total_intersections': 0,
            'total_roads': 0
        }
        
        lane_stats = {
            'total_segments': 0,
            'total_candidate_lanes': 0,
            'quality_scores': []
        }
        
        trajectory_stats = {
            'lengths': [],
            'durations': [],
            'speeds': [],
            'geometries': []
        }
        
        for result in results:
            if result.road_analysis_summary:
                road_stats['total_lanes'] += result.road_analysis_summary.get('total_lanes', 0)
                road_stats['total_intersections'] += result.road_analysis_summary.get('total_intersections', 0)
                road_stats['total_roads'] += result.road_analysis_summary.get('total_roads', 0)
            
            if result.lane_analysis_summary:
                lane_stats['total_segments'] += result.lane_analysis_summary.get('total_segments', 0)
                lane_stats['total_candidate_lanes'] += result.lane_analysis_summary.get('total_candidate_lanes', 0)
                quality_score = result.lane_analysis_summary.get('quality_score')
                if quality_score is not None:
                    lane_stats['quality_scores'].append(quality_score)
            
            # 轨迹特征统计
            traj_info = result.trajectory_info
            if traj_info.geometry:
                trajectory_stats['lengths'].append(traj_info.geometry.length)
                trajectory_stats['geometries'].append(traj_info.geometry)
            
            if traj_info.duration:
                trajectory_stats['durations'].append(traj_info.duration)
            
            if traj_info.avg_speed:
                trajectory_stats['speeds'].append(traj_info.avg_speed)
        
        # 计算空间边界
        spatial_bounds = None
        if trajectory_stats['geometries']:
            try:
                # 获取所有轨迹的边界点
                all_points = []
                for geom in trajectory_stats['geometries']:
                    all_points.extend(list(geom.coords))
                
                if all_points:
                    multi_point = MultiPoint(all_points)
                    spatial_bounds = multi_point.convex_hull
            except Exception as e:
                logger.warning(f"计算空间边界失败: {str(e)}")
        
        # 创建汇总数据
        summary_data = {
            'batch_id': batch_id,
            'batch_name': f"Batch_{batch_id}",
            'total_trajectories': total_trajectories,
            'successful_trajectories': successful_trajectories,
            'failed_trajectories': failed_trajectories,
            'success_rate': success_rate,
            'total_processing_time_seconds': sum(processing_times),
            'avg_processing_time_seconds': sum(processing_times) / len(processing_times) if processing_times else 0,
            'min_processing_time_seconds': min(processing_times) if processing_times else 0,
            'max_processing_time_seconds': max(processing_times) if processing_times else 0,
            'total_lanes_found': road_stats['total_lanes'],
            'total_intersections_found': road_stats['total_intersections'],
            'total_roads_found': road_stats['total_roads'],
            'avg_lanes_per_trajectory': road_stats['total_lanes'] / successful_trajectories if successful_trajectories > 0 else 0,
            'avg_intersections_per_trajectory': road_stats['total_intersections'] / successful_trajectories if successful_trajectories > 0 else 0,
            'avg_roads_per_trajectory': road_stats['total_roads'] / successful_trajectories if successful_trajectories > 0 else 0,
            'total_lane_segments': lane_stats['total_segments'],
            'total_candidate_lanes': lane_stats['total_candidate_lanes'],
            'avg_quality_score': sum(lane_stats['quality_scores']) / len(lane_stats['quality_scores']) if lane_stats['quality_scores'] else 0,
            'avg_trajectory_length_meters': sum(trajectory_stats['lengths']) / len(trajectory_stats['lengths']) if trajectory_stats['lengths'] else 0,
            'avg_trajectory_duration_seconds': sum(trajectory_stats['durations']) / len(trajectory_stats['durations']) if trajectory_stats['durations'] else 0,
            'avg_trajectory_speed': sum(trajectory_stats['speeds']) / len(trajectory_stats['speeds']) if trajectory_stats['speeds'] else 0,
            'spatial_bounds': spatial_bounds.wkt if spatial_bounds else None,
            'detailed_statistics': {
                'quality_score_distribution': self._calculate_distribution(lane_stats['quality_scores']),
                'speed_distribution': self._calculate_distribution(trajectory_stats['speeds']),
                'length_distribution': self._calculate_distribution(trajectory_stats['lengths']),
                'duration_distribution': self._calculate_distribution(trajectory_stats['durations'])
            },
            'error_summary': self._calculate_error_summary(results)
        }
        
        # 保存到数据库
        self._save_batch_summary(summary_data)
        
        return summary_data
    
    def _calculate_distribution(self, values: List[float]) -> Dict[str, Any]:
        """计算数值分布统计"""
        if not values:
            return {'count': 0}
        
        values.sort()
        n = len(values)
        
        return {
            'count': n,
            'min': min(values),
            'max': max(values),
            'mean': sum(values) / n,
            'median': values[n // 2],
            'q25': values[n // 4],
            'q75': values[3 * n // 4],
            'std': (sum((x - sum(values) / n) ** 2 for x in values) / n) ** 0.5 if n > 1 else 0
        }
    
    def _calculate_error_summary(self, results: List[AnalysisResult]) -> Dict[str, Any]:
        """计算错误汇总"""
        error_types = {}
        failed_results = [r for r in results if r.status == 'failed']
        
        for result in failed_results:
            if result.error_message:
                # 提取错误类型（简化）
                error_type = result.error_message.split(':')[0] if ':' in result.error_message else 'Unknown'
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'total_errors': len(failed_results),
            'error_types': error_types,
            'error_rate': len(failed_results) / len(results) if results else 0
        }
    
    def _save_batch_summary(self, summary_data: Dict[str, Any]):
        """保存批次汇总到数据库"""
        from sqlalchemy import text
        
        # 准备数据
        data = {k: v for k, v in summary_data.items() if k not in ['spatial_bounds', 'detailed_statistics', 'error_summary']}
        data['detailed_statistics'] = json.dumps(summary_data['detailed_statistics'])
        data['error_summary'] = json.dumps(summary_data['error_summary'])
        
        # 构建SQL
        columns = ', '.join(data.keys())
        placeholders = ', '.join([f':{key}' for key in data.keys()])
        
        insert_sql = text(f"""
            INSERT INTO {self.config.integrated_summary_table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (batch_id) DO UPDATE SET
                total_trajectories = EXCLUDED.total_trajectories,
                successful_trajectories = EXCLUDED.successful_trajectories,
                failed_trajectories = EXCLUDED.failed_trajectories,
                success_rate = EXCLUDED.success_rate,
                detailed_statistics = EXCLUDED.detailed_statistics,
                error_summary = EXCLUDED.error_summary
        """)
        
        try:
            with self.road_analyzer.local_engine.connect() as conn:
                conn.execute(insert_sql, data)
                
                # 更新空间边界
                if summary_data['spatial_bounds']:
                    geometry_sql = text(f"""
                        UPDATE {self.config.integrated_summary_table}
                        SET spatial_bounds = ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
                        WHERE batch_id = :batch_id
                    """)
                    conn.execute(geometry_sql, {
                        'geometry_wkt': summary_data['spatial_bounds'],
                        'batch_id': summary_data['batch_id']
                    })
                
                conn.commit()
                logger.info(f"保存批次汇总: {summary_data['batch_id']}")
        except Exception as e:
            logger.error(f"保存批次汇总失败: {str(e)}")
    
    def export_results(
        self,
        analysis_ids: List[str],
        output_format: str = "json",
        output_path: Optional[str] = None
    ) -> str:
        """
        导出分析结果
        
        Args:
            analysis_ids: 分析ID列表
            output_format: 输出格式 ("json", "csv", "geojson")
            output_path: 输出路径
            
        Returns:
            导出文件路径
        """
        # 这里将在后续任务中实现
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"trajectory_analysis_results_{timestamp}.{output_format}"
        
        logger.info(f"导出分析结果到: {output_path}")
        return output_path

# 便捷接口函数
def analyze_trajectories_from_geojson(
    geojson_path: str,
    output_prefix: str = "integrated_analysis",
    config: Optional[TrajectoryIntegratedAnalysisConfig] = None
) -> List[AnalysisResult]:
    """
    从GeoJSON文件分析轨迹
    
    Args:
        geojson_path: GeoJSON文件路径
        output_prefix: 输出前缀
        config: 自定义配置
        
    Returns:
        分析结果列表
    """
    from .trajectory_data_processor import load_trajectories_from_geojson
    
    logger.info(f"从GeoJSON文件加载轨迹: {geojson_path}")
    
    # 加载轨迹数据
    trajectories = load_trajectories_from_geojson(geojson_path)
    
    if not trajectories:
        logger.warning("未加载到任何有效轨迹")
        return []
    
    # 创建分析器
    analyzer = TrajectoryIntegratedAnalyzer(config)
    
    # 执行分析
    results = analyzer.analyze_trajectory_batch(trajectories, output_prefix)
    
    return results

if __name__ == "__main__":
    # 简单的测试示例
    import argparse
    
    parser = argparse.ArgumentParser(description='轨迹综合分析模块')
    parser.add_argument('--input-geojson', required=True, help='输入GeoJSON文件路径')
    parser.add_argument('--output-prefix', default='integrated_analysis', help='输出前缀')
    parser.add_argument('--buffer-distance', type=float, default=3.0, help='缓冲区距离(m)')
    parser.add_argument('--sampling-strategy', choices=['distance', 'time', 'uniform'], 
                       default='distance', help='采样策略')
    parser.add_argument('--batch-size', type=int, default=10, help='批处理大小')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 创建配置
        config = TrajectoryIntegratedAnalysisConfig(
            buffer_distance=args.buffer_distance,
            sampling_strategy=args.sampling_strategy,
            batch_size=args.batch_size
        )
        
        # 执行分析
        results = analyze_trajectories_from_geojson(
            geojson_path=args.input_geojson,
            output_prefix=args.output_prefix,
            config=config
        )
        
        # 输出结果
        logger.info(f"分析完成: {len(results)} 个轨迹")
        success_count = len([r for r in results if r.status == "completed"])
        logger.info(f"成功: {success_count}/{len(results)} 个轨迹")
        
    except Exception as e:
        logger.error(f"分析失败: {e}")
        exit(1) 