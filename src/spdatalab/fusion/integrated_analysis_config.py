"""
集成轨迹分析配置模块

统一管理两阶段轨迹分析的配置参数：
1. 道路分析配置 (trajectory_road_analysis)
2. 车道分析配置 (trajectory_lane_analysis)
3. 批量处理配置
4. 输出配置
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

@dataclass
class TrajectoryRoadAnalysisConfig:
    """轨迹道路分析配置"""
    # 数据库连接配置
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_catalog: str = "rcdatalake_gy1"
    
    # 远程表名配置
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    road_table: str = "full_road"
    roadnextroad_table: str = "roadnextroad"
    intersection_inroad_table: str = "full_intersectiongoinroad"
    intersection_outroad_table: str = "full_intersectiongooutroad"
    
    # 分析参数
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
    
    # 结果表名
    analysis_table: str = "trajectory_road_analysis"
    lanes_table: str = "trajectory_road_lanes"
    intersections_table: str = "trajectory_road_intersections"
    roads_table: str = "trajectory_road_roads"

@dataclass
class TrajectoryLaneAnalysisConfig:
    """轨迹车道分析配置"""
    # 数据库连接配置
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    
    # 数据表配置
    point_table: str = "public.ddi_data_points"
    road_analysis_lanes_table: str = "trajectory_road_lanes"
    
    # 采样策略配置
    sampling_strategy: str = "distance"  # 'distance', 'time', 'uniform'
    distance_interval: float = 10.0  # 距离采样间隔(m)
    time_interval: float = 2.0  # 时间采样间隔(s)
    uniform_sample_count: int = 100  # 均匀采样点数
    
    # 滑窗分析配置
    window_size: int = 5  # 滑窗大小
    window_step: int = 1  # 滑窗步长
    min_lane_points: int = 3  # 单车道最少点数
    lane_change_threshold: float = 0.6  # 车道变化阈值
    
    # 缓冲区分析配置
    buffer_distance: float = 2.0  # 缓冲区距离(m)
    buffer_resolution: int = 8  # 缓冲区分辨率
    
    # 质量检查配置
    min_single_lane_points: int = 5  # 单车道最少点数
    min_multi_lane_points: int = 3  # 多车道最少点数
    quality_check_enabled: bool = True  # 启用质量检查
    
    # 轨迹重构配置
    enable_trajectory_reconstruction: bool = True  # 启用轨迹重构
    douglas_peucker_tolerance: float = 0.00001  # Douglas-Peucker简化容差
    
    # 批量处理配置
    batch_size: int = 10  # 批处理大小
    max_retries: int = 3  # 最大重试次数
    
    # 结果表名
    segments_table: str = "trajectory_lane_segments"
    buffer_table: str = "trajectory_lane_buffer"
    quality_table: str = "trajectory_quality_check"

@dataclass
class BatchProcessingConfig:
    """批量处理配置"""
    # 并发配置
    enable_parallel: bool = True  # 启用并行处理
    max_workers: int = 4  # 最大工作线程数
    
    # 批量大小配置
    road_analysis_batch_size: int = 20  # 道路分析批量大小
    lane_analysis_batch_size: int = 10  # 车道分析批量大小
    
    # 错误处理配置
    continue_on_error: bool = True  # 出错时继续处理
    max_retries: int = 3  # 最大重试次数
    retry_delay: float = 1.0  # 重试延迟(s)
    
    # 进度跟踪配置
    enable_progress_tracking: bool = True  # 启用进度跟踪
    progress_log_interval: int = 10  # 进度日志间隔
    
    # 中间结果保存配置
    save_intermediate_results: bool = True  # 保存中间结果
    intermediate_results_path: str = "intermediate_results"  # 中间结果保存路径

@dataclass
class OutputConfig:
    """输出配置"""
    # 输出格式配置
    generate_reports: bool = True  # 生成报告
    report_format: str = "markdown"  # 报告格式 ('markdown', 'html', 'json')
    
    # QGIS可视化配置
    create_qgis_views: bool = True  # 创建QGIS视图
    qgis_view_prefix: str = "integrated_analysis"  # QGIS视图前缀
    
    # 导出配置
    export_to_geojson: bool = False  # 导出为GeoJSON
    export_to_parquet: bool = False  # 导出为Parquet
    export_path: str = "output"  # 导出路径
    
    # 汇总统计配置
    generate_summary_stats: bool = True  # 生成汇总统计
    include_detailed_stats: bool = True  # 包含详细统计
    stats_output_path: str = "stats"  # 统计输出路径

@dataclass
class IntegratedAnalysisConfig:
    """集成轨迹分析配置"""
    # 子配置
    road_analysis_config: TrajectoryRoadAnalysisConfig = field(default_factory=TrajectoryRoadAnalysisConfig)
    lane_analysis_config: TrajectoryLaneAnalysisConfig = field(default_factory=TrajectoryLaneAnalysisConfig)
    batch_processing_config: BatchProcessingConfig = field(default_factory=BatchProcessingConfig)
    output_config: OutputConfig = field(default_factory=OutputConfig)
    
    # 全局配置
    analysis_name: str = "integrated_trajectory_analysis"  # 分析名称
    analysis_description: str = "两阶段轨迹分析：道路分析 + 车道分析"  # 分析描述
    
    # 日志配置
    log_level: str = "INFO"  # 日志级别
    log_file: Optional[str] = None  # 日志文件路径
    enable_detailed_logging: bool = False  # 启用详细日志
    
    # 调试配置
    debug_mode: bool = False  # 调试模式
    dry_run: bool = False  # 演习模式
    
    def __post_init__(self):
        """配置后处理"""
        # 确保数据库连接配置一致
        if self.road_analysis_config.local_dsn != self.lane_analysis_config.local_dsn:
            logger.warning("道路分析和车道分析的数据库连接配置不一致")
        
        # 验证配置参数
        self._validate_config()
    
    def _validate_config(self):
        """验证配置参数"""
        # 验证缓冲区距离
        if self.road_analysis_config.buffer_distance <= 0:
            raise ValueError("道路分析缓冲区距离必须大于0")
        
        if self.lane_analysis_config.buffer_distance <= 0:
            raise ValueError("车道分析缓冲区距离必须大于0")
        
        # 验证采样配置
        if self.lane_analysis_config.sampling_strategy not in ['distance', 'time', 'uniform']:
            raise ValueError("采样策略必须是 'distance', 'time', 'uniform' 之一")
        
        # 验证批量处理配置
        if self.batch_processing_config.max_workers <= 0:
            raise ValueError("最大工作线程数必须大于0")
        
        # 验证输出配置
        if self.output_config.report_format not in ['markdown', 'html', 'json']:
            raise ValueError("报告格式必须是 'markdown', 'html', 'json' 之一")
        
        logger.info("配置验证通过")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'road_analysis_config': self.road_analysis_config.__dict__,
            'lane_analysis_config': self.lane_analysis_config.__dict__,
            'batch_processing_config': self.batch_processing_config.__dict__,
            'output_config': self.output_config.__dict__,
            'analysis_name': self.analysis_name,
            'analysis_description': self.analysis_description,
            'log_level': self.log_level,
            'log_file': self.log_file,
            'enable_detailed_logging': self.enable_detailed_logging,
            'debug_mode': self.debug_mode,
            'dry_run': self.dry_run
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'IntegratedAnalysisConfig':
        """从字典创建配置"""
        road_config = TrajectoryRoadAnalysisConfig(**config_dict.get('road_analysis_config', {}))
        lane_config = TrajectoryLaneAnalysisConfig(**config_dict.get('lane_analysis_config', {}))
        batch_config = BatchProcessingConfig(**config_dict.get('batch_processing_config', {}))
        output_config = OutputConfig(**config_dict.get('output_config', {}))
        
        return cls(
            road_analysis_config=road_config,
            lane_analysis_config=lane_config,
            batch_processing_config=batch_config,
            output_config=output_config,
            analysis_name=config_dict.get('analysis_name', 'integrated_trajectory_analysis'),
            analysis_description=config_dict.get('analysis_description', '两阶段轨迹分析：道路分析 + 车道分析'),
            log_level=config_dict.get('log_level', 'INFO'),
            log_file=config_dict.get('log_file'),
            enable_detailed_logging=config_dict.get('enable_detailed_logging', False),
            debug_mode=config_dict.get('debug_mode', False),
            dry_run=config_dict.get('dry_run', False)
        )
    
    def save_to_file(self, file_path: str):
        """保存配置到文件"""
        import json
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info(f"配置已保存到: {file_path}")
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'IntegratedAnalysisConfig':
        """从文件加载配置"""
        import json
        with open(file_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        logger.info(f"配置已从文件加载: {file_path}")
        return cls.from_dict(config_dict)
    
    def create_summary(self) -> str:
        """创建配置摘要"""
        summary_lines = [
            f"# 集成轨迹分析配置摘要",
            f"",
            f"**分析名称**: {self.analysis_name}",
            f"**分析描述**: {self.analysis_description}",
            f"",
            f"## 道路分析配置",
            f"- 缓冲区距离: {self.road_analysis_config.buffer_distance}m",
            f"- 前向链路限制: {self.road_analysis_config.forward_chain_limit}m",
            f"- 后向链路限制: {self.road_analysis_config.backward_chain_limit}m",
            f"- 最大递归深度: {self.road_analysis_config.max_recursion_depth}",
            f"",
            f"## 车道分析配置",
            f"- 采样策略: {self.lane_analysis_config.sampling_strategy}",
            f"- 距离间隔: {self.lane_analysis_config.distance_interval}m",
            f"- 时间间隔: {self.lane_analysis_config.time_interval}s",
            f"- 缓冲区距离: {self.lane_analysis_config.buffer_distance}m",
            f"- 滑窗大小: {self.lane_analysis_config.window_size}",
            f"- 单车道最少点数: {self.lane_analysis_config.min_single_lane_points}",
            f"",
            f"## 批量处理配置",
            f"- 启用并行处理: {self.batch_processing_config.enable_parallel}",
            f"- 最大工作线程数: {self.batch_processing_config.max_workers}",
            f"- 道路分析批量大小: {self.batch_processing_config.road_analysis_batch_size}",
            f"- 车道分析批量大小: {self.batch_processing_config.lane_analysis_batch_size}",
            f"",
            f"## 输出配置",
            f"- 生成报告: {self.output_config.generate_reports}",
            f"- 报告格式: {self.output_config.report_format}",
            f"- 创建QGIS视图: {self.output_config.create_qgis_views}",
            f"- 导出GeoJSON: {self.output_config.export_to_geojson}",
            f"",
            f"## 全局配置",
            f"- 日志级别: {self.log_level}",
            f"- 调试模式: {self.debug_mode}",
            f"- 演习模式: {self.dry_run}",
        ]
        
        return "\n".join(summary_lines)

def create_default_config() -> IntegratedAnalysisConfig:
    """创建默认配置"""
    return IntegratedAnalysisConfig()

def create_fast_config() -> IntegratedAnalysisConfig:
    """创建快速处理配置"""
    config = IntegratedAnalysisConfig()
    
    # 减少分析精度以提高速度
    config.road_analysis_config.forward_chain_limit = 200.0
    config.road_analysis_config.backward_chain_limit = 50.0
    config.road_analysis_config.max_recursion_depth = 5
    
    config.lane_analysis_config.distance_interval = 20.0
    config.lane_analysis_config.uniform_sample_count = 50
    config.lane_analysis_config.window_size = 3
    
    # 增加批量处理大小
    config.batch_processing_config.road_analysis_batch_size = 50
    config.batch_processing_config.lane_analysis_batch_size = 20
    config.batch_processing_config.max_workers = 8
    
    return config

def create_high_precision_config() -> IntegratedAnalysisConfig:
    """创建高精度配置"""
    config = IntegratedAnalysisConfig()
    
    # 提高分析精度
    config.road_analysis_config.forward_chain_limit = 1000.0
    config.road_analysis_config.backward_chain_limit = 200.0
    config.road_analysis_config.max_recursion_depth = 15
    
    config.lane_analysis_config.distance_interval = 5.0
    config.lane_analysis_config.uniform_sample_count = 200
    config.lane_analysis_config.window_size = 7
    config.lane_analysis_config.buffer_distance = 1.0
    
    # 减少批量处理大小以保证精度
    config.batch_processing_config.road_analysis_batch_size = 10
    config.batch_processing_config.lane_analysis_batch_size = 5
    
    return config

if __name__ == "__main__":
    # 测试配置
    import tempfile
    import os
    
    # 创建默认配置
    config = create_default_config()
    print("默认配置创建成功")
    
    # 测试配置摘要
    summary = config.create_summary()
    print(summary)
    
    # 测试配置保存和加载
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config.save_to_file(f.name)
        
        # 加载配置
        loaded_config = IntegratedAnalysisConfig.load_from_file(f.name)
        print(f"配置保存和加载测试通过")
        
        # 删除临时文件
        os.unlink(f.name)
    
    # 测试不同的配置预设
    fast_config = create_fast_config()
    print(f"快速配置创建成功")
    
    high_precision_config = create_high_precision_config()
    print(f"高精度配置创建成功") 