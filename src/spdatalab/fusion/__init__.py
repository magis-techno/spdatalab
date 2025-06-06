"""
高性能空间融合和连接分析模块

提供生产级polygon相交解决方案，支持自动策略选择和大规模数据处理。

主要功能：
- 高性能polygon相交查询
- 自动选择最优查询策略（批量 vs 分块）
- 完整的性能统计和错误处理
- 支持城市过滤和自定义配置

性能指标：
- 100个bbox: ~1秒
- 1000个bbox: ~3秒 
- 10000个bbox: ~20秒
"""

# 生产级解决方案
from .spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig, 
    quick_spatial_join
)

# 配置模块
from .config import DatabaseConfig

# 向后兼容（如果需要旧版本）
try:
    from .archive.spatial_join import SpatialJoin, SpatialRelation, JoinType, SummaryMethod
except ImportError:
    # 如果archive不存在，提供占位符
    class SpatialJoin:
        def __init__(self, *args, **kwargs):
            raise ImportError("旧版SpatialJoin已归档，请使用ProductionSpatialJoin")
    
    SpatialRelation = JoinType = SummaryMethod = None

__all__ = [
    # 生产级API
    'ProductionSpatialJoin',
    'SpatialJoinConfig',
    'quick_spatial_join',
    'DatabaseConfig',
    
    # 向后兼容
    'SpatialJoin',
    'SpatialRelation', 
    'JoinType',
    'SummaryMethod'
]

# 版本信息
__version__ = "2.0.0"
__author__ = "SPDataLab Team" 