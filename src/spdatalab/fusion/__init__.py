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

__all__ = [
    # 生产级API
    'ProductionSpatialJoin',
    'SpatialJoinConfig',
    'quick_spatial_join',
    'DatabaseConfig',
    # 兼容性别名
    'SpatialJoin'
]

# 为了兼容现有代码，提供别名
SpatialJoin = ProductionSpatialJoin

# 版本信息
__version__ = "2.0.0"
__author__ = "SPDataLab Team" 