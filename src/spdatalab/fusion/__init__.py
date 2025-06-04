"""
Spatial fusion and overlay analysis module for trajectory and spatial data.
"""

from .spatial_join import SpatialJoin, SpatialRelation, JoinType, SummaryMethod

__all__ = [
    'SpatialJoin',
    'SpatialRelation',
    'JoinType', 
    'SummaryMethod'
] 