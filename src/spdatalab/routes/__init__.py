"""
Routes package for managing different data sources of route information.
"""

from .models import Route
from .amap import AmapRoute

__all__ = ['Route', 'AmapRoute'] 