"""
Data models for route information from different sources.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

Base = declarative_base()

class Route(Base):
    """Base model for storing route information."""
    
    __tablename__ = 'routes'
    
    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)  # 数据源（如 'amap'）
    route_id = Column(String(100), nullable=False)  # 数据源中的路线ID
    url = Column(String(500), nullable=False)  # 原始URL
    name = Column(String(200), nullable=False)
    region = Column(String(100))
    total_distance = Column(Float)
    is_active = Column(Boolean, default=True)
    route_metadata = Column(JSON)  # 存储额外的元数据
    geometry = Column(Geometry('LINESTRING', srid=4326))  # 路线几何对象
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # 关系
    segments = relationship("RouteSegment", back_populates="route")
    
    def __repr__(self):
        return f"<Route(source='{self.source}', route_id='{self.route_id}', name='{self.name}')>"

class RouteSegment(Base):
    """Model for storing route segment information."""
    
    __tablename__ = 'route_segments'
    
    id = Column(Integer, primary_key=True)
    route_id = Column(Integer, ForeignKey('routes.id'), nullable=False)
    segment_order = Column(Integer, nullable=False)
    gaode_link = Column(String(500), nullable=False)
    distance = Column(Float)
    duration = Column(Float)  # 预计时间（秒）
    instruction = Column(String(500))  # 导航指示
    start_point = Column(Geometry('POINT', srid=4326))
    end_point = Column(Geometry('POINT', srid=4326))
    path = Column(Geometry('LINESTRING', srid=4326))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # 关系
    route = relationship("Route", back_populates="segments")
    points = relationship("RoutePoint", back_populates="segment")
    
    def __repr__(self):
        return f"<RouteSegment(route_id={self.route_id}, order={self.segment_order})>"

class RoutePoint(Base):
    """Model for storing detailed route point information."""
    
    __tablename__ = 'route_points'
    
    id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, ForeignKey('route_segments.id'), nullable=False)
    point_order = Column(Integer, nullable=False)
    point = Column(Geometry('POINT', srid=4326), nullable=False)
    elevation = Column(Float)
    speed_limit = Column(Float)
    road_type = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # 关系
    segment = relationship("RouteSegment", back_populates="points")
    
    def __repr__(self):
        return f"<RoutePoint(segment_id={self.segment_id}, order={self.point_order})>" 