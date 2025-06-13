"""
Data models for route information from different sources.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, Float, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class Route(Base):
    """Base model for storing route information."""
    
    __tablename__ = 'routes'
    
    id = Column(Integer, primary_key=True)
    route_name = Column(String(200), nullable=False)
    region = Column(String(100))
    total_distance = Column(Float)
    is_active = Column(Boolean, default=True)
    allocation_count = Column(Integer, default=0)
    route_metadata = Column(Text, nullable=True, comment='Additional route metadata in JSON format')
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<Route(name='{self.route_name}', region='{self.region}')>"

class RouteSegment(Base):
    """Model for storing route segment information."""
    
    __tablename__ = 'route_segments'
    
    id = Column(Integer, primary_key=True)
    route_id = Column(Integer, ForeignKey('routes.id'), nullable=False)
    segment_id = Column(Integer, nullable=False)
    gaode_link = Column(String(500), nullable=False)
    route_points = Column(Text)
    segment_distance = Column(Float)
    geometry = Column(Geometry('LINESTRING', srid=4326))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<RouteSegment(route_id={self.route_id}, segment_id={self.segment_id})>" 