"""
Data models for route information from different sources.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Route(Base):
    """Base model for storing route information."""
    
    __tablename__ = 'routes'
    
    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False, comment='Data source (e.g., amap)')
    route_id = Column(String(100), nullable=False, comment='Route ID from the source')
    url = Column(String(500), nullable=False, comment='Original route URL')
    name = Column(String(200), nullable=True, comment='Route name/description')
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    metadata = Column(Text, nullable=True, comment='Additional route metadata in JSON format')
    
    def __repr__(self):
        return f"<Route(source='{self.source}', route_id='{self.route_id}')>" 