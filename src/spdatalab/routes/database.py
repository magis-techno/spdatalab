"""
Database operations for route management.
"""

from typing import Optional, List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from .models import Base, Route

class RouteDatabase:
    """Database handler for route management."""
    
    def __init__(self, connection_string: str):
        """
        Initialize database connection.
        
        Args:
            connection_string: SQLAlchemy database connection string
        """
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        
    def init_db(self):
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self.engine)
        
    def add_route(self, route: Route) -> bool:
        """
        Add a new route to the database.
        
        Args:
            route: Route instance to add
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.Session() as session:
                session.add(route)
                session.commit()
                return True
        except SQLAlchemyError:
            return False
            
    def get_route(self, source: str, route_id: str) -> Optional[Route]:
        """
        Get a route by source and route ID.
        
        Args:
            source: Route source (e.g., 'amap')
            route_id: Route ID from the source
            
        Returns:
            Route instance if found, None otherwise
        """
        with self.Session() as session:
            return session.query(Route).filter_by(
                source=source,
                route_id=route_id
            ).first()
            
    def list_routes(self, source: Optional[str] = None) -> List[Route]:
        """
        List all routes, optionally filtered by source.
        
        Args:
            source: Optional source to filter by
            
        Returns:
            List of Route instances
        """
        with self.Session() as session:
            query = session.query(Route)
            if source:
                query = query.filter_by(source=source)
            return query.all() 