"""
Handler for Amap (高德地图) route data.
"""

import re
from typing import Optional, Dict, Any
from urllib.parse import urlparse, parse_qs
from .models import Route

class AmapRoute:
    """Handler for Amap route data."""
    
    SOURCE_NAME = 'amap'
    
    @staticmethod
    def extract_route_id(url: str) -> Optional[str]:
        """
        Extract route ID from Amap URL.
        
        Args:
            url: Amap route URL (e.g., https://surl.amap.com/qujFVqt1v9bp)
            
        Returns:
            Route ID if found, None otherwise
        """
        try:
            parsed = urlparse(url)
            if 'amap.com' not in parsed.netloc:
                return None
                
            # Extract the route ID from the path
            path = parsed.path.strip('/')
            if path:
                return path
                
            return None
        except Exception:
            return None
    
    @classmethod
    def create_route(cls, url: str, name: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> Route:
        """
        Create a new Route instance for an Amap route.
        
        Args:
            url: Amap route URL
            name: Optional route name/description
            metadata: Optional additional metadata
            
        Returns:
            Route instance
            
        Raises:
            ValueError: If URL is invalid or route ID cannot be extracted
        """
        route_id = cls.extract_route_id(url)
        if not route_id:
            raise ValueError(f"Invalid Amap route URL: {url}")
            
        return Route(
            source=cls.SOURCE_NAME,
            route_id=route_id,
            url=url,
            name=name,
            metadata=metadata
        ) 