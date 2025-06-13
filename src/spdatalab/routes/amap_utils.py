"""
高德地图工具类，用于解析路线坐标
"""

import requests
import json
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from shapely.geometry import LineString, Point
from .amap import AmapRoute

class AmapRouteParser:
    """高德地图路线解析器"""
    
    def __init__(self, api_key: str = None):
        """
        初始化解析器
        
        Args:
            api_key: 高德地图API密钥（可选）
        """
        self.api_key = api_key
        self.base_url = "https://restapi.amap.com/v3/direction/driving"
    
    def expand_short_url(self, short_url: str) -> Optional[str]:
        """
        展开高德地图短链接
        
        Args:
            short_url: 高德地图短链接
            
        Returns:
            展开后的完整URL或None
        """
        try:
            response = requests.head(short_url, allow_redirects=True)
            return response.url
        except Exception as e:
            print(f"Error expanding short URL: {str(e)}")
            return None
    
    def extract_coordinates_from_url(self, url: str) -> Optional[Tuple[List[float], List[float]]]:
        """
        从URL中直接提取起点和终点坐标
        
        Args:
            url: 高德地图URL
            
        Returns:
            (起点坐标, 终点坐标) 或 None
        """
        try:
            # 处理短链接
            if 'surl.amap.com' in url:
                url = self.expand_short_url(url)
                if not url:
                    return None
            
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            
            # 尝试从查询参数中获取坐标
            if 'src' in query:
                src = query['src'][0]
                if ',' in src:
                    start_coords = [float(x) for x in src.split(',')]
                else:
                    return None
            else:
                return None
                
            if 'dest' in query:
                dest = query['dest'][0]
                if ',' in dest:
                    end_coords = [float(x) for x in dest.split(',')]
                else:
                    return None
            else:
                return None
                
            return (start_coords, end_coords)
        except Exception as e:
            print(f"Error extracting coordinates: {str(e)}")
            return None
    
    def get_route_coordinates(self, url: str) -> Optional[Dict[str, Any]]:
        """
        从高德地图URL获取路线坐标
        
        Args:
            url: 高德地图URL
            
        Returns:
            包含路线信息的字典，包括：
            - distance: 总距离（米）
            - duration: 预计时间（秒）
            - steps: 路线步骤列表，每个步骤包含：
              - distance: 步骤距离（米）
              - duration: 步骤时间（秒）
              - instruction: 导航指示
              - path: 坐标点列表 [[lon, lat], ...]
        """
        # 首先尝试直接从URL获取坐标
        coords = self.extract_coordinates_from_url(url)
        if coords:
            start_coords, end_coords = coords
            return {
                'distance': None,  # 需要API才能获取
                'duration': None,  # 需要API才能获取
                'steps': [{
                    'distance': None,
                    'duration': None,
                    'instruction': 'Direct route',
                    'path': [start_coords, end_coords]
                }]
            }
            
        # 如果没有API密钥，无法获取详细信息
        if not self.api_key:
            return None
            
        try:
            # 构建API请求参数
            params = {
                'key': self.api_key,
                'origin': '',  # 需要从URL中提取起点
                'destination': '',  # 需要从URL中提取终点
                'extensions': 'all',  # 返回详细信息
                'output': 'json'
            }
            
            # 发送请求
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            # 解析响应
            data = response.json()
            if data['status'] != '1':
                return None
                
            route = data['route']
            return {
                'distance': float(route['distance']),
                'duration': int(route['duration']),
                'steps': [{
                    'distance': float(step['distance']),
                    'duration': int(step['duration']),
                    'instruction': step['instruction'],
                    'path': self._parse_path(step['polyline'])
                } for step in route['steps']]
            }
        except Exception as e:
            print(f"Error getting route coordinates: {str(e)}")
            return None
    
    def _parse_path(self, polyline: str) -> List[List[float]]:
        """
        解析高德地图的polyline字符串为坐标列表
        
        Args:
            polyline: 高德地图的polyline字符串
            
        Returns:
            坐标点列表 [[lon, lat], ...]
        """
        points = []
        for point in polyline.split(';'):
            if point:
                lon, lat = map(float, point.split(','))
                points.append([lon, lat])
        return points
    
    def create_geometry(self, coordinates: List[List[float]]) -> LineString:
        """
        从坐标列表创建LineString几何对象
        
        Args:
            coordinates: 坐标点列表 [[lon, lat], ...]
            
        Returns:
            LineString几何对象
        """
        if not coordinates:
            return None
        return LineString(coordinates)
    
    @classmethod
    def create_route(cls, url: str, name: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> 'Route':
        """
        创建新的Route实例，并尝试获取路线坐标
        
        Args:
            url: 高德地图URL
            name: 可选的路线名称/描述
            metadata: 可选的额外元数据
            
        Returns:
            Route实例
            
        Raises:
            ValueError: 如果URL无效或无法提取路线ID
        """
        # 使用现有的AmapRoute类创建基本Route实例
        route = AmapRoute.create_route(url, name, metadata)
        
        # 尝试获取路线坐标
        parser = cls()
        route_info = parser.get_route_coordinates(url)
        
        if route_info:
            # 更新元数据
            if route.metadata is None:
                route.metadata = {}
            route.metadata.update({
                'distance': route_info['distance'],
                'duration': route_info['duration'],
                'steps': route_info['steps']
            })
            
            # 创建几何对象
            all_coordinates = []
            for step in route_info['steps']:
                all_coordinates.extend(step['path'])
            route.geometry = parser.create_geometry(all_coordinates)
        
        return route 