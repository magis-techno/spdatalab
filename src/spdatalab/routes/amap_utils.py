"""
高德地图工具类，用于解析路线坐标
"""

import requests
import json
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from shapely.geometry import LineString, Point

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
        从高德地图URL中提取起点和终点坐标
        
        Args:
            url: 高德地图URL
            
        Returns:
            (起点坐标, 终点坐标) 或 None
        """
        try:
            # 如果是短链接，先展开
            if 'surl.amap.com' in url:
                url = self.expand_short_url(url)
                if not url:
                    return None
            
            parsed = urlparse(url)
            if 'amap.com' not in parsed.netloc:
                return None
            
            # 解析查询参数
            query_params = parse_qs(parsed.query)
            
            # 尝试从不同参数中提取坐标
            # 1. 从path参数中提取
            if 'path' in query_params:
                path = query_params['path'][0]
                points = path.split(';')
                if len(points) >= 2:
                    start = list(map(float, points[0].split(',')))
                    end = list(map(float, points[-1].split(',')))
                    return start, end
            
            # 2. 从起点终点参数中提取
            if 's' in query_params and 'd' in query_params:
                start = list(map(float, query_params['s'][0].split(',')))
                end = list(map(float, query_params['d'][0].split(',')))
                return start, end
            
            return None
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
        # 首先尝试直接从URL中提取坐标
        coords = self.extract_coordinates_from_url(url)
        if coords:
            start, end = coords
            return {
                'distance': None,  # 需要API才能获取准确距离
                'duration': None,  # 需要API才能获取准确时间
                'steps': [{
                    'distance': None,
                    'duration': None,
                    'instruction': '从起点到终点',
                    'path': [start, end]  # 简化为只有起点和终点
                }]
            }
        
        # 如果没有API密钥，无法获取详细路线信息
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