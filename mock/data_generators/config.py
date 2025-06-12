"""Mock数据生成器配置"""

import os
from dataclasses import dataclass
from typing import Dict, List, Tuple

@dataclass
class DatabaseConfig:
    """数据库连接配置"""
    trajectory_dsn: str = "postgresql+psycopg://postgres:postgres@mock_trajectory_db:5432/trajectory"
    business_dsn: str = "postgresql+psycopg://postgres:postgres@mock_business_db:5432/business"
    map_dsn: str = "postgresql+psycopg://postgres:postgres@mock_map_db:5432/mapdb"

@dataclass
class DataGenerationConfig:
    """数据生成配置"""
    # 基础配置
    random_seed: int = 42
    
    # 数据规模配置
    small_scale: Dict = None
    medium_scale: Dict = None
    large_scale: Dict = None
    
    # 地理区域配置
    regions: Dict = None
    
    # 城市配置
    cities: List = None
    
    def __post_init__(self):
        if self.small_scale is None:
            self.small_scale = {
                'scenes': 100,
                'intersections': 50,
                'points_per_scene': 20,
                'datasets': 5
            }
        
        if self.medium_scale is None:
            self.medium_scale = {
                'scenes': 1000,
                'intersections': 200,
                'points_per_scene': 50,
                'datasets': 20
            }
        
        if self.large_scale is None:
            self.large_scale = {
                'scenes': 10000,
                'intersections': 1000,
                'points_per_scene': 100,
                'datasets': 100
            }
        
        if self.regions is None:
            self.regions = {
                'beijing': {
                    'center': (116.4074, 39.9042),
                    'bounds': ((116.0, 39.4), (117.0, 40.4)),
                    'city_ids': ['BJ1', 'BJ2']
                },
                'shanghai': {
                    'center': (121.4737, 31.2304),
                    'bounds': ((121.0, 30.8), (121.9, 31.6)),
                    'city_ids': ['SH1', 'SH2']
                },
                'guangzhou': {
                    'center': (113.2644, 23.1291),
                    'bounds': ((112.8, 22.8), (113.7, 23.5)),
                    'city_ids': ['GZ1', 'GZ2']
                },
                'shenzhen': {
                    'center': (114.0579, 22.5431),
                    'bounds': ((113.6, 22.2), (114.5, 22.9)),
                    'city_ids': ['SZ1', 'SZ2']
                },
                'mock_region_a': {
                    'center': (120.0, 30.0),
                    'bounds': ((119.5, 29.5), (120.5, 30.5)),
                    'city_ids': ['A01', 'A02', 'A03', 'A72']
                },
                'mock_region_b': {
                    'center': (118.0, 28.0),
                    'bounds': ((117.5, 27.5), (118.5, 28.5)),
                    'city_ids': ['B01', 'B02', 'B15']
                }
            }
        
        if self.cities is None:
            self.cities = [
                {'id': 'BJ1', 'name': 'Beijing Test 1', 'region': 'beijing'},
                {'id': 'BJ2', 'name': 'Beijing Test 2', 'region': 'beijing'},
                {'id': 'SH1', 'name': 'Shanghai Test 1', 'region': 'shanghai'},
                {'id': 'SH2', 'name': 'Shanghai Test 2', 'region': 'shanghai'},
                {'id': 'GZ1', 'name': 'Guangzhou Test 1', 'region': 'guangzhou'},
                {'id': 'GZ2', 'name': 'Guangzhou Test 2', 'region': 'guangzhou'},
                {'id': 'SZ1', 'name': 'Shenzhen Test 1', 'region': 'shenzhen'},
                {'id': 'SZ2', 'name': 'Shenzhen Test 2', 'region': 'shenzhen'},
                {'id': 'A01', 'name': 'Mock City A01', 'region': 'mock_region_a'},
                {'id': 'A02', 'name': 'Mock City A02', 'region': 'mock_region_a'},
                {'id': 'A03', 'name': 'Mock City A03', 'region': 'mock_region_a'},
                {'id': 'A72', 'name': 'Mock City A72', 'region': 'mock_region_a'},
                {'id': 'B01', 'name': 'Mock City B01', 'region': 'mock_region_b'},
                {'id': 'B02', 'name': 'Mock City B02', 'region': 'mock_region_b'},
                {'id': 'B15', 'name': 'Mock City B15', 'region': 'mock_region_b'}
            ]

# 路口类型配置
INTERSECTION_TYPES = {
    1: 'Intersection',
    2: 'Toll Station',
    3: 'Lane Change Area',
    4: 'T-Junction Area',
    5: 'Roundabout',
    6: 'H-Junction Area',
    7: 'Invalid',
    8: 'Toll Booth Area'
}

INTERSECTION_SUBTYPES = {
    1: 'Regular',
    2: 'T-Junction with Through Markings',
    3: 'Minor Junction (No Traffic Conflict)',
    4: 'Unmarked Junction',
    5: 'Secondary Junction',
    6: 'Conservative Through Junction',
    7: 'Invalid'
}

# 默认配置实例
DEFAULT_DB_CONFIG = DatabaseConfig()
DEFAULT_DATA_CONFIG = DataGenerationConfig()

def get_config():
    """获取配置，支持环境变量覆盖"""
    db_config = DatabaseConfig(
        trajectory_dsn=os.getenv('TRAJECTORY_DSN', DEFAULT_DB_CONFIG.trajectory_dsn),
        business_dsn=os.getenv('BUSINESS_DSN', DEFAULT_DB_CONFIG.business_dsn),
        map_dsn=os.getenv('MAP_DSN', DEFAULT_DB_CONFIG.map_dsn)
    )
    
    data_config = DEFAULT_DATA_CONFIG
    
    return db_config, data_config 