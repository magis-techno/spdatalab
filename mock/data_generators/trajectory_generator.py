"""轨迹数据生成器"""

import random
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from faker import Faker
from typing import List, Tuple, Dict
from datetime import datetime
import logging

from .config import get_config

logger = logging.getLogger(__name__)

class TrajectoryGenerator:
    """轨迹数据生成器"""
    
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
        
        self.db_config, self.data_config = get_config()
        self.engine = create_engine(self.db_config.trajectory_dsn)
        
    def generate_points_for_scene(
        self, 
        scene_id: str, 
        dataset_name: str,
        center: Tuple[float, float],
        radius: float = 0.01,
        num_points: int = 50
    ) -> List[Dict]:
        """为一个场景生成轨迹点"""
        
        points = []
        base_time = int(datetime.now().timestamp() * 1000)
        vehicle_id = f"vehicle_{scene_id}_{random.randint(1000, 9999)}"
        
        for i in range(num_points):
            # 在中心点周围随机生成点
            angle = random.uniform(0, 2 * np.pi)
            distance = random.uniform(0, radius)
            
            lat = center[1] + distance * np.cos(angle)
            lon = center[0] + distance * np.sin(angle)
            
            point = {
                'dataset_name': dataset_name,
                'point_lla': f"POINT({lon} {lat})",
                'workstage': random.choice([1, 2, 2, 2]),  # 偏向于workstage=2
                'scene_id': scene_id,
                'timestamp_ms': base_time + i * 100,  # 每100ms一个点
                'vehicle_id': vehicle_id
            }
            points.append(point)
            
        return points

    def generate_trajectory_data(self, scale: str = 'small') -> int:
        """生成轨迹数据"""
        scale_config = getattr(self.data_config, f"{scale}_scale")
        
        logger.info(f"开始生成{scale}规模轨迹数据 - {scale_config['scenes']}个场景")
        
        all_points = []
        regions = self.data_config.regions
        
        # 选择几个数据集名称
        dataset_names = [f"dataset_{i+1}" for i in range(scale_config['datasets'])]
        
        for scene_idx in range(scale_config['scenes']):
            # 随机选择一个区域
            region_name = random.choice(list(regions.keys()))
            region = regions[region_name]
            
            # 在区域范围内随机选择中心点
            bounds = region['bounds']
            center_lon = random.uniform(bounds[0][0], bounds[1][0])
            center_lat = random.uniform(bounds[0][1], bounds[1][1])
            center = (center_lon, center_lat)
            
            # 随机选择数据集
            dataset_name = random.choice(dataset_names)
            
            scene_id = f"scene_{scene_idx:06d}_{region_name}"
            
            # 生成这个场景的点
            points = self.generate_points_for_scene(
                scene_id=scene_id,
                dataset_name=dataset_name, 
                center=center,
                num_points=scale_config['points_per_scene']
            )
            
            all_points.extend(points)
        
        # 批量插入数据库
        self._insert_points_to_db(all_points)
        
        logger.info(f"✅ 轨迹数据生成完成: {len(all_points)} 个点")
        return len(all_points)
        
    def _insert_points_to_db(self, points: List[Dict]):
        """将点数据插入数据库"""
        if not points:
            return
            
        df = pd.DataFrame(points)
        
        # 转换几何数据
        insert_sql = """
        INSERT INTO public.ddi_data_points 
        (dataset_name, point_lla, workstage, scene_id, timestamp_ms, vehicle_id)
        VALUES (:dataset_name, ST_GeomFromText(:point_lla, 4326), 
                :workstage, :scene_id, :timestamp_ms, :vehicle_id)
        """
        
        with self.engine.connect() as conn:
            # 批量插入
            for _, row in df.iterrows():
                conn.execute(text(insert_sql), row.to_dict())
            conn.commit()
            
    def get_trajectory_stats(self) -> Dict:
        """获取轨迹数据统计信息"""
        try:
            with self.engine.connect() as conn:
                # 统计总点数
                result = conn.execute(text("SELECT COUNT(*) FROM public.ddi_data_points"))
                total_points = result.fetchone()[0]
                
                # 统计场景数
                result = conn.execute(text("SELECT COUNT(DISTINCT scene_id) FROM public.ddi_data_points"))
                total_scenes = result.fetchone()[0]
                
                # 统计数据集数
                result = conn.execute(text("SELECT COUNT(DISTINCT dataset_name) FROM public.ddi_data_points"))
                total_datasets = result.fetchone()[0]
                
                return {
                    'total_points': total_points,
                    'total_scenes': total_scenes,
                    'total_datasets': total_datasets
                }
        except Exception as e:
            logger.error(f"获取轨迹统计信息失败: {e}")
            return {
                'total_points': 0,
                'total_scenes': 0,
                'total_datasets': 0
            } 