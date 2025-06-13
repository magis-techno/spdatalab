"""
数据迁移脚本：将现有route_info_0607表中的数据迁移到新的表结构中。
"""

import json
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from shapely.geometry import LineString
from geoalchemy2.shape import from_shape

from .models import Base, Route
from .database import RouteDatabase

class RouteMigrator:
    """处理路线数据迁移的类"""
    
    def __init__(self, source_connection: str, target_connection: str):
        """
        初始化迁移器
        
        Args:
            source_connection: 源数据库连接字符串
            target_connection: 目标数据库连接字符串
        """
        self.source_engine = create_engine(source_connection)
        self.target_db = RouteDatabase(target_connection)
        self.target_db.init_db()
        
    def fetch_source_data(self) -> List[Dict[str, Any]]:
        """从源数据库获取数据"""
        with self.source_engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM route_info_0607"))
            return [dict(row) for row in result]
            
    def process_segments(self, segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理路线分段数据
        
        Args:
            segments: 原始分段数据列表
            
        Returns:
            处理后的分段数据列表
        """
        processed_segments = []
        for segment in segments:
            # 提取几何信息
            if 'geometry' in segment:
                try:
                    coords = json.loads(segment['geometry'])
                    if isinstance(coords, list) and len(coords) >= 2:
                        line = LineString(coords)
                        segment['geometry'] = from_shape(line, srid=4326)
                except (json.JSONDecodeError, ValueError):
                    continue
                    
            # 提取URL
            if 'gaode_link' in segment:
                segment['url'] = segment.pop('gaode_link')
                
            processed_segments.append(segment)
            
        return processed_segments
        
    def migrate(self):
        """执行数据迁移"""
        # 获取源数据
        source_data = self.fetch_source_data()
        
        # 处理每条路线
        for route_data in source_data:
            # 创建路线记录
            route = Route(
                source='amap',
                route_id=str(route_data.get('id', '')),
                url=route_data.get('gaode_link', ''),
                name=route_data.get('name', ''),
                metadata={
                    'original_data': {
                        k: v for k, v in route_data.items()
                        if k not in ['id', 'gaode_link', 'name', 'segments']
                    }
                }
            )
            
            # 保存路线
            if self.target_db.add_route(route):
                # 处理分段数据
                segments = self.process_segments(route_data.get('segments', []))
                
                # TODO: 保存分段数据到route_segments表
                # 这部分需要根据具体的数据库结构来实现
                
def main():
    """主函数"""
    # 配置数据库连接
    source_conn = "postgresql://user:password@host:5432/dataset_gy1"
    target_conn = "postgresql://postgres:postgres@localhost:5432/spdatalab"
    
    # 执行迁移
    migrator = RouteMigrator(source_conn, target_conn)
    migrator.migrate()

if __name__ == '__main__':
    main() 