"""
数据迁移脚本：从Hive数据库迁移路线数据到PostgreSQL
"""

import json
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from shapely.geometry import LineString
from geoalchemy2.shape import from_shape

from ..common.io_hive import hive_cursor
from .models import Base, Route, RouteSegment

def parse_route_points(points_str: str) -> List[Dict[str, float]]:
    """解析路线点字符串为坐标列表"""
    points = []
    for line in points_str.strip().split('\n'):
        if line.strip():
            # 这里需要根据实际数据格式进行解析
            # 示例格式：重庆市渝北区G50沪渝高速
            # 实际实现需要根据具体数据格式调整
            pass
    return points

def create_geometry_from_points(points: List[Dict[str, float]]) -> LineString:
    """从点列表创建LineString几何对象"""
    if not points:
        return None
    return LineString([(p['lon'], p['lat']) for p in points])

def migrate_routes():
    """从Hive迁移路线数据到PostgreSQL"""
    # 连接PostgreSQL
    pg_engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
    Session = sessionmaker(bind=pg_engine)
    session = Session()
    
    try:
        # 连接Hive
        with hive_cursor() as cursor:
            # 查询路线数据
            query = "SELECT * FROM pnc_simulation.route_info_0607"
            cursor.execute(query)
            routes = cursor.fetchall()
            
            for route_data in routes:
                try:
                    # 解析数据
                    id, route_name, region, distance, is_active, allocation_count, segments = route_data
                    
                    # 创建路线记录
                    route = Route(
                        route_name=route_name,
                        region=region,
                        total_distance=distance,
                        is_active=is_active,
                        allocation_count=allocation_count
                    )
                    session.add(route)
                    session.flush()  # 获取route.id
                    
                    # 处理每个分段
                    for segment in segments:
                        # 解析路线点
                        points = parse_route_points(segment['route_point'])
                        geometry = create_geometry_from_points(points)
                        
                        # 创建分段记录
                        route_segment = RouteSegment(
                            route_id=route.id,
                            segment_id=segment['seg_id'],
                            gaode_link=segment['gaode_link'],
                            route_points=segment['route_point'],
                            segment_distance=segment['seg_distance'],
                            geometry=from_shape(geometry, srid=4326) if geometry else None
                        )
                        session.add(route_segment)
                    
                    session.commit()
                except Exception as e:
                    print(f"Error processing record: {route_data}")
                    print(f"Error details: {str(e)}")
                    session.rollback()
                    continue
                
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

if __name__ == '__main__':
    migrate_routes() 