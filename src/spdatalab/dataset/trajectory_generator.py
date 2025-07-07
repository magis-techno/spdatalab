"""轨迹数据生成器，基于场景列表生成轨迹数据（简化版）。"""

import logging
from typing import List, Optional
from pathlib import Path
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from shapely.geometry import LineString

from ..common.io_hive import hive_cursor

logger = logging.getLogger(__name__)

class TrajectoryGenerator:
    """轨迹数据生成器（简化版）。"""
    
    def __init__(self):
        """初始化轨迹生成器。"""
        pass
        
    def create_trajectory_table_if_not_exists(self, eng, table_name: str = "clips_trajectory"):
        """创建轨迹表结构。
        
        Args:
            eng: 数据库引擎
            table_name: 轨迹表名
        """
        # 检查表是否已存在
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        try:
            with eng.connect() as conn:
                result = conn.execute(check_table_sql)
                table_exists = result.scalar()
                
                if table_exists:
                    print(f"轨迹表 {table_name} 已存在，跳过创建")
                    return True
                    
                print(f"轨迹表 {table_name} 不存在，开始创建...")
                
                # 简化的轨迹表结构
                create_sql = text(f"""
                    CREATE TABLE {table_name}(
                        id serial PRIMARY KEY,
                        scene_token text NOT NULL,
                        data_name text,
                        trajectory_length integer,
                        start_time double precision,
                        end_time double precision,
                        max_speed double precision,
                        all_good boolean,
                        created_at timestamp DEFAULT NOW()
                    );
                """)
                
                # 使用PostGIS添加几何列（轨迹线）
                add_geom_sql = text(f"""
                    SELECT AddGeometryColumn('public', '{table_name}', 'trajectory', 4326, 'LINESTRING', 2);
                """)
                
                # 添加几何约束
                constraint_sql = text(f"""
                    ALTER TABLE {table_name} ADD CONSTRAINT check_{table_name}_traj_type 
                        CHECK (ST_GeometryType(trajectory) = 'ST_LineString');
                """)
                
                # 创建索引
                index_sql = text(f"""
                    CREATE INDEX idx_{table_name}_trajectory ON {table_name} USING GIST(trajectory);
                    CREATE INDEX idx_{table_name}_scene_token ON {table_name}(scene_token);
                    CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                    CREATE INDEX idx_{table_name}_timestamp ON {table_name}(start_time);
                """)
                
                # 执行SQL语句，需要分步提交
                conn.execute(create_sql)
                conn.commit()  # 先提交表创建
                
                # 执行PostGIS相关操作
                conn.execute(add_geom_sql)
                conn.execute(constraint_sql)
                conn.commit()  # 提交几何列和约束
                
                # 创建索引
                conn.execute(index_sql)
                conn.commit()  # 最后提交索引
                
                print(f"成功创建轨迹表 {table_name} 及相关索引")
                return True
                
        except Exception as e:
            print(f"创建轨迹表时出错: {str(e)}")
            return False
    
    def fetch_trajectory_data(self, scene_tokens: List[str]) -> pd.DataFrame:
        """从数据库获取轨迹数据。
        
        Args:
            scene_tokens: 场景token列表
            
        Returns:
            包含轨迹数据的DataFrame
        """
        if not scene_tokens:
            return pd.DataFrame()
            
        print(f"🔍 正在查询 {len(scene_tokens)} 个场景的轨迹数据...")
        
        # 可能的表名列表（按优先级顺序）
        possible_tables = [
            "public.ddi_data_points",
            "ddi_data_points",
            "public.clips_data_points",
            "clips_data_points"
        ]
        
        # 获取轨迹点数据
        sql_template = """
        SELECT 
            id,
            dataset_name,
            ST_X(point_lla) as lon,
            ST_Y(point_lla) as lat,
            ST_Z(point_lla) as alt,
            "timestamp",
            workstage
        FROM {} 
        WHERE dataset_name = ANY(%(scene_tokens)s)
        ORDER BY dataset_name, "timestamp"
        """
        
        # 尝试不同的表名
        for table_name in possible_tables:
            try:
                sql = sql_template.format(table_name)
                print(f"🔍 尝试使用表: {table_name}")
                
                with hive_cursor() as cur:
                    cur.execute(sql, {"scene_tokens": scene_tokens})
                    cols = [d[0] for d in cur.description]
                    result_df = pd.DataFrame(cur.fetchall(), columns=cols)
                    
                    if not result_df.empty:
                        unique_scenes = result_df['dataset_name'].nunique()
                        print(f"✅ 使用表 {table_name} 找到 {unique_scenes} 个场景的 {len(result_df)} 个轨迹点")
                        return result_df
                    else:
                        print(f"⚠️  表 {table_name} 中没有找到轨迹数据")
                        
            except Exception as e:
                error_msg = str(e).lower()
                if "does not exist" in error_msg or "relation" in error_msg:
                    print(f"❌ 表 {table_name} 不存在，尝试下一个表...")
                    continue
                else:
                    logger.error(f"查询表 {table_name} 失败: {str(e)}")
                    print(f"❌ 查询表 {table_name} 失败: {str(e)}")
                    continue
        
        # 如果所有表都尝试失败，返回空DataFrame
        print("❌ 所有可能的表都不存在或无数据，请检查数据库配置")
        print("💡 提示：请确保以下表之一存在并包含轨迹数据:")
        for table in possible_tables:
            print(f"   - {table}")
        
        return pd.DataFrame()
    
    def process_trajectory_data(self, trajectory_df: pd.DataFrame) -> gpd.GeoDataFrame:
        """处理轨迹数据，构建轨迹线。
        
        Args:
            trajectory_df: 轨迹点数据
            
        Returns:
            包含轨迹线的GeoDataFrame
        """
        if trajectory_df.empty:
            return gpd.GeoDataFrame()
            
        print(f"🔄 正在处理轨迹数据，构建轨迹线...")
        
        trajectory_list = []
        processed_count = 0
        
        # 按dataset_name分组处理每条轨迹
        for dataset_name, group in trajectory_df.groupby('dataset_name'):
            try:
                # 按时间排序
                group = group.sort_values('timestamp')
                
                # 过滤有效点（去除NaN和异常值）
                valid_points = group.dropna(subset=['lon', 'lat'])
                
                if len(valid_points) < 2:
                    logger.warning(f"场景 {dataset_name} 轨迹点不足，跳过")
                    continue
                
                # 构建轨迹线
                coords = list(zip(valid_points['lon'], valid_points['lat']))
                trajectory_line = LineString(coords)
                
                # 计算统计信息
                start_time = float(valid_points['timestamp'].min())
                end_time = float(valid_points['timestamp'].max())
                duration = end_time - start_time
                
                # 计算轨迹长度（地理距离）
                trajectory_length = int(trajectory_line.length * 111000)  # 近似转换为米
                
                # 计算最大速度（简单估算）
                max_speed = 0.0
                if duration > 0 and len(coords) > 1:
                    speeds = []
                    timestamps = valid_points['timestamp'].values
                    
                    for i in range(1, len(coords)):
                        # 简单距离计算（度数转米的近似）
                        dx = (coords[i][0] - coords[i-1][0]) * 111000 * np.cos(np.radians(coords[i][1]))
                        dy = (coords[i][1] - coords[i-1][1]) * 111000
                        dist = np.sqrt(dx*dx + dy*dy)
                        
                        time_diff = float(timestamps[i] - timestamps[i-1])
                        if time_diff > 0:
                            speed = dist / time_diff  # m/s
                            speeds.append(speed)
                    
                    max_speed = max(speeds) if speeds else 0.0
                
                # 判断轨迹质量
                all_good = bool(valid_points['workstage'].eq(2).all())
                
                trajectory_record = {
                    'scene_token': dataset_name,
                    'data_name': dataset_name,
                    'trajectory': trajectory_line,
                    'trajectory_length': trajectory_length,
                    'start_time': start_time,
                    'end_time': end_time,
                    'max_speed': max_speed,
                    'all_good': all_good
                }
                
                trajectory_list.append(trajectory_record)
                processed_count += 1
                
                if processed_count % 100 == 0:
                    print(f"  已处理 {processed_count} 条轨迹...")
                
            except Exception as e:
                logger.error(f"处理场景 {dataset_name} 轨迹失败: {str(e)}")
                continue
        
        if trajectory_list:
            print(f"✅ 成功构建 {len(trajectory_list)} 条轨迹线")
            return gpd.GeoDataFrame(trajectory_list, crs=4326)
        else:
            print("⚠️  没有成功构建任何轨迹线")
            return gpd.GeoDataFrame()
    
    def generate_trajectories_from_scene_list(
        self,
        eng,
        scene_tokens: List[str],
        trajectory_table_name: str = "clips_trajectory"
    ) -> int:
        """基于场景列表生成轨迹（核心方法）。
        
        Args:
            eng: 数据库引擎
            scene_tokens: 场景token列表
            trajectory_table_name: 轨迹表名
            
        Returns:
            生成的轨迹数量
        """
        if not scene_tokens:
            print("⚠️  场景列表为空")
            return 0
        
        print(f"🚀 开始为 {len(scene_tokens)} 个场景生成轨迹")
        
        # 创建轨迹表
        if not self.create_trajectory_table_if_not_exists(eng, trajectory_table_name):
            raise Exception("无法创建轨迹表")
        
        # 获取轨迹数据
        trajectory_df = self.fetch_trajectory_data(scene_tokens)
        
        if trajectory_df.empty:
            print("⚠️  没有找到轨迹数据")
            return 0
        
        # 处理轨迹数据
        trajectory_gdf = self.process_trajectory_data(trajectory_df)
        
        if trajectory_gdf.empty:
            print("⚠️  轨迹数据处理失败")
            return 0
        
        # 插入到轨迹表
        try:
            print(f"💾 正在将 {len(trajectory_gdf)} 条轨迹插入表 {trajectory_table_name}")
            
            trajectory_gdf.to_postgis(
                trajectory_table_name,
                eng,
                if_exists='append',
                index=False
            )
            
            print(f"✅ 成功生成 {len(trajectory_gdf)} 条轨迹")
            return len(trajectory_gdf)
            
        except Exception as e:
            logger.error(f"插入轨迹数据失败: {str(e)}")
            print(f"❌ 插入轨迹数据失败: {str(e)}")
            return 0 