"""轨迹生成模块，从scene_id列表生成轨迹线和变化点检测。"""

from __future__ import annotations
import argparse
import json
import signal
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Iterator, Tuple, Union
import logging

import geopandas as gpd
import pandas as pd
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

# 检查是否有parquet支持
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 全局变量用于优雅退出
interrupted = False

# 日志配置
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """信号处理函数，用于优雅退出"""
    global interrupted
    print(f"\n接收到中断信号 ({signum})，正在优雅退出...")
    print("等待当前处理完成，请稍候...")
    interrupted = True

def setup_signal_handlers():
    """设置信号处理器"""
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号

def load_dataset_scene_mappings(dataset_file: str) -> pd.DataFrame:
    """从dataset文件加载scene_id和data_name的映射。
    
    Args:
        dataset_file: dataset文件路径（JSON或Parquet格式）
        
    Returns:
        包含scene_id和data_name映射的DataFrame
    """
    try:
        # 使用dataset_manager加载数据集
        from ..dataset.dataset_manager import DatasetManager
        
        dataset_manager = DatasetManager()
        dataset = dataset_manager.load_dataset(dataset_file)
        
        mappings = []
        for subdataset in dataset.subdatasets:
            # 检查是否有scene_attributes（问题单数据集）
            scene_attributes = subdataset.metadata.get('scene_attributes', {})
            
            for scene_id in subdataset.scene_ids:
                if scene_attributes and scene_id in scene_attributes:
                    # 从scene_attributes中获取data_name
                    data_name = scene_attributes[scene_id].get('data_name')
                    if data_name:
                        mappings.append({
                            'scene_id': scene_id,
                            'data_name': data_name,
                            'subdataset_name': subdataset.name
                        })
                        logger.debug(f"从dataset获取映射: {scene_id} -> {data_name}")
                    else:
                        # 没有data_name，需要后续查询数据库
                        mappings.append({
                            'scene_id': scene_id,
                            'data_name': None,
                            'subdataset_name': subdataset.name
                        })
                else:
                    # 标准数据集，没有data_name，需要查询数据库
                    mappings.append({
                        'scene_id': scene_id,
                        'data_name': None,
                        'subdataset_name': subdataset.name
                    })
        
        result_df = pd.DataFrame(mappings)
        logger.info(f"从dataset文件加载了 {len(result_df)} 个scene_id映射")
        
        return result_df
        
    except Exception as e:
        logger.error(f"加载dataset文件失败: {dataset_file}, 错误: {str(e)}")
        raise

def load_scene_ids_from_text(file_path: str) -> List[str]:
    """从文本文件加载scene_id列表。
    
    Args:
        file_path: 文件路径，支持txt格式
        
    Returns:
        scene_id列表
    """
    scene_ids = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # 去除空白字符
                line = line.strip()
                
                # 跳过空行和注释行
                if not line or line.startswith('#'):
                    continue
                
                # 简单验证scene_id格式（假设是UUID或类似格式）
                if len(line) > 0 and not line.isspace():
                    scene_ids.append(line)
                    logger.debug(f"加载scene_id: {line}")
                else:
                    logger.warning(f"第{line_num}行格式异常，跳过: {line}")
        
        logger.info(f"成功加载 {len(scene_ids)} 个scene_id")
        return scene_ids
        
    except FileNotFoundError:
        logger.error(f"文件不存在: {file_path}")
        raise
    except Exception as e:
        logger.error(f"加载scene_id文件失败: {file_path}, 错误: {str(e)}")
        raise

def load_scene_data_mappings(file_path: str) -> pd.DataFrame:
    """智能加载scene_id和data_name映射，自动检测文件格式。
    
    Args:
        file_path: 文件路径
        
    Returns:
        包含scene_id和data_name映射的DataFrame
    """
    file_path = Path(file_path)
    
    # 检测是否为dataset文件格式
    if file_path.suffix.lower() in ['.json', '.parquet']:
        try:
            # 尝试作为dataset文件加载
            mappings_df = load_dataset_scene_mappings(str(file_path))
            logger.info(f"识别为dataset文件格式: {file_path}")
            return mappings_df
        except Exception as e:
            logger.warning(f"作为dataset文件加载失败: {str(e)}")
            # 如果失败，尝试作为scene_id列表处理
    
    # 作为简单的scene_id列表处理
    scene_ids = load_scene_ids_from_text(str(file_path))
    mappings_df = pd.DataFrame({
        'scene_id': scene_ids,
        'data_name': [None] * len(scene_ids),  # 需要后续查询数据库
        'subdataset_name': ['unknown'] * len(scene_ids)
    })
    
    logger.info(f"识别为scene_id列表格式: {file_path}")
    return mappings_df

def fetch_data_names_from_scene_ids(scene_ids: List[str]) -> pd.DataFrame:
    """根据scene_id批量查询对应的data_name。
    
    Args:
        scene_ids: 场景ID列表
        
    Returns:
        包含scene_id和data_name映射的DataFrame
    """
    if not scene_ids:
        return pd.DataFrame()
    
    try:
        sql = ("SELECT id AS scene_id, origin_name AS data_name "
               "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s")
        
        with hive_cursor() as cur:
            cur.execute(sql, {"tok": tuple(scene_ids)})
            cols = [d[0] for d in cur.description]
            result_df = pd.DataFrame(cur.fetchall(), columns=cols)
            
        logger.debug(f"查询到 {len(result_df)} 个scene_id对应的data_name")
        return result_df
        
    except Exception as e:
        logger.error(f"查询scene_id到data_name映射失败: {str(e)}")
        return pd.DataFrame()

def fetch_trajectory_points(data_name: str) -> pd.DataFrame:
    """查询单个data_name的轨迹点数据。
    
    Args:
        data_name: 数据名称（不是scene_id）
        
    Returns:
        包含轨迹点信息的DataFrame
    """
    sql = text(f"""
        SELECT 
            dataset_name,
            timestamp,
            point_lla,
            twist_linear,
            avp_flag,
            workstage,
            ST_X(point_lla) as longitude,
            ST_Y(point_lla) as latitude
        FROM {POINT_TABLE}
        WHERE dataset_name = :data_name
        AND point_lla IS NOT NULL
        ORDER BY timestamp ASC
    """)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        with eng.connect() as conn:
            result = conn.execute(sql, {"data_name": data_name})
            
            if result.rowcount == 0:
                logger.warning(f"未找到data_name的轨迹数据: {data_name}")
                return pd.DataFrame()
            
            # 获取列名
            columns = result.keys()
            rows = result.fetchall()
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            logger.debug(f"查询到 {len(df)} 个轨迹点: {data_name}")
            
            return df
            
    except Exception as e:
        logger.error(f"查询轨迹点失败: {data_name}, 错误: {str(e)}")
        return pd.DataFrame()

def build_trajectory(scene_id: str, data_name: str, points_df: pd.DataFrame) -> Dict:
    """从轨迹点构建轨迹线几何和统计信息。
    
    Args:
        scene_id: 场景ID
        data_name: 数据名称
        points_df: 轨迹点DataFrame
        
    Returns:
        包含轨迹信息的字典
    """
    if points_df.empty:
        return {}
    
    try:
        # 确保数据按时间排序
        points_df = points_df.sort_values('timestamp')
        
        # 提取坐标点
        coordinates = list(zip(points_df['longitude'], points_df['latitude']))
        
        if len(coordinates) < 2:
            logger.warning(f"轨迹点数量不足，无法构建轨迹线: {len(coordinates)}")
            return {}
        
        # 构建LineString几何
        from shapely.geometry import LineString
        trajectory_geom = LineString(coordinates)
        
        # 计算统计信息
        stats = {
            'scene_id': scene_id,
            'data_name': data_name,
            'start_time': points_df['timestamp'].min(),
            'end_time': points_df['timestamp'].max(),
            'duration': points_df['timestamp'].max() - points_df['timestamp'].min(),
            'geometry': trajectory_geom
        }
        
        # 速度统计（保留2位小数）
        if 'twist_linear' in points_df.columns:
            speed_data = points_df['twist_linear'].dropna()
            if len(speed_data) > 0:
                stats.update({
                    'avg_speed': round(float(speed_data.mean()), 2),
                    'max_speed': round(float(speed_data.max()), 2),
                    'min_speed': round(float(speed_data.min()), 2),
                    'std_speed': round(float(speed_data.std()) if len(speed_data) > 1 else 0.0, 2)
                })
        
        # AVP统计（保留3位小数）
        if 'avp_flag' in points_df.columns:
            avp_data = points_df['avp_flag'].dropna()
            if len(avp_data) > 0:
                stats.update({
                    'avp_ratio': round(float((avp_data == 1).mean()), 3)
                })
        
        logger.debug(f"构建轨迹: {scene_id} ({data_name}), 点数: {len(points_df)}")
        return stats
        
    except Exception as e:
        logger.error(f"构建轨迹失败: {scene_id} ({data_name}), 错误: {str(e)}")
        return {}

def create_trajectory_table(eng, table_name: str) -> bool:
    """创建轨迹表。
    
    Args:
        eng: 数据库引擎
        table_name: 表名
        
    Returns:
        创建是否成功
    """
    try:
        # 检查表是否已存在
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        with eng.connect() as conn:
            result = conn.execute(check_table_sql)
            table_exists = result.scalar()
            
            if table_exists:
                logger.info(f"轨迹表 {table_name} 已存在，跳过创建")
                return True
            
            logger.info(f"创建轨迹表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    scene_id text NOT NULL,
                    data_name text NOT NULL,
                    start_time bigint,
                    end_time bigint,
                    duration bigint,
                    avg_speed numeric(8,2),
                    max_speed numeric(8,2),
                    min_speed numeric(8,2),
                    std_speed numeric(8,2),
                    avp_ratio numeric(5,3),
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 添加几何列
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'LINESTRING', 2);
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_start_time ON {table_name}(start_time);
            """)
            
            # 执行SQL
            conn.execute(create_sql)
            conn.commit()
            
            conn.execute(add_geom_sql)
            conn.commit()
            
            conn.execute(index_sql)
            conn.commit()
            
            logger.info(f"成功创建轨迹表 {table_name}")
            return True
            
    except Exception as e:
        logger.error(f"创建轨迹表失败: {table_name}, 错误: {str(e)}")
        return False

def insert_trajectory_data(eng, table_name: str, trajectory_data: List[Dict]) -> int:
    """批量插入轨迹数据。
    
    Args:
        eng: 数据库引擎
        table_name: 表名
        trajectory_data: 轨迹数据列表
        
    Returns:
        插入成功的记录数
    """
    if not trajectory_data:
        return 0
    
    try:
        # 准备数据和几何对象
        gdf_data = []
        geometries = []
        
        for traj in trajectory_data:
            if 'geometry' in traj and traj['geometry'] is not None:
                # 分离几何和属性数据
                row = {k: v for k, v in traj.items() if k != 'geometry'}
                gdf_data.append(row)
                geometries.append(traj['geometry'])
        
        if not gdf_data:
            logger.warning("没有有效的轨迹数据可插入")
            return 0
        
        # 创建GeoDataFrame，直接传入geometry参数
        gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
        
        # 插入数据
        gdf.to_postgis(table_name, eng, if_exists='append', index=False)
        
        inserted_count = len(gdf)
        logger.info(f"成功插入 {inserted_count} 条轨迹记录到 {table_name}")
        return inserted_count
        
    except Exception as e:
        logger.error(f"插入轨迹数据失败: {str(e)}")
        return 0

def detect_avp_changes(points_df: pd.DataFrame) -> List[Dict]:
    """检测AVP状态变化点。
    
    Args:
        points_df: 轨迹点DataFrame
        
    Returns:
        变化点列表
    """
    if points_df.empty or 'avp_flag' not in points_df.columns:
        return []
    
    try:
        # 确保数据按时间排序
        points_df = points_df.sort_values('timestamp')
        
        # 填充缺失值，默认为0，确保类型为整数
        points_df['avp_flag'] = points_df['avp_flag'].fillna(0).astype(int)
        
        # 检测变化点
        changes = []
        prev_avp = None
        
        for idx, row in points_df.iterrows():
            current_avp = int(row['avp_flag'])
            
            # 检测状态变化
            if prev_avp is not None and prev_avp != current_avp:
                change_event = {
                    'timestamp': int(row['timestamp']),
                    'longitude': float(row['longitude']),
                    'latitude': float(row['latitude']),
                    'event_type': 'avp_change',
                    'from_value': float(prev_avp),  # 明确转换为float
                    'to_value': float(current_avp),  # 明确转换为float
                    'description': f"AVP状态从{prev_avp}变为{current_avp}"
                }
                changes.append(change_event)
                logger.debug(f"检测到AVP变化: {change_event['description']}")
            
            prev_avp = current_avp
        
        logger.info(f"检测到 {len(changes)} 个AVP变化点")
        return changes
        
    except Exception as e:
        logger.error(f"AVP变化检测失败: {str(e)}")
        return []

def detect_speed_spikes(points_df: pd.DataFrame, threshold_std: float = 2.0) -> List[Dict]:
    """检测速度突变点。
    
    Args:
        points_df: 轨迹点DataFrame
        threshold_std: 突变阈值（标准差倍数）
        
    Returns:
        速度突变点列表
    """
    if points_df.empty or 'twist_linear' not in points_df.columns:
        return []
    
    try:
        # 确保数据按时间排序
        points_df = points_df.sort_values('timestamp')
        
        # 过滤有效速度数据
        speed_data = points_df['twist_linear'].dropna()
        if len(speed_data) < 3:  # 至少需要3个点来计算统计量
            return []
        
        # 计算速度统计量
        speed_mean = speed_data.mean()
        speed_std = speed_data.std()
        
        if speed_std == 0:  # 避免除零
            return []
        
        # 检测突变点
        spikes = []
        
        for idx, row in points_df.iterrows():
            if pd.isna(row['twist_linear']):
                continue
                
            current_speed = row['twist_linear']
            
            # 计算偏差
            deviation = abs(current_speed - speed_mean)
            z_score = deviation / speed_std
            
            # 判断是否为突变点
            if z_score > threshold_std:
                spike_event = {
                    'timestamp': int(row['timestamp']),
                    'longitude': float(row['longitude']),
                    'latitude': float(row['latitude']),
                    'event_type': 'speed_spike',
                    'speed_value': round(float(current_speed), 2),
                    'speed_mean': round(float(speed_mean), 2),
                    'z_score': round(float(z_score), 2),
                    'description': f"速度突变: {current_speed:.2f} (Z-score: {z_score:.2f})"
                }
                spikes.append(spike_event)
                logger.debug(f"检测到速度突变: {spike_event['description']}")
        
        logger.info(f"检测到 {len(spikes)} 个速度突变点")
        return spikes
        
    except Exception as e:
        logger.error(f"速度突变检测失败: {str(e)}")
        return []

def create_events_table(eng, table_name: str) -> bool:
    """创建变化点事件表。
    
    Args:
        eng: 数据库引擎
        table_name: 表名
        
    Returns:
        创建是否成功
    """
    try:
        # 检查表是否已存在
        check_table_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        
        with eng.connect() as conn:
            result = conn.execute(check_table_sql)
            table_exists = result.scalar()
            
            if table_exists:
                logger.info(f"变化点表 {table_name} 已存在，跳过创建")
                return True
            
            logger.info(f"创建变化点表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    scene_id text NOT NULL,
                    timestamp bigint,
                    event_type text NOT NULL,
                    from_value numeric,
                    to_value numeric,
                    speed_value numeric,
                    speed_mean numeric,
                    z_score numeric,
                    description text,
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 添加几何列（Point）
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POINT', 2);
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_timestamp ON {table_name}(timestamp);
                CREATE INDEX idx_{table_name}_event_type ON {table_name}(event_type);
            """)
            
            # 执行SQL
            conn.execute(create_sql)
            conn.commit()
            
            conn.execute(add_geom_sql)
            conn.commit()
            
            conn.execute(index_sql)
            conn.commit()
            
            logger.info(f"成功创建变化点表 {table_name}")
            return True
            
    except Exception as e:
        logger.error(f"创建变化点表失败: {table_name}, 错误: {str(e)}")
        return False

def insert_events_data(eng, table_name: str, scene_id: str, events_data: List[Dict]) -> int:
    """批量插入变化点数据。
    
    Args:
        eng: 数据库引擎
        table_name: 表名
        scene_id: 场景ID
        events_data: 变化点数据列表
        
    Returns:
        插入成功的记录数
    """
    if not events_data:
        return 0
    
    try:
        # 准备GeoDataFrame
        gdf_data = []
        geometries = []
        
        for event in events_data:
            # 基础数据
            row = {
                'scene_id': scene_id,
                'timestamp': event['timestamp'],
                'event_type': event['event_type'],
                'description': event['description']
            }
            
            # 根据事件类型添加特定字段
            if event['event_type'] == 'avp_change':
                row['from_value'] = event.get('from_value')
                row['to_value'] = event.get('to_value')
            elif event['event_type'] == 'speed_spike':
                row['speed_value'] = event.get('speed_value')
                row['speed_mean'] = event.get('speed_mean')
                row['z_score'] = event.get('z_score')
            
            gdf_data.append(row)
            
            # 创建点几何
            from shapely.geometry import Point
            point_geom = Point(event['longitude'], event['latitude'])
            geometries.append(point_geom)
        
        if not gdf_data:
            logger.warning("没有有效的变化点数据可插入")
            return 0
        
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
        
        # 插入数据
        gdf.to_postgis(table_name, eng, if_exists='append', index=False)
        
        inserted_count = len(gdf)
        logger.info(f"成功插入 {inserted_count} 条变化点记录到 {table_name}")
        return inserted_count
        
    except Exception as e:
        logger.error(f"插入变化点数据失败: {str(e)}")
        return 0

def process_scene_mappings(mappings_df: pd.DataFrame, table_name: str, 
                          batch_size: int = 100, detect_avp: bool = False, 
                          detect_speed: bool = False, speed_threshold: float = 2.0) -> Dict:
    """处理scene_id和data_name映射，生成轨迹数据。
    
    Args:
        mappings_df: 包含scene_id和data_name映射的DataFrame
        table_name: 目标表名
        batch_size: 批处理大小
        detect_avp: 是否检测AVP变化点
        detect_speed: 是否检测速度突变点
        speed_threshold: 速度突变阈值（标准差倍数）
        
    Returns:
        处理统计信息
    """
    setup_signal_handlers()
    
    stats = {
        'total_scenes': len(mappings_df),
        'processed_scenes': 0,
        'successful_trajectories': 0,
        'failed_scenes': 0,
        'empty_scenes': 0,
        'missing_data_names': 0,
        'total_avp_changes': 0,
        'total_speed_spikes': 0,
        'start_time': datetime.now()
    }
    
    # 创建数据库连接
    eng = create_engine(LOCAL_DSN, future=True)
    
    # 查询缺失的data_name
    missing_data_names = mappings_df[mappings_df['data_name'].isna()]
    if len(missing_data_names) > 0:
        logger.info(f"需要查询 {len(missing_data_names)} 个scene_id对应的data_name")
        
        # 批量查询data_name
        scene_ids_to_query = missing_data_names['scene_id'].tolist()
        db_mappings = fetch_data_names_from_scene_ids(scene_ids_to_query)
        
        if not db_mappings.empty:
            # 更新mappings_df中的data_name
            for idx, row in db_mappings.iterrows():
                scene_id = row['scene_id']
                data_name = row['data_name']
                mask = (mappings_df['scene_id'] == scene_id) & (mappings_df['data_name'].isna())
                mappings_df.loc[mask, 'data_name'] = data_name
                logger.debug(f"更新映射: {scene_id} -> {data_name}")
        else:
            logger.warning("数据库查询未返回任何data_name映射")
    
    # 统计仍然缺失data_name的记录
    still_missing = mappings_df[mappings_df['data_name'].isna()]
    if len(still_missing) > 0:
        stats['missing_data_names'] = len(still_missing)
        logger.warning(f"仍有 {len(still_missing)} 个scene_id无法获取data_name，将跳过处理")
    
    # 创建轨迹表
    if not create_trajectory_table(eng, table_name):
        logger.error("创建轨迹表失败，退出处理")
        return stats
    
    # 创建变化点表（如果需要）
    events_table_name = f"{table_name}_events"
    if detect_avp or detect_speed:
        if not create_events_table(eng, events_table_name):
            logger.error("创建变化点表失败，退出处理")
            return stats
    
    # 批量处理
    trajectory_batch = []
    
    # 过滤出有效的映射（有data_name的记录）
    valid_mappings = mappings_df[mappings_df['data_name'].notna()]
    
    for i, (idx, row) in enumerate(valid_mappings.iterrows()):
        if interrupted:
            logger.info("处理被中断")
            break
        
        scene_id = row['scene_id']
        data_name = row['data_name']
        
        logger.info(f"处理场景 [{i+1}/{len(valid_mappings)}]: {scene_id} ({data_name})")
        
        # 查询轨迹点
        points_df = fetch_trajectory_points(data_name)
        
        if points_df.empty:
            stats['empty_scenes'] += 1
            logger.warning(f"data_name无轨迹数据: {data_name}")
            continue
        
        # 构建轨迹
        trajectory = build_trajectory(scene_id, data_name, points_df)
        
        if trajectory:
            trajectory_batch.append(trajectory)
            stats['successful_trajectories'] += 1
            
            # 检测变化点
            all_events = []
            
            if detect_avp:
                avp_changes = detect_avp_changes(points_df)
                all_events.extend(avp_changes)
                stats['total_avp_changes'] += len(avp_changes)
            
            if detect_speed:
                speed_spikes = detect_speed_spikes(points_df, speed_threshold)
                all_events.extend(speed_spikes)
                stats['total_speed_spikes'] += len(speed_spikes)
            
            # 插入变化点数据
            if all_events:
                inserted_events = insert_events_data(eng, events_table_name, scene_id, all_events)
                logger.debug(f"场景 {scene_id} 插入 {inserted_events} 个变化点")
        else:
            stats['failed_scenes'] += 1
            logger.warning(f"轨迹构建失败: {scene_id} ({data_name})")
        
        stats['processed_scenes'] += 1
        
        # 批量插入轨迹数据
        if len(trajectory_batch) >= batch_size:
            inserted = insert_trajectory_data(eng, table_name, trajectory_batch)
            trajectory_batch = []
            
            if inserted > 0:
                logger.info(f"批量插入完成，已处理 {stats['processed_scenes']} 个场景")
    
    # 处理剩余数据
    if trajectory_batch:
        inserted = insert_trajectory_data(eng, table_name, trajectory_batch)
        logger.info(f"最终批量插入完成")
    
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats

def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='轨迹生成模块 - 从scene_id生成连续轨迹线',
        epilog="""
支持的输入格式:
  1. 文本文件 (.txt): 每行一个scene_id
  2. Dataset文件 (.json/.parquet): 包含scene_id和data_name映射的数据集文件
  
示例:
  python -m spdatalab.dataset.trajectory --input scenes.txt --table my_trajectories
  python -m spdatalab.dataset.trajectory --input dataset.json --table my_trajectories --detect-avp --detect-speed
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', required=True, 
                       help='输入文件：scene_id列表(.txt)或dataset文件(.json/.parquet)')
    parser.add_argument('--table', required=True, help='输出的轨迹表名')
    parser.add_argument('--batch-size', type=int, default=100, help='批处理大小')
    parser.add_argument('--detect-avp', action='store_true', help='检测AVP变化点')
    parser.add_argument('--detect-speed', action='store_true', help='检测速度突变点')
    parser.add_argument('--speed-threshold', type=float, default=2.0, help='速度突变阈值（标准差倍数）')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 加载scene_id和data_name映射
        logger.info(f"加载输入文件: {args.input}")
        mappings_df = load_scene_data_mappings(args.input)
        
        if mappings_df.empty:
            logger.error("未加载到任何scene_id映射")
            return 1
        
        # 输出配置信息
        logger.info(f"轨迹表: {args.table}")
        if args.detect_avp or args.detect_speed:
            logger.info(f"变化点表: {args.table}_events")
        if args.detect_avp:
            logger.info("启用AVP变化点检测")
        if args.detect_speed:
            logger.info(f"启用速度突变检测 (阈值: {args.speed_threshold}σ)")
        
        # 输出映射统计
        total_mappings = len(mappings_df)
        pre_filled_data_names = mappings_df['data_name'].notna().sum()
        logger.info(f"总计映射: {total_mappings} 个")
        logger.info(f"已有data_name: {pre_filled_data_names} 个")
        logger.info(f"需查询data_name: {total_mappings - pre_filled_data_names} 个")
        
        # 处理轨迹生成
        logger.info(f"开始处理 {total_mappings} 个场景")
        stats = process_scene_mappings(
            mappings_df, args.table, args.batch_size,
            detect_avp=args.detect_avp,
            detect_speed=args.detect_speed,
            speed_threshold=args.speed_threshold
        )
        
        # 输出统计信息
        logger.info("=== 处理完成 ===")
        logger.info(f"总场景数: {stats['total_scenes']}")
        logger.info(f"处理场景数: {stats['processed_scenes']}")
        logger.info(f"成功轨迹数: {stats['successful_trajectories']}")
        logger.info(f"失败场景数: {stats['failed_scenes']}")
        logger.info(f"空场景数: {stats['empty_scenes']}")
        
        if stats.get('missing_data_names', 0) > 0:
            logger.info(f"缺失data_name数: {stats['missing_data_names']}")
        
        # 变化点统计
        if args.detect_avp:
            logger.info(f"AVP变化点数: {stats['total_avp_changes']}")
        if args.detect_speed:
            logger.info(f"速度突变点数: {stats['total_speed_spikes']}")
        
        logger.info(f"处理时间: {stats['duration']}")
        
        return 0 if stats['successful_trajectories'] > 0 else 1
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 