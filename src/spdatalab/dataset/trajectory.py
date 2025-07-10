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

def load_scene_ids(file_path: str) -> List[str]:
    """从文件加载scene_id列表。
    
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

def fetch_trajectory_points(scene_id: str) -> pd.DataFrame:
    """查询单个场景的轨迹点数据。
    
    Args:
        scene_id: 场景ID
        
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
        WHERE dataset_name = :scene_id
        ORDER BY timestamp ASC
    """)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        with eng.connect() as conn:
            result = conn.execute(sql, {"scene_id": scene_id})
            
            if result.rowcount == 0:
                logger.warning(f"未找到scene_id的轨迹数据: {scene_id}")
                return pd.DataFrame()
            
            # 获取列名
            columns = result.keys()
            rows = result.fetchall()
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            logger.debug(f"查询到 {len(df)} 个轨迹点: {scene_id}")
            
            return df
            
    except Exception as e:
        logger.error(f"查询轨迹点失败: {scene_id}, 错误: {str(e)}")
        return pd.DataFrame()

def build_trajectory(points_df: pd.DataFrame) -> Dict:
    """从轨迹点构建轨迹线几何和统计信息。
    
    Args:
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
        
        # 构建LineString几何（在数据库中进行）
        from shapely.geometry import LineString
        trajectory_geom = LineString(coordinates)
        
        # 计算统计信息
        stats = {
            'scene_id': points_df['dataset_name'].iloc[0],
            'point_count': len(points_df),
            'start_time': points_df['timestamp'].min(),
            'end_time': points_df['timestamp'].max(),
            'duration': points_df['timestamp'].max() - points_df['timestamp'].min(),
            'geometry': trajectory_geom
        }
        
        # 速度统计
        if 'twist_linear' in points_df.columns:
            speed_data = points_df['twist_linear'].dropna()
            if len(speed_data) > 0:
                stats.update({
                    'avg_speed': float(speed_data.mean()),
                    'max_speed': float(speed_data.max()),
                    'min_speed': float(speed_data.min()),
                    'std_speed': float(speed_data.std()) if len(speed_data) > 1 else 0.0
                })
        
        # AVP统计
        if 'avp_flag' in points_df.columns:
            avp_data = points_df['avp_flag'].dropna()
            if len(avp_data) > 0:
                stats.update({
                    'avp_point_count': int((avp_data == 1).sum()),
                    'avp_ratio': float((avp_data == 1).mean())
                })
        
        logger.debug(f"构建轨迹: {stats['scene_id']}, 点数: {stats['point_count']}")
        return stats
        
    except Exception as e:
        logger.error(f"构建轨迹失败: {str(e)}")
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
                    point_count integer,
                    start_time bigint,
                    end_time bigint,
                    duration bigint,
                    avg_speed numeric,
                    max_speed numeric,
                    min_speed numeric,
                    std_speed numeric,
                    avp_point_count integer,
                    avp_ratio numeric,
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
        # 准备GeoDataFrame
        gdf_data = []
        for traj in trajectory_data:
            if 'geometry' in traj and traj['geometry'] is not None:
                row = {k: v for k, v in traj.items() if k != 'geometry'}
                gdf_data.append(row)
        
        if not gdf_data:
            logger.warning("没有有效的轨迹数据可插入")
            return 0
        
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(gdf_data, crs=4326)
        gdf['geometry'] = [traj['geometry'] for traj in trajectory_data if 'geometry' in traj]
        
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
        
        # 填充缺失值，默认为0
        points_df['avp_flag'] = points_df['avp_flag'].fillna(0)
        
        # 检测变化点
        changes = []
        prev_avp = None
        
        for idx, row in points_df.iterrows():
            current_avp = int(row['avp_flag'])
            
            # 检测状态变化
            if prev_avp is not None and prev_avp != current_avp:
                change_event = {
                    'timestamp': row['timestamp'],
                    'longitude': row['longitude'],
                    'latitude': row['latitude'],
                    'event_type': 'avp_change',
                    'from_value': prev_avp,
                    'to_value': current_avp,
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
                    'timestamp': row['timestamp'],
                    'longitude': row['longitude'],
                    'latitude': row['latitude'],
                    'event_type': 'speed_spike',
                    'speed_value': current_speed,
                    'speed_mean': speed_mean,
                    'z_score': z_score,
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
                row['from_value'] = event['from_value']
                row['to_value'] = event['to_value']
            elif event['event_type'] == 'speed_spike':
                row['speed_value'] = event['speed_value']
                row['speed_mean'] = event['speed_mean']
                row['z_score'] = event['z_score']
            
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

def process_scene_ids(scene_ids: List[str], table_name: str, 
                     batch_size: int = 100, detect_avp: bool = False, 
                     detect_speed: bool = False, speed_threshold: float = 2.0) -> Dict:
    """处理场景ID列表，生成轨迹数据。
    
    Args:
        scene_ids: 场景ID列表
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
        'total_scenes': len(scene_ids),
        'processed_scenes': 0,
        'successful_trajectories': 0,
        'failed_scenes': 0,
        'empty_scenes': 0,
        'total_avp_changes': 0,
        'total_speed_spikes': 0,
        'start_time': datetime.now()
    }
    
    # 创建数据库连接
    eng = create_engine(LOCAL_DSN, future=True)
    
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
    
    for i, scene_id in enumerate(scene_ids):
        if interrupted:
            logger.info("处理被中断")
            break
        
        logger.info(f"处理场景 [{i+1}/{len(scene_ids)}]: {scene_id}")
        
        # 查询轨迹点
        points_df = fetch_trajectory_points(scene_id)
        
        if points_df.empty:
            stats['empty_scenes'] += 1
            logger.warning(f"场景无轨迹数据: {scene_id}")
            continue
        
        # 构建轨迹
        trajectory = build_trajectory(points_df)
        
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
            logger.warning(f"轨迹构建失败: {scene_id}")
        
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
    parser = argparse.ArgumentParser(description='轨迹生成模块')
    parser.add_argument('--input', required=True, help='输入的scene_id列表文件')
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
        # 加载scene_id列表
        logger.info(f"加载scene_id列表: {args.input}")
        scene_ids = load_scene_ids(args.input)
        
        if not scene_ids:
            logger.error("未加载到任何scene_id")
            return 1
        
        # 输出配置信息
        logger.info(f"轨迹表: {args.table}")
        if args.detect_avp or args.detect_speed:
            logger.info(f"变化点表: {args.table}_events")
        if args.detect_avp:
            logger.info("启用AVP变化点检测")
        if args.detect_speed:
            logger.info(f"启用速度突变检测 (阈值: {args.speed_threshold}σ)")
        
        # 处理轨迹生成
        logger.info(f"开始处理 {len(scene_ids)} 个场景")
        stats = process_scene_ids(
            scene_ids, args.table, args.batch_size,
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