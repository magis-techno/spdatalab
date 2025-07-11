"""
轨迹道路分析模块

功能：
1. 基于自车轨迹分析相关道路元素
2. 轨迹膨胀3m缓冲区
3. 查找相交的lane、intersection，补齐intersection的inlane/outlane
4. 基于road_id扩展相关lane
5. 基于lanenextlane关系扩展前后lane链路
6. 收集相关road信息
"""

import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class TrajectoryRoadAnalysisConfig:
    """轨迹道路分析配置"""
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_dsn: str = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"
    
    # 远程表名配置
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    lanenextlane_table: str = "full_lanenextlane"
    road_table: str = "full_road"
    
    # 分析参数
    buffer_distance: float = 3.0  # 轨迹膨胀距离(m)
    forward_chain_limit: float = 500.0  # 前向链路扩展限制(m)
    backward_chain_limit: float = 100.0  # 后向链路扩展限制(m)
    max_recursion_depth: int = 50  # 最大递归深度
    
    # 查询限制参数
    max_lanes_per_query: int = 1000  # 单次查询最大lane数量
    max_intersections_per_query: int = 100  # 单次查询最大intersection数量
    max_forward_chains: int = 500  # 前向链路最大数量
    max_backward_chains: int = 200  # 后向链路最大数量
    query_timeout: int = 60  # 查询超时时间（秒）
    recursive_query_timeout: int = 120  # 递归查询超时时间（秒）
    
    # 结果表名
    analysis_table: str = "trajectory_road_analysis"
    lanes_table: str = "trajectory_road_lanes"
    intersections_table: str = "trajectory_road_intersections"
    roads_table: str = "trajectory_road_roads"

class TrajectoryRoadAnalyzer:
    """
    轨迹道路分析器
    
    主要功能：
    1. 基于轨迹膨胀缓冲区查找相关道路元素
    2. 扩展lane链路和road信息
    3. 生成完整的道路分析结果
    """
    
    def __init__(self, config: Optional[TrajectoryRoadAnalysisConfig] = None):
        self.config = config or TrajectoryRoadAnalysisConfig()
        self.local_engine = create_engine(
            self.config.local_dsn,
            future=True,
            connect_args={"client_encoding": "utf8"},
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10
        )
        self.remote_engine = create_engine(
            self.config.remote_dsn,
            future=True,
            connect_args={"client_encoding": "utf8", "connect_timeout": 30},
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10
        )
        
        # 初始化分析表
        self._init_analysis_tables()
    
    def _init_analysis_tables(self):
        """初始化轨迹道路分析相关的表"""
        self._init_analysis_table()
        self._init_lanes_table()
        self._init_intersections_table()
        self._init_roads_table()
    
    def _init_analysis_table(self):
        """初始化主分析表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.analysis_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                trajectory_id VARCHAR(100) NOT NULL,
                original_trajectory_geom GEOMETRY,
                buffer_trajectory_geom GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, trajectory_id)
            )
        """)
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.commit()
            logger.info(f"主分析表 {self.config.analysis_table} 初始化完成")
        except Exception as e:
            logger.warning(f"主分析表初始化失败: {e}")
    
    def _init_lanes_table(self):
        """初始化lane结果表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.lanes_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                lane_id BIGINT NOT NULL,
                lane_type VARCHAR(50), -- 'direct_intersect', 'intersection_related', 'road_related', 'chain_forward', 'chain_backward'
                road_id BIGINT,
                distance_from_trajectory FLOAT,
                chain_depth INTEGER,
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, lane_id)
            )
        """)
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.commit()
            logger.info(f"Lane结果表 {self.config.lanes_table} 初始化完成")
        except Exception as e:
            logger.warning(f"Lane结果表初始化失败: {e}")
    
    def _init_intersections_table(self):
        """初始化intersection结果表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.intersections_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                intersection_id BIGINT NOT NULL,
                intersection_type INTEGER,
                intersection_subtype INTEGER,
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, intersection_id)
            )
        """)
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.commit()
            logger.info(f"Intersection结果表 {self.config.intersections_table} 初始化完成")
        except Exception as e:
            logger.warning(f"Intersection结果表初始化失败: {e}")
    
    def _init_roads_table(self):
        """初始化road结果表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.roads_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                road_id BIGINT NOT NULL,
                lane_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, road_id)
            )
        """)
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.commit()
            logger.info(f"Road结果表 {self.config.roads_table} 初始化完成")
        except Exception as e:
            logger.warning(f"Road结果表初始化失败: {e}") 

    def analyze_trajectory_roads(
        self,
        trajectory_id: str,
        trajectory_geom: str,
        analysis_id: Optional[str] = None
    ) -> str:
        """
        分析轨迹相关的道路元素
        
        Args:
            trajectory_id: 轨迹ID
            trajectory_geom: 轨迹几何WKT字符串
            analysis_id: 分析ID（可选，自动生成）
            
        Returns:
            分析ID
        """
        if not analysis_id:
            analysis_id = f"trajectory_road_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"开始轨迹道路分析: {analysis_id}")
        
        # 1. 创建轨迹缓冲区
        buffer_geom = self._create_trajectory_buffer(trajectory_geom)
        
        # 2. 保存分析记录
        self._save_analysis_record(analysis_id, trajectory_id, trajectory_geom, buffer_geom)
        
        # 3. 查找相交的lane和intersection
        intersecting_lanes = self._find_intersecting_lanes(analysis_id, buffer_geom)
        intersecting_intersections = self._find_intersecting_intersections(analysis_id, buffer_geom)
        
        # 4. 扩展intersection相关的lane
        intersection_lanes = self._expand_intersection_lanes(analysis_id, intersecting_intersections)
        
        # 5. 基于road_id扩展lane
        all_lanes = pd.concat([intersecting_lanes, intersection_lanes], ignore_index=True)
        road_lanes = self._expand_road_lanes(analysis_id, all_lanes)
        
        # 6. 扩展lane链路
        final_lanes = pd.concat([all_lanes, road_lanes], ignore_index=True)
        chain_lanes = self._expand_lane_chains(analysis_id, final_lanes)
        
        # 7. 收集road信息
        all_final_lanes = pd.concat([final_lanes, chain_lanes], ignore_index=True)
        self._collect_roads(analysis_id, all_final_lanes)
        
        logger.info(f"轨迹道路分析完成: {analysis_id}")
        return analysis_id
    
    def _create_trajectory_buffer(self, trajectory_geom: str) -> str:
        """创建轨迹缓冲区"""
        buffer_sql = text(f"""
            SELECT ST_AsText(
                ST_Buffer(
                    ST_SetSRID(ST_GeomFromText('{trajectory_geom}'), 4326)::geography,
                    {self.config.buffer_distance}
                )::geometry
            ) as buffer_geom
        """)
        
        try:
            with self.local_engine.connect() as conn:
                result = conn.execute(buffer_sql).fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"创建轨迹缓冲区失败: {e}")
            return None
    
    def _save_analysis_record(self, analysis_id: str, trajectory_id: str, 
                             trajectory_geom: str, buffer_geom: str):
        """保存分析记录"""
        save_sql = text(f"""
            INSERT INTO {self.config.analysis_table} 
            (analysis_id, trajectory_id, original_trajectory_geom, buffer_trajectory_geom)
            VALUES (
                :analysis_id, 
                :trajectory_id, 
                ST_SetSRID(ST_GeomFromText(:trajectory_geom), 4326),
                ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326)
            )
            ON CONFLICT (analysis_id, trajectory_id) DO NOTHING
        """)
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(save_sql, {
                    'analysis_id': analysis_id,
                    'trajectory_id': trajectory_id,
                    'trajectory_geom': trajectory_geom,
                    'buffer_geom': buffer_geom
                })
                conn.commit()
            logger.info(f"保存分析记录: {analysis_id}")
        except Exception as e:
            logger.error(f"保存分析记录失败: {e}")
    
    def _find_intersecting_lanes(self, analysis_id: str, buffer_geom: str) -> pd.DataFrame:
        """查找与轨迹缓冲区相交的lane"""
        
        # 输出buffer几何信息用于调试
        logger.info(f"=== 查找相交lane调试信息 ===")
        logger.info(f"Analysis ID: {analysis_id}")
        logger.info(f"Buffer几何长度: {len(buffer_geom)} 字符")
        logger.info(f"Buffer几何前100字符: {buffer_geom[:100]}...")
        logger.info(f"Buffer几何后100字符: ...{buffer_geom[-100:]}")
        
        lanes_sql = text("""
            SELECT 
                id as lane_id,
                roadid as road_id,
                ST_AsText(wkb_geometry) as geometry_wkt,
                ST_Distance(
                    ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326)::geography,
                    wkb_geometry::geography
                ) as distance
            FROM {lane_table}
            WHERE ST_Intersects(
                ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326),
                wkb_geometry
            )
            AND wkb_geometry IS NOT NULL
            LIMIT 1000
        """.format(lane_table=self.config.lane_table))
        
        # 输出完整的SQL语句
        final_sql = str(lanes_sql).replace(':buffer_geom', f"'{buffer_geom}'")
        logger.info(f"=== 完整SQL语句 ===")
        logger.info(final_sql)
        
        # 输出可以在DataGrip中直接使用的SQL
        logger.info(f"=== DataGrip可执行SQL ===")
        datagrip_sql = f"""
-- 完整查询（可能较慢）
SELECT 
    id as lane_id,
    roadid as road_id,
    ST_AsText(wkb_geometry) as geometry_wkt,
    ST_Distance(
        ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326)::geography,
        wkb_geometry::geography
    ) as distance
FROM {self.config.lane_table}
WHERE ST_Intersects(
    ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326),
    wkb_geometry
)
AND wkb_geometry IS NOT NULL
LIMIT 1000;
"""
        logger.info(datagrip_sql)
        
        try:
            # 分步调试
            logger.info("=== 分步调试 ===")
            
            with self.remote_engine.connect() as conn:
                # 设置查询超时为60秒
                conn.execute(text("SET statement_timeout = '60s'"))
                
                # 步骤1：验证buffer几何是否有效
                logger.info("步骤1：验证buffer几何有效性")
                buffer_check_sql = text(f"""
                    SELECT 
                        ST_IsValid(ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326)) as is_valid,
                        ST_GeometryType(ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326)) as geom_type,
                        ST_AsText(ST_Envelope(ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326))) as bbox
                """)
                
                buffer_result = conn.execute(buffer_check_sql, {'buffer_geom': buffer_geom}).fetchone()
                if buffer_result:
                    logger.info(f"Buffer几何有效性: {buffer_result[0]}")
                    logger.info(f"Buffer几何类型: {buffer_result[1]}")
                    logger.info(f"Buffer边界框: {buffer_result[2]}")
                
                # 步骤2：检查表和索引
                logger.info("步骤2：检查表基本信息")
                table_check_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_lanes,
                        COUNT(CASE WHEN wkb_geometry IS NOT NULL THEN 1 END) as lanes_with_geom,
                        ST_AsText(ST_Extent(wkb_geometry)) as table_extent
                    FROM {self.config.lane_table}
                """)
                
                table_result = conn.execute(table_check_sql).fetchone()
                if table_result:
                    logger.info(f"表总lane数: {table_result[0]}")
                    logger.info(f"有几何的lane数: {table_result[1]}")
                    logger.info(f"表几何范围: {table_result[2]}")
                
                # 步骤3：简化查询测试（只检查相交，不计算距离）
                logger.info("步骤3：简化相交查询")
                simple_sql = text(f"""
                    SELECT 
                        id as lane_id,
                        roadid as road_id,
                        ST_AsText(wkb_geometry) as geometry_wkt
                    FROM {self.config.lane_table}
                    WHERE ST_Intersects(
                        ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326),
                        wkb_geometry
                    )
                    AND wkb_geometry IS NOT NULL
                    LIMIT 10
                """)
                
                simple_result = pd.read_sql(simple_sql, conn, params={'buffer_geom': buffer_geom})
                logger.info(f"简化查询结果: {len(simple_result)} 个lane")
                
                # 步骤4：执行完整查询
                logger.info("步骤4：执行完整查询")
                lanes_df = pd.read_sql(lanes_sql, conn, params={'buffer_geom': buffer_geom})
            
            if not lanes_df.empty:
                # 保存到结果表
                self._save_lanes_results(analysis_id, lanes_df, 'direct_intersect')
                logger.info(f"✓ 找到 {len(lanes_df)} 个相交lane")
                
                # 输出前几个结果用于验证
                logger.info("前3个结果:")
                for i, row in lanes_df.head(3).iterrows():
                    logger.info(f"  Lane {row['lane_id']}: road_id={row['road_id']}, distance={row.get('distance', 'N/A')}")
            else:
                logger.info("未找到相交的lane")
            
            return lanes_df
            
        except Exception as e:
            logger.error(f"查找相交lane失败: {e}")
            logger.error(f"错误类型: {type(e).__name__}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _find_intersecting_intersections(self, analysis_id: str, buffer_geom: str) -> pd.DataFrame:
        """查找与轨迹缓冲区相交的intersection"""
        
        logger.info(f"=== 查找相交intersection调试信息 ===")
        
        intersections_sql = text("""
            SELECT 
                id as intersection_id,
                intersectiontype,
                intersectionsubtype,
                ST_AsText(wkb_geometry) as geometry_wkt
            FROM {intersection_table}
            WHERE ST_Intersects(
                ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326),
                wkb_geometry
            )
            AND wkb_geometry IS NOT NULL
            LIMIT 100
        """.format(intersection_table=self.config.intersection_table))
        
        # 输出DataGrip可执行SQL
        datagrip_sql = f"""
-- 查找相交intersection
SELECT 
    id as intersection_id,
    intersectiontype,
    intersectionsubtype,
    ST_AsText(wkb_geometry) as geometry_wkt
FROM {self.config.intersection_table}
WHERE ST_Intersects(
    ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326),
    wkb_geometry
)
AND wkb_geometry IS NOT NULL
LIMIT 100;
"""
        logger.info(f"=== DataGrip Intersection SQL ===")
        logger.info(datagrip_sql)
        
        try:
            with self.remote_engine.connect() as conn:
                # 设置查询超时为60秒
                conn.execute(text("SET statement_timeout = '60s'"))
                
                # 检查intersection表基本信息
                logger.info("检查intersection表基本信息")
                table_check_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_intersections,
                        COUNT(CASE WHEN wkb_geometry IS NOT NULL THEN 1 END) as intersections_with_geom,
                        ST_AsText(ST_Extent(wkb_geometry)) as table_extent
                    FROM {self.config.intersection_table}
                """)
                
                table_result = conn.execute(table_check_sql).fetchone()
                if table_result:
                    logger.info(f"表总intersection数: {table_result[0]}")
                    logger.info(f"有几何的intersection数: {table_result[1]}")
                    logger.info(f"表几何范围: {table_result[2]}")
                
                intersections_df = pd.read_sql(intersections_sql, conn, params={'buffer_geom': buffer_geom})
            
            if not intersections_df.empty:
                # 保存到结果表
                self._save_intersections_results(analysis_id, intersections_df)
                logger.info(f"✓ 找到 {len(intersections_df)} 个相交intersection")
                
                # 输出前几个结果用于验证
                logger.info("前3个intersection结果:")
                for i, row in intersections_df.head(3).iterrows():
                    logger.info(f"  Intersection {row['intersection_id']}: type={row['intersectiontype']}, subtype={row['intersectionsubtype']}")
            else:
                logger.info("未找到相交的intersection")
            
            return intersections_df
        except Exception as e:
            logger.error(f"查找相交intersection失败: {e}")
            logger.error(f"错误类型: {type(e).__name__}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def _save_lanes_results(self, analysis_id: str, lanes_df: pd.DataFrame, 
                           lane_type: str, chain_depth: int = 0):
        """保存lane结果到数据库"""
        if lanes_df.empty:
            return
        
        for _, row in lanes_df.iterrows():
            save_sql = text(f"""
                INSERT INTO {self.config.lanes_table} 
                (analysis_id, lane_id, lane_type, road_id, distance_from_trajectory, 
                 chain_depth, geometry)
                VALUES (
                    :analysis_id, :lane_id, :lane_type, :road_id, 
                    :distance, :chain_depth,
                    ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
                )
                ON CONFLICT (analysis_id, lane_id) DO NOTHING
            """)
            
            try:
                with self.local_engine.connect() as conn:
                    conn.execute(save_sql, {
                        'analysis_id': analysis_id,
                        'lane_id': int(row['lane_id']),
                        'lane_type': lane_type,
                        'road_id': int(row['road_id']) if pd.notna(row['road_id']) else None,
                        'distance': float(row.get('distance', 0)),
                        'chain_depth': chain_depth,
                        'geometry_wkt': row['geometry_wkt']
                    })
                    conn.commit()
            except Exception as e:
                logger.error(f"保存lane结果失败: {e}")
    
    def _save_intersections_results(self, analysis_id: str, intersections_df: pd.DataFrame):
        """保存intersection结果到数据库"""
        if intersections_df.empty:
            return
        
        for _, row in intersections_df.iterrows():
            save_sql = text(f"""
                INSERT INTO {self.config.intersections_table} 
                (analysis_id, intersection_id, intersection_type, intersection_subtype, geometry)
                VALUES (
                    :analysis_id, :intersection_id, :intersection_type, 
                    :intersection_subtype,
                    ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
                )
                ON CONFLICT (analysis_id, intersection_id) DO NOTHING
            """)
            
            try:
                with self.local_engine.connect() as conn:
                    conn.execute(save_sql, {
                        'analysis_id': analysis_id,
                        'intersection_id': int(row['intersection_id']),
                        'intersection_type': int(row['intersectiontype']) if pd.notna(row['intersectiontype']) else None,
                        'intersection_subtype': int(row['intersectionsubtype']) if pd.notna(row['intersectionsubtype']) else None,
                        'geometry_wkt': row['geometry_wkt']
                    })
                    conn.commit()
            except Exception as e:
                logger.error(f"保存intersection结果失败: {e}") 

    def _expand_intersection_lanes(self, analysis_id: str, intersections_df: pd.DataFrame) -> pd.DataFrame:
        """扩展intersection相关的lane（inlane和outlane）"""
        if intersections_df.empty:
            return pd.DataFrame()
        
        intersection_ids = intersections_df['intersection_id'].tolist()
        if not intersection_ids:
            return pd.DataFrame()
        
        # 查找intersection相关的lane
        intersection_lanes_sql = text("""
            SELECT 
                id as lane_id,
                roadid as road_id,
                ST_AsText(wkb_geometry) as geometry_wkt,
                intersectionid,
                isintersectioninlane,
                isintersectionoutlane
            FROM {lane_table}
            WHERE intersectionid = ANY(:intersection_ids)
            AND (isintersectioninlane = true OR isintersectionoutlane = true)
            AND wkb_geometry IS NOT NULL
            LIMIT 500
        """.format(lane_table=self.config.lane_table))
        
        try:
            with self.remote_engine.connect() as conn:
                # 设置查询超时为60秒
                conn.execute(text("SET statement_timeout = '60s'"))
                lanes_df = pd.read_sql(intersection_lanes_sql, conn, params={'intersection_ids': intersection_ids})
            
            if not lanes_df.empty:
                # 保存到结果表
                self._save_lanes_results(analysis_id, lanes_df, 'intersection_related')
                logger.info(f"扩展intersection相关lane: {len(lanes_df)} 个")
            else:
                logger.info("未找到intersection相关的lane")
            
            return lanes_df
        except Exception as e:
            logger.error(f"扩展intersection相关lane失败: {e}")
            return pd.DataFrame()
    
    def _expand_road_lanes(self, analysis_id: str, lanes_df: pd.DataFrame) -> pd.DataFrame:
        """基于road_id扩展所有相关lane"""
        if lanes_df.empty:
            return pd.DataFrame()
        
        road_ids = lanes_df['road_id'].dropna().unique().tolist()
        if not road_ids:
            return pd.DataFrame()
        
        # 查找所有同road_id的lane
        road_lanes_sql = text("""
            SELECT 
                id as lane_id,
                roadid as road_id,
                ST_AsText(wkb_geometry) as geometry_wkt
            FROM {lane_table}
            WHERE roadid = ANY(:road_ids)
            AND wkb_geometry IS NOT NULL
            LIMIT 2000
        """.format(lane_table=self.config.lane_table))
        
        try:
            with self.remote_engine.connect() as conn:
                # 设置查询超时为60秒
                conn.execute(text("SET statement_timeout = '60s'"))
                road_lanes_df = pd.read_sql(road_lanes_sql, conn, params={'road_ids': road_ids})
            
            if not road_lanes_df.empty:
                # 过滤掉已经存在的lane
                existing_lane_ids = set(lanes_df['lane_id'].tolist())
                new_lanes = road_lanes_df[~road_lanes_df['lane_id'].isin(existing_lane_ids)]
                
                if not new_lanes.empty:
                    # 保存到结果表
                    self._save_lanes_results(analysis_id, new_lanes, 'road_related')
                    logger.info(f"基于road_id扩展lane: {len(new_lanes)} 个")
                    return new_lanes
                else:
                    logger.info("基于road_id未找到新的lane")
            else:
                logger.info("基于road_id未找到任何lane")
            
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"基于road_id扩展lane失败: {e}")
            return pd.DataFrame()
    
    def _expand_lane_chains(self, analysis_id: str, lanes_df: pd.DataFrame) -> pd.DataFrame:
        """扩展lane链路（前向500m，后向100m）"""
        if lanes_df.empty:
            return pd.DataFrame()
        
        lane_ids = lanes_df['lane_id'].tolist()
        if not lane_ids:
            return pd.DataFrame()
        
        # 前向扩展
        forward_lanes = self._expand_forward_chains(analysis_id, lane_ids)
        
        # 后向扩展
        backward_lanes = self._expand_backward_chains(analysis_id, lane_ids)
        
        # 合并结果
        all_chain_lanes = []
        if not forward_lanes.empty:
            all_chain_lanes.append(forward_lanes)
        if not backward_lanes.empty:
            all_chain_lanes.append(backward_lanes)
        
        if all_chain_lanes:
            result = pd.concat(all_chain_lanes, ignore_index=True)
            # 去重
            result = result.drop_duplicates(subset=['lane_id'])
            logger.info(f"扩展lane链路: {len(result)} 个")
            return result
        
        return pd.DataFrame()
    
    def _expand_forward_chains(self, analysis_id: str, lane_ids: List[int]) -> pd.DataFrame:
        """向前扩展lane链路（不超过500m）"""
        # 限制递归深度，避免复杂查询
        max_depth = min(self.config.max_recursion_depth, 10)
        
        forward_sql = text("""
            WITH RECURSIVE lane_chain AS (
                -- 基础查询：当前lane作为起点
                SELECT 
                    lnl.laneid,
                    lnl.nextlaneid,
                    0 as depth,
                    0 as distance,
                    COALESCE(l.length, 0) as current_length
                FROM {lanenextlane_table} lnl
                JOIN {lane_table} l ON l.id = lnl.nextlaneid
                WHERE lnl.laneid = ANY(:lane_ids)
                AND lnl.ismeet = true
                AND l.wkb_geometry IS NOT NULL
                
                UNION ALL
                
                -- 递归查询：继续向前
                SELECT 
                    lnl.laneid,
                    lnl.nextlaneid,
                    lc.depth + 1,
                    lc.distance + lc.current_length,
                    COALESCE(l.length, 0) as current_length
                FROM {lanenextlane_table} lnl
                JOIN lane_chain lc ON lnl.laneid = lc.nextlaneid
                JOIN {lane_table} l ON l.id = lnl.nextlaneid
                WHERE lc.distance + lc.current_length <= :forward_limit
                AND lc.depth < :max_depth
                AND lnl.ismeet = true
                AND l.wkb_geometry IS NOT NULL
            )
            SELECT DISTINCT 
                lc.nextlaneid as lane_id,
                l.roadid as road_id,
                ST_AsText(l.wkb_geometry) as geometry_wkt,
                lc.depth as chain_depth,
                lc.distance
            FROM lane_chain lc
            JOIN {lane_table} l ON l.id = lc.nextlaneid
            WHERE lc.nextlaneid IS NOT NULL
            LIMIT 500
        """.format(
            lanenextlane_table=self.config.lanenextlane_table,
            lane_table=self.config.lane_table
        ))
        
        try:
            with self.remote_engine.connect() as conn:
                # 设置查询超时为120秒（递归查询可能需要更长时间）
                conn.execute(text("SET statement_timeout = '120s'"))
                forward_df = pd.read_sql(forward_sql, conn, params={
                    'lane_ids': lane_ids,
                    'forward_limit': self.config.forward_chain_limit,
                    'max_depth': max_depth
                })
            
            if not forward_df.empty:
                # 保存到结果表
                self._save_lanes_results(analysis_id, forward_df, 'chain_forward')
                logger.info(f"向前扩展lane链路: {len(forward_df)} 个")
            else:
                logger.info("未找到向前扩展的lane链路")
            
            return forward_df
        except Exception as e:
            logger.error(f"向前扩展lane链路失败: {e}")
            return pd.DataFrame()
    
    def _expand_backward_chains(self, analysis_id: str, lane_ids: List[int]) -> pd.DataFrame:
        """向后扩展lane链路（不超过100m）"""
        # 限制递归深度，避免复杂查询
        max_depth = min(self.config.max_recursion_depth, 10)
        
        backward_sql = text("""
            WITH RECURSIVE lane_chain AS (
                -- 基础查询：当前lane作为终点
                SELECT 
                    lnl.laneid,
                    lnl.nextlaneid,
                    0 as depth,
                    0 as distance,
                    COALESCE(l.length, 0) as current_length
                FROM {lanenextlane_table} lnl
                JOIN {lane_table} l ON l.id = lnl.laneid
                WHERE lnl.nextlaneid = ANY(:lane_ids)
                AND lnl.ismeet = true
                AND l.wkb_geometry IS NOT NULL
                
                UNION ALL
                
                -- 递归查询：继续向后
                SELECT 
                    lnl.laneid,
                    lnl.nextlaneid,
                    lc.depth + 1,
                    lc.distance + lc.current_length,
                    COALESCE(l.length, 0) as current_length
                FROM {lanenextlane_table} lnl
                JOIN lane_chain lc ON lnl.nextlaneid = lc.laneid
                JOIN {lane_table} l ON l.id = lnl.laneid
                WHERE lc.distance + lc.current_length <= :backward_limit
                AND lc.depth < :max_depth
                AND lnl.ismeet = true
                AND l.wkb_geometry IS NOT NULL
            )
            SELECT DISTINCT 
                lc.laneid as lane_id,
                l.roadid as road_id,
                ST_AsText(l.wkb_geometry) as geometry_wkt,
                lc.depth as chain_depth,
                lc.distance
            FROM lane_chain lc
            JOIN {lane_table} l ON l.id = lc.laneid
            WHERE lc.laneid IS NOT NULL
            LIMIT 200
        """.format(
            lanenextlane_table=self.config.lanenextlane_table,
            lane_table=self.config.lane_table
        ))
        
        try:
            with self.remote_engine.connect() as conn:
                # 设置查询超时为120秒（递归查询可能需要更长时间）
                conn.execute(text("SET statement_timeout = '120s'"))
                backward_df = pd.read_sql(backward_sql, conn, params={
                    'lane_ids': lane_ids,
                    'backward_limit': self.config.backward_chain_limit,
                    'max_depth': max_depth
                })
            
            if not backward_df.empty:
                # 保存到结果表
                self._save_lanes_results(analysis_id, backward_df, 'chain_backward')
                logger.info(f"向后扩展lane链路: {len(backward_df)} 个")
            else:
                logger.info("未找到向后扩展的lane链路")
            
            return backward_df
        except Exception as e:
            logger.error(f"向后扩展lane链路失败: {e}")
            return pd.DataFrame()
    
    def _collect_roads(self, analysis_id: str, lanes_df: pd.DataFrame):
        """收集road信息"""
        if lanes_df.empty:
            return
        
        road_ids = lanes_df['road_id'].dropna().unique().tolist()
        if not road_ids:
            return
        
        # 统计每个road的lane数量
        road_lane_counts = lanes_df.groupby('road_id').size().reset_index(name='lane_count')
        
        # 保存road信息
        for _, row in road_lane_counts.iterrows():
            save_sql = text(f"""
                INSERT INTO {self.config.roads_table} 
                (analysis_id, road_id, lane_count)
                VALUES (:analysis_id, :road_id, :lane_count)
                ON CONFLICT (analysis_id, road_id) DO UPDATE SET
                lane_count = EXCLUDED.lane_count
            """)
            
            try:
                with self.local_engine.connect() as conn:
                    conn.execute(save_sql, {
                        'analysis_id': analysis_id,
                        'road_id': int(row['road_id']),
                        'lane_count': int(row['lane_count'])
                    })
                    conn.commit()
            except Exception as e:
                logger.error(f"保存road信息失败: {e}")
        
        logger.info(f"收集road信息: {len(road_lane_counts)} 个road")
    
    def get_analysis_summary(self, analysis_id: str) -> Dict[str, Any]:
        """获取分析汇总信息"""
        summary = {'analysis_id': analysis_id}
        
        try:
            # 统计各类lane数量
            lane_stats_sql = text(f"""
                SELECT 
                    lane_type,
                    COUNT(*) as count
                FROM {self.config.lanes_table}
                WHERE analysis_id = :analysis_id
                GROUP BY lane_type
            """)
            
            with self.local_engine.connect() as conn:
                lane_stats = pd.read_sql(lane_stats_sql, conn, params={'analysis_id': analysis_id})
                
                # 转换为字典
                lane_stats_dict = dict(zip(lane_stats['lane_type'], lane_stats['count']))
                summary.update(lane_stats_dict)
            
            # 统计intersection数量
            intersection_count_sql = text(f"""
                SELECT COUNT(*) as intersection_count
                FROM {self.config.intersections_table}
                WHERE analysis_id = :analysis_id
            """)
            
            with self.local_engine.connect() as conn:
                result = conn.execute(intersection_count_sql, {'analysis_id': analysis_id}).fetchone()
                summary['intersection_count'] = result[0] if result else 0
            
            # 统计road数量
            road_count_sql = text(f"""
                SELECT COUNT(*) as road_count
                FROM {self.config.roads_table}
                WHERE analysis_id = :analysis_id
            """)
            
            with self.local_engine.connect() as conn:
                result = conn.execute(road_count_sql, {'analysis_id': analysis_id}).fetchone()
                summary['road_count'] = result[0] if result else 0
            
            summary['analysis_time'] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"获取分析汇总失败: {e}")
            summary['error'] = str(e)
        
        return summary
    
    def export_results_for_qgis(self, analysis_id: str) -> Dict[str, str]:
        """导出结果供QGIS可视化"""
        export_info = {}
        
        # 导出轨迹和缓冲区
        trajectory_view = f"qgis_trajectory_{analysis_id.replace('-', '_')}"
        create_trajectory_view_sql = text(f"""
            CREATE OR REPLACE VIEW {trajectory_view} AS
            SELECT 
                id,
                analysis_id,
                trajectory_id,
                original_trajectory_geom,
                buffer_trajectory_geom,
                created_at
            FROM {self.config.analysis_table}
            WHERE analysis_id = :analysis_id
        """)
        
        # 导出lane结果
        lanes_view = f"qgis_lanes_{analysis_id.replace('-', '_')}"
        create_lanes_view_sql = text(f"""
            CREATE OR REPLACE VIEW {lanes_view} AS
            SELECT 
                id,
                analysis_id,
                lane_id,
                lane_type,
                road_id,
                distance_from_trajectory,
                chain_depth,
                geometry,
                created_at
            FROM {self.config.lanes_table}
            WHERE analysis_id = :analysis_id
        """)
        
        # 导出intersection结果
        intersections_view = f"qgis_intersections_{analysis_id.replace('-', '_')}"
        create_intersections_view_sql = text(f"""
            CREATE OR REPLACE VIEW {intersections_view} AS
            SELECT 
                id,
                analysis_id,
                intersection_id,
                intersection_type,
                intersection_subtype,
                geometry,
                created_at
            FROM {self.config.intersections_table}
            WHERE analysis_id = :analysis_id
        """)
        
        try:
            with self.local_engine.connect() as conn:
                # 创建视图
                conn.execute(create_trajectory_view_sql, {'analysis_id': analysis_id})
                conn.execute(create_lanes_view_sql, {'analysis_id': analysis_id})
                conn.execute(create_intersections_view_sql, {'analysis_id': analysis_id})
                conn.commit()
                
                export_info.update({
                    'trajectory_view': trajectory_view,
                    'lanes_view': lanes_view,
                    'intersections_view': intersections_view
                })
                
                logger.info(f"创建QGIS视图: {list(export_info.keys())}")
                
        except Exception as e:
            logger.error(f"创建QGIS视图失败: {e}")
            
        return export_info 


# 便捷接口函数
def analyze_trajectory_road_elements(
    trajectory_id: str,
    trajectory_geom: str,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> Tuple[str, Dict[str, Any]]:
    """
    一站式轨迹道路元素分析
    
    Args:
        trajectory_id: 轨迹ID
        trajectory_geom: 轨迹几何WKT字符串
        config: 自定义配置
        
    Returns:
        (分析ID, 分析汇总信息)
    """
    analyzer = TrajectoryRoadAnalyzer(config or TrajectoryRoadAnalysisConfig())
    
    # 执行分析
    analysis_id = analyzer.analyze_trajectory_roads(
        trajectory_id=trajectory_id,
        trajectory_geom=trajectory_geom
    )
    
    # 获取汇总信息
    summary = analyzer.get_analysis_summary(analysis_id)
    
    return analysis_id, summary


def get_trajectory_road_analysis_summary(
    analysis_id: str,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> Dict[str, Any]:
    """
    获取轨迹道路分析汇总信息
    
    Args:
        analysis_id: 分析ID
        config: 自定义配置
        
    Returns:
        分析汇总字典
    """
    analyzer = TrajectoryRoadAnalyzer(config)
    return analyzer.get_analysis_summary(analysis_id)


def export_trajectory_road_results_for_qgis(
    analysis_id: str,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> Dict[str, str]:
    """
    导出轨迹道路分析结果供QGIS可视化
    
    Args:
        analysis_id: 分析ID
        config: 自定义配置
        
    Returns:
        导出信息字典
    """
    analyzer = TrajectoryRoadAnalyzer(config)
    return analyzer.export_results_for_qgis(analysis_id)


def analyze_trajectory_from_table(
    trajectory_table: str,
    trajectory_id_column: str = 'scene_id',
    trajectory_geom_column: str = 'geometry',
    limit: Optional[int] = None,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    批量分析轨迹表中的轨迹数据
    
    Args:
        trajectory_table: 轨迹表名
        trajectory_id_column: 轨迹ID列名
        trajectory_geom_column: 轨迹几何列名
        limit: 限制分析数量
        config: 自定义配置
        
    Returns:
        分析结果列表 [(trajectory_id, analysis_id, summary), ...]
    """
    analyzer = TrajectoryRoadAnalyzer(config or TrajectoryRoadAnalysisConfig())
    
    # 查询轨迹数据
    limit_clause = f"LIMIT {limit}" if limit else ""
    query_sql = text(f"""
        SELECT 
            {trajectory_id_column} as trajectory_id,
            ST_AsText({trajectory_geom_column}) as trajectory_geom
        FROM {trajectory_table}
        WHERE {trajectory_geom_column} IS NOT NULL
        {limit_clause}
    """)
    
    results = []
    
    try:
        with analyzer.local_engine.connect() as conn:
            trajectories_df = pd.read_sql(query_sql, conn)
        
        logger.info(f"找到 {len(trajectories_df)} 个轨迹进行分析")
        
        for idx, row in trajectories_df.iterrows():
            trajectory_id = row['trajectory_id']
            trajectory_geom = row['trajectory_geom']
            
            try:
                logger.info(f"分析轨迹 [{idx+1}/{len(trajectories_df)}]: {trajectory_id}")
                
                # 执行分析
                analysis_id = analyzer.analyze_trajectory_roads(
                    trajectory_id=trajectory_id,
                    trajectory_geom=trajectory_geom
                )
                
                # 获取汇总信息
                summary = analyzer.get_analysis_summary(analysis_id)
                
                results.append((trajectory_id, analysis_id, summary))
                
            except Exception as e:
                logger.error(f"分析轨迹失败: {trajectory_id}, 错误: {e}")
                results.append((trajectory_id, None, {'error': str(e)}))
        
        logger.info(f"批量分析完成，成功: {len([r for r in results if r[1] is not None])}")
        
    except Exception as e:
        logger.error(f"批量分析失败: {e}")
    
    return results


def create_trajectory_road_analysis_report(
    analysis_id: str,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> str:
    """
    创建轨迹道路分析报告
    
    Args:
        analysis_id: 分析ID
        config: 自定义配置
        
    Returns:
        报告文本
    """
    analyzer = TrajectoryRoadAnalyzer(config)
    summary = analyzer.get_analysis_summary(analysis_id)
    
    report_lines = [
        f"# 轨迹道路分析报告",
        f"",
        f"**分析ID**: {analysis_id}",
        f"**分析时间**: {summary.get('analysis_time', 'N/A')}",
        f"",
        f"## 分析结果汇总",
        f"",
        f"- **Lane统计**:",
    ]
    
    # Lane统计
    lane_types = ['direct_intersect', 'intersection_related', 'road_related', 'chain_forward', 'chain_backward']
    for lane_type in lane_types:
        count = summary.get(lane_type, 0)
        type_name = {
            'direct_intersect': '直接相交',
            'intersection_related': '路口相关',
            'road_related': '道路相关',
            'chain_forward': '前向链路',
            'chain_backward': '后向链路'
        }.get(lane_type, lane_type)
        report_lines.append(f"  - {type_name}: {count} 个")
    
    total_lanes = sum(summary.get(t, 0) for t in lane_types)
    report_lines.append(f"  - **总计**: {total_lanes} 个")
    
    # 其他统计
    report_lines.extend([
        f"",
        f"- **Intersection数量**: {summary.get('intersection_count', 0)} 个",
        f"- **Road数量**: {summary.get('road_count', 0)} 个",
        f"",
        f"## QGIS可视化",
        f"",
        f"可以通过以下视图在QGIS中查看结果:",
    ])
    
    # 导出QGIS视图
    export_info = analyzer.export_results_for_qgis(analysis_id)
    for view_type, view_name in export_info.items():
        report_lines.append(f"- {view_type}: `{view_name}`")
    
    if 'error' in summary:
        report_lines.extend([
            f"",
            f"## 错误信息",
            f"",
            f"```",
            f"{summary['error']}",
            f"```"
        ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    # 简单的测试示例
    import argparse
    
    parser = argparse.ArgumentParser(description='轨迹道路分析模块测试')
    parser.add_argument('--trajectory-id', required=True, help='轨迹ID')
    parser.add_argument('--trajectory-geom', required=True, help='轨迹几何WKT字符串')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 执行分析
        logger.info(f"开始分析轨迹: {args.trajectory_id}")
        
        analysis_id, summary = analyze_trajectory_road_elements(
            trajectory_id=args.trajectory_id,
            trajectory_geom=args.trajectory_geom
        )
        
        # 输出结果
        logger.info(f"分析完成: {analysis_id}")
        
        # 生成报告
        report = create_trajectory_road_analysis_report(analysis_id)
        print(report)
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        exit(1) 