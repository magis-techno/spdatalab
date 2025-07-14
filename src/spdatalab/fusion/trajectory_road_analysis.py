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

from spdatalab.common.io_hive import hive_cursor

logger = logging.getLogger(__name__)

@dataclass
class TrajectoryRoadAnalysisConfig:
    """轨迹道路分析配置"""
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_catalog: str = "rcdatalake_gy1"  # 使用Hive连接器的catalog
    
    # 远程表名配置（Hive数据湖中的表）
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    road_table: str = "full_road"
    roadnextroad_table: str = "roadnextroad"
    intersection_inroad_table: str = "full_intersectiongoinroad"
    intersection_outroad_table: str = "full_intersectiongooutroad"
    
    # 分析参数
    buffer_distance: float = 3.0  # 轨迹膨胀距离(m)
    forward_chain_limit: float = 500.0  # 前向链路扩展限制(m)
    backward_chain_limit: float = 100.0  # 后向链路扩展限制(m)
    max_recursion_depth: int = 10  # 最大递归深度（降低以提高性能）
    
    # 查询限制参数（基于road的新限制）
    max_roads_per_query: int = 100  # 单次查询最大road数量
    max_intersections_per_query: int = 50  # 单次查询最大intersection数量
    max_forward_road_chains: int = 200  # 前向road链路最大数量
    max_backward_road_chains: int = 100  # 后向road链路最大数量
    max_lanes_from_roads: int = 5000  # 从road查找lane的最大数量
    query_timeout: int = 120  # 查询超时时间（秒，增加）
    recursive_query_timeout: int = 180  # 递归查询超时时间（秒，增加）
    
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
        
        # 本地PostgreSQL连接（用于结果存储）
        self.local_engine = create_engine(
            self.config.local_dsn,
            future=True,
            connect_args={"client_encoding": "utf8"},
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10
        )
        
        # 远程数据使用Hive连接器（无需单独的engine）
        logger.info(f"配置远程catalog: {self.config.remote_catalog}")
        
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
        分析轨迹相关的道路元素（基于road的新策略）
        
        Args:
            trajectory_id: 轨迹ID
            trajectory_geom: 轨迹几何WKT字符串
            analysis_id: 分析ID（可选，自动生成）
            
        Returns:
            分析ID
        """
        if not analysis_id:
            analysis_id = f"trajectory_road_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"开始轨迹道路分析（基于road策略）: {analysis_id}")
        
        # 1. 创建轨迹缓冲区
        buffer_geom = self._create_trajectory_buffer(trajectory_geom)
        
        # 2. 保存分析记录
        self._save_analysis_record(analysis_id, trajectory_id, trajectory_geom, buffer_geom)
        
        # 3. 查找相交的road和intersection
        intersecting_roads = self._find_intersecting_roads(analysis_id, buffer_geom)
        intersecting_intersections = self._find_intersecting_intersections(analysis_id, buffer_geom)
        
        # 4. 扩展road链路（前向500m，后向100m）
        road_ids = intersecting_roads['road_id'].tolist() if not intersecting_roads.empty else []
        chain_roads = self._expand_road_chains(analysis_id, road_ids)
        
        # 5. 补齐intersection的inroad和outroad
        intersection_ids = intersecting_intersections['intersection_id'].tolist() if not intersecting_intersections.empty else []
        intersection_roads = self._expand_intersection_roads(analysis_id, intersection_ids)
        
        # 6. 合并所有road
        all_roads = []
        if not intersecting_roads.empty:
            all_roads.append(intersecting_roads)
        if not chain_roads.empty:
            all_roads.append(chain_roads)
        if not intersection_roads.empty:
            all_roads.append(intersection_roads)
        
        if all_roads:
            final_roads = pd.concat(all_roads, ignore_index=True)
            # 去重
            final_roads = final_roads.drop_duplicates(subset=['road_id'])
            
            # 7. 根据所有road查找对应的lane
            final_road_ids = final_roads['road_id'].tolist()
            all_lanes = self._collect_lanes_from_roads(analysis_id, final_road_ids)
            
            # 8. 保存road信息
            self._save_roads_results(analysis_id, final_roads)
            
            logger.info(f"轨迹道路分析完成: {analysis_id}")
            logger.info(f"  - 总roads: {len(final_roads)}")
            logger.info(f"  - 总lanes: {len(all_lanes) if not all_lanes.empty else 0}")
            logger.info(f"  - 总intersections: {len(intersecting_intersections)}")
        else:
            logger.warning(f"未找到任何相关道路元素: {analysis_id}")
        
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
    
    def _find_intersecting_roads(self, analysis_id: str, buffer_geom: str) -> pd.DataFrame:
        """查找与轨迹缓冲区相交的road"""
        
        # 输出buffer几何信息用于调试
        logger.info(f"=== 查找相交road调试信息 ===")
        logger.info(f"Analysis ID: {analysis_id}")
        logger.info(f"Buffer几何长度: {len(buffer_geom)} 字符")
        logger.info(f"Buffer几何前100字符: {buffer_geom[:100]}...")
        logger.info(f"Buffer几何后100字符: ...{buffer_geom[-100:]}")
        
# SQL将在hive_cursor中直接构建
        
        # 输出可以在DataGrip中直接使用的SQL
        logger.info(f"=== DataGrip可执行SQL ===")
        datagrip_sql = f"""
-- 查找相交road
SELECT 
    id as road_id,
    ST_AsText(wkb_geometry) as geometry_wkt
FROM {self.config.road_table}
WHERE ST_Intersects(
    ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326),
    wkb_geometry
)
AND wkb_geometry IS NOT NULL
LIMIT {self.config.max_roads_per_query};
"""
        logger.info(datagrip_sql)
        
        try:
            # 使用hive_cursor连接远程数据湖
            logger.info(f"连接远程catalog: {self.config.remote_catalog}")
            
            with hive_cursor(self.config.remote_catalog) as cur:
                logger.info("成功建立Hive连接")
                
                # 检查road表基本信息
                logger.info("检查road表基本信息")
                table_check_sql = f"""
                    SELECT COUNT(*) as total_roads
                    FROM {self.config.road_table}
                    LIMIT 1
                """
                
                logger.info("执行表信息查询...")
                cur.execute(table_check_sql)
                table_result = cur.fetchone()
                if table_result:
                    logger.info(f"表总road数: {table_result[0]}")
                
                # 先测试简化查询
                logger.info("执行简化road查询测试...")
                simple_road_sql = f"""
                    SELECT 
                        id as road_id,
                        ST_AsText(wkb_geometry) as geometry_wkt
                    FROM {self.config.road_table}
                    WHERE ST_Intersects(
                        ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326),
                        wkb_geometry
                    )
                    AND wkb_geometry IS NOT NULL
                    LIMIT 5
                """
                
                logger.info("开始执行简化查询...")
                cur.execute(simple_road_sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                
                logger.info(f"简化查询返回 {len(rows)} 行结果")
                
                if len(rows) > 0:
                    # 如果简化查询成功，执行完整查询
                    logger.info("简化查询成功，执行完整查询...")
                    
                    # 构建完整查询SQL
                    full_road_sql = f"""
                        SELECT 
                            id as road_id,
                            ST_AsText(wkb_geometry) as geometry_wkt
                        FROM {self.config.road_table}
                        WHERE ST_Intersects(
                            ST_SetSRID(ST_GeomFromText('{buffer_geom}'), 4326),
                            wkb_geometry
                        )
                        AND wkb_geometry IS NOT NULL
                        LIMIT {self.config.max_roads_per_query}
                    """
                    
                    cur.execute(full_road_sql)
                    all_rows = cur.fetchall()
                    all_columns = [d[0] for d in cur.description]
                    
                    # 手动创建DataFrame
                    roads_df = pd.DataFrame(all_rows, columns=all_columns)
                    logger.info(f"完整查询返回 {len(roads_df)} 行结果")
                else:
                    logger.warning("简化查询没有返回结果")
                    roads_df = pd.DataFrame()
            
            if not roads_df.empty:
                logger.info(f"✓ 找到 {len(roads_df)} 个相交road")
                
                # 输出前几个结果用于验证
                logger.info("前3个结果:")
                for i, row in roads_df.head(3).iterrows():
                    logger.info(f"  Road {row['road_id']}")
            else:
                logger.info("未找到相交的road")
            
            return roads_df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            if "statement timeout" in error_msg or "timeout" in error_msg:
                logger.error(f"查询超时错误: {e}")
                logger.error("建议：1. 检查网络连接 2. 增加超时时间 3. 简化查询条件")
            elif "connection" in error_msg:
                logger.error(f"数据库连接错误: {e}")
                logger.error("建议：检查数据库连接配置和网络")
            else:
                logger.error(f"查找相交road失败: {e}")
                logger.error(f"错误类型: {type(e).__name__}")
                
            # 输出详细错误信息以便调试
            import traceback
            logger.error(f"详细错误堆栈: {traceback.format_exc()}")
            
            # 输出当前配置用于调试
            logger.error(f"当前配置: road_table={self.config.road_table}, max_roads={self.config.max_roads_per_query}")
            logger.error(f"Buffer几何长度: {len(buffer_geom) if buffer_geom else 'None'}")
            
            return pd.DataFrame()
    
    def _find_intersecting_intersections(self, analysis_id: str, buffer_geom: str) -> pd.DataFrame:
        """查找与轨迹缓冲区相交的intersection"""
        
        logger.info(f"=== 查找相交intersection调试信息 ===")
        
# SQL将在hive_cursor中直接构建
        
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
            with hive_cursor(self.config.remote_catalog) as cur:
                logger.info("检查intersection表基本信息")
                table_check_sql = f"""
                    SELECT COUNT(*) as total_intersections
                    FROM {self.config.intersection_table}
                    LIMIT 1
                """
                
                cur.execute(table_check_sql)
                table_result = cur.fetchone()
                if table_result:
                    logger.info(f"表总intersection数: {table_result[0]}")
                
                # 构建intersection查询SQL
                full_intersection_sql = f"""
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
                    LIMIT {self.config.max_intersections_per_query}
                """
                
                cur.execute(full_intersection_sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                intersections_df = pd.DataFrame(rows, columns=columns)
            
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

    def _expand_road_chains(self, analysis_id: str, road_ids: List[int]) -> pd.DataFrame:
        """扩展road链路（前向500m，后向100m）"""
        if not road_ids:
            return pd.DataFrame()
        
        logger.info(f"开始扩展road链路，起始road数: {len(road_ids)}")
        
        # 前向扩展
        forward_roads = self._expand_forward_road_chains(analysis_id, road_ids)
        
        # 后向扩展
        backward_roads = self._expand_backward_road_chains(analysis_id, road_ids)
        
        # 合并结果
        all_chain_roads = []
        if not forward_roads.empty:
            all_chain_roads.append(forward_roads)
        if not backward_roads.empty:
            all_chain_roads.append(backward_roads)
        
        if all_chain_roads:
            result = pd.concat(all_chain_roads, ignore_index=True)
            # 去重
            result = result.drop_duplicates(subset=['road_id'])
            logger.info(f"扩展road链路完成: {len(result)} 个")
            return result
        
        return pd.DataFrame()
    
    def _expand_forward_road_chains(self, analysis_id: str, road_ids: List[int]) -> pd.DataFrame:
        """向前扩展road链路（不超过500m）"""
# SQL将在hive_cursor中直接构建
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 构建SQL语句，使用字符串格式化替换参数
                road_ids_str = ','.join(map(str, road_ids))
                formatted_sql = f"""
                    WITH RECURSIVE road_chain AS (
                        -- 基础查询：当前road作为起点
                        SELECT 
                            rnr.roadid,
                            rnr.nextroadid,
                            0 as depth,
                            0 as distance,
                            COALESCE(rnr.length, 0) as current_length
                        FROM {self.config.roadnextroad_table} rnr
                        WHERE rnr.roadid IN ({road_ids_str})
                        AND rnr.nextroadid IS NOT NULL
                        
                        UNION ALL
                        
                        -- 递归查询：继续向前
                        SELECT 
                            rnr.roadid,
                            rnr.nextroadid,
                            rc.depth + 1,
                            rc.distance + rc.current_length,
                            COALESCE(rnr.length, 0) as current_length
                        FROM {self.config.roadnextroad_table} rnr
                        JOIN road_chain rc ON rnr.roadid = rc.nextroadid
                        WHERE rc.distance + rc.current_length <= {self.config.forward_chain_limit}
                        AND rc.depth < {self.config.max_recursion_depth}
                        AND rnr.nextroadid IS NOT NULL
                    )
                    SELECT DISTINCT 
                        rc.nextroadid as road_id,
                        rc.depth as chain_depth,
                        rc.distance,
                        'forward' as chain_direction
                    FROM road_chain rc
                    WHERE rc.nextroadid IS NOT NULL
                    LIMIT {self.config.max_forward_road_chains}
                """
                
                cur.execute(formatted_sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                forward_df = pd.DataFrame(rows, columns=columns)
            
            if not forward_df.empty:
                logger.info(f"向前扩展road链路: {len(forward_df)} 个")
            else:
                logger.info("未找到向前扩展的road链路")
            
            return forward_df
        except Exception as e:
            logger.error(f"向前扩展road链路失败: {e}")
            return pd.DataFrame()
    
    def _expand_backward_road_chains(self, analysis_id: str, road_ids: List[int]) -> pd.DataFrame:
        """向后扩展road链路（不超过100m）"""
# SQL将在hive_cursor中直接构建
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                # 构建SQL语句，使用字符串格式化替换参数
                road_ids_str = ','.join(map(str, road_ids))
                formatted_sql = f"""
                    WITH RECURSIVE road_chain AS (
                        -- 基础查询：当前road作为终点
                        SELECT 
                            rnr.roadid,
                            rnr.nextroadid,
                            0 as depth,
                            0 as distance,
                            COALESCE(rnr.length, 0) as current_length
                        FROM {self.config.roadnextroad_table} rnr
                        WHERE rnr.nextroadid IN ({road_ids_str})
                        AND rnr.roadid IS NOT NULL
                        
                        UNION ALL
                        
                        -- 递归查询：继续向后
                        SELECT 
                            rnr.roadid,
                            rnr.nextroadid,
                            rc.depth + 1,
                            rc.distance + rc.current_length,
                            COALESCE(rnr.length, 0) as current_length
                        FROM {self.config.roadnextroad_table} rnr
                        JOIN road_chain rc ON rnr.nextroadid = rc.roadid
                        WHERE rc.distance + rc.current_length <= {self.config.backward_chain_limit}
                        AND rc.depth < {self.config.max_recursion_depth}
                        AND rnr.roadid IS NOT NULL
                    )
                    SELECT DISTINCT 
                        rc.roadid as road_id,
                        rc.depth as chain_depth,
                        rc.distance,
                        'backward' as chain_direction
                    FROM road_chain rc
                    WHERE rc.roadid IS NOT NULL
                    LIMIT {self.config.max_backward_road_chains}
                """
                
                cur.execute(formatted_sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                backward_df = pd.DataFrame(rows, columns=columns)
            
            if not backward_df.empty:
                logger.info(f"向后扩展road链路: {len(backward_df)} 个")
            else:
                logger.info("未找到向后扩展的road链路")
            
            return backward_df
        except Exception as e:
            logger.error(f"向后扩展road链路失败: {e}")
            return pd.DataFrame()
    
    def _expand_intersection_roads(self, analysis_id: str, intersection_ids: List[int]) -> pd.DataFrame:
        """补齐intersection的inroad和outroad"""
        if not intersection_ids:
            return pd.DataFrame()
        
        logger.info(f"开始补齐intersection的inroad/outroad，intersection数: {len(intersection_ids)}")
        
# SQL将在hive_cursor中直接构建
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                intersection_ids_str = ','.join(map(str, intersection_ids))
                
                # 查询inroad
                inroad_formatted_sql = f"""
                    SELECT 
                        igr.roadid as road_id,
                        igr.intersectionid,
                        'intersection_inroad' as road_type
                    FROM {self.config.intersection_inroad_table} igr
                    WHERE igr.intersectionid IN ({intersection_ids_str})
                    LIMIT 200
                """
                
                cur.execute(inroad_formatted_sql)
                inroad_rows = cur.fetchall()
                inroad_columns = [d[0] for d in cur.description]
                inroads_df = pd.DataFrame(inroad_rows, columns=inroad_columns)
                
                # 查询outroad
                outroad_formatted_sql = f"""
                    SELECT 
                        ior.roadid as road_id,
                        ior.intersectionid,
                        'intersection_outroad' as road_type
                    FROM {self.config.intersection_outroad_table} ior
                    WHERE ior.intersectionid IN ({intersection_ids_str})
                    LIMIT 200
                """
                
                cur.execute(outroad_formatted_sql)
                outroad_rows = cur.fetchall()
                outroad_columns = [d[0] for d in cur.description]
                outroads_df = pd.DataFrame(outroad_rows, columns=outroad_columns)
            
            # 合并结果
            all_roads = []
            if not inroads_df.empty:
                all_roads.append(inroads_df)
                logger.info(f"找到intersection inroad: {len(inroads_df)} 个")
            if not outroads_df.empty:
                all_roads.append(outroads_df)
                logger.info(f"找到intersection outroad: {len(outroads_df)} 个")
            
            if all_roads:
                result = pd.concat(all_roads, ignore_index=True)
                # 去重
                result = result.drop_duplicates(subset=['road_id'])
                logger.info(f"补齐intersection roads完成: {len(result)} 个")
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"补齐intersection roads失败: {e}")
            return pd.DataFrame()
    
    def _collect_lanes_from_roads(self, analysis_id: str, road_ids: List[int]) -> pd.DataFrame:
        """根据road_id收集所有对应的lane"""
        if not road_ids:
            return pd.DataFrame()
        
        logger.info(f"开始根据road收集lane，road数: {len(road_ids)}")
        
# SQL将在hive_cursor中直接构建
        
        try:
            with hive_cursor(self.config.remote_catalog) as cur:
                road_ids_str = ','.join(map(str, road_ids))
                
                formatted_sql = f"""
                    SELECT 
                        l.id as lane_id,
                        l.roadid as road_id,
                        ST_AsText(l.wkb_geometry) as geometry_wkt,
                        l.intersectionid,
                        l.isintersectioninlane,
                        l.isintersectionoutlane
                    FROM {self.config.lane_table} l
                    WHERE l.roadid IN ({road_ids_str})
                    AND l.wkb_geometry IS NOT NULL
                    LIMIT {self.config.max_lanes_from_roads}
                """
                
                cur.execute(formatted_sql)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                lanes_df = pd.DataFrame(rows, columns=columns)
            
            if not lanes_df.empty:
                # 保存到结果表
                self._save_lanes_results(analysis_id, lanes_df, 'road_based')
                logger.info(f"✓ 根据road收集lane: {len(lanes_df)} 个")
            else:
                logger.info("未找到任何lane")
            
            return lanes_df
            
        except Exception as e:
            logger.error(f"根据road收集lane失败: {e}")
            return pd.DataFrame()
    
    def _save_roads_results(self, analysis_id: str, roads_df: pd.DataFrame):
        """保存road结果到数据库"""
        if roads_df.empty:
            return
        
        # 统计每个road（如果有多条记录，按road_id分组计数）
        road_counts = roads_df.groupby('road_id').size().reset_index(name='occurrence_count')
        
        # 保存road信息
        for _, row in road_counts.iterrows():
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
                        'lane_count': int(row['occurrence_count'])  # 暂时用occurrence_count，后续会被实际lane数覆盖
                    })
                    conn.commit()
            except Exception as e:
                logger.error(f"保存road信息失败: {e}")
        
        logger.info(f"保存road信息: {len(road_counts)} 个road")

    # 注意：以下旧方法已被基于road的新策略替代
    # _expand_intersection_lanes, _expand_road_lanes, _expand_lane_chains 等方法已删除
    
    def get_analysis_summary(self, analysis_id: str) -> Dict[str, Any]:
        """获取分析汇总信息（基于road策略）"""
        summary = {'analysis_id': analysis_id}
        
        try:
            # 统计lane数量（基于road策略的lane都是road_based类型）
            lane_count_sql = text(f"""
                SELECT COUNT(*) as total_lanes
                FROM {self.config.lanes_table}
                WHERE analysis_id = :analysis_id
            """)
            
            with self.local_engine.connect() as conn:
                result = conn.execute(lane_count_sql, {'analysis_id': analysis_id}).fetchone()
                summary['total_lanes'] = result[0] if result else 0
            
            # 统计intersection数量
            intersection_count_sql = text(f"""
                SELECT COUNT(*) as total_intersections
                FROM {self.config.intersections_table}
                WHERE analysis_id = :analysis_id
            """)
            
            with self.local_engine.connect() as conn:
                result = conn.execute(intersection_count_sql, {'analysis_id': analysis_id}).fetchone()
                summary['total_intersections'] = result[0] if result else 0
            
            # 统计road数量和详细信息
            road_stats_sql = text(f"""
                SELECT 
                    COUNT(*) as total_roads,
                    SUM(lane_count) as total_lane_count_from_roads
                FROM {self.config.roads_table}
                WHERE analysis_id = :analysis_id
            """)
            
            with self.local_engine.connect() as conn:
                result = conn.execute(road_stats_sql, {'analysis_id': analysis_id}).fetchone()
                if result:
                    summary['total_roads'] = result[0] if result[0] else 0
                    summary['total_lane_count_from_roads'] = result[1] if result[1] else 0
                else:
                    summary['total_roads'] = 0
                    summary['total_lane_count_from_roads'] = 0
            
            summary['analysis_time'] = datetime.now().isoformat()
            summary['strategy'] = 'road_based'  # 标识使用的策略
            
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
        f"- **Intersection数量**: {summary.get('total_intersections', 0)} 个",
        f"- **Road数量**: {summary.get('total_roads', 0)} 个",
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


def batch_analyze_trajectories_from_geojson(
    geojson_file: str,
    batch_analysis_id: Optional[str] = None,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    从GeoJSON文件批量分析轨迹道路元素
    
    Args:
        geojson_file: GeoJSON文件路径
        batch_analysis_id: 批量分析ID（可选，自动生成）
        config: 自定义配置
        
    Returns:
        分析结果列表 [(trajectory_id, analysis_id, summary), ...]
    """
    from .geojson_utils import load_trajectories_from_geojson
    
    if not batch_analysis_id:
        batch_analysis_id = f"batch_road_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"开始批量轨迹道路分析: {batch_analysis_id}")
    logger.info(f"GeoJSON文件: {geojson_file}")
    
    # 加载轨迹记录
    try:
        trajectories = load_trajectories_from_geojson(geojson_file)
        if not trajectories:
            logger.warning("未加载到任何轨迹记录")
            return []
        
        logger.info(f"加载到 {len(trajectories)} 条轨迹记录")
    except Exception as e:
        logger.error(f"加载GeoJSON文件失败: {e}")
        return []
    
    # 批量分析
    analyzer = TrajectoryRoadAnalyzer(config or TrajectoryRoadAnalysisConfig())
    results = []
    
    for i, trajectory in enumerate(trajectories):
        try:
            logger.info(f"分析轨迹 [{i+1}/{len(trajectories)}]: {trajectory.scene_id}")
            
            # 生成分析ID
            analysis_id = f"{batch_analysis_id}_{trajectory.scene_id}"
            
            # 执行道路分析
            result_analysis_id = analyzer.analyze_trajectory_roads(
                trajectory_id=trajectory.scene_id,
                trajectory_geom=trajectory.geometry_wkt,
                analysis_id=analysis_id
            )
            
            # 获取汇总信息
            summary = analyzer.get_analysis_summary(result_analysis_id)
            summary['data_name'] = trajectory.data_name
            summary['properties'] = trajectory.properties
            # **关键修复**：保存输入轨迹几何信息供后续车道分析使用
            summary['input_trajectory_geom'] = trajectory.geometry_wkt
            
            results.append((trajectory.scene_id, result_analysis_id, summary))
            
            logger.info(f"✓ 完成轨迹分析: {trajectory.scene_id}")
            
        except Exception as e:
            logger.error(f"分析轨迹失败: {trajectory.scene_id}, 错误: {e}")
            results.append((trajectory.scene_id, None, {
                'error': str(e),
                'data_name': trajectory.data_name,
                'properties': trajectory.properties,
                'input_trajectory_geom': trajectory.geometry_wkt  # 即使失败也保存几何信息
            }))
    
    # 统计结果
    successful_count = len([r for r in results if r[1] is not None])
    failed_count = len(results) - successful_count
    
    logger.info(f"批量轨迹道路分析完成: {batch_analysis_id}")
    logger.info(f"  - 成功: {successful_count}")
    logger.info(f"  - 失败: {failed_count}")
    logger.info(f"  - 总计: {len(results)}")
    
    return results


def batch_analyze_trajectories_from_records(
    trajectory_records: List,
    batch_analysis_id: Optional[str] = None,
    config: Optional[TrajectoryRoadAnalysisConfig] = None
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    从轨迹记录列表批量分析轨迹道路元素
    
    Args:
        trajectory_records: 轨迹记录列表 (TrajectoryRecord对象)
        batch_analysis_id: 批量分析ID（可选，自动生成）
        config: 自定义配置
        
    Returns:
        分析结果列表 [(trajectory_id, analysis_id, summary), ...]
    """
    if not batch_analysis_id:
        batch_analysis_id = f"batch_road_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"开始批量轨迹道路分析: {batch_analysis_id}")
    logger.info(f"轨迹记录数: {len(trajectory_records)}")
    
    # 批量分析
    analyzer = TrajectoryRoadAnalyzer(config or TrajectoryRoadAnalysisConfig())
    results = []
    
    for i, trajectory in enumerate(trajectory_records):
        try:
            logger.info(f"分析轨迹 [{i+1}/{len(trajectory_records)}]: {trajectory.scene_id}")
            
            # 生成分析ID
            analysis_id = f"{batch_analysis_id}_{trajectory.scene_id}"
            
            # 执行道路分析
            result_analysis_id = analyzer.analyze_trajectory_roads(
                trajectory_id=trajectory.scene_id,
                trajectory_geom=trajectory.geometry_wkt,
                analysis_id=analysis_id
            )
            
            # 获取汇总信息
            summary = analyzer.get_analysis_summary(result_analysis_id)
            summary['data_name'] = trajectory.data_name
            summary['properties'] = trajectory.properties
            summary['input_trajectory_geom'] = trajectory.geometry_wkt
            
            results.append((trajectory.scene_id, result_analysis_id, summary))
            
            logger.info(f"✓ 完成轨迹分析: {trajectory.scene_id}")
            
        except Exception as e:
            logger.error(f"分析轨迹失败: {trajectory.scene_id}, 错误: {e}")
            results.append((trajectory.scene_id, None, {
                'error': str(e),
                'data_name': trajectory.data_name,
                'properties': trajectory.properties,
                'input_trajectory_geom': trajectory.geometry_wkt
            }))
    
    # 统计结果
    successful_count = len([r for r in results if r[1] is not None])
    failed_count = len(results) - successful_count
    
    logger.info(f"批量轨迹道路分析完成: {batch_analysis_id}")
    logger.info(f"  - 成功: {successful_count}")
    logger.info(f"  - 失败: {failed_count}")
    logger.info(f"  - 总计: {len(results)}")
    
    return results


def create_batch_road_analysis_report(
    results: List[Tuple[str, str, Dict[str, Any]]],
    batch_analysis_id: str
) -> str:
    """
    创建批量轨迹道路分析报告
    
    Args:
        results: 批量分析结果
        batch_analysis_id: 批量分析ID
        
    Returns:
        报告文本
    """
    successful_results = [r for r in results if r[1] is not None]
    failed_results = [r for r in results if r[1] is None]
    
    report_lines = [
        f"# 批量轨迹道路分析报告",
        f"",
        f"**批量分析ID**: {batch_analysis_id}",
        f"**分析时间**: {datetime.now().isoformat()}",
        f"",
        f"## 总体统计",
        f"",
        f"- **总轨迹数**: {len(results)}",
        f"- **成功分析**: {len(successful_results)}",
        f"- **失败分析**: {len(failed_results)}",
        f"- **成功率**: {len(successful_results)/len(results)*100:.1f}%",
        f"",
    ]
    
    if successful_results:
        # 成功分析统计
        total_lanes = sum(r[2].get('total_lanes', 0) for r in successful_results)
        total_intersections = sum(r[2].get('total_intersections', 0) for r in successful_results)
        total_roads = sum(r[2].get('total_roads', 0) for r in successful_results)
        
        report_lines.extend([
            f"## 成功分析汇总",
            f"",
            f"- **总Lane数**: {total_lanes}",
            f"- **总Intersection数**: {total_intersections}",
            f"- **总Road数**: {total_roads}",
            f"- **平均Lane数/轨迹**: {total_lanes/len(successful_results):.1f}",
            f"",
        ])
        
        # 成功分析详情
        report_lines.extend([
            f"## 成功分析详情",
            f"",
            f"| 轨迹ID | 分析ID | Lane数 | Intersection数 | Road数 |",
            f"|--------|--------|-------|---------------|-------|",
        ])
        
        for trajectory_id, analysis_id, summary in successful_results[:10]:  # 只显示前10个
            lanes = summary.get('total_lanes', 0)
            intersections = summary.get('total_intersections', 0)
            roads = summary.get('total_roads', 0)
            report_lines.append(f"| {trajectory_id} | {analysis_id} | {lanes} | {intersections} | {roads} |")
        
        if len(successful_results) > 10:
            report_lines.append(f"| ... | ... | ... | ... | ... |")
            report_lines.append(f"| (共{len(successful_results)}个成功分析) | | | | |")
    
    if failed_results:
        # 失败分析详情
        report_lines.extend([
            f"",
            f"## 失败分析详情",
            f"",
        ])
        
        for trajectory_id, _, summary in failed_results:
            error_msg = summary.get('error', '未知错误')
            report_lines.append(f"- **{trajectory_id}**: {error_msg}")
    
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