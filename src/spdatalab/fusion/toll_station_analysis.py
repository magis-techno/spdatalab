"""
收费站数据分析模块

功能：
1. 直接查找intersectiontype=2的收费站数据（不依赖bbox）
2. 基于收费站范围查询轨迹数据
3. 按dataset_name对轨迹数据进行聚合分析
4. 支持独立的收费站空间分析
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
class TollStationAnalysisConfig:
    """收费站分析配置"""
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_dsn: str = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"
    trajectory_dsn: str = "postgresql+psycopg://**:**@10.170.30.193:9001/dataset_gy1"
    # 收费站专用表配置
    toll_station_table: str = "toll_station_analysis"
    trajectory_results_table: str = "toll_station_trajectories"
    # 分析参数
    toll_station_type: int = 2  # intersectiontype = 2 对应收费站
    buffer_distance_meters: float = 100.0  # 收费站缓冲区距离
    max_trajectory_records: int = 10000  # 单次查询轨迹数据的最大记录数
    # 远程表名配置
    intersection_table: str = "full_intersection"  # 远程intersection表名
    trajectory_table: str = "public.ddi_data_points"  # 远程轨迹表名

class TollStationAnalyzer:
    """
    收费站数据分析器
    
    主要功能：
    1. 直接查找收费站数据（intersectiontype=2），不依赖bbox
    2. 基于收费站几何范围查询轨迹数据
    3. 对轨迹数据按dataset_name进行聚合分析
    """
    
    def __init__(self, config: Optional[TollStationAnalysisConfig] = None):
        self.config = config or TollStationAnalysisConfig()
        self.local_engine = create_engine(
            self.config.local_dsn,
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        self.remote_engine = create_engine(
            self.config.remote_dsn,
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        self.trajectory_engine = create_engine(
            self.config.trajectory_dsn,
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        # 初始化分析表
        self._init_analysis_tables()
    
    def _init_analysis_tables(self):
        """初始化收费站分析相关的表"""
        self._init_toll_station_table()
        self._init_trajectory_results_table()
    
    def _init_toll_station_table(self):
        """初始化收费站数据表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.toll_station_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                intersection_id BIGINT NOT NULL,
                intersectiontype INTEGER,
                intersectionsubtype INTEGER,
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, intersection_id)
            )
        """)
        
        # 创建索引
        create_indexes_sql = [
            text(f"CREATE INDEX IF NOT EXISTS idx_{self.config.toll_station_table}_analysis_id ON {self.config.toll_station_table} (analysis_id)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_{self.config.toll_station_table}_intersection_id ON {self.config.toll_station_table} (intersection_id)")
        ]
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                for index_sql in create_indexes_sql:
                    try:
                        conn.execute(index_sql)
                    except Exception as idx_e:
                        logger.warning(f"索引创建失败: {idx_e}")
                conn.commit()
            logger.info(f"收费站表 {self.config.toll_station_table} 初始化完成")
        except Exception as e:
            logger.warning(f"收费站表初始化失败: {e}")
    
    def _init_trajectory_results_table(self):
        """初始化轨迹分析结果表"""
        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {self.config.trajectory_results_table} (
                id SERIAL PRIMARY KEY,
                analysis_id VARCHAR(100) NOT NULL,
                toll_station_id BIGINT NOT NULL,
                dataset_name VARCHAR(255) NOT NULL,
                trajectory_count INTEGER NOT NULL,
                point_count INTEGER NOT NULL,
                min_timestamp BIGINT,
                max_timestamp BIGINT,
                workstage_2_count INTEGER DEFAULT 0,
                workstage_2_ratio FLOAT DEFAULT 0.0,
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(analysis_id, toll_station_id, dataset_name)
            )
        """)
        
        # 创建索引
        create_indexes_sql = [
            text(f"CREATE INDEX IF NOT EXISTS idx_{self.config.trajectory_results_table}_analysis_id ON {self.config.trajectory_results_table} (analysis_id)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_{self.config.trajectory_results_table}_toll_station_id ON {self.config.trajectory_results_table} (toll_station_id)"),
            text(f"CREATE INDEX IF NOT EXISTS idx_{self.config.trajectory_results_table}_dataset_name ON {self.config.trajectory_results_table} (dataset_name)")
        ]
        
        try:
            with self.local_engine.connect() as conn:
                conn.execute(create_table_sql)
                for index_sql in create_indexes_sql:
                    try:
                        conn.execute(index_sql)
                    except Exception as idx_e:
                        logger.warning(f"索引创建失败: {idx_e}")
                conn.commit()
            logger.info(f"轨迹结果表 {self.config.trajectory_results_table} 初始化完成")
        except Exception as e:
            logger.warning(f"轨迹结果表初始化失败: {e}")
    
    def find_toll_stations(
        self,
        limit: Optional[int] = None,
        analysis_id: Optional[str] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        直接查找收费站数据（intersectiontype=2）
        
        Args:
            limit: 限制查找的收费站数量（可选）
            analysis_id: 分析ID（可选，自动生成）
            
        Returns:
            (收费站数据DataFrame, 分析ID)
        """
        if not analysis_id:
            analysis_id = f"toll_station_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"开始查找收费站数据: {analysis_id}")
        
        # 直接从intersection表查找收费站
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        toll_station_sql = text(f"""
            SELECT 
                id as intersection_id,
                intersectiontype,
                intersectionsubtype,
                ST_AsText(wkb_geometry) as intersection_geometry
            FROM {self.config.intersection_table}
            WHERE intersectiontype = {self.config.toll_station_type}
            AND wkb_geometry IS NOT NULL
            ORDER BY id
            {limit_clause}
        """)
        
        try:
            with self.remote_engine.connect() as conn:
                toll_stations_df = pd.read_sql(toll_station_sql, conn)
            
            if toll_stations_df.empty:
                logger.warning("未找到收费站数据")
                return toll_stations_df, analysis_id
            
            logger.info(f"找到 {len(toll_stations_df)} 个收费站")
            
            # 保存收费站数据到分析表
            self._save_toll_stations(toll_stations_df, analysis_id)
            
            return toll_stations_df, analysis_id
            
        except Exception as e:
            logger.error(f"查找收费站失败: {e}")
            return pd.DataFrame(), analysis_id
    
    def _save_toll_stations(self, toll_stations_df: pd.DataFrame, analysis_id: str):
        """保存收费站数据到分析表"""
        if toll_stations_df.empty:
            return
        
        # 准备保存记录，直接使用原始几何
        save_records = []
        for _, row in toll_stations_df.iterrows():
            # 直接使用WKT几何字符串，让PostgreSQL转换为几何类型
            geometry_wkt = row.get('intersection_geometry')
            
            if geometry_wkt:
                record = {
                    'analysis_id': analysis_id,
                    'intersection_id': int(row['intersection_id']),
                    'intersectiontype': int(row['intersectiontype']) if pd.notna(row['intersectiontype']) else None,
                    'intersectionsubtype': int(row['intersectionsubtype']) if pd.notna(row['intersectionsubtype']) else None,
                    'geometry_wkt': geometry_wkt  # 保存WKT字符串，在SQL中转换
                }
                save_records.append(record)
        
        # 保存到数据库
        if save_records:
            try:
                # 使用SQL INSERT来正确处理几何类型
                for record in save_records:
                    insert_sql = text(f"""
                        INSERT INTO {self.config.toll_station_table} 
                        (analysis_id, intersection_id, intersectiontype, intersectionsubtype, geometry)
                        VALUES (
                            :analysis_id, 
                            :intersection_id, 
                            :intersectiontype, 
                            :intersectionsubtype, 
                            ST_SetSRID(ST_GeomFromText(:geometry_wkt), 4326)
                        )
                        ON CONFLICT (analysis_id, intersection_id) DO NOTHING
                    """)
                    
                    with self.local_engine.connect() as conn:
                        conn.execute(insert_sql, record)
                        conn.commit()
                
                logger.info(f"成功保存 {len(save_records)} 个收费站数据")
            except Exception as e:
                logger.error(f"保存收费站数据失败: {e}")
    
    def analyze_trajectories_in_toll_stations(
        self,
        analysis_id: str
    ) -> pd.DataFrame:
        """
        分析收费站范围内的轨迹数据（直接使用原始几何相交）
        当一个轨迹点落入收费站时，保存该轨迹的完整片段
        
        Args:
            analysis_id: 分析ID
            
        Returns:
            轨迹分析结果DataFrame
        """
        logger.info(f"开始分析收费站轨迹数据: {analysis_id}")
        
        # 1. 获取收费站数据
        toll_stations_sql = text(f"""
            SELECT 
                intersection_id,
                ST_AsText(geometry) as geometry_wkt
            FROM {self.config.toll_station_table} 
            WHERE analysis_id = '{analysis_id}' 
            AND geometry IS NOT NULL
        """)
        
        toll_stations = pd.read_sql(toll_stations_sql, self.local_engine)
        logger.info(f"找到 {len(toll_stations)} 个收费站")
        
        if toll_stations.empty:
            logger.warning("没有找到收费站数据")
            return pd.DataFrame()
        
        # 2. 对每个收费站分析轨迹
        all_results = []
        
        for _, toll_station in toll_stations.iterrows():
            toll_station_id = toll_station['intersection_id']
            geometry_wkt = toll_station['geometry_wkt']
            
            logger.info(f"分析收费站 {toll_station_id} 的轨迹数据...")
            
            # 2.1 首先找出与收费站相交的轨迹点
            intersecting_points_sql = text(f"""
                WITH intersecting_points AS (
                    SELECT DISTINCT 
                        scene_token,
                        dataset_name
                    FROM {self.config.trajectory_table}
                    WHERE ST_Intersects(
                        ST_SetSRID(ST_GeomFromText('{geometry_wkt}'), 4326),
                        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                    )
                    LIMIT {self.config.max_trajectory_records}
                )
                SELECT 
                    t.*,
                    ST_AsText(
                        ST_SetSRID(
                            ST_MakeLine(
                                ST_MakePoint(longitude, latitude) ORDER BY timestamp
                            ),
                            4326
                        )
                    ) as trajectory_geometry
                FROM {self.config.trajectory_table} t
                INNER JOIN intersecting_points ip 
                    ON t.scene_token = ip.scene_token 
                    AND t.dataset_name = ip.dataset_name
                ORDER BY t.scene_token, t.timestamp
            """)
            
            try:
                # 使用trajectory_engine查询轨迹数据
                trajectory_data = pd.read_sql(intersecting_points_sql, self.trajectory_engine)
                
                if not trajectory_data.empty:
                    # 按dataset_name和scene_token分组统计
                    grouped = trajectory_data.groupby(['dataset_name', 'scene_token']).agg({
                        'longitude': 'count',  # 轨迹点数量
                        'trajectory_geometry': 'first'  # 获取轨迹线几何
                    }).reset_index()
                    
                    grouped = grouped.rename(columns={'longitude': 'point_count'})
                    grouped['toll_station_id'] = toll_station_id
                    grouped['analysis_id'] = analysis_id
                    
                    all_results.append(grouped)
                    logger.info(f"收费站 {toll_station_id} 找到 {len(grouped)} 个轨迹片段")
                else:
                    logger.info(f"收费站 {toll_station_id} 没有找到相交的轨迹数据")
                    
            except Exception as e:
                logger.error(f"查询收费站 {toll_station_id} 的轨迹数据时出错: {e}")
                continue
        
        if not all_results:
            logger.warning("没有找到任何轨迹数据")
            return pd.DataFrame()
        
        # 3. 合并所有结果
        final_results = pd.concat(all_results, ignore_index=True)
        logger.info(f"总共找到 {len(final_results)} 个轨迹片段")
        
        # 4. 保存结果到数据库
        try:
            final_results.to_sql(
                self.config.trajectory_results_table,
                self.local_engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"成功保存 {len(final_results)} 条轨迹分析结果")
        except Exception as e:
            logger.error(f"保存轨迹分析结果时出错: {e}")
        
        return final_results
    
    def get_analysis_summary(self, analysis_id: str) -> Dict[str, Any]:
        """
        获取分析汇总信息
        
        Args:
            analysis_id: 分析ID
            
        Returns:
            分析汇总字典
        """
        summary = {}
        
        try:
            # 收费站统计
            toll_station_sql = text(f"""
                SELECT 
                    COUNT(*) as total_toll_stations
                FROM {self.config.toll_station_table} 
                WHERE analysis_id = '{analysis_id}'
            """)
            
            with self.local_engine.connect() as conn:
                toll_stats = conn.execute(toll_station_sql).fetchone()
                if toll_stats:
                    summary.update({
                        'total_toll_stations': toll_stats[0]
                    })
            
            # 轨迹统计
            trajectory_sql = text(f"""
                SELECT 
                    COUNT(DISTINCT dataset_name) as unique_datasets,
                    SUM(trajectory_count) as total_trajectories,
                    SUM(point_count) as total_points,
                    AVG(workstage_2_ratio) as avg_workstage_2_ratio
                FROM {self.config.trajectory_results_table} 
                WHERE analysis_id = '{analysis_id}'
            """)
            
            with self.local_engine.connect() as conn:
                traj_stats = conn.execute(trajectory_sql).fetchone()
                if traj_stats:
                    summary.update({
                        'unique_datasets': traj_stats[0] or 0,
                        'total_trajectories': traj_stats[1] or 0,
                        'total_points': traj_stats[2] or 0,
                        'avg_workstage_2_ratio': round(traj_stats[3] or 0, 2)
                    })
            
            summary['analysis_id'] = analysis_id
            summary['analysis_time'] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"获取分析汇总失败: {e}")
            summary = {'error': str(e)}
        
        return summary
    
    def export_results_for_qgis(
        self, 
        analysis_id: str,
        export_toll_stations: bool = True,
        export_trajectories: bool = True
    ) -> Dict[str, str]:
        """
        导出结果供QGIS可视化
        
        Args:
            analysis_id: 分析ID
            export_toll_stations: 是否导出收费站数据
            export_trajectories: 是否导出轨迹统计数据
            
        Returns:
            导出信息字典
        """
        export_info = {}
        
        if export_toll_stations:
            # 导出收费站数据（包含几何信息）
            toll_station_view = f"qgis_toll_stations_{analysis_id.replace('-', '_')}"
            
            create_view_sql = text(f"""
                CREATE OR REPLACE VIEW {toll_station_view} AS
                SELECT 
                    id,
                    analysis_id,
                    intersection_id,
                    intersectiontype,
                    intersectionsubtype,
                    geometry,
                    created_at
                FROM {self.config.toll_station_table}
                WHERE analysis_id = '{analysis_id}'
            """)
            
            try:
                with self.local_engine.connect() as conn:
                    conn.execute(create_view_sql)
                    conn.commit()
                export_info['toll_stations_view'] = toll_station_view
                logger.info(f"创建收费站视图: {toll_station_view}")
            except Exception as e:
                logger.error(f"创建收费站视图失败: {e}")
        
        if export_trajectories:
            # 导出轨迹统计数据视图
            trajectory_view = f"qgis_trajectories_{analysis_id.replace('-', '_')}"

            
            create_view_sql = text(f"""
                CREATE OR REPLACE VIEW {trajectory_view} AS
                SELECT 
                    tr.id,
                    tr.analysis_id,
                    tr.toll_station_id,
                    tr.dataset_name,
                    tr.trajectory_count,
                    tr.point_count,
                    tr.workstage_2_count,
                    tr.workstage_2_ratio,
                    tr.geometry as trajectory_geometry,
                    ts.geometry as toll_station_geometry,
                    tr.created_at
                FROM {self.config.trajectory_results_table} tr
                LEFT JOIN {self.config.toll_station_table} ts 
                    ON tr.analysis_id = ts.analysis_id 
                    AND tr.toll_station_id = ts.intersection_id
                WHERE tr.analysis_id = '{analysis_id}'
            """)
            
            try:
                with self.local_engine.connect() as conn:
                    conn.execute(create_view_sql)
                    conn.commit()
                export_info['trajectories_view'] = trajectory_view
                logger.info(f"创建轨迹统计视图: {trajectory_view}")
            except Exception as e:
                logger.error(f"创建轨迹统计视图失败: {e}")
        
        return export_info


# 便捷接口函数
def analyze_toll_station_trajectories(
    limit: Optional[int] = None,
    config: Optional[TollStationAnalysisConfig] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    一站式收费站轨迹分析（不依赖bbox，直接使用几何相交）
    
    Args:
        limit: 限制分析的收费站数量
        config: 自定义配置
        
    Returns:
        (收费站数据, 轨迹分析结果, 分析ID)
    """
    analyzer = TollStationAnalyzer(config or TollStationAnalysisConfig())
    
    # 1. 查找收费站
    toll_stations, analysis_id = analyzer.find_toll_stations(
        limit=limit,
        analysis_id=None
    )
    
    if toll_stations.empty:
        return toll_stations, pd.DataFrame(), analysis_id
    
    # 2. 分析轨迹数据
    trajectory_results = analyzer.analyze_trajectories_in_toll_stations(
        analysis_id=analysis_id
    )
    
    return toll_stations, trajectory_results, analysis_id


def get_toll_station_analysis_summary(
    analysis_id: str,
    config: Optional[TollStationAnalysisConfig] = None
) -> Dict[str, Any]:
    """
    获取收费站分析汇总信息
    
    Args:
        analysis_id: 分析ID
        config: 自定义配置
        
    Returns:
        分析汇总字典
    """
    analyzer = TollStationAnalyzer(config)
    return analyzer.get_analysis_summary(analysis_id)


def export_toll_station_results_for_qgis(
    analysis_id: str,
    config: Optional[TollStationAnalysisConfig] = None
) -> Dict[str, str]:
    """
    导出收费站分析结果供QGIS可视化
    
    Args:
        analysis_id: 分析ID
        config: 自定义配置
        
    Returns:
        导出信息字典
    """
    analyzer = TollStationAnalyzer(config)
    return analyzer.export_results_for_qgis(analysis_id)