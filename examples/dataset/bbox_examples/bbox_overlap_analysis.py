#!/usr/bin/env python3
"""
BBox叠置分析示例
===============

本示例展示如何对bbox分表数据进行空间叠置分析，找出重叠数量最高的位置。

功能特性：
- 基于统一视图的空间叠置分析
- 重叠热点识别和排序
- QGIS兼容的结果导出
- 支持多种过滤和分组条件

工作流程：
1. 确保bbox统一视图存在
2. 执行空间叠置分析SQL
3. 保存结果到分析结果表
4. 创建QGIS兼容视图
5. 提供可视化指导

使用方法：
    python examples/dataset/bbox_examples/bbox_overlap_analysis.py
    
    # 或指定参数
    python examples/dataset/bbox_examples/bbox_overlap_analysis.py --city beijing --min-overlap-area 0.0001
"""

import sys
import os
import signal
import atexit
from pathlib import Path
import argparse
from datetime import datetime
import pandas as pd
import logging
from typing import Optional, List, Dict, Any

# 添加项目路径，支持多种环境
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 尝试直接导入，如果失败则添加src路径
try:
    from spdatalab.dataset.bbox import (
        create_qgis_compatible_unified_view,
        list_bbox_tables,
        LOCAL_DSN
    )
except ImportError:
    # 如果直接导入失败，尝试添加src路径
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import (
        create_qgis_compatible_unified_view,
        list_bbox_tables,
        LOCAL_DSN
    )

from sqlalchemy import create_engine, text

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BBoxOverlapAnalyzer:
    """BBox叠置分析器"""
    
    def __init__(self, dsn: str = LOCAL_DSN):
        """初始化分析器"""
        self.dsn = dsn
        self.engine = create_engine(dsn, future=True)
        self.analysis_table = "bbox_overlap_analysis_results"
        self.unified_view = "clips_bbox_unified_qgis"
        self.qgis_view = "qgis_bbox_overlap_hotspots"
        
        # 优雅退出控制
        self.shutdown_requested = False
        self.current_connection = None
        self.current_analysis_id = None
        self.analysis_start_time = None
        
        # 注册信号处理器和清理函数
        self._setup_signal_handlers()
        atexit.register(self._cleanup_on_exit)
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            print(f"\n\n🛑 收到退出信号 ({signal.Signals(signum).name})")
            self._initiate_graceful_shutdown()
        
        # 注册常见的退出信号
        try:
            signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
            signal.signal(signal.SIGTERM, signal_handler)  # 终止信号
            if hasattr(signal, 'SIGBREAK'):  # Windows
                signal.signal(signal.SIGBREAK, signal_handler)
        except ValueError:
            # 在某些环境中可能无法注册信号
            logger.warning("无法注册信号处理器")
    
    def _initiate_graceful_shutdown(self):
        """启动优雅退出流程"""
        self.shutdown_requested = True
        print(f"🔄 正在安全退出...")
        
        if self.current_analysis_id:
            print(f"📝 当前分析ID: {self.current_analysis_id}")
            
        if self.analysis_start_time:
            elapsed = datetime.now() - self.analysis_start_time
            print(f"⏱️ 已运行时间: {elapsed}")
        
        print(f"🧹 清理资源中...")
        self._cleanup_resources()
        
        print(f"✅ 优雅退出完成")
        sys.exit(0)
    
    def _cleanup_resources(self):
        """清理资源"""
        try:
            if self.current_connection:
                self.current_connection.close()
                print(f"✅ 数据库连接已关闭")
                self.current_connection = None
        except Exception as e:
            print(f"⚠️ 关闭连接时出错: {e}")
    
    def _cleanup_on_exit(self):
        """程序退出时的清理函数"""
        if not self.shutdown_requested:
            self._cleanup_resources()
    
    def _check_shutdown(self):
        """检查是否需要退出"""
        if self.shutdown_requested:
            print(f"🛑 检测到退出请求，停止执行")
            raise KeyboardInterrupt("用户请求退出")
        
    def ensure_unified_view(self, force_refresh: bool = False) -> bool:
        """确保统一视图存在并且是最新的
        
        Args:
            force_refresh: 是否强制刷新视图
        """
        print("🔍 检查bbox统一视图...")
        
        try:
            with self.engine.connect() as conn:
                # 1. 检查视图是否存在
                check_view_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = '{self.unified_view}'
                    );
                """)
                
                result = conn.execute(check_view_sql)
                view_exists = result.scalar()
                
                # 2. 获取当前分表数量
                current_tables = list_bbox_tables(self.engine)
                bbox_partition_tables = [t for t in current_tables if t.startswith('clips_bbox_') and t != 'clips_bbox']
                current_table_count = len(bbox_partition_tables)
                
                print(f"📋 发现 {current_table_count} 个bbox分表")
                
                # 3. 检查视图是否需要更新
                view_needs_update = False
                
                if not view_exists:
                    print(f"📌 视图 {self.unified_view} 不存在，需要创建")
                    view_needs_update = True
                elif force_refresh:
                    print(f"🔄 强制刷新模式，将重新创建视图")
                    view_needs_update = True
                elif current_table_count == 0:
                    print(f"⚠️ 没有发现bbox分表，无法创建统一视图")
                    return False
                else:
                    # 检查视图的表数量是否匹配当前分表数量
                    try:
                        # 尝试从视图定义中获取表数量（简化检查）
                        check_count_sql = text(f"SELECT COUNT(DISTINCT source_table) FROM {self.unified_view} LIMIT 1;")
                        view_table_result = conn.execute(check_count_sql)
                        view_table_count = view_table_result.scalar()
                        
                        if view_table_count != current_table_count:
                            print(f"🔄 视图包含 {view_table_count} 个表，当前有 {current_table_count} 个分表，需要更新")
                            view_needs_update = True
                    except Exception as e:
                        print(f"⚠️ 检查视图状态失败: {str(e)[:100]}...")
                        print(f"🔄 为安全起见，将重新创建视图")
                        view_needs_update = True
                
                # 4. 创建或更新视图
                if view_needs_update:
                    if current_table_count == 0:
                        print(f"❌ 无法创建视图：没有可用的bbox分表")
                        return False
                    
                    print(f"🛠️ 正在创建/更新统一视图...")
                    success = create_qgis_compatible_unified_view(self.engine, self.unified_view)
                    if not success:
                        print("❌ 创建统一视图失败")
                        return False
                    print(f"✅ 统一视图 {self.unified_view} 创建/更新成功")
                else:
                    print(f"✅ 统一视图 {self.unified_view} 已是最新状态")
                
                # 5. 验证视图数据
                try:
                    count_sql = text(f"SELECT COUNT(*) FROM {self.unified_view};")
                    count_result = conn.execute(count_sql)
                    row_count = count_result.scalar()
                    print(f"📊 统一视图包含 {row_count:,} 条bbox记录")
                    
                    if row_count == 0:
                        print(f"⚠️ 统一视图为空，可能分表中没有数据")
                        return False
                    
                    # 显示数据分布概况
                    sample_sql = text(f"""
                        SELECT 
                            COUNT(DISTINCT subdataset_name) as subdataset_count,
                            COUNT(DISTINCT city_id) as city_count,
                            COUNT(*) FILTER (WHERE all_good = true) as good_quality_count,
                            COUNT(*) FILTER (WHERE all_good = false OR all_good IS NULL) as poor_quality_count,
                            ROUND(100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*), 2) as good_quality_percent,
                            MIN(created_at) as earliest_data,
                            MAX(created_at) as latest_data
                        FROM {self.unified_view} 
                        WHERE city_id IS NOT NULL;
                    """)
                    sample_result = conn.execute(sample_sql).fetchone()
                    if sample_result:
                        print(f"📈 数据概况: {sample_result.subdataset_count} 个子数据集, {sample_result.city_count} 个城市")
                        print(f"📊 质量分布: {sample_result.good_quality_count:,} 合格 ({sample_result.good_quality_percent}%), {sample_result.poor_quality_count:,} 不合格")
                        
                        # 显示按城市的质量分布
                        city_quality_sql = text(f"""
                            SELECT 
                                city_id,
                                COUNT(*) as total_count,
                                COUNT(*) FILTER (WHERE all_good = true) as good_count,
                                ROUND(100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*), 1) as good_percent
                            FROM {self.unified_view} 
                            WHERE city_id IS NOT NULL
                            GROUP BY city_id
                            ORDER BY total_count DESC
                            LIMIT 5;
                        """)
                        city_results = conn.execute(city_quality_sql).fetchall()
                        if city_results:
                            print(f"🏙️ TOP 5城市质量分布:")
                            for city_result in city_results:
                                print(f"   {city_result.city_id}: {city_result.good_count:,}/{city_result.total_count:,} ({city_result.good_percent}%)")
                            print(f"💡 只有all_good=true的数据会参与叠置分析")
                    
                    return True
                    
                except Exception as e:
                    print(f"⚠️ 视图数据验证失败: {str(e)[:100]}...")
                    return False
                
        except Exception as e:
            print(f"❌ 检查统一视图失败: {str(e)}")
            return False
    
    def create_analysis_table(self) -> bool:
        """创建分析结果表"""
        print("🛠️ 创建分析结果表...")
        
        # 读取创建表的SQL脚本
        sql_file = Path(__file__).parent / "sql" / "create_analysis_tables.sql"
        
        try:
            if sql_file.exists():
                # 如果SQL文件存在，使用文件中的SQL
                with open(sql_file, 'r', encoding='utf-8') as f:
                    create_sql = f.read()
            else:
                # 否则使用内置的SQL
                create_sql = f"""
                -- 创建bbox叠置分析结果表
                CREATE TABLE IF NOT EXISTS {self.analysis_table} (
                    id SERIAL PRIMARY KEY,
                    analysis_id VARCHAR(100) NOT NULL,
                    analysis_type VARCHAR(50) DEFAULT 'bbox_overlap',
                    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    hotspot_rank INTEGER,
                    overlap_count INTEGER,
                    total_overlap_area NUMERIC,
                    subdataset_count INTEGER,
                    scene_count INTEGER,
                    involved_subdatasets TEXT[],
                    involved_scenes TEXT[],
                    analysis_params TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- 添加PostGIS几何列
                DO $$
                BEGIN
                    -- 检查几何列是否已存在
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{self.analysis_table}' 
                        AND column_name = 'geometry'
                    ) THEN
                        PERFORM AddGeometryColumn('public', '{self.analysis_table}', 'geometry', 4326, 'GEOMETRY', 2);
                    END IF;
                END $$;

                -- 创建索引
                CREATE INDEX IF NOT EXISTS idx_bbox_overlap_analysis_id ON {self.analysis_table} (analysis_id);
                CREATE INDEX IF NOT EXISTS idx_bbox_overlap_rank ON {self.analysis_table} (hotspot_rank);
                CREATE INDEX IF NOT EXISTS idx_bbox_overlap_count ON {self.analysis_table} (overlap_count);
                CREATE INDEX IF NOT EXISTS idx_bbox_overlap_geom ON {self.analysis_table} USING GIST (geometry);
                """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
                
            print(f"✅ 分析结果表 {self.analysis_table} 创建成功")
            return True
            
        except Exception as e:
            print(f"❌ 创建分析结果表失败: {str(e)}")
            return False
    
    def run_overlap_analysis(
        self, 
        analysis_id: Optional[str] = None,
        city_filter: Optional[str] = None,
        subdataset_filter: Optional[List[str]] = None,
        min_overlap_area: float = 0.0,
        top_n: int = 20,
        intersect_only: bool = False
    ) -> str:
        """执行叠置分析
        
        Args:
            analysis_id: 分析ID，如果为None则自动生成
            city_filter: 城市过滤
            subdataset_filter: 子数据集过滤
            min_overlap_area: 最小重叠面积阈值
            top_n: 返回的热点数量
            
        Returns:
            分析ID
        """
        if not analysis_id:
            analysis_id = f"bbox_overlap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 设置当前分析状态
        self.current_analysis_id = analysis_id
        self.analysis_start_time = datetime.now()
        
        print(f"🚀 开始叠置分析: {analysis_id}")
        print(f"参数: city_filter={city_filter}, min_overlap_area={min_overlap_area}, top_n={top_n}")
        if intersect_only:
            print(f"🎯 简化模式: 只要相交就算重叠（忽略面积阈值）")
        print(f"💡 可以使用 Ctrl+C 安全退出")
        
        # 检查是否需要退出
        self._check_shutdown()
        
        # 构建过滤条件
        where_conditions = []
        
        # 🎯 城市过滤（注意：现在基础WHERE条件已包含a.city_id = b.city_id）
        if city_filter:
            # 城市过滤只需要限制其中一个表即可，因为已经有相同城市约束
            where_conditions.append(f"a.city_id = '{city_filter}'")
            print(f"🏙️ 城市过滤: {city_filter}")
        
        if subdataset_filter:
            subdataset_list = "', '".join(subdataset_filter)
            where_conditions.append(f"a.subdataset_name IN ('{subdataset_list}') AND b.subdataset_name IN ('{subdataset_list}')")
            print(f"📦 子数据集过滤: {len(subdataset_filter)} 个")
        
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        # 读取分析SQL脚本
        sql_file = Path(__file__).parent / "sql" / "overlap_analysis.sql"
        
        try:
            if sql_file.exists():
                # 如果SQL文件存在，读取并替换参数
                with open(sql_file, 'r', encoding='utf-8') as f:
                    analysis_sql_template = f.read()
                
                # 根据intersect_only参数决定是否使用面积阈值
                if intersect_only:
                    # 如果是简化模式，注释掉面积阈值条件
                    analysis_sql_template = analysis_sql_template.replace(
                        "AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}",
                        "-- 🎯 简化模式：忽略面积阈值\n        -- AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}"
                    )
                
                # 替换参数
                analysis_sql = analysis_sql_template.format(
                    unified_view=self.unified_view,
                    analysis_table=self.analysis_table,
                    analysis_id=analysis_id,
                    where_clause=where_clause,
                    min_overlap_area=min_overlap_area,
                    top_n=top_n
                )
            else:
                # 内置SQL
                analysis_sql = f"""
                WITH overlapping_areas AS (
                    SELECT 
                        a.qgis_id as bbox_a_id,
                        b.qgis_id as bbox_b_id,
                        a.subdataset_name as subdataset_a,
                        b.subdataset_name as subdataset_b,
                        a.scene_token as scene_a,
                        b.scene_token as scene_b,
                        ST_Intersection(a.geometry, b.geometry) as overlap_geometry,
                        ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
                    FROM {self.unified_view} a
                    JOIN {self.unified_view} b ON a.qgis_id < b.qgis_id
                    WHERE ST_Intersects(a.geometry, b.geometry)
                    {"" if intersect_only else f"AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}"}
                    AND NOT ST_Equals(a.geometry, b.geometry)
                    -- 🎯 只分析相同城市的bbox
                    AND a.city_id = b.city_id
                    AND a.city_id IS NOT NULL
                    -- 🎯 只分析质量合格的数据
                    AND a.all_good = true
                    AND b.all_good = true
                    {where_clause}
                ),
                overlap_hotspots AS (
                    SELECT 
                        ST_Union(overlap_geometry) as hotspot_geometry,
                        COUNT(*) as overlap_count,
                        ARRAY_AGG(DISTINCT subdataset_a) || ARRAY_AGG(DISTINCT subdataset_b) as involved_subdatasets,
                        ARRAY_AGG(DISTINCT scene_a) || ARRAY_AGG(DISTINCT scene_b) as involved_scenes,
                        SUM(overlap_area) as total_overlap_area
                    FROM overlapping_areas
                    GROUP BY ST_SnapToGrid(overlap_geometry, 0.001)
                    HAVING COUNT(*) >= 2
                )
                INSERT INTO {self.analysis_table} 
                (analysis_id, hotspot_rank, overlap_count, total_overlap_area, 
                 subdataset_count, scene_count, involved_subdatasets, involved_scenes, geometry, analysis_params)
                SELECT 
                    '{analysis_id}' as analysis_id,
                    ROW_NUMBER() OVER (ORDER BY overlap_count DESC) as hotspot_rank,
                    overlap_count,
                    total_overlap_area,
                    ARRAY_LENGTH(involved_subdatasets, 1) as subdataset_count,
                    ARRAY_LENGTH(involved_scenes, 1) as scene_count,
                    involved_subdatasets,
                    involved_scenes,
                    hotspot_geometry as geometry,
                    '{{"city_filter": "{city_filter}", "min_overlap_area": {min_overlap_area}, "top_n": {top_n}}}' as analysis_params
                FROM overlap_hotspots
                ORDER BY overlap_count DESC
                LIMIT {top_n};
                """
            
            print(f"⚡ 执行空间叠置分析SQL...")
            self._check_shutdown()  # 执行前检查
            
            with self.engine.connect() as conn:
                self.current_connection = conn  # 保存连接引用
                
                result = conn.execute(text(analysis_sql))
                self._check_shutdown()  # SQL执行后检查
                
                conn.commit()
                print(f"✅ SQL执行完成，正在统计结果...")
                
                # 获取插入的记录数
                count_sql = text(f"SELECT COUNT(*) FROM {self.analysis_table} WHERE analysis_id = '{analysis_id}';")
                count_result = conn.execute(count_sql)
                inserted_count = count_result.scalar()
                
                self.current_connection = None  # 清除连接引用
                
            print(f"✅ 叠置分析完成，发现 {inserted_count} 个重叠热点")
            elapsed = datetime.now() - self.analysis_start_time
            print(f"⏱️ 总耗时: {elapsed}")
            return analysis_id
            
        except Exception as e:
            print(f"❌ 叠置分析失败: {str(e)}")
            raise
    
    def create_qgis_view(self, analysis_id: Optional[str] = None) -> bool:
        """创建QGIS兼容视图"""
        print("🎨 创建QGIS兼容视图...")
        
        # 读取视图创建SQL
        sql_file = Path(__file__).parent / "sql" / "qgis_views.sql"
        
        try:
            if sql_file.exists():
                with open(sql_file, 'r', encoding='utf-8') as f:
                    view_sql = f.read()
                    view_sql = view_sql.format(
                        qgis_view=self.qgis_view,
                        analysis_table=self.analysis_table
                    )
            else:
                # 内置SQL
                where_clause = f"WHERE analysis_id = '{analysis_id}'" if analysis_id else ""
                
                view_sql = f"""
                CREATE OR REPLACE VIEW {self.qgis_view} AS
                SELECT 
                    id as qgis_id,
                    analysis_id,
                    hotspot_rank,
                    overlap_count,
                    total_overlap_area,
                    subdataset_count,
                    scene_count,
                    involved_subdatasets,
                    involved_scenes,
                    CASE 
                        WHEN overlap_count >= 10 THEN 'High Density'
                        WHEN overlap_count >= 5 THEN 'Medium Density'
                        ELSE 'Low Density'
                    END as density_level,
                    geometry,
                    created_at
                FROM {self.analysis_table}
                WHERE analysis_type = 'bbox_overlap'
                {where_clause}
                ORDER BY hotspot_rank;
                """
            
            with self.engine.connect() as conn:
                conn.execute(text(view_sql))
                conn.commit()
                
            print(f"✅ QGIS视图 {self.qgis_view} 创建成功")
            return True
            
        except Exception as e:
            print(f"❌ 创建QGIS视图失败: {str(e)}")
            return False
    
    def get_city_analysis_suggestions(self) -> pd.DataFrame:
        """获取城市分析建议，帮助用户选择合适的城市"""
        print("🔍 分析各城市的数据分布，生成分析建议...")
        
        sql = text(f"""
            WITH city_stats AS (
                SELECT 
                    city_id,
                    COUNT(*) as total_bbox_count,
                    COUNT(*) FILTER (WHERE all_good = true) as good_bbox_count,
                    COUNT(DISTINCT subdataset_name) as subdataset_count,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE all_good = true) / COUNT(*), 1) as good_percent,
                    -- 估算可能的重叠对数量（基于数据密度）
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN 'High'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN 'Medium' 
                        ELSE 'Low'
                    END as analysis_complexity,
                    -- 预估分析时间（基于数据量）
                    CASE 
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 50000 THEN '> 10分钟'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN '2-10分钟'
                        WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN '< 2分钟'
                        ELSE '< 30秒'
                    END as estimated_time
                FROM {self.unified_view}
                WHERE city_id IS NOT NULL
                GROUP BY city_id
                HAVING COUNT(*) FILTER (WHERE all_good = true) > 0
            )
            SELECT 
                city_id,
                total_bbox_count,
                good_bbox_count,
                subdataset_count,
                good_percent,
                analysis_complexity,
                estimated_time,
                -- 推荐度评分
                CASE 
                    WHEN good_bbox_count BETWEEN 1000 AND 20000 AND good_percent > 90 THEN '⭐⭐⭐ 推荐'
                    WHEN good_bbox_count BETWEEN 500 AND 50000 AND good_percent > 85 THEN '⭐⭐ 较好'
                    WHEN good_bbox_count > 100 THEN '⭐ 可用'
                    ELSE '❌ 不建议'
                END as recommendation
            FROM city_stats
            ORDER BY 
                CASE 
                    WHEN good_bbox_count BETWEEN 1000 AND 20000 AND good_percent > 90 THEN 1
                    WHEN good_bbox_count BETWEEN 500 AND 50000 AND good_percent > 85 THEN 2
                    WHEN good_bbox_count > 100 THEN 3
                    ELSE 4
                END,
                good_bbox_count DESC;
        """)
        
        try:
            result_df = pd.read_sql(sql, self.engine)
            
            if not result_df.empty:
                print(f"\n📊 城市分析建议表:")
                print(result_df.to_string(index=False))
                
                # 提供具体建议
                recommended = result_df[result_df['recommendation'].str.contains('⭐⭐⭐')]
                if not recommended.empty:
                    best_city = recommended.iloc[0]['city_id']
                    print(f"\n💡 推荐城市: {best_city}")
                    print(f"   - 数据量适中，质量较高")
                    print(f"   - 预估分析时间: {recommended.iloc[0]['estimated_time']}")
                    print(f"   - 建议命令: --city {best_city}")
                
                return result_df
            else:
                print("❌ 未找到可用的城市数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 获取城市建议失败: {str(e)}")
            return pd.DataFrame()
    
    def estimate_analysis_time(self, city_filter: str = None) -> dict:
        """估算分析时间和数据量"""
        print("⏱️ 估算分析时间...")
        
        where_condition = f"WHERE city_id = '{city_filter}'" if city_filter else "WHERE city_id IS NOT NULL"
        
        sql = text(f"""
            SELECT 
                COUNT(*) FILTER (WHERE all_good = true) as analyzable_count,
                -- 估算可能的重叠对数量（n*(n-1)/2的简化估算）
                CASE 
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 0 THEN
                        LEAST(
                            COUNT(*) FILTER (WHERE all_good = true) * (COUNT(*) FILTER (WHERE all_good = true) - 1) / 2,
                            1000000  -- 限制最大估算数
                        )
                    ELSE 0
                END as estimated_pairs,
                CASE 
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 100000 THEN '⚠️ 很长 (>30分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 50000 THEN '⏳ 较长 (10-30分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 10000 THEN '⏰ 中等 (2-10分钟)'
                    WHEN COUNT(*) FILTER (WHERE all_good = true) > 1000 THEN '⚡ 较快 (<2分钟)'
                    ELSE '🚀 很快 (<30秒)'
                END as time_estimate,
                {f"'{city_filter}'" if city_filter else "'全部城市'"} as scope
            FROM {self.unified_view}
            {where_condition};
        """)
        
        try:
            result = self.engine.execute(sql).fetchone()
            
            estimate = {
                'analyzable_count': result.analyzable_count,
                'estimated_pairs': result.estimated_pairs,
                'time_estimate': result.time_estimate,
                'scope': result.scope
            }
            
            print(f"📊 分析范围: {estimate['scope']}")
            print(f"📈 可分析数据: {estimate['analyzable_count']:,} 个bbox")
            print(f"🔗 预估配对数: {estimate['estimated_pairs']:,}")
            print(f"⏱️ 预估时间: {estimate['time_estimate']}")
            
            if estimate['analyzable_count'] > 50000:
                print(f"💡 建议: 数据量较大，建议指定具体城市进行分析")
                print(f"💡 命令: --city your_city_name")
            
            return estimate
            
        except Exception as e:
            print(f"❌ 时间估算失败: {str(e)}")
            return {}

    def get_analysis_summary(self, analysis_id: str) -> pd.DataFrame:
        """获取分析结果摘要"""
        sql = text(f"""
            SELECT 
                hotspot_rank,
                overlap_count,
                ROUND(total_overlap_area::numeric, 4) as total_overlap_area,
                subdataset_count,
                scene_count,
                involved_subdatasets,
                density_level
            FROM {self.qgis_view}
            WHERE analysis_id = :analysis_id
            ORDER BY hotspot_rank
            LIMIT 10;
        """)
        
        return pd.read_sql(sql, self.engine, params={'analysis_id': analysis_id})
    
    def export_for_qgis(self, analysis_id: str) -> Dict[str, Any]:
        """导出QGIS可视化信息"""
        return {
            'analysis_id': analysis_id,
            'qgis_view': self.qgis_view,
            'unified_view': self.unified_view,
            'connection_info': {
                'host': 'local_pg',
                'port': 5432,
                'database': 'postgres',
                'username': 'postgres'
            },
            'visualization_tips': {
                'primary_key': 'qgis_id',
                'geometry_column': 'geometry',
                'style_column': 'density_level',
                'label_column': 'overlap_count',
                'filter_column': 'analysis_id'
            }
        }
    
    def list_analysis_results(self, pattern: str = None) -> pd.DataFrame:
        """列出所有分析结果
        
        Args:
            pattern: 可选的analysis_id过滤模式（支持SQL LIKE语法）
            
        Returns:
            包含分析结果摘要的DataFrame
        """
        print("📋 查询分析结果...")
        
        where_clause = ""
        if pattern:
            where_clause = f"WHERE analysis_id LIKE '{pattern}'"
            print(f"🔍 过滤条件: analysis_id LIKE '{pattern}'")
        
        sql = text(f"""
            SELECT 
                analysis_id,
                analysis_type,
                analysis_time,
                COUNT(*) as hotspot_count,
                MAX(hotspot_rank) as max_rank,
                SUM(overlap_count) as total_overlaps,
                ROUND(SUM(total_overlap_area)::numeric, 6) as total_area,
                STRING_AGG(DISTINCT UNNEST(involved_subdatasets), ', ') as subdatasets,
                MIN(created_at) as created_at
            FROM {self.analysis_table}
            {where_clause}
            GROUP BY analysis_id, analysis_type, analysis_time
            ORDER BY created_at DESC;
        """)
        
        try:
            with self.engine.connect() as conn:
                result_df = pd.read_sql(sql, conn)
                
                if not result_df.empty:
                    print(f"📊 找到 {len(result_df)} 个分析结果:")
                    print(result_df.to_string(index=False))
                else:
                    print("📭 没有找到分析结果")
                
                return result_df
                
        except Exception as e:
            print(f"❌ 查询分析结果失败: {str(e)}")
            return pd.DataFrame()
    
    def cleanup_analysis_results(
        self, 
        analysis_ids: Optional[List[str]] = None,
        pattern: str = None,
        older_than_days: int = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """清理分析结果
        
        Args:
            analysis_ids: 指定要删除的analysis_id列表
            pattern: 按模式删除（支持SQL LIKE语法，如'bbox_overlap_2023%'）
            older_than_days: 删除N天前的结果
            dry_run: 是否为试运行模式（不实际删除）
            
        Returns:
            清理结果统计
        """
        print("🧹 开始清理分析结果...")
        
        # 构建删除条件
        where_conditions = []
        
        if analysis_ids:
            id_list = "', '".join(analysis_ids)
            where_conditions.append(f"analysis_id IN ('{id_list}')")
            print(f"🎯 按ID删除: {len(analysis_ids)} 个")
            
        if pattern:
            where_conditions.append(f"analysis_id LIKE '{pattern}'")
            print(f"🔍 按模式删除: '{pattern}'")
            
        if older_than_days:
            where_conditions.append(f"created_at < NOW() - INTERVAL '{older_than_days} days'")
            print(f"📅 删除 {older_than_days} 天前的结果")
        
        if not where_conditions:
            print("⚠️ 未指定删除条件，为安全起见不执行清理")
            return {"deleted_count": 0, "error": "未指定删除条件"}
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        try:
            with self.engine.connect() as conn:
                # 先查询要删除的记录
                preview_sql = text(f"""
                    SELECT 
                        analysis_id,
                        analysis_type,
                        COUNT(*) as record_count,
                        MIN(created_at) as earliest,
                        MAX(created_at) as latest
                    FROM {self.analysis_table}
                    {where_clause}
                    GROUP BY analysis_id, analysis_type
                    ORDER BY earliest DESC;
                """)
                
                preview_df = pd.read_sql(preview_sql, conn)
                
                if preview_df.empty:
                    print("📭 没有找到匹配的记录")
                    return {"deleted_count": 0, "preview": preview_df}
                
                print(f"\n📋 将要清理的记录:")
                print(preview_df.to_string(index=False))
                
                total_records = preview_df['record_count'].sum()
                total_analyses = len(preview_df)
                
                print(f"\n📊 清理摘要:")
                print(f"   分析数量: {total_analyses}")
                print(f"   记录总数: {total_records}")
                
                if dry_run:
                    print(f"\n🧪 试运行模式 - 未实际删除")
                    print(f"💡 使用 dry_run=False 执行实际删除")
                    return {
                        "deleted_count": 0,
                        "would_delete": total_records,
                        "analysis_count": total_analyses,
                        "preview": preview_df
                    }
                
                # 实际删除
                print(f"\n🗑️ 执行删除...")
                delete_sql = text(f"DELETE FROM {self.analysis_table} {where_clause};")
                result = conn.execute(delete_sql)
                deleted_count = result.rowcount
                conn.commit()
                
                print(f"✅ 清理完成，删除了 {deleted_count} 条记录")
                
                return {
                    "deleted_count": deleted_count,
                    "analysis_count": total_analyses,
                    "preview": preview_df
                }
                
        except Exception as e:
            print(f"❌ 清理失败: {str(e)}")
            return {"deleted_count": 0, "error": str(e)}
    
    def cleanup_qgis_views(self, confirm: bool = False) -> bool:
        """清理QGIS视图
        
        Args:
            confirm: 是否确认删除
            
        Returns:
            是否成功
        """
        print("🎨 清理QGIS视图...")
        
        views_to_check = [
            self.qgis_view,
            "qgis_bbox_overlap_hotspots"
        ]
        
        try:
            with self.engine.connect() as conn:
                existing_views = []
                
                for view_name in views_to_check:
                    check_sql = text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.views 
                            WHERE table_schema = 'public' 
                            AND table_name = '{view_name}'
                        );
                    """)
                    
                    if conn.execute(check_sql).scalar():
                        existing_views.append(view_name)
                
                if not existing_views:
                    print("📭 没有找到相关的QGIS视图")
                    return True
                
                print(f"📋 找到以下视图:")
                for view in existing_views:
                    print(f"   - {view}")
                
                if not confirm:
                    print(f"\n🧪 试运行模式 - 未实际删除")
                    print(f"💡 使用 confirm=True 执行实际删除")
                    return False
                
                # 删除视图
                for view_name in existing_views:
                    drop_sql = text(f"DROP VIEW IF EXISTS {view_name};")
                    conn.execute(drop_sql)
                    print(f"✅ 删除视图: {view_name}")
                
                conn.commit()
                print(f"✅ 清理完成，删除了 {len(existing_views)} 个视图")
                return True
                
        except Exception as e:
            print(f"❌ 清理视图失败: {str(e)}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='BBox叠置分析')
    parser.add_argument('--city', help='城市过滤')
    parser.add_argument('--subdatasets', nargs='+', help='子数据集过滤')
    parser.add_argument('--min-overlap-area', type=float, default=0.0, help='最小重叠面积阈值')
    parser.add_argument('--top-n', type=int, default=20, help='返回的热点数量')
    parser.add_argument('--analysis-id', help='自定义分析ID')
    parser.add_argument('--refresh-view', action='store_true', help='强制刷新统一视图（适用于数据更新后）')
    parser.add_argument('--suggest-city', action='store_true', help='显示城市分析建议并退出')
    parser.add_argument('--estimate-time', action='store_true', help='估算分析时间并退出')
    
    # 清理相关参数
    parser.add_argument('--list-results', action='store_true', help='列出所有分析结果')
    parser.add_argument('--cleanup', action='store_true', help='清理分析结果')
    parser.add_argument('--cleanup-pattern', help='按模式清理（如"bbox_overlap_2023%"）')
    parser.add_argument('--cleanup-ids', nargs='+', help='按ID清理（可指定多个）')
    parser.add_argument('--cleanup-older-than', type=int, help='清理N天前的结果')
    parser.add_argument('--cleanup-views', action='store_true', help='清理QGIS视图')
    parser.add_argument('--confirm-cleanup', action='store_true', help='确认执行清理（默认为试运行）')
    parser.add_argument('--intersect-only', action='store_true', help='简化模式：只要相交就算重叠（忽略面积阈值）')
    
    args = parser.parse_args()
    
    print("🎯 BBox叠置分析示例")
    print("=" * 60)
    
    # 初始化分析器
    analyzer = BBoxOverlapAnalyzer()
    
    try:
        # 1. 确保统一视图存在
        print("\n📋 步骤1: 检查数据准备")
        # 对于大量数据的情况，我们优先使用现有视图，只在必要时刷新
        force_refresh = args.refresh_view
        if not analyzer.ensure_unified_view(force_refresh=force_refresh):
            print("❌ 统一视图检查失败，退出")
            return
        
        # 如果用户只想查看城市建议
        if args.suggest_city:
            print("\n🏙️ 城市分析建议")
            print("-" * 40)
            analyzer.get_city_analysis_suggestions()
            return
        
        # 如果用户只想估算时间
        if args.estimate_time:
            print("\n⏱️ 分析时间估算")
            print("-" * 40)
            analyzer.estimate_analysis_time(args.city)
            return
        
        # 如果用户想列出分析结果
        if args.list_results:
            print("\n📋 分析结果列表")
            print("-" * 40)
            analyzer.list_analysis_results(args.cleanup_pattern)
            return
        
        # 如果用户想清理分析结果
        if args.cleanup:
            print("\n🧹 清理分析结果")
            print("-" * 40)
            
            result = analyzer.cleanup_analysis_results(
                analysis_ids=args.cleanup_ids,
                pattern=args.cleanup_pattern,
                older_than_days=args.cleanup_older_than,
                dry_run=not args.confirm_cleanup
            )
            
            if not args.confirm_cleanup and result.get("would_delete", 0) > 0:
                print(f"\n💡 如要实际执行删除，请添加 --confirm-cleanup 参数")
            
            return
        
        # 如果用户想清理QGIS视图
        if args.cleanup_views:
            print("\n🎨 清理QGIS视图")
            print("-" * 40)
            analyzer.cleanup_qgis_views(confirm=args.confirm_cleanup)
            return
        
        # 2. 创建分析结果表
        print("\n🛠️ 步骤2: 准备分析环境")
        if not analyzer.create_analysis_table():
            print("❌ 分析表创建失败，退出")
            return
        
        # 3. 分析前的时间估算和确认
        print("\n⏱️ 步骤3a: 分析前估算")
        print("-" * 40)
        estimate = analyzer.estimate_analysis_time(args.city)
        
        # 如果数据量很大，给出警告和建议
        if estimate and estimate.get('analyzable_count', 0) > 50000:
            print(f"\n⚠️ 数据量警告:")
            print(f"   当前分析范围包含 {estimate['analyzable_count']:,} 个bbox")
            print(f"   预估分析时间: {estimate.get('time_estimate', '未知')}")
            print(f"   💡 建议: 使用 --city 参数缩小分析范围")
            print(f"   💡 获取城市建议: --suggest-city")
            
            if not args.city:
                print(f"\n🤔 是否继续全量分析？这可能需要很长时间...")
                print(f"💡 建议先运行: --suggest-city 查看推荐城市")
        
        # 3b. 执行叠置分析
        print(f"\n🚀 步骤3b: 执行叠置分析")
        print("-" * 40)
        analysis_id = analyzer.run_overlap_analysis(
            analysis_id=args.analysis_id,
            city_filter=args.city,
            subdataset_filter=args.subdatasets,
            min_overlap_area=args.min_overlap_area,
            top_n=args.top_n,
            intersect_only=args.intersect_only
        )
        
        # 4. 创建QGIS视图
        print("\n🎨 步骤4: 创建QGIS视图")
        if not analyzer.create_qgis_view(analysis_id):
            print("❌ QGIS视图创建失败")
            return
        
        # 5. 显示分析结果摘要
        print("\n📊 步骤5: 分析结果摘要")
        summary = analyzer.get_analysis_summary(analysis_id)
        if not summary.empty:
            print("TOP 10 重叠热点:")
            print(summary.to_string(index=False))
        else:
            print("未发现重叠热点")
        
        # 6. 提供QGIS可视化指导
        print("\n🎯 步骤6: QGIS可视化指导")
        qgis_info = analyzer.export_for_qgis(analysis_id)
        
        print("📋 数据库连接信息:")
        conn_info = qgis_info['connection_info']
        for key, value in conn_info.items():
            print(f"   {key}: {value}")
        
        print(f"\n📊 在QGIS中加载以下图层:")
        print(f"   1. {qgis_info['unified_view']} - 所有bbox数据（底图）")
        print(f"   2. {qgis_info['qgis_view']} - 重叠热点区域")
        
        print(f"\n🎨 可视化建议:")
        vis_tips = qgis_info['visualization_tips']
        print(f"   • 主键: {vis_tips['primary_key']}")
        print(f"   • 几何列: {vis_tips['geometry_column']}")
        print(f"   • 按 {vis_tips['style_column']} 字段设置颜色")
        print(f"   • 显示 {vis_tips['label_column']} 标签")
        print(f"   • 使用 {vis_tips['filter_column']} = '{analysis_id}' 过滤")
        
        print(f"\n✅ 叠置分析完成！分析ID: {analysis_id}")
        print(f"现在可以在QGIS中连接数据库并加载这些图层进行可视化分析。")
        
    except Exception as e:
        print(f"\n❌ 分析过程中出现错误: {str(e)}")
        logger.exception("详细错误信息")


if __name__ == "__main__":
    main()
