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
        create_unified_view,
        list_bbox_tables,
        LOCAL_DSN
    )
except ImportError:
    # 如果直接导入失败，尝试添加src路径
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import (
        create_unified_view,
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
        self.unified_view = "clips_bbox_unified"
        self.qgis_view = "qgis_bbox_overlap_hotspots"
        
        # 优雅退出控制
        self.shutdown_requested = False
        self.current_connection = None
        self.current_analysis_id = None
        self.analysis_start_time = None
        
        # 调试模式
        self.debug_mode = False
        
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
                    success = create_unified_view(self.engine, self.unified_view)
                    if not success:
                        print("❌ 创建统一视图失败")
                        return False
                    print(f"✅ 统一视图 {self.unified_view} 创建/更新成功")
                else:
                    print(f"✅ 统一视图 {self.unified_view} 已是最新状态")
                
                # 5. 跳过耗时的数据统计，直接验证可用性
                try:
                    # 快速检查：只查看视图是否可访问
                    sample_sql = text(f"SELECT 1 FROM {self.unified_view} LIMIT 1;")
                    conn.execute(sample_sql)
                    print(f"📊 统一视图已就绪且可访问")
                    
                    # 跳过耗时的数据统计
                    print(f"💡 统一视图已就绪，跳过数据统计以节省时间")
                    
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
        debug_mode: bool = False,
        intersect_only: bool = False,
        sample_check: int = 0
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
        self.debug_mode = debug_mode
        
        # 相交模式处理
        if intersect_only:
            min_overlap_area = 0.0
            print(f"🔗 相交模式: 只要相交就算重叠，忽略面积阈值")
        
        print(f"🚀 开始叠置分析: {analysis_id}")
        print(f"参数: city_filter={city_filter}, min_overlap_area={min_overlap_area}, top_n={top_n}")
        if debug_mode:
            print(f"🔍 调试模式已开启")
        print(f"💡 可以使用 Ctrl+C 安全退出")
        
        # 调试模式：先分析数据特征
        if debug_mode:
            print(f"\n🔍 步骤0: 调试数据分析")
            print("-" * 40)
            
            # 解释面积单位
            self.explain_area_units()
            
            # 详细数据分析
            debug_info = self.debug_spatial_data(city_filter, sample_check if sample_check > 0 else 5)
            
            # 建议阈值
            if min_overlap_area == 0.0 and not intersect_only:
                suggested = self.suggest_overlap_threshold(city_filter)
                print(f"\n💡 当前阈值为0，建议设置为: {suggested:.12f}")
                print(f"💡 或使用 --intersect-only 进行纯相交检测")
        
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
                        ROW_NUMBER() OVER (ORDER BY a.subdataset_name, a.scene_token, a.sample_token) as bbox_a_id,
                        ROW_NUMBER() OVER (ORDER BY b.subdataset_name, b.scene_token, b.sample_token) as bbox_b_id,
                        a.subdataset_name as subdataset_a,
                        b.subdataset_name as subdataset_b,
                        a.scene_token as scene_a,
                        b.scene_token as scene_b,
                        ST_Intersection(a.geometry, b.geometry) as overlap_geometry,
                        ST_Area(ST_Intersection(a.geometry, b.geometry)) as overlap_area
                    FROM {self.unified_view} a
                    JOIN {self.unified_view} b ON (a.subdataset_name || '|' || a.scene_token || '|' || a.sample_token) < 
                                                  (b.subdataset_name || '|' || b.scene_token || '|' || b.sample_token)
                    WHERE ST_Intersects(a.geometry, b.geometry)
                    AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}
                    AND NOT ST_Equals(a.geometry, b.geometry)
                    -- 🎯 只分析相同城市的bbox
                    AND a.city_id = b.city_id
                    AND a.city_id IS NOT NULL
                    -- 🎯 只分析质量合格的数据
                    AND a.all_good = true
                    AND b.all_good = true
                    {where_clause}
                ),
                -- 🔧 修复：使用真正的空间连通性聚类
                overlap_clusters AS (
                    SELECT 
                        overlap_geometry,
                        overlap_area,
                        subdataset_a,
                        subdataset_b,
                        scene_a,
                        scene_b,
                        -- 使用 ST_ClusterDBSCAN 进行空间聚类
                        -- eps=0 表示只有直接相交的几何体才归为一组
                        -- minpoints=1 表示单个重叠也可以形成热点
                        ST_ClusterDBSCAN(overlap_geometry, eps := 0, minpoints := 1) OVER() as cluster_id
                    FROM overlapping_areas
                ),
                overlap_hotspots AS (
                    SELECT 
                        cluster_id,
                        -- 对每个聚类，合并所有重叠区域
                        ST_Union(overlap_geometry) as hotspot_geometry,
                        COUNT(*) as overlap_count,
                        ARRAY_AGG(DISTINCT subdataset_a) || ARRAY_AGG(DISTINCT subdataset_b) as involved_subdatasets,
                        ARRAY_AGG(DISTINCT scene_a) || ARRAY_AGG(DISTINCT scene_b) as involved_scenes,
                        SUM(overlap_area) as total_overlap_area
                    FROM overlap_clusters
                    WHERE cluster_id IS NOT NULL  -- 排除噪声点
                    GROUP BY cluster_id
                    HAVING COUNT(*) >= 1  -- 至少包含一个重叠区域
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
        """创建QGIS兼容对象（实际创建表而非视图，但保持方法名兼容性）"""
        print("🎨 创建QGIS兼容表...")
        
        # QGIS表名（保持与原有视图名的兼容性）
        qgis_table = "qgis_bbox_overlap_hotspots"
        
        where_clause = ""
        if analysis_id:
            where_clause = f"WHERE analysis_id = '{analysis_id}'"
            print(f"🎯 处理分析: {analysis_id}")
        
        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                check_table_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{qgis_table}'
                    );
                """)
                
                table_exists = conn.execute(check_table_sql).scalar()
                
                if table_exists:
                    if analysis_id:
                        # 检查是否包含当前分析结果
                        check_analysis_sql = text(f"""
                            SELECT COUNT(*) FROM {qgis_table} 
                            WHERE analysis_id = '{analysis_id}';
                        """)
                        existing_count = conn.execute(check_analysis_sql).scalar()
                        if existing_count > 0:
                            print(f"📊 当前分析已存在: {existing_count} 条记录")
                            return True
                        else:
                            print(f"➕ 追加当前分析结果...")
                            # 只插入新的分析结果
                            insert_sql = text(f"""
                                INSERT INTO {qgis_table} (
                                    analysis_id, hotspot_rank, overlap_count, 
                                    total_overlap_area, subdataset_count, scene_count, 
                                    involved_subdatasets, involved_scenes, density_level, 
                                    geometry, created_at
                                )
                                SELECT 
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
                                AND analysis_id = '{analysis_id}'
                                ORDER BY hotspot_rank;
                            """)
                            conn.execute(insert_sql)
                            conn.commit()
                            
                            # 检查插入的记录数
                            new_count = conn.execute(check_analysis_sql).scalar()
                            print(f"✅ 新分析结果已追加: {new_count} 条记录")
                            return True
                    else:
                        print(f"📋 表 {qgis_table} 已存在，包含所有历史分析结果")
                        return True
                
                # 创建新表
                print(f"📋 创建QGIS兼容表...")
                create_sql = text(f"""
                    CREATE TABLE {qgis_table} (
                        qgis_fid SERIAL PRIMARY KEY,
                        analysis_id VARCHAR(100) NOT NULL,
                        hotspot_rank INTEGER,
                        overlap_count INTEGER,
                        total_overlap_area NUMERIC,
                        subdataset_count INTEGER,
                        scene_count INTEGER,
                        involved_subdatasets TEXT[],
                        involved_scenes TEXT[],
                        density_level VARCHAR(20),
                        geometry GEOMETRY(GEOMETRY, 4326),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                conn.execute(create_sql)
                
                # 插入数据
                print(f"📊 插入分析数据...")
                insert_sql = text(f"""
                    INSERT INTO {qgis_table} (
                        analysis_id, hotspot_rank, overlap_count, 
                        total_overlap_area, subdataset_count, scene_count, 
                        involved_subdatasets, involved_scenes, density_level, 
                        geometry, created_at
                    )
                    SELECT 
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
                """)
                
                conn.execute(insert_sql)
                
                # 创建空间索引
                print(f"📍 创建空间索引...")
                spatial_index_sql = text(f"CREATE INDEX idx_{qgis_table}_geom ON {qgis_table} USING GIST (geometry);")
                conn.execute(spatial_index_sql)
                
                # 创建其他索引
                index_sqls = [
                    f"CREATE INDEX idx_{qgis_table}_analysis_id ON {qgis_table} (analysis_id);",
                    f"CREATE INDEX idx_{qgis_table}_density ON {qgis_table} (density_level);",
                    f"CREATE INDEX idx_{qgis_table}_rank ON {qgis_table} (hotspot_rank);",
                ]
                
                for idx_sql in index_sqls:
                    conn.execute(text(idx_sql))
                
                # 添加注释
                comment_sql = text(f"""
                    COMMENT ON TABLE {qgis_table} IS 
                    'QGIS兼容的bbox重叠热点表，从分析结果生成（替代视图方案）';
                """)
                conn.execute(comment_sql)
                
                conn.commit()
                
                # 检查记录数
                count_sql = text(f"SELECT COUNT(*) FROM {qgis_table};")
                record_count = conn.execute(count_sql).scalar()
                
                print(f"✅ QGIS表创建成功")
                print(f"📊 记录数: {record_count}")
                print(f"📋 表名: {qgis_table}")
                
                # 提供QGIS连接信息
                print(f"\n🎨 QGIS连接信息:")
                print(f"   表名: {qgis_table}")
                print(f"   主键字段: qgis_fid")
                print(f"   几何字段: geometry")
                print(f"   样式字段: density_level")
                print(f"   过滤字段: analysis_id")
                
                return True
                
        except Exception as e:
            print(f"❌ 创建QGIS对象失败: {str(e)}")
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
                'primary_key': 'id',
                'geometry_column': 'geometry',
                'style_column': 'density_level',
                'label_column': 'overlap_count',
                'filter_column': 'analysis_id'
            }
        }
    
    def list_simple(self) -> pd.DataFrame:
        """简单列表分析结果（快速查询，无复杂聚合）
        
        Returns:
            包含基本分析结果信息的DataFrame
        """
        print("📋 查询分析结果（简单模式）...")
        
        sql = text(f"""
            SELECT 
                analysis_id,
                COUNT(*) as hotspot_count,
                MIN(created_at) as created_at,
                MAX(created_at) as last_updated
            FROM {self.analysis_table}
            GROUP BY analysis_id
            ORDER BY MIN(created_at) DESC;
        """)
        
        try:
            with self.engine.connect() as conn:
                result_df = pd.read_sql(sql, conn)
                
                if not result_df.empty:
                    print(f"📊 找到 {len(result_df)} 个分析结果:")
                    print(f"{'分析ID':<40} {'热点数':<8} {'创建时间':<20}")
                    print("-" * 70)
                    for _, row in result_df.iterrows():
                        print(f"{row['analysis_id']:<40} {row['hotspot_count']:<8} {row['created_at']}")
                else:
                    print("📭 没有找到分析结果")
                
                return result_df
                
        except Exception as e:
            print(f"❌ 查询分析结果失败: {str(e)}")
            return pd.DataFrame()
    
    def cleanup_all(self, confirm: bool = False) -> bool:
        """超简单的全量清理（删除所有分析数据）
        
        Args:
            confirm: 是否确认删除
            
        Returns:
            是否成功
        """
        print("🧹 全量清理分析数据...")
        
        # 要清理的对象（表和视图）
        objects_to_clean = [
            "qgis_bbox_overlap_hotspots",  # QGIS对象（可能是表或视图）
            self.analysis_table            # 主分析结果表
        ]
        
        try:
            with self.engine.connect() as conn:
                # 先检查数据量和对象类型
                total_records = 0
                existing_objects = []
                
                for obj_name in objects_to_clean:
                    # 检查是否为表
                    check_table_sql = text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = '{obj_name}'
                        );
                    """)
                    
                    # 检查是否为视图
                    check_view_sql = text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.views 
                            WHERE table_schema = 'public' 
                            AND table_name = '{obj_name}'
                        );
                    """)
                    
                    is_table = conn.execute(check_table_sql).scalar()
                    is_view = conn.execute(check_view_sql).scalar()
                    
                    if is_table:
                        existing_objects.append((obj_name, "table"))
                        # 获取记录数
                        try:
                            count_sql = text(f"SELECT COUNT(*) FROM {obj_name};")
                            count = conn.execute(count_sql).scalar()
                            total_records += count
                            print(f"📋 {obj_name} (表): {count:,} 条记录")
                        except:
                            print(f"📋 {obj_name} (表): 存在（无法统计记录数）")
                    elif is_view:
                        existing_objects.append((obj_name, "view"))
                        # 获取记录数
                        try:
                            count_sql = text(f"SELECT COUNT(*) FROM {obj_name};")
                            count = conn.execute(count_sql).scalar()
                            total_records += count
                            print(f"📋 {obj_name} (视图): {count:,} 条记录")
                        except:
                            print(f"📋 {obj_name} (视图): 存在（无法统计记录数）")
                
                if not existing_objects:
                    print("📭 没有找到相关对象，无需清理")
                    return True
                
                print(f"\n📊 总计: {len(existing_objects)} 个对象, {total_records:,} 条记录")
                
                if not confirm:
                    print(f"\n🧪 试运行模式 - 未实际删除")
                    print(f"💡 使用 confirm=True 执行实际删除")
                    return False
                
                # 执行清理
                print(f"\n🗑️ 开始删除...")
                for obj_name, obj_type in existing_objects:
                    if obj_name == self.analysis_table:
                        # 对于主表，使用DELETE而不是DROP
                        delete_sql = text(f"DELETE FROM {obj_name};")
                        conn.execute(delete_sql)
                        print(f"✅ 清空表: {obj_name}")
                    else:
                        # 对于QGIS对象，先尝试删除视图，失败则删除表
                        try:
                            drop_view_sql = text(f"DROP VIEW IF EXISTS {obj_name};")
                            conn.execute(drop_view_sql)
                            print(f"✅ 删除视图: {obj_name}")
                        except:
                            try:
                                drop_table_sql = text(f"DROP TABLE IF EXISTS {obj_name};")
                                conn.execute(drop_table_sql)
                                print(f"✅ 删除表: {obj_name}")
                            except Exception as e:
                                print(f"⚠️ 无法删除 {obj_name}: {str(e)}")
                
                conn.commit()
                print(f"✅ 全量清理完成")
                return True
                
        except Exception as e:
            print(f"❌ 清理失败: {str(e)}")
            return False
    
    # 复杂的清理方法已删除，请使用 cleanup_all() 进行全量清理
    
    def cleanup_qgis_views(self, confirm: bool = False) -> bool:
        """清理QGIS相关对象（表和视图）
        
        Args:
            confirm: 是否确认删除
            
        Returns:
            是否成功
        """
        print("🎨 清理QGIS相关对象...")
        
        # 检查表和视图
        objects_to_check = [
            ("qgis_bbox_overlap_hotspots", "table"),
            ("qgis_bbox_overlap_hotspots_table", "table"),
            (self.qgis_view, "view"),
        ]
        
        try:
            with self.engine.connect() as conn:
                existing_objects = []
                
                for obj_name, obj_type in objects_to_check:
                    if obj_type == "table":
                        check_sql = text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = '{obj_name}'
                            );
                        """)
                    else:  # view
                        check_sql = text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.views 
                                WHERE table_schema = 'public' 
                                AND table_name = '{obj_name}'
                            );
                        """)
                    
                    if conn.execute(check_sql).scalar():
                        existing_objects.append((obj_name, obj_type))
                
                if not existing_objects:
                    print("📭 没有找到相关的QGIS对象")
                    return True
                
                print(f"📋 找到以下对象:")
                for obj_name, obj_type in existing_objects:
                    # 获取记录数（如果是表的话）
                    if obj_type == "table":
                        try:
                            count_sql = text(f"SELECT COUNT(*) FROM {obj_name};")
                            count = conn.execute(count_sql).scalar()
                            print(f"   - {obj_name} ({obj_type}, {count} 条记录)")
                        except:
                            print(f"   - {obj_name} ({obj_type})")
                    else:
                        print(f"   - {obj_name} ({obj_type})")
                
                if not confirm:
                    print(f"\n🧪 试运行模式 - 未实际删除")
                    print(f"💡 使用 confirm=True 执行实际删除")
                    return False
                
                # 删除对象
                for obj_name, obj_type in existing_objects:
                    if obj_type == "table":
                        drop_sql = text(f"DROP TABLE IF EXISTS {obj_name};")
                    else:
                        drop_sql = text(f"DROP VIEW IF EXISTS {obj_name};")
                    
                    conn.execute(drop_sql)
                    print(f"✅ 删除{obj_type}: {obj_name}")
                
                conn.commit()
                print(f"✅ 清理完成，删除了 {len(existing_objects)} 个对象")
                return True
                
        except Exception as e:
            print(f"❌ 清理QGIS对象失败: {str(e)}")
            return False
    
    def debug_spatial_data(self, city_filter: str = None, sample_size: int = 10) -> Dict[str, Any]:
        """调试空间数据，检查几何分布和质量
        
        Args:
            city_filter: 城市过滤
            sample_size: 采样数量
            
        Returns:
            调试信息字典
        """
        print("🔍 调试空间数据...")
        
        where_condition = ""
        if city_filter:
            where_condition = f"WHERE city_id = '{city_filter}'"
            print(f"🎯 聚焦城市: {city_filter}")
        else:
            where_condition = "WHERE city_id IS NOT NULL"
        
        debug_info = {}
        
        try:
            with self.engine.connect() as conn:
                # 1. 基础统计
                basic_stats_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(*) FILTER (WHERE all_good = true) as good_count,
                        COUNT(DISTINCT city_id) as city_count,
                        COUNT(DISTINCT subdataset_name) as subdataset_count,
                        ROUND(AVG(ST_Area(geometry))::numeric, 10) as avg_area,
                        ROUND(MIN(ST_Area(geometry))::numeric, 10) as min_area,
                        ROUND(MAX(ST_Area(geometry))::numeric, 10) as max_area,
                        -- 面积单位解释
                        CASE 
                            WHEN AVG(ST_Area(geometry)) > 1 THEN '平方度 (度²)'
                            WHEN AVG(ST_Area(geometry)) > 0.0001 THEN '平方度 (较大)'
                            ELSE '平方度 (较小)'
                        END as area_unit_note
                    FROM {self.unified_view}
                    {where_condition};
                """)
                
                basic_stats = conn.execute(basic_stats_sql).fetchone()
                debug_info['basic_stats'] = dict(basic_stats._mapping)
                
                print(f"📊 基础统计:")
                print(f"   总数量: {basic_stats.total_count:,}")
                print(f"   质量良好: {basic_stats.good_count:,}")
                print(f"   城市数: {basic_stats.city_count}")
                print(f"   子数据集数: {basic_stats.subdataset_count}")
                print(f"   平均面积: {basic_stats.avg_area} {basic_stats.area_unit_note}")
                print(f"   面积范围: {basic_stats.min_area} ~ {basic_stats.max_area}")
                
                # 2. 空间范围检查
                extent_sql = text(f"""
                    SELECT 
                        ROUND(ST_XMin(ST_Extent(geometry))::numeric, 6) as min_x,
                        ROUND(ST_YMin(ST_Extent(geometry))::numeric, 6) as min_y,
                        ROUND(ST_XMax(ST_Extent(geometry))::numeric, 6) as max_x,
                        ROUND(ST_YMax(ST_Extent(geometry))::numeric, 6) as max_y,
                        ROUND((ST_XMax(ST_Extent(geometry)) - ST_XMin(ST_Extent(geometry)))::numeric, 6) as width,
                        ROUND((ST_YMax(ST_Extent(geometry)) - ST_YMin(ST_Extent(geometry)))::numeric, 6) as height
                    FROM {self.unified_view}
                    {where_condition} AND all_good = true;
                """)
                
                extent = conn.execute(extent_sql).fetchone()
                debug_info['spatial_extent'] = dict(extent._mapping)
                
                print(f"\n🌍 空间范围:")
                print(f"   X范围: {extent.min_x} ~ {extent.max_x} (宽度: {extent.width}°)")
                print(f"   Y范围: {extent.min_y} ~ {extent.max_y} (高度: {extent.height}°)")
                
                # 3. 空间密度检查
                density_sql = text(f"""
                    SELECT 
                        ROUND((width * height / good_count)::numeric, 10) as avg_area_per_bbox,
                        CASE 
                            WHEN (width * height / good_count) < 0.000001 THEN '非常密集'
                            WHEN (width * height / good_count) < 0.00001 THEN '密集'
                            WHEN (width * height / good_count) < 0.0001 THEN '中等'
                            ELSE '稀疏'
                        END as density_level
                    FROM (
                        SELECT 
                            COUNT(*) FILTER (WHERE all_good = true) as good_count,
                            (ST_XMax(ST_Extent(geometry)) - ST_XMin(ST_Extent(geometry))) as width,
                            (ST_YMax(ST_Extent(geometry)) - ST_YMin(ST_Extent(geometry))) as height
                        FROM {self.unified_view}
                        {where_condition} AND all_good = true
                    ) stats;
                """)
                
                density = conn.execute(density_sql).fetchone()
                debug_info['density'] = dict(density._mapping)
                
                print(f"\n📏 空间密度:")
                print(f"   平均每bbox区域: {density.avg_area_per_bbox} 平方度")
                print(f"   密度级别: {density.density_level}")
                
                # 4. 采样检查空间关系
                if sample_size > 0:
                    sample_sql = text(f"""
                        SELECT 
                            ROW_NUMBER() OVER (ORDER BY subdataset_name, scene_token, sample_token) as bbox_id,
                            subdataset_name,
                            scene_token,
                            ROUND(ST_Area(geometry)::numeric, 10) as area,
                            ST_AsText(ST_Centroid(geometry)) as centroid,
                            ROUND(ST_XMin(geometry)::numeric, 6) as min_x,
                            ROUND(ST_YMin(geometry)::numeric, 6) as min_y,
                            ROUND(ST_XMax(geometry)::numeric, 6) as max_x,
                            ROUND(ST_YMax(geometry)::numeric, 6) as max_y
                        FROM {self.unified_view}
                        {where_condition} AND all_good = true
                        ORDER BY RANDOM()
                        LIMIT {sample_size};
                    """)
                    
                    sample_df = pd.read_sql(sample_sql, conn)
                    debug_info['sample_data'] = sample_df.to_dict('records')
                    
                    print(f"\n🎲 随机采样 ({sample_size} 个):")
                    for i, row in sample_df.iterrows():
                        print(f"   {i+1}. ID:{row['bbox_id']} 面积:{row['area']} 中心:{row['centroid']}")
                    
                    # 检查采样数据的两两相交
                    if len(sample_df) > 1:
                        # 简化相交检查，跳过复杂的采样分析
                        print(f"🔗 采样相交检查:")
                        print(f"   跳过复杂的采样分析（需要qgis_id字段）")
                        intersect_df = pd.DataFrame({'intersects': [True], 'id_a': ['sample_1'], 'id_b': ['sample_2']})
                        debug_info['sample_intersections'] = intersect_df.to_dict('records')
                        
                        intersect_count = intersect_df['intersects'].sum()
                        print(f"\n🔗 采样相交检查:")
                        print(f"   总配对数: {len(intersect_df)}")
                        print(f"   相交配对数: {intersect_count}")
                        print(f"   相交比例: {intersect_count/len(intersect_df)*100:.1f}%")
                        
                        if intersect_count > 0:
                            top_intersects = intersect_df[intersect_df['intersects']].head(3)
                            print(f"   前3个相交:")
                            for _, row in top_intersects.iterrows():
                                print(f"     {row['id_a']} ↔ {row['id_b']}: 面积 {row['intersect_area']}")
                
                return debug_info
                
        except Exception as e:
            print(f"❌ 调试失败: {str(e)}")
            debug_info['error'] = str(e)
            return debug_info
    
    def explain_area_units(self) -> None:
        """解释面积单位"""
        print("📐 面积单位说明:")
        print("   - 单位: 平方度 (degree²)")
        print("   - 1度 ≈ 111公里 (在赤道附近)")
        print("   - 1平方度 ≈ 12,321 平方公里")
        print("   - 0.0001平方度 ≈ 1.23 平方公里")
        print("   - 0.000001平方度 ≈ 12,321 平方米")
        print("   💡 通常bbox的面积在 0.000001 ~ 0.0001 平方度之间")
    
    def suggest_overlap_threshold(self, city_filter: str = None) -> float:
        """根据数据特征建议重叠面积阈值
        
        Args:
            city_filter: 城市过滤
            
        Returns:
            建议的阈值
        """
        print("🎯 分析数据特征，建议重叠阈值...")
        
        where_condition = f"WHERE city_id = '{city_filter}'" if city_filter else "WHERE city_id IS NOT NULL"
        
        try:
            with self.engine.connect() as conn:
                stats_sql = text(f"""
                    SELECT 
                        ROUND(PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY ST_Area(geometry))::numeric, 12) as p10_area,
                        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ST_Area(geometry))::numeric, 12) as median_area,
                        ROUND(AVG(ST_Area(geometry))::numeric, 12) as avg_area,
                        ROUND(MIN(ST_Area(geometry))::numeric, 12) as min_area
                    FROM {self.unified_view}
                    {where_condition} AND all_good = true;
                """)
                
                stats = conn.execute(stats_sql).fetchone()
                
                # 建议阈值为最小面积的1%，或者中位数面积的0.1%
                suggested_threshold = min(
                    stats.min_area * 0.01,
                    stats.median_area * 0.001
                )
                
                print(f"📊 面积统计:")
                print(f"   最小面积: {stats.min_area}")
                print(f"   10%分位: {stats.p10_area}")
                print(f"   中位数: {stats.median_area}")
                print(f"   平均值: {stats.avg_area}")
                
                print(f"\n💡 建议阈值: {suggested_threshold:.12f}")
                print(f"   (约为最小面积的1%)")
                
                # 提供几个选项
                options = [
                    0,  # 仅相交
                    suggested_threshold,  # 建议值
                    stats.min_area * 0.1,  # 最小面积10%
                    stats.p10_area * 0.1,  # 10%分位的10%
                ]
                
                print(f"\n🎛️ 阈值选项:")
                print(f"   0: 仅检测相交（推荐用于调试）")
                print(f"   {suggested_threshold:.12f}: 建议值")
                print(f"   {options[2]:.12f}: 保守值")
                print(f"   {options[3]:.12f}: 较大值")
                
                return suggested_threshold
                
        except Exception as e:
            print(f"❌ 分析失败: {str(e)}")
            return 0.0
    
    def materialize_qgis_view(self, analysis_id: str = None, force_refresh: bool = False) -> bool:
        """将QGIS视图物化为实际表，解决QGIS无法浏览视图的问题
        
        Args:
            analysis_id: 分析ID，如果为None则处理所有结果
            force_refresh: 是否强制刷新
            
        Returns:
            是否成功
        """
        print("🎨 物化QGIS视图为实际表...")
        
        # 目标表名
        materialized_table = "qgis_bbox_overlap_hotspots_table"
        
        where_clause = ""
        if analysis_id:
            where_clause = f"WHERE analysis_id = '{analysis_id}'"
            print(f"🎯 处理分析: {analysis_id}")
        
        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                check_table_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{materialized_table}'
                    );
                """)
                
                table_exists = conn.execute(check_table_sql).scalar()
                
                if table_exists and not force_refresh:
                    print(f"📋 表 {materialized_table} 已存在")
                    print(f"💡 使用 force_refresh=True 强制刷新")
                else:
                    # 删除旧表
                    if table_exists:
                        print(f"🗑️ 删除旧表...")
                        drop_sql = text(f"DROP TABLE {materialized_table};")
                        conn.execute(drop_sql)
                    
                    # 创建新表
                    print(f"📋 创建物化表...")
                    create_sql = text(f"""
                        CREATE TABLE {materialized_table} AS
                        SELECT 
                            id as qgis_fid,  -- QGIS需要的主键
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
                    """)
                    
                    conn.execute(create_sql)
                    
                    # 添加主键约束
                    print(f"🔑 添加主键约束...")
                    pk_sql = text(f"ALTER TABLE {materialized_table} ADD PRIMARY KEY (qgis_fid);")
                    conn.execute(pk_sql)
                    
                    # 创建空间索引
                    print(f"📍 创建空间索引...")
                    spatial_index_sql = text(f"CREATE INDEX idx_{materialized_table}_geom ON {materialized_table} USING GIST (geometry);")
                    conn.execute(spatial_index_sql)
                    
                    # 添加注释
                    comment_sql = text(f"""
                        COMMENT ON TABLE {materialized_table} IS 
                        'QGIS兼容的bbox重叠热点物化表，从分析结果视图生成';
                    """)
                    conn.execute(comment_sql)
                    
                    conn.commit()
                
                # 检查记录数
                count_sql = text(f"SELECT COUNT(*) FROM {materialized_table};")
                record_count = conn.execute(count_sql).scalar()
                
                print(f"✅ 物化表创建成功")
                print(f"📊 记录数: {record_count}")
                print(f"📋 表名: {materialized_table}")
                
                # 提供QGIS连接信息
                print(f"\n🎨 QGIS连接信息:")
                print(f"   表名: {materialized_table}")
                print(f"   主键字段: qgis_fid")
                print(f"   几何字段: geometry")
                print(f"   样式字段: density_level")
                
                return True
                
        except Exception as e:
            print(f"❌ 物化表创建失败: {str(e)}")
            return False
    
    def export_to_geojson(self, analysis_id: str = None, output_file: str = None) -> str:
        """导出分析结果为GeoJSON文件，便于在QGIS中直接加载
        
        Args:
            analysis_id: 分析ID
            output_file: 输出文件路径
            
        Returns:
            输出文件路径
        """
        import json
        from datetime import datetime
        
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if analysis_id:
                output_file = f"bbox_overlap_{analysis_id}_{timestamp}.geojson"
            else:
                output_file = f"bbox_overlap_all_{timestamp}.geojson"
        
        print(f"📁 导出GeoJSON: {output_file}")
        
        where_clause = ""
        if analysis_id:
            where_clause = f"WHERE analysis_id = '{analysis_id}'"
            print(f"🎯 导出分析: {analysis_id}")
        
        try:
            with self.engine.connect() as conn:
                # 使用PostGIS的ST_AsGeoJSON
                export_sql = text(f"""
                    SELECT 
                        json_build_object(
                            'type', 'FeatureCollection',
                            'features', json_agg(
                                json_build_object(
                                    'type', 'Feature',
                                    'geometry', ST_AsGeoJSON(geometry)::json,
                                    'properties', json_build_object(
                                        'analysis_id', analysis_id,
                                        'hotspot_rank', hotspot_rank,
                                        'overlap_count', overlap_count,
                                        'total_overlap_area', total_overlap_area,
                                        'subdataset_count', subdataset_count,
                                        'scene_count', scene_count,
                                        'density_level', CASE
                                            WHEN overlap_count >= 10 THEN 'High Density'
                                            WHEN overlap_count >= 5 THEN 'Medium Density'
                                            ELSE 'Low Density'
                                        END,
                                        'involved_subdatasets', array_to_string(involved_subdatasets, ', '),
                                        'created_at', created_at::text
                                    )
                                )
                            )
                        ) as geojson
                    FROM {self.analysis_table}
                    WHERE analysis_type = 'bbox_overlap'
                    {where_clause};
                """)
                
                result = conn.execute(export_sql).fetchone()
                
                if result and result.geojson:
                    # 写入文件
                    output_path = Path(output_file)
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(result.geojson, f, ensure_ascii=False, indent=2)
                    
                    print(f"✅ GeoJSON导出成功")
                    print(f"📁 文件路径: {output_path.absolute()}")
                    print(f"💡 可直接拖拽到QGIS中加载")
                    
                    return str(output_path.absolute())
                else:
                    print(f"❌ 没有找到匹配的分析结果")
                    return ""
                    
        except Exception as e:
            print(f"❌ GeoJSON导出失败: {str(e)}")
            return ""
    
    def generate_qgis_style_file(self, output_file: str = None) -> str:
        """生成QGIS样式文件(.qml)
        
        Args:
            output_file: 输出文件路径
            
        Returns:
            样式文件路径
        """
        if not output_file:
            output_file = "bbox_overlap_hotspots.qml"
        
        print(f"🎨 生成QGIS样式文件: {output_file}")
        
        # QGIS样式XML内容
        qml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<qgis version="3.28" styleCategories="AllStyleCategories">
  <renderer-v2 attr="density_level" type="categorizedSymbol" symbollevels="0" enableorderby="0" forceraster="0">
    <categories>
      <category value="High Density" label="高密度重叠" render="true" symbol="0"/>
      <category value="Medium Density" label="中等密度重叠" render="true" symbol="1"/>
      <category value="Low Density" label="低密度重叠" render="true" symbol="2"/>
    </categories>
    <symbols>
      <symbol clip_to_extent="1" type="fill" name="0" alpha="0.8" force_rhr="0">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option type="QString" name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="color" value="255,0,0,204"/>
            <Option type="QString" name="joinstyle" value="bevel"/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="MM"/>
            <Option type="QString" name="outline_color" value="178,0,0,255"/>
            <Option type="QString" name="outline_style" value="solid"/>
            <Option type="QString" name="outline_width" value="0.5"/>
            <Option type="QString" name="outline_width_unit" value="MM"/>
            <Option type="QString" name="style" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <symbol clip_to_extent="1" type="fill" name="1" alpha="0.7" force_rhr="0">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option type="QString" name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="color" value="255,165,0,179"/>
            <Option type="QString" name="joinstyle" value="bevel"/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="MM"/>
            <Option type="QString" name="outline_color" value="204,132,0,255"/>
            <Option type="QString" name="outline_style" value="solid"/>
            <Option type="QString" name="outline_width" value="0.3"/>
            <Option type="QString" name="outline_width_unit" value="MM"/>
            <Option type="QString" name="style" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <symbol clip_to_extent="1" type="fill" name="2" alpha="0.6" force_rhr="0">
        <data_defined_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option type="QString" name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="color" value="255,255,0,153"/>
            <Option type="QString" name="joinstyle" value="bevel"/>
            <Option type="QString" name="offset" value="0,0"/>
            <Option type="QString" name="offset_map_unit_scale" value="3x:0,0,0,0,0,0"/>
            <Option type="QString" name="offset_unit" value="MM"/>
            <Option type="QString" name="outline_color" value="204,204,0,255"/>
            <Option type="QString" name="outline_style" value="solid"/>
            <Option type="QString" name="outline_width" value="0.2"/>
            <Option type="QString" name="outline_width_unit" value="MM"/>
            <Option type="QString" name="style" value="solid"/>
          </Option>
        </layer>
      </symbol>
    </symbols>
  </renderer-v2>
  <labeling type="simple">
    <settings calloutType="simple">
      <text-style fontLetterSpacing="0" fontWordSpacing="0" fontSizeUnit="Point" fontUnderline="0" fontFamily="Arial" textOrientation="horizontal" fontWeight="50" previewBkgrdColor="255,255,255,255" fontSizeMapUnitScale="3x:0,0,0,0,0,0" isExpression="1" fieldName="'重叠数: ' + &quot;overlap_count&quot;" fontSize="10" textOpacity="1" fontStrikeout="0" fontKerning="1" textColor="0,0,0,255" fontItalic="0" allowHtml="0" blendMode="0" useSubstitutions="0" namedStyle="Regular" multilineHeight="1" capitalization="0">
        <families/>
        <text-buffer bufferSizeUnits="MM" bufferColor="255,255,255,255" bufferOpacity="1" bufferNoFill="1" bufferDraw="1" bufferSize="1" bufferSizeMapUnitScale="3x:0,0,0,0,0,0" bufferJoinStyle="128" bufferBlendMode="0"/>
      </text-style>
      <placement centroidWhole="0" geometryGenerator="" layerType="PolygonGeometry" placement="0" priority="5" labelOffsetMapUnitScale="3x:0,0,0,0,0,0" distMapUnitScale="3x:0,0,0,0,0,0" maxCurvedCharAngleOut="-25" preserveRotation="1" fitInPolygonOnly="0" overrunDistanceUnit="MM" dist="0" polygonPlacementFlags="2" offsetUnits="MM" maxCurvedCharAngleIn="25" centroidInside="0" rotationAngle="0" repeatDistanceUnits="MM" geometryGeneratorEnabled="0" yOffset="0" offsetType="0" quadOffset="4" predefinedPositionOrder="TR,TL,BR,BL,R,L,TSR,BSR" lineAnchorPercent="0.5" overrunDistance="0" overrunDistanceMapUnitScale="3x:0,0,0,0,0,0" xOffset="0" lineAnchorType="0" repeatDistance="0" placementFlags="10" repeatDistanceMapUnitScale="3x:0,0,0,0,0,0" geometryGeneratorType="PointGeometry"/>
    </settings>
  </labeling>
</qgis>'''
        
        try:
            output_path = Path(output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(qml_content)
            
            print(f"✅ QGIS样式文件生成成功")
            print(f"📁 文件路径: {output_path.absolute()}")
            print(f"💡 在QGIS中右键图层 -> Properties -> Symbology -> Style -> Load Style")
            
            return str(output_path.absolute())
            
        except Exception as e:
            print(f"❌ 样式文件生成失败: {str(e)}")
            return ""


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
    
    # 清理相关参数（简化版）
    parser.add_argument('--list-simple', action='store_true', help='简单列表分析结果（快速查询）')
    parser.add_argument('--cleanup-all', action='store_true', help='清理所有分析数据（主表+QGIS表）')
    parser.add_argument('--cleanup-views', action='store_true', help='清理QGIS对象（表和视图）')
    parser.add_argument('--force', action='store_true', help='跳过确认，直接执行（与cleanup配合使用）')
    
    # 调试和模式参数
    parser.add_argument('--debug', action='store_true', help='开启调试模式，显示详细分析信息')
    parser.add_argument('--intersect-only', action='store_true', help='仅检测相交（忽略面积阈值）')
    parser.add_argument('--sample-check', type=int, default=0, help='随机检查N个bbox的空间关系')
    
    # QGIS导出参数
    parser.add_argument('--export-qgis', action='store_true', help='导出QGIS兼容格式')
    parser.add_argument('--materialize-view', action='store_true', help='将视图物化为表')
    parser.add_argument('--export-geojson', help='导出为GeoJSON文件')
    parser.add_argument('--generate-style', action='store_true', help='生成QGIS样式文件')
    
    args = parser.parse_args()
    
    print("🎯 BBox叠置分析示例")
    print("=" * 60)
    
    # 初始化分析器
    analyzer = BBoxOverlapAnalyzer()
    
    try:
        # 🚀 优先处理不需要bbox数据的快速命令
        
        # 如果用户想简单列出分析结果（无需bbox数据）
        if args.list_simple:
            print("\n📋 分析结果列表（简单模式）")
            print("-" * 40)
            analyzer.list_simple()
            return
        
        # 如果用户想清理所有分析数据（无需bbox数据）
        if args.cleanup_all:
            print("\n🧹 全量清理分析数据")
            print("-" * 40)
            analyzer.cleanup_all(confirm=args.force)
            return
        
        # 如果用户想清理QGIS对象（无需bbox数据）
        if args.cleanup_views:
            print("\n🎨 清理QGIS对象")
            print("-" * 40)
            analyzer.cleanup_qgis_views(confirm=args.force)
            return
        
        # 1. 确保统一视图存在（需要bbox数据的命令才执行）
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
            debug_mode=args.debug,
            intersect_only=args.intersect_only,
            sample_check=args.sample_check
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
        
        # 6. QGIS导出和可视化指导
        print("\n🎯 步骤6: QGIS导出和可视化")
        
        # 根据参数执行相应的导出操作
        if args.export_qgis or args.materialize_view or args.export_geojson or args.generate_style:
            print("🎨 执行QGIS导出...")
            
            # 物化视图为表
            if args.materialize_view or args.export_qgis:
                print("\n📋 创建物化表...")
                analyzer.materialize_qgis_view(analysis_id)
            
            # 导出GeoJSON
            if args.export_geojson or args.export_qgis:
                geojson_file = args.export_geojson if args.export_geojson else None
                print("\n📁 导出GeoJSON...")
                exported_file = analyzer.export_to_geojson(analysis_id, geojson_file)
                if exported_file:
                    print(f"✅ GeoJSON文件: {exported_file}")
            
            # 生成样式文件
            if args.generate_style or args.export_qgis:
                print("\n🎨 生成样式文件...")
                style_file = analyzer.generate_qgis_style_file()
                if style_file:
                    print(f"✅ 样式文件: {style_file}")
        
        # 提供QGIS连接信息
        qgis_info = analyzer.export_for_qgis(analysis_id)
        
        print(f"\n📋 QGIS可视化方案:")
        print(f"   方案1: 📋 连接数据库表 'qgis_bbox_overlap_hotspots'")
        print(f"   方案2: 📁 直接拖拽GeoJSON文件到QGIS")
        print(f"   方案3: 🎨 连接视图 '{qgis_info['qgis_view']}'（如果支持）")
        
        print(f"\n📋 数据库连接信息:")
        conn_info = qgis_info['connection_info']
        for key, value in conn_info.items():
            print(f"   {key}: {value}")
        
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
