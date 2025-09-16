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
                            MIN(created_at) as earliest_data,
                            MAX(created_at) as latest_data
                        FROM {self.unified_view} 
                        WHERE created_at IS NOT NULL;
                    """)
                    sample_result = conn.execute(sample_sql).fetchone()
                    if sample_result:
                        print(f"📈 数据概况: {sample_result.subdataset_count} 个子数据集, {sample_result.city_count} 个城市")
                    
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
        top_n: int = 20
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
        
        print(f"🚀 开始叠置分析: {analysis_id}")
        print(f"参数: city_filter={city_filter}, min_overlap_area={min_overlap_area}, top_n={top_n}")
        
        # 构建过滤条件
        where_conditions = []
        if city_filter:
            where_conditions.append(f"a.city_id = '{city_filter}' AND b.city_id = '{city_filter}'")
        
        if subdataset_filter:
            subdataset_list = "', '".join(subdataset_filter)
            where_conditions.append(f"a.subdataset_name IN ('{subdataset_list}') AND b.subdataset_name IN ('{subdataset_list}')")
        
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
                    AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > {min_overlap_area}
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
            
            with self.engine.connect() as conn:
                result = conn.execute(text(analysis_sql))
                conn.commit()
                
                # 获取插入的记录数
                count_sql = text(f"SELECT COUNT(*) FROM {self.analysis_table} WHERE analysis_id = '{analysis_id}';")
                count_result = conn.execute(count_sql)
                inserted_count = count_result.scalar()
                
            print(f"✅ 叠置分析完成，发现 {inserted_count} 个重叠热点")
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


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='BBox叠置分析')
    parser.add_argument('--city', help='城市过滤')
    parser.add_argument('--subdatasets', nargs='+', help='子数据集过滤')
    parser.add_argument('--min-overlap-area', type=float, default=0.0, help='最小重叠面积阈值')
    parser.add_argument('--top-n', type=int, default=20, help='返回的热点数量')
    parser.add_argument('--analysis-id', help='自定义分析ID')
    parser.add_argument('--refresh-view', action='store_true', help='强制刷新统一视图（适用于数据更新后）')
    
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
        
        # 2. 创建分析结果表
        print("\n🛠️ 步骤2: 准备分析环境")
        if not analyzer.create_analysis_table():
            print("❌ 分析表创建失败，退出")
            return
        
        # 3. 执行叠置分析
        print("\n🚀 步骤3: 执行叠置分析")
        analysis_id = analyzer.run_overlap_analysis(
            analysis_id=args.analysis_id,
            city_filter=args.city,
            subdataset_filter=args.subdatasets,
            min_overlap_area=args.min_overlap_area,
            top_n=args.top_n
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
