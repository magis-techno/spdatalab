#!/usr/bin/env python3
# STATUS: one_time - 测试几何数据修复效果的临时验证脚本
"""
测试几何数据修复效果
验证spatial_analysis_results表中的几何数据是否正确填充
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig,
    export_analysis_to_qgis,
    get_qgis_connection_info
)
from sqlalchemy import create_engine, text
import pandas as pd
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_geometry_fix():
    """测试几何数据修复"""
    print("🔧 测试几何数据修复效果")
    print("=" * 50)
    
    # 1. 重新初始化表结构
    print("\n1. 重新初始化表结构...")
    config = SpatialJoinConfig()
    spatial_join = ProductionSpatialJoin(config)
    
    # 2. 导出一个简单的分析数据
    print("\n2. 导出测试分析数据...")
    analysis_id = export_analysis_to_qgis(
        analysis_type="intersection_type",
        city_filter="A253",  # 使用第一个城市
        include_geometry=True
    )
    print(f"✅ 导出分析ID: {analysis_id}")
    
    # 3. 检查几何数据
    print("\n3. 检查几何数据...")
    engine = create_engine(config.local_dsn)
    
    with engine.connect() as conn:
        # 检查表结构
        print("\n📋 表结构检查:")
        structure_sql = text(f"""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_name = '{config.analysis_results_table}'
            AND column_name = 'geometry';
        """)
        structure = conn.execute(structure_sql).fetchall()
        for col in structure:
            print(f"   geometry列: {col[0]} -> {col[1]} ({col[2]})")
        
        # 检查几何数据
        print("\n🗺️ 几何数据检查:")
        geom_check_sql = text(f"""
            SELECT 
                analysis_id,
                group_dimension,
                group_value,
                group_value_name,
                CASE 
                    WHEN geometry IS NULL THEN 'NULL'
                    WHEN LENGTH(geometry) = 0 THEN 'EMPTY'
                    WHEN LENGTH(geometry) < 20 THEN 'TOO_SHORT'
                    ELSE 'VALID'
                END as geometry_status,
                LEFT(geometry, 50) as geometry_sample
            FROM {config.analysis_results_table}
            WHERE analysis_id = '{analysis_id}'
            ORDER BY group_value;
        """)
        
        geom_data = pd.read_sql(geom_check_sql, conn)
        print(geom_data.to_string(index=False))
        
        # 统计几何数据
        print("\n📊 几何数据统计:")
        stats_sql = text(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END) as has_geometry,
                COUNT(CASE WHEN geometry IS NOT NULL AND LENGTH(geometry) > 20 THEN 1 END) as valid_geometry
            FROM {config.analysis_results_table}
            WHERE analysis_id = '{analysis_id}';
        """)
        
        stats = conn.execute(stats_sql).fetchone()
        print(f"   总记录数: {stats[0]}")
        print(f"   有几何数据: {stats[1]}")
        print(f"   有效几何数据: {stats[2]}")
        
        # 验证几何数据是否为有效WKT
        if stats[2] > 0:
            print("\n🔍 验证几何数据有效性:")
            validate_sql = text(f"""
                SELECT 
                    group_value_name,
                    CASE 
                        WHEN geometry LIKE 'POINT%' THEN 'POINT'
                        WHEN geometry LIKE 'POLYGON%' THEN 'POLYGON'
                        WHEN geometry LIKE 'MULTIPOLYGON%' THEN 'MULTIPOLYGON'
                        ELSE 'UNKNOWN'
                    END as geometry_type
                FROM {config.analysis_results_table}
                WHERE analysis_id = '{analysis_id}' AND geometry IS NOT NULL
                ORDER BY group_value;
            """)
            
            geom_types = pd.read_sql(validate_sql, conn)
            print(geom_types.to_string(index=False))
    
    # 4. 测试QGIS连接信息
    print("\n4. QGIS连接信息:")
    qgis_info = get_qgis_connection_info(config)
    print(f"   数据库: {qgis_info.get('database')}")
    print(f"   主机: {qgis_info.get('host')}:{qgis_info.get('port')}")
    print(f"   结果表: {qgis_info.get('results_table')}")
    
    # 5. 给出QGIS使用指导
    print("\n5. QGIS使用指导:")
    print("   a) 在QGIS中添加PostGIS连接，使用以下参数:")
    print(f"      Host: {qgis_info.get('host')}")
    print(f"      Port: {qgis_info.get('port')}")
    print(f"      Database: {qgis_info.get('database')}")
    print(f"      Username: {qgis_info.get('username')}")
    print("   b) 在浏览器面板中展开数据库连接")
    print(f"   c) 查找表: {qgis_info.get('results_table')}")
    
    if stats[2] > 0:
        print("   ✅ 几何数据已正确填充，应该可以在QGIS中看到")
    else:
        print("   ❌ 几何数据仍然为空，需要进一步调试")
    
    return stats[2] > 0

if __name__ == "__main__":
    success = test_geometry_fix()
    if success:
        print("\n🎉 几何数据修复成功！")
    else:
        print("\n❌ 几何数据修复失败，需要进一步调试") 