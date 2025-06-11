#!/usr/bin/env python3
"""
QGIS兼容性测试脚本

验证创建的视图是否符合QGIS的要求
"""

import os
import sys
sys.path.insert(0, 'src')

from spdatalab.db import get_psql_engine
from spdatalab.dataset.bbox import (
    list_bbox_tables,
    create_qgis_compatible_unified_view,
    create_materialized_unified_view,
    refresh_materialized_view
)
from sqlalchemy import text
import pandas as pd


def test_qgis_compatibility():
    """测试QGIS兼容性"""
    
    print("🧪 开始QGIS兼容性测试...")
    
    # 获取数据库连接
    eng = get_psql_engine()
    
    # 检查现有分表
    print("\n1️⃣ 检查现有分表...")
    tables = list_bbox_tables(eng)
    print(f"发现 {len(tables)} 个bbox表:")
    for table in tables:
        print(f"  - {table}")
    
    if len(tables) < 2:
        print("❌ 分表不足，请先运行分表处理流程")
        return False
    
    # 测试创建QGIS兼容视图
    print("\n2️⃣ 测试创建QGIS兼容视图...")
    success = create_qgis_compatible_unified_view(eng, 'test_qgis_view')
    
    if not success:
        print("❌ 创建QGIS兼容视图失败")
        return False
    
    # 验证视图结构
    print("\n3️⃣ 验证视图结构...")
    try:
        with eng.connect() as conn:
            # 检查视图是否存在
            check_view_sql = text("""
                SELECT COUNT(*) 
                FROM information_schema.views 
                WHERE table_name = 'test_qgis_view'
            """)
            result = conn.execute(check_view_sql).scalar()
            
            if result == 0:
                print("❌ 视图不存在")
                return False
            
            print("✅ 视图存在")
            
            # 检查视图列
            check_columns_sql = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'test_qgis_view'
                ORDER BY ordinal_position
            """)
            columns = conn.execute(check_columns_sql).fetchall()
            
            print("📋 视图列结构:")
            required_columns = {'qgis_id', 'geometry', 'original_id', 'subdataset_name'}
            found_columns = set()
            
            for col_name, col_type in columns:
                print(f"  - {col_name}: {col_type}")
                found_columns.add(col_name)
            
            # 检查必需列
            missing_columns = required_columns - found_columns
            if missing_columns:
                print(f"❌ 缺少必需列: {missing_columns}")
                return False
            
            print("✅ 所有必需列都存在")
            
            # 检查数据
            sample_sql = text("SELECT COUNT(*) FROM test_qgis_view")
            count = conn.execute(sample_sql).scalar()
            print(f"📊 视图总记录数: {count}")
            
            if count == 0:
                print("❌ 视图没有数据")
                return False
            
            # 检查qgis_id唯一性
            unique_check_sql = text("""
                SELECT COUNT(*) as total, COUNT(DISTINCT qgis_id) as unique_count
                FROM test_qgis_view
            """)
            result = conn.execute(unique_check_sql).fetchone()
            
            if result.total != result.unique_count:
                print(f"❌ qgis_id不唯一: 总数={result.total}, 唯一数={result.unique_count}")
                return False
            
            print("✅ qgis_id唯一性检查通过")
            
            # 样本数据查看
            sample_data_sql = text("SELECT * FROM test_qgis_view LIMIT 3")
            sample_data = pd.read_sql(sample_data_sql, conn)
            print("\n📋 样本数据:")
            print(sample_data[['qgis_id', 'original_id', 'subdataset_name', 'source_table']].to_string())
    
    except Exception as e:
        print(f"❌ 视图验证失败: {str(e)}")
        return False
    
    # 测试物化视图
    print("\n4️⃣ 测试创建物化视图...")
    success = create_materialized_unified_view(eng, 'test_qgis_mat_view')
    
    if not success:
        print("❌ 创建物化视图失败")
        return False
    
    # 验证物化视图
    print("\n5️⃣ 验证物化视图...")
    try:
        with eng.connect() as conn:
            # 检查物化视图
            check_mat_view_sql = text("""
                SELECT COUNT(*) 
                FROM pg_matviews 
                WHERE matviewname = 'test_qgis_mat_view'
            """)
            result = conn.execute(check_mat_view_sql).scalar()
            
            if result == 0:
                print("❌ 物化视图不存在")
                return False
            
            print("✅ 物化视图存在")
            
            # 检查数据
            mat_count_sql = text("SELECT COUNT(*) FROM test_qgis_mat_view")
            mat_count = conn.execute(mat_count_sql).scalar()
            print(f"📊 物化视图总记录数: {mat_count}")
            
            # 检查索引
            check_indexes_sql = text("""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = 'test_qgis_mat_view'
            """)
            indexes = conn.execute(check_indexes_sql).fetchall()
            
            print("📋 物化视图索引:")
            for idx_name, idx_def in indexes:
                print(f"  - {idx_name}")
            
            # 验证空间索引
            spatial_index_exists = any('gist' in idx_def.lower() for _, idx_def in indexes)
            if not spatial_index_exists:
                print("⚠️  警告：未找到空间索引")
            else:
                print("✅ 空间索引存在")
    
    except Exception as e:
        print(f"❌ 物化视图验证失败: {str(e)}")
        return False
    
    # 测试刷新物化视图
    print("\n6️⃣ 测试刷新物化视图...")
    success = refresh_materialized_view(eng, 'test_qgis_mat_view')
    
    if not success:
        print("❌ 刷新物化视图失败")
        return False
    
    # 清理测试视图
    print("\n7️⃣ 清理测试视图...")
    try:
        with eng.connect() as conn:
            cleanup_sql = text("DROP VIEW IF EXISTS test_qgis_view")
            conn.execute(cleanup_sql)
            
            cleanup_mat_sql = text("DROP MATERIALIZED VIEW IF EXISTS test_qgis_mat_view")
            conn.execute(cleanup_mat_sql)
            
            conn.commit()
        
        print("✅ 测试视图清理完成")
    
    except Exception as e:
        print(f"⚠️  清理失败: {str(e)}")
    
    print("\n🎉 QGIS兼容性测试全部通过！")
    return True


def print_qgis_usage_guide():
    """打印QGIS使用指南"""
    
    print("\n" + "="*60)
    print("📋 QGIS使用指南")
    print("="*60)
    
    print("\n🔧 创建QGIS兼容视图：")
    print("python -m spdatalab.cli --create-qgis-view")
    print("python -m spdatalab.cli --create-qgis-view --view-name my_custom_view")
    
    print("\n🔧 创建物化视图（推荐用于大数据）：")
    print("python -m spdatalab.cli --create-materialized-view")
    print("python -m spdatalab.cli --create-materialized-view --view-name my_mat_view")
    
    print("\n🔧 刷新物化视图：")
    print("python -m spdatalab.cli --refresh-materialized-view")
    print("python -m spdatalab.cli --refresh-materialized-view --view-name my_mat_view")
    
    print("\n📍 在QGIS中加载数据的步骤：")
    print("1. Layer → Add Layer → Add PostGIS Layers")
    print("2. 创建数据库连接（如果尚未创建）")
    print("3. 在表列表中找到你的视图")
    print("4. 🔑 重要：在'Primary key'列中选择 'qgis_id'")
    print("5. 在'Geometry column'中选择 'geometry'")
    print("6. 点击'Add'加载图层")
    
    print("\n💡 QGIS兼容性要点：")
    print("- 使用qgis_id作为主键（全局唯一整数）")
    print("- 原始表ID保存在original_id列中")
    print("- subdataset_name列标识数据来源")
    print("- source_table列标识源表名")
    
    print("\n⚠️  注意事项：")
    print("- 普通视图：数据实时更新，但查询较慢")
    print("- 物化视图：查询快速，但需要手动刷新")
    print("- 大数据量建议使用物化视图")


if __name__ == "__main__":
    success = test_qgis_compatibility()
    
    if success:
        print_qgis_usage_guide()
    else:
        print("\n❌ 测试失败，请检查错误信息")
        sys.exit(1) 