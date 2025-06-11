#!/usr/bin/env python3
"""
统一视图修复验证脚本

用于验证修复后的create_unified_view函数是否正常工作
"""

import sys
import os
sys.path.insert(0, 'src')

from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    create_unified_view,
    maintain_unified_view,
    list_bbox_tables
)

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def test_unified_view_creation():
    """测试统一视图创建功能"""
    print("🔧 测试统一视图创建修复")
    print("=" * 50)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 步骤1: 检查现有的bbox表
        print("\n步骤1: 检查现有bbox表")
        bbox_tables = list_bbox_tables(eng)
        
        if not bbox_tables:
            print("❌ 没有找到任何bbox表，无法测试统一视图")
            print("💡 提示: 请先运行分表模式处理以创建一些分表")
            return False
        
        print(f"✅ 找到 {len(bbox_tables)} 个bbox表:")
        for table in bbox_tables:
            print(f"   - {table}")
        
        # 步骤2: 测试统一视图创建
        print("\n步骤2: 创建测试统一视图")
        test_view_name = "clips_bbox_unified_test_fix"
        
        success = create_unified_view(eng, test_view_name)
        
        if success:
            print(f"✅ 统一视图 {test_view_name} 创建成功")
        else:
            print(f"❌ 统一视图 {test_view_name} 创建失败")
            return False
        
        # 步骤3: 验证视图查询
        print("\n步骤3: 验证统一视图查询")
        test_query_sql = text(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT subdataset_name) as subdataset_count,
                COUNT(DISTINCT source_table) as table_count
            FROM {test_view_name};
        """)
        
        with eng.connect() as conn:
            result = conn.execute(test_query_sql)
            row = result.fetchone()
            
            print(f"   查询结果:")
            print(f"   - 总记录数: {row[0]}")
            print(f"   - 子数据集数: {row[1]}")
            print(f"   - 源表数: {row[2]}")
        
        # 步骤4: 测试geometry列查询
        print("\n步骤4: 测试geometry列查询")
        geometry_query_sql = text(f"""
            SELECT 
                ST_GeometryType(geometry) as geom_type,
                COUNT(*) as count
            FROM {test_view_name}
            WHERE geometry IS NOT NULL
            GROUP BY ST_GeometryType(geometry)
            LIMIT 5;
        """)
        
        with eng.connect() as conn:
            result = conn.execute(geometry_query_sql)
            rows = result.fetchall()
            
            if rows:
                print(f"   几何类型统计:")
                for row in rows:
                    print(f"   - {row[0]}: {row[1]} 条记录")
            else:
                print("   - 没有几何数据或所有几何数据为NULL")
        
        # 步骤5: 清理测试视图
        print("\n步骤5: 清理测试视图")
        cleanup_sql = text(f"DROP VIEW IF EXISTS {test_view_name};")
        
        with eng.connect() as conn:
            conn.execute(cleanup_sql)
            conn.commit()
        
        print(f"✅ 测试视图 {test_view_name} 已清理")
        
        print("\n" + "=" * 50)
        print("🎉 统一视图修复验证成功！")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_maintain_view():
    """测试视图维护功能"""
    print("\n🔄 测试视图维护功能")
    print("-" * 30)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 测试维护默认视图
        success = maintain_unified_view(eng, "clips_bbox_unified")
        
        if success:
            print("✅ 视图维护成功")
        else:
            print("❌ 视图维护失败")
            return False
        
        # 验证维护后的视图
        verify_sql = text("""
            SELECT COUNT(*) FROM clips_bbox_unified;
        """)
        
        with eng.connect() as conn:
            result = conn.execute(verify_sql)
            count = result.scalar()
            print(f"✅ 维护后的视图包含 {count} 条记录")
        
        return True
        
    except Exception as e:
        print(f"❌ 视图维护测试失败: {str(e)}")
        return False

def debug_view_sql():
    """调试统一视图SQL生成"""
    print("\n🔍 调试统一视图SQL生成")
    print("-" * 30)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        bbox_tables = list_bbox_tables(eng)
        
        if not bbox_tables:
            print("没有bbox表可供调试")
            return
        
        print("生成的SQL预览:")
        print("-" * 20)
        
        union_parts = []
        for table_name in bbox_tables[:2]:  # 只显示前2个表
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
            print(f"表 {table_name} 的查询部分:")
            print(union_part)
            print("-" * 20)
        
        if len(bbox_tables) > 2:
            print(f"... 还有 {len(bbox_tables) - 2} 个表的查询部分")
        
    except Exception as e:
        print(f"调试失败: {str(e)}")

def main():
    """主函数"""
    print("🧪 统一视图修复验证测试")
    print("=" * 60)
    
    # 调试SQL生成
    debug_view_sql()
    
    # 测试统一视图创建
    create_success = test_unified_view_creation()
    
    if not create_success:
        print("\n❌ 统一视图创建测试失败，终止测试")
        sys.exit(1)
    
    # 测试视图维护
    maintain_success = test_maintain_view()
    
    if maintain_success:
        print("\n🎉 所有测试通过！统一视图修复成功")
    else:
        print("\n❌ 视图维护测试失败")
        sys.exit(1)

if __name__ == "__main__":
    main() 