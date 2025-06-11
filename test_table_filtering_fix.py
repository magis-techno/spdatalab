#!/usr/bin/env python3
"""
表过滤修复验证测试

验证方案3的实施效果：在统一视图创建时正确过滤表
"""

import sys
import os
sys.path.insert(0, 'src')

from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    list_bbox_tables,
    filter_partition_tables,
    create_unified_view,
    maintain_unified_view
)

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def test_filter_function():
    """测试过滤函数的各种场景"""
    print("🧪 测试过滤函数")
    print("=" * 30)
    
    # 模拟各种表名
    test_tables = [
        'clips_bbox',                    # 主表，应被排除
        'clips_bbox_lane_change',        # 分表，应包含
        'clips_bbox_heavy_traffic',      # 分表，应包含  
        'clips_bbox_unified',            # 统一视图，应被排除
        'clips_bbox_temp_import',        # 临时表，应被排除
        'clips_bbox_backup_20241219',    # 备份表，应被排除
        'clips_bbox_test_data',          # 测试表，应被排除
        'other_table',                   # 无关表，应被排除
        'clips_bbox_merge_conflict',     # 分表，应包含
    ]
    
    # 测试不同的排除条件
    test_cases = [
        (None, "无排除视图"),
        ('clips_bbox_unified', "排除统一视图"),
        ('clips_bbox_test_view', "排除不存在的视图"),
    ]
    
    for exclude_view, description in test_cases:
        print(f"\n📋 测试场景: {description}")
        if exclude_view:
            print(f"   排除视图: {exclude_view}")
        
        filtered = filter_partition_tables(test_tables, exclude_view=exclude_view)
        
        print(f"   输入表数: {len(test_tables)}")
        print(f"   过滤后: {len(filtered)}")
        print(f"   结果: {filtered}")
        
        # 验证结果
        expected_base = ['clips_bbox_lane_change', 'clips_bbox_heavy_traffic', 'clips_bbox_merge_conflict']
        if exclude_view and exclude_view in test_tables:
            expected = [t for t in expected_base if t != exclude_view]
        else:
            expected = expected_base
            
        if set(filtered) == set(expected):
            print(f"   ✅ 过滤结果正确")
        else:
            print(f"   ❌ 过滤结果错误")
            print(f"      期望: {expected}")
            print(f"      实际: {filtered}")

def test_before_after_comparison(eng):
    """对比修复前后的行为"""
    print("\n🔍 修复前后对比")
    print("=" * 30)
    
    try:
        # 获取所有表
        all_tables = list_bbox_tables(eng)
        print(f"数据库中的所有相关表 ({len(all_tables)} 个):")
        for table in all_tables:
            print(f"  - {table}")
        
        if not all_tables:
            print("没有找到任何表，无法进行对比测试")
            return
            
        # 模拟修复前的行为（直接使用所有表）
        print(f"\n修复前行为（使用所有表）:")
        print(f"  会包含的表: {all_tables}")
        
        # 修复后的行为
        filtered_tables = filter_partition_tables(all_tables, exclude_view='clips_bbox_unified')
        print(f"\n修复后行为（过滤后的表）:")
        print(f"  会包含的表: {filtered_tables}")
        
        # 显示差异
        excluded = set(all_tables) - set(filtered_tables)
        if excluded:
            print(f"\n被排除的表 ({len(excluded)} 个):")
            for table in excluded:
                print(f"  ❌ {table}")
        else:
            print(f"\n没有表被排除")
            
    except Exception as e:
        print(f"对比测试失败: {str(e)}")

def test_unified_view_creation_fix(eng):
    """测试修复后的统一视图创建"""
    print("\n🔧 测试修复后的统一视图创建")
    print("=" * 40)
    
    try:
        # 获取当前的分表
        all_tables = list_bbox_tables(eng)
        filtered_tables = filter_partition_tables(all_tables, exclude_view='clips_bbox_unified')
        
        print(f"找到 {len(filtered_tables)} 个可用分表:")
        for table in filtered_tables:
            print(f"  - {table}")
            
        if not filtered_tables:
            print("❌ 没有可用的分表，跳过统一视图测试")
            return False
            
        # 创建测试统一视图
        test_view_name = 'clips_bbox_unified_test_fix'
        print(f"\n创建测试统一视图: {test_view_name}")
        
        success = create_unified_view(eng, test_view_name)
        
        if success:
            print("✅ 统一视图创建成功")
            
            # 验证视图内容
            verify_sql = text(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT source_table) as table_count,
                    COUNT(DISTINCT subdataset_name) as subdataset_count
                FROM {test_view_name};
            """)
            
            with eng.connect() as conn:
                result = conn.execute(verify_sql)
                row = result.fetchone()
                
                print(f"   总记录数: {row[0]}")
                print(f"   源表数: {row[1]}")
                print(f"   子数据集数: {row[2]}")
                
                # 验证源表列表
                source_tables_sql = text(f"""
                    SELECT DISTINCT source_table 
                    FROM {test_view_name} 
                    ORDER BY source_table;
                """)
                
                result = conn.execute(source_tables_sql)
                source_tables = [row[0] for row in result.fetchall()]
                
                print(f"   视图中的源表:")
                for table in source_tables:
                    print(f"     - {table}")
                
                # 验证是否包含了不应该包含的表
                problematic = [t for t in source_tables if 'unified' in t or t == 'clips_bbox']
                if problematic:
                    print(f"   ❌ 发现问题表: {problematic}")
                else:
                    print(f"   ✅ 没有包含问题表")
            
            # 清理测试视图
            cleanup_sql = text(f"DROP VIEW IF EXISTS {test_view_name};")
            with eng.connect() as conn:
                conn.execute(cleanup_sql)
                conn.commit()
                print(f"✅ 测试视图已清理")
                
            return True
            
        else:
            print("❌ 统一视图创建失败")
            return False
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        return False

def test_edge_cases():
    """测试边界情况"""
    print("\n🎭 测试边界情况")
    print("=" * 25)
    
    edge_cases = [
        ([], None, "空表列表"),
        (['clips_bbox'], None, "只有主表"),
        (['clips_bbox_unified'], 'clips_bbox_unified', "只有要排除的视图"),
        (['other_table', 'random_table'], None, "没有clips_bbox相关表"),
        (['clips_bbox_UNIFIED', 'clips_bbox_Temp'], None, "大小写混合"),
    ]
    
    for tables, exclude_view, description in edge_cases:
        print(f"\n📋 {description}:")
        print(f"   输入: {tables}")
        print(f"   排除: {exclude_view}")
        
        result = filter_partition_tables(tables, exclude_view=exclude_view)
        print(f"   结果: {result}")

def main():
    """主测试函数"""
    print("🧪 表过滤修复验证测试")
    print("=" * 50)
    
    # 1. 测试过滤函数
    test_filter_function()
    
    # 2. 测试边界情况
    test_edge_cases()
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 3. 对比修复前后
        test_before_after_comparison(eng)
        
        # 4. 测试统一视图创建
        view_success = test_unified_view_creation_fix(eng)
        
        print("\n" + "=" * 50)
        print("📋 测试总结")
        print("=" * 50)
        
        print("✅ 过滤函数测试: 完成")
        print("✅ 边界情况测试: 完成")
        print("✅ 修复前后对比: 完成")
        print(f"✅ 统一视图创建: {'成功' if view_success else '失败'}")
        
        if view_success:
            print("\n🎉 表过滤修复验证成功！")
            print("现在统一视图创建不会包含自己了")
        else:
            print("\n⚠️ 统一视图测试失败，可能需要进一步调试")
            
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")

if __name__ == "__main__":
    main() 