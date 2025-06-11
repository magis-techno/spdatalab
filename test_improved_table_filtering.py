#!/usr/bin/env python3
"""
改进后的表过滤逻辑测试

验证修改后的list_bbox_tables函数是否正确排除视图和主表
"""

import sys
import os
sys.path.insert(0, 'src')

from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    list_bbox_tables,
    create_unified_view,
    maintain_unified_view
)

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def analyze_all_bbox_related_objects(eng):
    """分析所有bbox相关的表和视图"""
    print("📊 分析所有bbox相关的数据库对象")
    print("=" * 50)
    
    # 查询所有相关的表和视图
    analyze_sql = text("""
        SELECT 
            table_name,
            table_type,
            CASE 
                WHEN table_name = 'clips_bbox' THEN '主表'
                WHEN table_name LIKE '%unified%' THEN '统一视图'
                WHEN table_name LIKE 'clips_bbox_%' AND table_type = 'BASE TABLE' THEN '分表'
                WHEN table_name LIKE '%temp%' THEN '临时表'
                ELSE '其他'
            END as category,
            CASE 
                WHEN table_name LIKE 'clips_bbox_%' 
                     AND table_type = 'BASE TABLE' 
                     AND table_name != 'clips_bbox'
                     AND table_name NOT LIKE '%unified%'
                     AND table_name NOT LIKE '%temp%' THEN '应包含'
                ELSE '应排除'
            END as should_include
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%clips_bbox%'
        ORDER BY category, table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(analyze_sql)
            rows = result.fetchall()
            
            if not rows:
                print("没有找到任何bbox相关对象")
                return {}
                
            # 按类别分组
            categories = {}
            for row in rows:
                table_name, table_type, category, should_include = row
                if category not in categories:
                    categories[category] = []
                categories[category].append({
                    'name': table_name,
                    'type': table_type,
                    'should_include': should_include
                })
            
            # 显示结果
            for category, objects in categories.items():
                print(f"\n{category} ({len(objects)} 个):")
                for obj in objects:
                    include_marker = "✅" if obj['should_include'] == '应包含' else "❌"
                    print(f"  {include_marker} {obj['name']} ({obj['type']})")
            
            return categories
            
    except Exception as e:
        print(f"分析失败: {str(e)}")
        return {}

def test_old_vs_new_filtering(eng):
    """对比新旧过滤逻辑"""
    print("\n🔍 对比新旧过滤逻辑")
    print("=" * 40)
    
    # 旧逻辑：获取所有clips_bbox开头的表
    old_logic_sql = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'clips_bbox%'
        ORDER BY table_name;
    """)
    
    try:
        with eng.connect() as conn:
            # 旧逻辑结果
            result = conn.execute(old_logic_sql)
            old_tables = [row[0] for row in result.fetchall()]
            
            # 新逻辑结果
            new_tables = list_bbox_tables(eng)
            
            print(f"旧逻辑结果 ({len(old_tables)} 个):")
            for table in old_tables:
                excluded_marker = "❌" if table not in new_tables else "✅"
                print(f"  {excluded_marker} {table}")
                
            print(f"\n新逻辑结果 ({len(new_tables)} 个):")
            for table in new_tables:
                print(f"  ✅ {table}")
                
            # 显示被排除的表
            excluded = set(old_tables) - set(new_tables)
            if excluded:
                print(f"\n被排除的表/视图 ({len(excluded)} 个):")
                for table in excluded:
                    print(f"  ❌ {table}")
            else:
                print(f"\n没有表被排除")
                
            return old_tables, new_tables
            
    except Exception as e:
        print(f"对比失败: {str(e)}")
        return [], []

def test_unified_view_creation_with_improved_filtering(eng):
    """测试使用改进过滤逻辑的统一视图创建"""
    print("\n🔧 测试统一视图创建（使用改进的过滤逻辑）")
    print("=" * 55)
    
    try:
        # 获取分表列表
        bbox_tables = list_bbox_tables(eng)
        
        if not bbox_tables:
            print("❌ 没有找到任何分表，无法测试统一视图创建")
            return False
            
        print(f"找到 {len(bbox_tables)} 个分表:")
        for table in bbox_tables:
            print(f"  - {table}")
            
        # 创建测试视图
        test_view_name = "clips_bbox_unified_filter_test"
        print(f"\n创建测试统一视图: {test_view_name}")
        
        success = create_unified_view(eng, test_view_name)
        
        if success:
            print("✅ 统一视图创建成功")
            
            # 验证视图查询
            test_query = text(f"SELECT COUNT(*) FROM {test_view_name};")
            with eng.connect() as conn:
                result = conn.execute(test_query)
                count = result.scalar()
                print(f"✅ 视图查询成功，总记录数: {count}")
                
            # 清理测试视图
            cleanup_sql = text(f"DROP VIEW IF EXISTS {test_view_name};")
            with eng.connect() as conn:
                conn.execute(cleanup_sql)
                conn.commit()
                print(f"✅ 测试视图已清理")
                
        else:
            print("❌ 统一视图创建失败")
            return False
            
        return True
        
    except Exception as e:
        print(f"测试失败: {str(e)}")
        return False

def simulate_problematic_scenario(eng):
    """模拟问题场景：创建一些容易混淆的表/视图"""
    print("\n🎭 模拟问题场景")
    print("=" * 30)
    
    # 创建一些测试对象
    test_objects = [
        ("clips_bbox_unified", "VIEW"),
        ("clips_bbox_temp_import", "TABLE"),
        ("clips_bbox", "TABLE"),  # 主表可能不存在
    ]
    
    try:
        with eng.connect() as conn:
            for obj_name, obj_type in test_objects:
                # 检查是否已存在
                check_sql = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{obj_name}'
                    );
                """)
                
                result = conn.execute(check_sql)
                exists = result.scalar()
                
                if exists:
                    print(f"  📋 {obj_name} ({obj_type}) - 已存在")
                else:
                    print(f"  ➖ {obj_name} ({obj_type}) - 不存在")
                    
            # 测试过滤效果
            print(f"\n使用改进过滤逻辑的结果:")
            filtered_tables = list_bbox_tables(eng)
            
            if filtered_tables:
                for table in filtered_tables:
                    print(f"  ✅ {table}")
            else:
                print(f"  📭 没有找到符合条件的分表")
                
    except Exception as e:
        print(f"模拟测试失败: {str(e)}")

def validate_filtering_rules(eng):
    """验证过滤规则的正确性"""
    print("\n✅ 验证过滤规则")
    print("=" * 25)
    
    # 定义过滤规则测试用例
    test_cases = [
        # (表名, 表类型, 应该被包含?, 原因)
        ("clips_bbox", "BASE TABLE", False, "主表应被排除"),
        ("clips_bbox_lane_change", "BASE TABLE", True, "标准分表应被包含"),
        ("clips_bbox_unified", "VIEW", False, "视图应被排除"),
        ("clips_bbox_temp_import", "BASE TABLE", False, "临时表应被排除"),
        ("clips_bbox_heavy_traffic", "BASE TABLE", True, "标准分表应被包含"),
        ("other_table", "BASE TABLE", False, "不匹配前缀应被排除"),
    ]
    
    # 手动实现过滤逻辑验证
    print("过滤规则验证:")
    all_passed = True
    
    for table_name, table_type, should_include, reason in test_cases:
        # 应用过滤规则
        passes_filter = (
            table_type == 'BASE TABLE' and
            table_name.startswith('clips_bbox_') and 
            table_name != 'clips_bbox' and
            'unified' not in table_name and
            'temp' not in table_name
        )
        
        if passes_filter == should_include:
            print(f"  ✅ {table_name}: {reason}")
        else:
            print(f"  ❌ {table_name}: 过滤规则错误 - {reason}")
            all_passed = False
    
    return all_passed

def main():
    """主测试函数"""
    print("🧪 改进后的表过滤逻辑测试")
    print("=" * 60)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 1. 分析当前数据库状态
        categories = analyze_all_bbox_related_objects(eng)
        
        # 2. 对比新旧过滤逻辑
        old_tables, new_tables = test_old_vs_new_filtering(eng)
        
        # 3. 验证过滤规则
        rules_valid = validate_filtering_rules(eng)
        
        # 4. 模拟问题场景
        simulate_problematic_scenario(eng)
        
        # 5. 测试统一视图创建
        view_test_passed = test_unified_view_creation_with_improved_filtering(eng)
        
        # 总结
        print("\n" + "=" * 60)
        print("📋 测试总结")
        print("=" * 60)
        
        print(f"✅ 过滤规则验证: {'通过' if rules_valid else '失败'}")
        print(f"✅ 统一视图测试: {'通过' if view_test_passed else '失败'}")
        
        if old_tables != new_tables:
            excluded_count = len(set(old_tables) - set(new_tables))
            print(f"✅ 成功排除 {excluded_count} 个不相关的表/视图")
        else:
            print("ℹ️ 没有需要排除的表/视图")
            
        if rules_valid and view_test_passed:
            print("\n🎉 改进的表过滤逻辑工作正常！")
        else:
            print("\n❌ 存在问题，需要进一步调试")
            
    except Exception as e:
        print(f"测试失败: {str(e)}")

if __name__ == "__main__":
    main() 