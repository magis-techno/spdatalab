#!/usr/bin/env python3
"""
表管理方案对比和改进建议

展示不同的解决方案来避免统一视图被误包含在分表列表中
"""

import sys
import os
sys.path.insert(0, 'src')

from sqlalchemy import create_engine, text

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def current_list_bbox_tables_demo(eng):
    """当前的表查找逻辑（存在问题）"""
    print("🔍 当前的表查找逻辑（存在问题）:")
    print("-" * 40)
    
    list_tables_sql = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'clips_bbox%'
        ORDER BY table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(list_tables_sql)
            tables = [row[0] for row in result.fetchall()]
            
            print(f"找到 {len(tables)} 个表/视图:")
            for table in tables:
                print(f"  - {table}")
            
            # 指出问题
            problematic = [t for t in tables if 'unified' in t or t == 'clips_bbox']
            if problematic:
                print(f"\n⚠️ 问题表/视图:")
                for table in problematic:
                    print(f"  - {table} (应该被排除)")
                    
            return tables
    except Exception as e:
        print(f"查询失败: {str(e)}")
        return []

def improved_list_bbox_tables_v1(eng):
    """改进方案1: 排除视图和主表"""
    print("\n🔧 改进方案1: 排除视图和主表")
    print("-" * 40)
    
    list_tables_sql = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'  -- 只要表，不要视图
        AND table_name LIKE 'clips_bbox_%'  -- 必须有下划线
        AND table_name != 'clips_bbox'  -- 排除主表
        AND table_name NOT LIKE '%unified%'  -- 排除包含unified的表
        ORDER BY table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(list_tables_sql)
            tables = [row[0] for row in result.fetchall()]
            
            print(f"找到 {len(tables)} 个分表:")
            for table in tables:
                print(f"  - {table}")
                
            return tables
    except Exception as e:
        print(f"查询失败: {str(e)}")
        return []

def improved_list_bbox_tables_v2(eng):
    """改进方案2: 基于元数据标识"""
    print("\n🔧 改进方案2: 基于元数据标识")
    print("-" * 40)
    
    # 这个方案需要在创建表时添加注释标识
    list_tables_sql = text("""
        SELECT t.table_name, 
               pg_description.description
        FROM information_schema.tables t
        LEFT JOIN pg_class ON pg_class.relname = t.table_name
        LEFT JOIN pg_description ON pg_description.objoid = pg_class.oid
        WHERE t.table_schema = 'public' 
        AND t.table_type = 'BASE TABLE'
        AND t.table_name LIKE 'clips_bbox_%'
        AND (pg_description.description LIKE 'subdataset_table:%' OR pg_description.description IS NULL)
        ORDER BY t.table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(list_tables_sql)
            rows = result.fetchall()
            
            tables = []
            print(f"找到 {len(rows)} 个表:")
            for row in rows:
                table_name, description = row
                if description and 'subdataset_table:' in description:
                    tables.append(table_name)
                    print(f"  - {table_name} (有标识: {description})")
                elif description is None and table_name != 'clips_bbox':
                    # 没有描述但符合命名规则的表
                    tables.append(table_name)
                    print(f"  - {table_name} (无标识，按命名规则包含)")
                else:
                    print(f"  - {table_name} (跳过: {description or '无描述'})")
                
            return tables
    except Exception as e:
        print(f"查询失败: {str(e)}")
        return []

def schema_based_solution_demo():
    """方案3: 基于Schema的解决方案（概念演示）"""
    print("\n🏗️ 方案3: 基于Schema的解决方案（概念演示）")
    print("-" * 50)
    
    print("概念:")
    print("  - 创建专门的schema: clips_bbox_partitions")
    print("  - 子数据集表: clips_bbox_partitions.lane_change")
    print("  - 统一视图: public.clips_bbox_unified")
    print("  - 主表: public.clips_bbox")
    
    print("\n优点:")
    print("  ✅ 完全隔离，不会误包含")
    print("  ✅ 清晰的组织结构")
    print("  ✅ 方便权限管理")
    
    print("\n缺点:")
    print("  ⚠️ 需要创建和管理额外的schema")
    print("  ⚠️ 跨schema查询可能稍微复杂")
    print("  ⚠️ QGIS等工具可能需要额外配置")
    
    # 演示SQL
    demo_sqls = [
        "CREATE SCHEMA IF NOT EXISTS clips_bbox_partitions;",
        "CREATE TABLE clips_bbox_partitions.lane_change (...);",
        "CREATE VIEW public.clips_bbox_unified AS ...",
        "SELECT * FROM clips_bbox_partitions.lane_change;",
    ]
    
    print(f"\n示例SQL:")
    for sql in demo_sqls:
        print(f"  {sql}")

def naming_convention_solution_demo():
    """方案4: 命名约定解决方案（概念演示）"""
    print("\n📝 方案4: 严格命名约定解决方案（概念演示）")
    print("-" * 55)
    
    print("命名约定:")
    print("  - 分表前缀: clips_bbox_part_")
    print("  - 统一视图: clips_bbox_unified")
    print("  - 主表: clips_bbox")  
    print("  - 临时表: clips_bbox_temp_*")
    
    print("\n示例:")
    print("  - clips_bbox_part_lane_change")
    print("  - clips_bbox_part_heavy_traffic")
    print("  - clips_bbox_unified (视图)")
    print("  - clips_bbox (主表)")
    
    print("\n查询逻辑:")
    print("  SELECT table_name FROM information_schema.tables")
    print("  WHERE table_name LIKE 'clips_bbox_part_%';")
    
    print("\n优点:")
    print("  ✅ 简单直接，无需额外配置")
    print("  ✅ 向后兼容")
    print("  ✅ 易于实现")
    
    print("\n缺点:")
    print("  ⚠️ 表名会变长")
    print("  ⚠️ 需要修改现有代码")

def analyze_current_tables(eng):
    """分析当前数据库中的表情况"""
    print("\n📊 当前数据库表分析")
    print("-" * 30)
    
    analyze_sql = text("""
        SELECT 
            table_name,
            table_type,
            CASE 
                WHEN table_name = 'clips_bbox' THEN '主表'
                WHEN table_name LIKE '%unified%' THEN '统一视图'
                WHEN table_name LIKE 'clips_bbox_%' THEN '分表'
                ELSE '其他'
            END as table_category
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%clips_bbox%'
        ORDER BY table_category, table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(analyze_sql)
            rows = result.fetchall()
            
            if not rows:
                print("  没有找到相关表")
                return
                
            print(f"找到 {len(rows)} 个相关表/视图:")
            current_category = None
            for row in rows:
                table_name, table_type, category = row
                if category != current_category:
                    print(f"\n  {category}:")
                    current_category = category
                print(f"    - {table_name} ({table_type})")
                
    except Exception as e:
        print(f"分析失败: {str(e)}")

def main():
    """主函数"""
    print("🧪 表管理方案对比和改进建议")
    print("=" * 80)
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 分析当前情况
        analyze_current_tables(eng)
        
        # 演示当前问题
        current_tables = current_list_bbox_tables_demo(eng)
        
        # 演示改进方案
        improved_v1_tables = improved_list_bbox_tables_v1(eng)
        improved_v2_tables = improved_list_bbox_tables_v2(eng)
        
        # 概念方案演示
        schema_based_solution_demo()
        naming_convention_solution_demo()
        
        print("\n" + "=" * 80)
        print("📋 推荐方案总结:")
        print("=" * 80)
        
        print("\n🥇 推荐: 方案1 - 改进过滤逻辑")
        print("  - 最简单，立即可用")
        print("  - 无需修改现有表结构")
        print("  - 可以很好解决当前问题")
        
        print("\n🥈 备选: 方案4 - 严格命名约定") 
        print("  - 长远更清晰")
        print("  - 需要一些代码修改")
        print("  - 适合未来扩展")
        
        print("\n🥉 高级: 方案3 - Schema分离")
        print("  - 最清晰的架构")
        print("  - 适合大型项目")
        print("  - 实施成本较高")
        
    except Exception as e:
        print(f"连接数据库失败: {str(e)}")

if __name__ == "__main__":
    main() 