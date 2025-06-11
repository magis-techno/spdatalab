#!/usr/bin/env python3
"""
Sprint 2 功能测试脚本

测试分表模式和统一视图功能：
1. 分表模式数据处理
2. 统一视图创建和维护
3. 跨数据集查询验证
"""

import sys
import os
sys.path.insert(0, 'src')

import argparse
from pathlib import Path
from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    run_with_partitioning,
    create_unified_view,
    maintain_unified_view,
    list_bbox_tables,
    group_scenes_by_subdataset,
    batch_create_tables_for_subdatasets
)

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def test_partitioning_workflow(dataset_file: str, work_dir: str = "./test_sprint2_logs"):
    """测试完整的分表工作流程
    
    Args:
        dataset_file: 数据集文件路径
        work_dir: 工作目录
    """
    print("=" * 60)
    print("🎯 测试Sprint 2: 分表模式工作流程")
    print("=" * 60)
    
    try:
        # 步骤1: 测试场景分组功能
        print("\n=== 步骤1: 测试场景分组 ===")
        scene_groups = group_scenes_by_subdataset(dataset_file)
        
        if not scene_groups:
            print("❌ 场景分组失败，没有找到有效数据")
            return False
        
        print(f"✅ 场景分组成功，找到 {len(scene_groups)} 个子数据集")
        
        # 显示分组统计
        total_scenes = sum(len(scenes) for scenes in scene_groups.values())
        print(f"   总场景数: {total_scenes}")
        
        # 限制测试规模（只处理前3个子数据集的少量数据）
        limited_groups = {}
        for i, (name, scenes) in enumerate(scene_groups.items()):
            if i >= 3:  # 只处理前3个子数据集
                break
            # 每个子数据集最多测试10个场景
            limited_scenes = scenes[:10]
            limited_groups[name] = limited_scenes
            print(f"   - {name}: {len(limited_scenes)} 个场景（测试用）")
        
        # 步骤2: 测试分表创建
        print("\n=== 步骤2: 测试分表创建 ===")
        eng = create_engine(LOCAL_DSN, future=True)
        table_mapping = batch_create_tables_for_subdatasets(eng, list(limited_groups.keys()))
        
        if not table_mapping:
            print("❌ 分表创建失败")
            return False
        
        print(f"✅ 分表创建成功，创建了 {len(table_mapping)} 个分表")
        for subdataset, table_name in table_mapping.items():
            print(f"   {subdataset} -> {table_name}")
        
        # 步骤3: 测试统一视图创建
        print("\n=== 步骤3: 测试统一视图创建 ===")
        view_success = create_unified_view(eng, "clips_bbox_unified_test")
        
        if view_success:
            print("✅ 统一视图创建成功")
        else:
            print("❌ 统一视图创建失败")
            return False
        
        # 步骤4: 测试视图查询
        print("\n=== 步骤4: 测试统一视图查询 ===")
        test_view_query(eng, "clips_bbox_unified_test")
        
        # 步骤5: 测试表管理功能
        print("\n=== 步骤5: 测试表管理功能 ===")
        bbox_tables = list_bbox_tables(eng)
        print(f"当前bbox表数量: {len(bbox_tables)}")
        for table in bbox_tables:
            print(f"   - {table}")
        
        # 步骤6: 测试视图维护
        print("\n=== 步骤6: 测试统一视图维护 ===")
        maintain_success = maintain_unified_view(eng, "clips_bbox_unified_test")
        
        if maintain_success:
            print("✅ 统一视图维护成功")
        else:
            print("❌ 统一视图维护失败")
            return False
        
        print("\n" + "=" * 60)
        print("🎉 Sprint 2 核心功能测试全部通过！")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_view_query(eng, view_name: str):
    """测试统一视图查询功能
    
    Args:
        eng: 数据库引擎
        view_name: 视图名称
    """
    try:
        # 测试基本查询
        query_sql = text(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT subdataset_name) as subdataset_count,
                COUNT(DISTINCT source_table) as table_count
            FROM {view_name};
        """)
        
        with eng.connect() as conn:
            result = conn.execute(query_sql)
            row = result.fetchone()
            
            print(f"   视图查询结果:")
            print(f"   - 总记录数: {row[0]}")
            print(f"   - 子数据集数: {row[1]}")
            print(f"   - 源表数: {row[2]}")
        
        # 测试分组查询
        group_query_sql = text(f"""
            SELECT 
                subdataset_name,
                source_table,
                COUNT(*) as record_count
            FROM {view_name}
            GROUP BY subdataset_name, source_table
            ORDER BY subdataset_name
            LIMIT 10;
        """)
        
        with eng.connect() as conn:
            result = conn.execute(group_query_sql)
            rows = result.fetchall()
            
            print(f"   按子数据集分组查询结果:")
            for row in rows:
                print(f"   - {row[0]}: {row[2]} 条记录 (表: {row[1]})")
        
        print("✅ 统一视图查询测试成功")
        
    except Exception as e:
        print(f"❌ 统一视图查询测试失败: {str(e)}")

def test_small_scale_processing(dataset_file: str):
    """测试小规模数据的分表处理
    
    Args:
        dataset_file: 数据集文件路径
    """
    print("\n" + "=" * 60)
    print("🧪 测试小规模分表数据处理")
    print("=" * 60)
    
    try:
        # 使用分表模式处理小规模数据
        run_with_partitioning(
            input_path=dataset_file,
            batch=10,  # 小批次
            insert_batch=10,  # 小插入批次
            work_dir="./test_sprint2_processing",
            create_unified_view_flag=True,
            maintain_view_only=False
        )
        
        print("✅ 小规模分表处理测试完成")
        return True
        
    except Exception as e:
        print(f"❌ 小规模分表处理测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def cleanup_test_resources():
    """清理测试资源"""
    print("\n=== 清理测试资源 ===")
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 删除测试视图
        with eng.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS clips_bbox_unified_test;"))
            conn.commit()
        
        print("✅ 测试资源清理完成")
        
    except Exception as e:
        print(f"❌ 清理测试资源失败: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Sprint 2 功能测试")
    parser.add_argument(
        '--dataset-file', 
        required=True,
        help='数据集文件路径（JSON/Parquet格式）'
    )
    parser.add_argument(
        '--test-processing', 
        action='store_true',
        help='测试小规模数据处理（需要实际数据库连接）'
    )
    parser.add_argument(
        '--cleanup', 
        action='store_true',
        help='清理测试资源'
    )
    
    args = parser.parse_args()
    
    if args.cleanup:
        cleanup_test_resources()
        return
    
    if not Path(args.dataset_file).exists():
        print(f"❌ 数据集文件不存在: {args.dataset_file}")
        sys.exit(1)
    
    # 测试核心功能
    success = test_partitioning_workflow(args.dataset_file)
    
    if not success:
        print("\n❌ Sprint 2 核心功能测试失败")
        sys.exit(1)
    
    # 可选的数据处理测试
    if args.test_processing:
        print("\n" + "🔄 开始数据处理测试...")
        processing_success = test_small_scale_processing(args.dataset_file)
        
        if not processing_success:
            print("\n❌ 数据处理测试失败")
            sys.exit(1)
        
        print("\n🎉 所有测试都通过了！")
    else:
        print("\n💡 提示: 使用 --test-processing 参数可以测试实际的数据处理功能")
    
    print("\n" + "=" * 60)
    print("✅ Sprint 2 测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main() 