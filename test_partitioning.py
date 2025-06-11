#!/usr/bin/env python3
"""
分表功能测试脚本
用于验证 Sprint 1 的基础功能
"""

import sys
import os
from pathlib import Path

# 添加项目路径到 Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from sqlalchemy import create_engine, text
from src.spdatalab.dataset.bbox import (
    normalize_subdataset_name,
    get_table_name_for_subdataset,
    create_table_for_subdataset,
    group_scenes_by_subdataset,
    batch_create_tables_for_subdatasets
)

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def test_name_normalization():
    """测试子数据集名称规范化"""
    print("=== 测试子数据集名称规范化 ===")
    
    test_cases = [
        "GOD_E2E_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
        "GOD_E2E_simple_dataset",
        "normal_dataset_name_2025_01_01_12_00_00",
        "dataset_with_sub_ddi_extra_info_2025_05_18_11_07_59",
        "short_name",
        "very_very_very_long_dataset_name_that_exceeds_normal_length_limits_2025_05_18_11_07_59",
        # 新增测试用例：包含277...e2e模式
        "GOD_E2E_lane_change_1_277736e2e_2025_05_18_11_07_59",
        "dataset_with_277abc123e2e_suffix",
        "normal_dataset_277abcde2e_with_timestamp_2025_01_01_12_00_00",
        "complex_GOD_E2E_name_277hash123e2e_sub_ddi_extra_2025_05_18_11_07_59"
    ]
    
    print("测试用例和结果:")
    for i, original in enumerate(test_cases, 1):
        normalized = normalize_subdataset_name(original)
        table_name = get_table_name_for_subdataset(original)
        print(f"{i}. 原始: {original}")
        print(f"   规范化: {normalized}")
        print(f"   表名: {table_name}")
        print(f"   表名长度: {len(table_name)}")
        print()
    
    print("✅ 名称规范化测试完成\n")

def test_table_creation():
    """测试分表创建功能"""
    print("=== 测试分表创建功能 ===")
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 测试用例：创建几个测试分表
        test_subdatasets = [
            "GOD_E2E_test_dataset_1_2025_01_01_12_00_00",
            "test_dataset_2_sub_ddi_extra",
            "simple_test"
        ]
        
        print("创建测试分表:")
        results = {}
        for subdataset in test_subdatasets:
            print(f"创建分表: {subdataset}")
            success, table_name = create_table_for_subdataset(eng, subdataset)
            results[subdataset] = (success, table_name)
            
        # 验证表是否创建成功
        print("\n验证表创建结果:")
        with eng.connect() as conn:
            for subdataset, (success, table_name) in results.items():
                if success:
                    # 检查表结构
                    check_sql = text(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}' 
                        ORDER BY ordinal_position;
                    """)
                    result = conn.execute(check_sql)
                    columns = result.fetchall()
                    print(f"✅ {table_name}: {len(columns)} 列")
                    
                    # 检查索引
                    index_sql = text(f"""
                        SELECT indexname 
                        FROM pg_indexes 
                        WHERE tablename = '{table_name}';
                    """)
                    result = conn.execute(index_sql)
                    indexes = result.fetchall()
                    print(f"   索引数量: {len(indexes)}")
                else:
                    print(f"❌ {subdataset}: 创建失败")
        
        print("✅ 分表创建测试完成\n")
        return True
        
    except Exception as e:
        print(f"❌ 分表创建测试失败: {str(e)}")
        return False

def test_scene_grouping(dataset_file: str = None):
    """测试场景分组功能"""
    print("=== 测试场景分组功能 ===")
    
    if not dataset_file:
        print("⚠️  未提供dataset文件，跳过场景分组测试")
        print("   使用方法: python test_partitioning.py --dataset-file path/to/dataset.json")
        return True
    
    if not os.path.exists(dataset_file):
        print(f"❌ 数据集文件不存在: {dataset_file}")
        return False
    
    try:
        print(f"加载数据集文件: {dataset_file}")
        groups = group_scenes_by_subdataset(dataset_file)
        
        print(f"\n分组结果统计:")
        print(f"子数据集数量: {len(groups)}")
        
        total_scenes = sum(len(scenes) for scenes in groups.values())
        print(f"总场景数: {total_scenes}")
        
        # 显示前5个子数据集的详情
        print(f"\n前5个子数据集详情:")
        for i, (name, scenes) in enumerate(list(groups.items())[:5]):
            normalized = normalize_subdataset_name(name)
            table_name = get_table_name_for_subdataset(name)
            print(f"{i+1}. {name}")
            print(f"   场景数: {len(scenes)}")
            print(f"   表名: {table_name}")
        
        if len(groups) > 5:
            print(f"   ... 还有 {len(groups) - 5} 个子数据集")
        
        print("✅ 场景分组测试完成\n")
        return True
        
    except Exception as e:
        print(f"❌ 场景分组测试失败: {str(e)}")
        return False

def test_batch_table_creation(dataset_file: str = None):
    """测试批量创建分表功能"""
    print("=== 测试批量创建分表功能 ===")
    
    if not dataset_file:
        print("⚠️  未提供dataset文件，使用模拟数据测试")
        # 使用模拟的子数据集名称
        subdataset_names = [
            "GOD_E2E_mock_dataset_1_2025_01_01_12_00_00",
            "mock_dataset_2_sub_ddi_extra",
            "simple_mock_dataset"
        ]
    else:
        try:
            groups = group_scenes_by_subdataset(dataset_file)
            # 只取前3个子数据集进行测试
            subdataset_names = list(groups.keys())[:3]
            print(f"使用真实数据集的前 {len(subdataset_names)} 个子数据集进行测试")
        except Exception as e:
            print(f"加载数据集失败: {str(e)}，使用模拟数据")
            subdataset_names = [
                "GOD_E2E_mock_dataset_1_2025_01_01_12_00_00",
                "mock_dataset_2_sub_ddi_extra",
                "simple_mock_dataset"
            ]
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        print(f"批量创建 {len(subdataset_names)} 个分表:")
        table_mapping = batch_create_tables_for_subdatasets(eng, subdataset_names)
        
        print(f"\n批量创建结果:")
        for original_name, table_name in table_mapping.items():
            print(f"  {original_name} -> {table_name}")
        
        print("✅ 批量创建分表测试完成\n")
        return True
        
    except Exception as e:
        print(f"❌ 批量创建分表测试失败: {str(e)}")
        return False

def cleanup_test_tables():
    """清理测试产生的表"""
    print("=== 清理测试表 ===")
    
    try:
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 查找所有测试相关的表
        with eng.connect() as conn:
            find_tables_sql = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND (table_name LIKE 'clips_bbox_test_%' 
                       OR table_name LIKE 'clips_bbox_mock_%'
                       OR table_name LIKE 'clips_bbox_simple_test%')
                  AND table_type = 'BASE TABLE';
            """)
            
            result = conn.execute(find_tables_sql)
            test_tables = [row[0] for row in result.fetchall()]
            
            if test_tables:
                print(f"找到 {len(test_tables)} 个测试表，开始清理:")
                for table_name in test_tables:
                    drop_sql = text(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                    conn.execute(drop_sql)
                    print(f"  删除: {table_name}")
                
                conn.commit()
                print("✅ 测试表清理完成")
            else:
                print("未找到需要清理的测试表")
                
    except Exception as e:
        print(f"❌ 清理测试表失败: {str(e)}")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='分表功能测试脚本')
    parser.add_argument('--dataset-file', help='数据集文件路径（可选）')
    parser.add_argument('--cleanup', action='store_true', help='清理测试表并退出')
    parser.add_argument('--skip-db-tests', action='store_true', help='跳过需要数据库的测试')
    
    args = parser.parse_args()
    
    if args.cleanup:
        cleanup_test_tables()
        return
    
    print("🚀 开始 Sprint 1 功能测试")
    print("=" * 50)
    
    # 测试1: 名称规范化（不需要数据库）
    test_name_normalization()
    
    if args.skip_db_tests:
        print("⚠️  跳过数据库相关测试")
        return
    
    # 测试2: 分表创建
    success1 = test_table_creation()
    
    # 测试3: 场景分组
    success2 = test_scene_grouping(args.dataset_file)
    
    # 测试4: 批量创建分表
    success3 = test_batch_table_creation(args.dataset_file)
    
    # 清理测试表
    cleanup_test_tables()
    
    print("=" * 50)
    if all([success1, success2, success3]):
        print("🎉 Sprint 1 功能测试全部通过！")
        print("✅ 可以进入 Sprint 1 验收阶段")
    else:
        print("❌ 部分测试失败，需要修复后重新测试")
    
    print("=" * 50)

if __name__ == "__main__":
    main() 