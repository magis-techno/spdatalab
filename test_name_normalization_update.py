#!/usr/bin/env python3
"""
子数据集名称规范化测试脚本 (移除日期版本)

测试修改后的normalize_subdataset_name函数是否正确移除日期
"""

import sys
import os
sys.path.insert(0, 'src')

from src.spdatalab.dataset.bbox import (
    normalize_subdataset_name,
    get_table_name_for_subdataset,
)

def test_normalize_subdataset_name():
    """测试子数据集名称规范化功能（移除日期版本）"""
    print("🧪 测试子数据集名称规范化 (移除日期版本)")
    print("=" * 60)
    
    # 测试用例：各种原始名称和期望的规范化结果
    test_cases = [
        # (原始名称, 期望的规范化结果, 描述)
        ("GOD_E2E_lane_change_2024_05_18_10_56_18", "lane_change", "基本场景：去前缀+移除日期"),
        ("GOD_E2E_lane_change_heavy_traffic_2024_05_18_10_56_18", "lane_change_heavy_traffic", "复杂场景：保留中间内容+移除日期"),
        ("GOD_E2E_lane_change_sub_ddi_extra_2024_05_18_10_56_18", "lane_change", "sub_ddi截断+移除日期"),
        ("GOD_E2E_lane_change_277736e2e_extra_2024_05_18_10_56_18", "lane_change", "哈希截断+移除日期"),
        ("GOD_E2E_lane_change_277abc123e2e_more_stuff_2024_05_18_10_56_18", "lane_change", "复杂哈希截断+移除日期"),
        ("lane_change_2024_05_18_10_56_18", "lane_change", "无前缀+移除日期"),
        ("lane_change", "lane_change", "无前缀无日期"),
        ("GOD_E2E_lane_change", "lane_change", "有前缀无日期"),
        ("GOD_E2E_very_long_dataset_name_with_lots_of_details_2024_05_18_10_56_18", "very_long_dataset_name_with_lots_of_details", "长名称+移除日期"),
        ("GOD_E2E_complex_name_sub_ddi_should_be_cut_2024_05_18_10_56_18", "complex_name", "复杂截断+移除日期"),
    ]
    
    all_passed = True
    
    for original, expected, description in test_cases:
        print(f"\n📋 测试: {description}")
        print(f"   原始: {original}")
        print(f"   期望: {expected}")
        
        try:
            result = normalize_subdataset_name(original)
            if result == expected:
                print(f"   ✅ 通过: {result}")
            else:
                print(f"   ❌ 失败: 实际 '{result}' != 期望 '{expected}'")
                all_passed = False
        except Exception as e:
            print(f"   ❌ 异常: {str(e)}")
            all_passed = False
    
    return all_passed

def test_table_name_generation():
    """测试表名生成功能"""
    print("\n🏗️ 测试表名生成功能")
    print("=" * 40)
    
    test_subdatasets = [
        "GOD_E2E_lane_change_2024_05_18_10_56_18",
        "GOD_E2E_lane_change_heavy_traffic_2024_05_18_10_56_18", 
        "GOD_E2E_complex_name_sub_ddi_should_be_cut_2024_05_18_10_56_18",
        "GOD_E2E_lane_change_277abc123e2e_more_stuff_2024_05_18_10_56_18",
        "lane_change_no_prefix_2024_05_18_10_56_18",
    ]
    
    for subdataset in test_subdatasets:
        print(f"\n📝 子数据集: {subdataset}")
        try:
            table_name = get_table_name_for_subdataset(subdataset)
            print(f"   表名: {table_name}")
            print(f"   长度: {len(table_name)} 字符")
            
            # 验证表名格式
            if table_name.startswith('clips_bbox_'):
                print(f"   ✅ 表名前缀正确")
            else:
                print(f"   ❌ 表名前缀错误")
            
            # 验证长度
            if len(table_name) <= 50:
                print(f"   ✅ 表名长度合理")
            else:
                print(f"   ⚠️ 表名可能过长")
                
        except Exception as e:
            print(f"   ❌ 生成失败: {str(e)}")

def test_date_removal_specifically():
    """专门测试日期移除功能"""
    print("\n📅 专门测试日期移除功能")
    print("=" * 40)
    
    date_test_cases = [
        # 各种日期格式的测试
        ("dataset_name_2024_05_18_10_56_18", "dataset_name"),
        ("dataset_name_2023_12_31_23_59_59", "dataset_name"), 
        ("dataset_name_2025_01_01_00_00_00", "dataset_name"),
        ("prefix_dataset_name_2024_05_18_10_56_18_suffix", "prefix_dataset_name_2024_05_18_10_56_18_suffix"),  # 只移除末尾的
        ("dataset_name_with_2024_in_middle_2024_05_18_10_56_18", "dataset_name_with_2024_in_middle"),
        ("dataset_name_no_date", "dataset_name_no_date"),
        ("dataset_name_2024_05_18", "dataset_name_2024_05_18"),  # 不完整的日期格式不移除
    ]
    
    for original, expected in date_test_cases:
        print(f"\n   测试: {original}")
        result = normalize_subdataset_name(original)
        if result == expected:
            print(f"   ✅ 正确: {result}")
        else:
            print(f"   ❌ 错误: 实际 '{result}' != 期望 '{expected}'")

def show_before_after_comparison():
    """显示修改前后的对比"""
    print("\n🔄 修改前后对比示例")
    print("=" * 50)
    
    examples = [
        "GOD_E2E_lane_change_2024_05_18_10_56_18",
        "GOD_E2E_lane_change_heavy_traffic_2024_05_18_10_56_18",
        "GOD_E2E_complex_scenario_sub_ddi_extra_2024_05_18_10_56_18",
    ]
    
    print("修改前 (保留日期) vs 修改后 (移除日期):")
    print("-" * 50)
    
    for example in examples:
        # 模拟修改前的结果（手动计算）
        before_result = example.replace("GOD_E2E_", "")  # 只去前缀，保留日期
        
        # 实际的修改后结果
        after_result = normalize_subdataset_name(example)
        
        print(f"\n原始名称:")
        print(f"  {example}")
        print(f"修改前 (保留日期):")
        print(f"  {before_result}")
        print(f"修改后 (移除日期):")
        print(f"  {after_result}")
        print(f"表名:")
        table_name = get_table_name_for_subdataset(example)
        print(f"  {table_name}")

def main():
    """主测试函数"""
    print("🧪 子数据集名称规范化测试 (移除日期版本)")
    print("=" * 80)
    
    # 显示修改前后对比
    show_before_after_comparison()
    
    # 专门测试日期移除
    test_date_removal_specifically()
    
    # 测试名称规范化
    normalize_passed = test_normalize_subdataset_name()
    
    # 测试表名生成
    test_table_name_generation()
    
    print("\n" + "=" * 80)
    if normalize_passed:
        print("🎉 所有名称规范化测试通过！日期移除功能正常工作")
    else:
        print("❌ 部分测试失败，请检查规范化逻辑")
    print("=" * 80)

if __name__ == "__main__":
    main() 