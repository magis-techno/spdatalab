#!/usr/bin/env python3
"""
调试event_id数据类型问题
检查从数据库读取到保存过程中的数据类型变化
"""

import pandas as pd
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from spdatalab.common.config import hive_cursor

def debug_event_id_types():
    """调试event_id数据类型问题"""
    
    # 1. 模拟查询一些数据
    test_data_names = [
        'sample_dataset_001',
        'sample_dataset_002', 
        'sample_dataset_003'
    ]
    
    try:
        sql = """
            SELECT origin_name AS data_name, 
                   id AS scene_id,
                   event_id,
                   event_name
            FROM (
                SELECT origin_name, 
                       id, 
                       event_id,
                       event_name,
                       ROW_NUMBER() OVER (PARTITION BY origin_name ORDER BY updated_at DESC) as rn
                FROM transform.ods_t_data_fragment_datalake 
                WHERE origin_name IN %(tok)s
            ) ranked
            WHERE rn = 1
            LIMIT 10
        """
        
        with hive_cursor() as cur:
            cur.execute(sql, {"tok": tuple(test_data_names)})
            cols = [d[0] for d in cur.description]
            result_df = pd.DataFrame(cur.fetchall(), columns=cols)
            
        print("=== 数据库查询结果 ===")
        print(f"DataFrame shape: {result_df.shape}")
        print(f"Columns: {result_df.columns.tolist()}")
        print("\n=== 数据类型 ===")
        print(result_df.dtypes)
        
        if not result_df.empty:
            print("\n=== event_id 列详细信息 ===")
            print(f"event_id类型: {result_df['event_id'].dtype}")
            print(f"event_id样本值:")
            for i, val in enumerate(result_df['event_id'].head()):
                print(f"  [{i}] {val} (type: {type(val)})")
            
            print(f"\n=== event_id 是否有空值 ===")
            print(f"空值数量: {result_df['event_id'].isna().sum()}")
            print(f"非空值数量: {result_df['event_id'].notna().sum()}")
            
            # 2. 测试字典映射过程
            print("\n=== 字典映射测试 ===")
            data_name_to_event_id = dict(zip(result_df['data_name'], result_df['event_id']))
            print("字典内容:")
            for k, v in list(data_name_to_event_id.items())[:5]:
                print(f"  {k}: {v} (type: {type(v)})")
            
            # 3. 测试获取值的过程
            print("\n=== 值获取测试 ===")
            first_data_name = result_df['data_name'].iloc[0]
            retrieved_event_id = data_name_to_event_id.get(first_data_name, None)
            print(f"获取的event_id: {retrieved_event_id} (type: {type(retrieved_event_id)})")
            
            # 4. 测试轨迹记录构建
            print("\n=== 轨迹记录构建测试 ===")
            stats = {
                'dataset_name': first_data_name,
                'event_id': retrieved_event_id,
            }
            print(f"轨迹记录中的event_id: {stats['event_id']} (type: {type(stats['event_id'])})")
            
            # 5. 测试GeoDataFrame创建
            print("\n=== GeoDataFrame测试 ===")
            test_gdf_data = [stats]
            gdf = pd.DataFrame(test_gdf_data)
            print(f"GDF中event_id类型: {gdf['event_id'].dtype}")
            print(f"GDF中event_id值: {gdf['event_id'].iloc[0]} (type: {type(gdf['event_id'].iloc[0])})")
            
            # 6. 强制转换为整数
            print("\n=== 强制转换测试 ===")
            if 'event_id' in gdf.columns and gdf['event_id'].notna().any():
                gdf['event_id'] = gdf['event_id'].astype('Int64')  # 使用可空整数类型
                print(f"转换后event_id类型: {gdf['event_id'].dtype}")
                print(f"转换后event_id值: {gdf['event_id'].iloc[0]} (type: {type(gdf['event_id'].iloc[0])})")
        
        else:
            print("没有查询到数据")
            
    except Exception as e:
        print(f"调试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_event_id_types() 