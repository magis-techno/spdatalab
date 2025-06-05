#!/usr/bin/env python3
"""
最基本的测试 - 只测试核心功能
"""

import sys
from pathlib import Path
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """基础测试"""
    print("🧪 开始基础测试...")
    
    try:
        # 创建连接器
        joiner = SpatialJoin()
        print("✅ 连接器创建成功")
        
        # 只测试最基本的相交功能
        result = joiner.batch_spatial_join_with_remote(
            batch_by_city=True,
            limit_batches=1,  # 只处理1个城市
            spatial_relation=SpatialRelation.INTERSECTS,
            summarize=True,
            summary_fields={
                "intersection_count": "count"  # 只统计数量，不计算距离
            }
        )
        
        print(f"✅ 测试成功！返回 {len(result)} 条结果")
        
        if not result.empty:
            print(f"📊 结果预览:")
            print(result.head())
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 