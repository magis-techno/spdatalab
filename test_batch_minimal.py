import time
from src.spdatalab.fusion.spatial_join_minimal import simple_polygon_intersect

# 测试不同批量大小
test_sizes = [5, 10, 20, 50, 100]

print("批量测试最简单的polygon相交判断...")
print("=" * 60)

for num_bbox in test_sizes:
    print(f"\n测试 {num_bbox} 个bbox:")
    
    start_time = time.time()
    result = simple_polygon_intersect(num_bbox=num_bbox)
    end_time = time.time()
    
    total_time = end_time - start_time
    avg_time = total_time / len(result) if len(result) > 0 else 0
    
    print(f"  结果数量: {len(result)}")
    print(f"  总耗时: {total_time:.2f}秒")
    print(f"  平均每个bbox: {avg_time:.3f}秒")
    print(f"  处理速度: {len(result)/total_time:.1f} bbox/秒")
    
    # 显示前几个结果
    if len(result) > 0:
        print(f"  示例结果:")
        print(result.head(3).to_string(index=False))
    
    # 如果时间太长，就停止测试
    if total_time > 30:  # 30秒以上就停止
        print(f"  ⚠️  耗时超过30秒，停止更大批量测试")
        break 