import time
from src.spdatalab.fusion.spatial_join_minimal import simple_polygon_intersect
from src.spdatalab.fusion.spatial_join_batch import batch_polygon_intersect, chunked_batch_intersect

def test_performance_comparison():
    """对比不同批量处理方法的性能"""
    
    test_cases = [10, 20, 50]
    
    print("🚀 批量polygon相交性能对比测试")
    print("=" * 80)
    
    for num_bbox in test_cases:
        print(f"\n📊 测试规模: {num_bbox} 个bbox")
        print("-" * 50)
        
        # 方法1: 逐个查询（原始方法）
        print("方法1: 逐个查询")
        start_time = time.time()
        try:
            result1 = simple_polygon_intersect(num_bbox)
            time1 = time.time() - start_time
            print(f"  ✅ 耗时: {time1:.2f}秒 | 结果: {len(result1)}条 | 速度: {len(result1)/time1:.1f} bbox/秒")
        except Exception as e:
            print(f"  ❌ 出错: {e}")
            time1 = None
        
        # 方法2: 批量查询（UNION ALL）
        print("方法2: 批量查询(UNION ALL)")
        start_time = time.time()
        try:
            result2 = batch_polygon_intersect(num_bbox)
            time2 = time.time() - start_time
            print(f"  ✅ 耗时: {time2:.2f}秒 | 结果: {len(result2)}条 | 速度: {len(result2)/time2:.1f} bbox/秒")
            
            if time1:
                speedup = time1 / time2
                print(f"  🚀 相比逐个查询提速: {speedup:.1f}x")
        except Exception as e:
            print(f"  ❌ 出错: {e}")
            time2 = None
        
        # 方法3: 分块批量查询
        if num_bbox >= 20:  # 只在数量较大时测试分块
            print("方法3: 分块批量查询")
            start_time = time.time()
            try:
                chunk_size = max(10, num_bbox // 4)  # 动态chunk size
                result3 = chunked_batch_intersect(num_bbox, chunk_size)
                time3 = time.time() - start_time
                print(f"  ✅ 耗时: {time3:.2f}秒 | 结果: {len(result3)}条 | 速度: {len(result3)/time3:.1f} bbox/秒")
                print(f"  📦 块大小: {chunk_size}")
                
                if time1:
                    speedup = time1 / time3
                    print(f"  🚀 相比逐个查询提速: {speedup:.1f}x")
            except Exception as e:
                print(f"  ❌ 出错: {e}")
        
        print()

def test_large_scale():
    """测试大规模批量处理"""
    print("\n🎯 大规模批量处理测试")
    print("=" * 80)
    
    large_sizes = [100, 200, 500]
    
    for size in large_sizes:
        print(f"\n📈 大规模测试: {size} 个bbox")
        print("-" * 40)
        
        # 只测试分块批量方法（适合大规模）
        start_time = time.time()
        try:
            result = chunked_batch_intersect(size, chunk_size=50)
            elapsed = time.time() - start_time
            
            print(f"  ✅ 处理{size}个bbox")
            print(f"  ⏱️  总耗时: {elapsed:.2f}秒")
            print(f"  📊 结果数: {len(result)}条")
            print(f"  ⚡ 处理速度: {len(result)/elapsed:.1f} bbox/秒")
            print(f"  💡 平均每bbox: {elapsed/len(result)*1000:.0f}毫秒")
            
            # 如果时间太长就停止
            if elapsed > 60:  # 超过1分钟
                print(f"  ⚠️  耗时超过1分钟，停止更大规模测试")
                break
                
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            break

if __name__ == "__main__":
    test_performance_comparison()
    test_large_scale() 