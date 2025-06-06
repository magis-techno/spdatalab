import time
from src.spdatalab.fusion.spatial_join_minimal import simple_polygon_intersect
from src.spdatalab.fusion.spatial_join_batch import batch_polygon_intersect, chunked_batch_intersect

# 全局变量存储测试结果
test_results = []

def test_performance_comparison():
    """对比不同批量处理方法的性能"""
    
    test_cases = [10, 20, 50, 100, 200, 500]  # 增加更大规模
    
    print("🚀 批量polygon相交性能对比测试")
    print("=" * 80)
    
    for num_bbox in test_cases:
        print(f"\n📊 测试规模: {num_bbox} 个bbox")
        print("-" * 50)
        
        # 存储当前测试的结果
        current_result = {
            'bbox_count': num_bbox,
            'method1_time': None,  # 逐个查询
            'method2_time': None,  # 批量查询
            'method3_time': None,  # 分块查询
            'result_count': 0
        }
        
        # 方法1: 逐个查询（小规模测试）
        if num_bbox <= 100:  # 只在小规模时测试逐个查询
            print("方法1: 逐个查询")
            start_time = time.time()
            try:
                result1 = simple_polygon_intersect(num_bbox)
                time1 = time.time() - start_time
                current_result['method1_time'] = time1
                current_result['result_count'] = len(result1)
                print(f"  ✅ 耗时: {time1:.2f}秒 | 结果: {len(result1)}条 | 速度: {len(result1)/time1:.1f} bbox/秒")
            except Exception as e:
                print(f"  ❌ 出错: {e}")
        else:
            print("方法1: 逐个查询 (跳过-规模太大)")
        
        # 方法2: 批量查询（UNION ALL）
        if num_bbox <= 200:  # 中等规模测试批量查询
            print("方法2: 批量查询(UNION ALL)")
            start_time = time.time()
            try:
                result2 = batch_polygon_intersect(num_bbox)
                time2 = time.time() - start_time
                current_result['method2_time'] = time2
                if current_result['result_count'] == 0:
                    current_result['result_count'] = len(result2)
                print(f"  ✅ 耗时: {time2:.2f}秒 | 结果: {len(result2)}条 | 速度: {len(result2)/time2:.1f} bbox/秒")
                
                if current_result['method1_time']:
                    speedup = current_result['method1_time'] / time2
                    print(f"  🚀 相比逐个查询提速: {speedup:.1f}x")
            except Exception as e:
                print(f"  ❌ 出错: {e}")
        else:
            print("方法2: 批量查询(UNION ALL) (跳过-SQL可能过长)")
        
        # 方法3: 分块批量查询（所有规模）
        print("方法3: 分块批量查询")
        start_time = time.time()
        try:
            chunk_size = min(50, max(10, num_bbox // 5))  # 动态chunk size
            result3 = chunked_batch_intersect(num_bbox, chunk_size)
            time3 = time.time() - start_time
            current_result['method3_time'] = time3
            if current_result['result_count'] == 0:
                current_result['result_count'] = len(result3)
            print(f"  ✅ 耗时: {time3:.2f}秒 | 结果: {len(result3)}条 | 速度: {len(result3)/time3:.1f} bbox/秒")
            print(f"  📦 块大小: {chunk_size}")
            
            if current_result['method1_time']:
                speedup = current_result['method1_time'] / time3
                print(f"  🚀 相比逐个查询提速: {speedup:.1f}x")
        except Exception as e:
            print(f"  ❌ 出错: {e}")
        
        test_results.append(current_result)
        print()

def test_large_scale():
    """测试超大规模批量处理"""
    print("\n🎯 超大规模批量处理测试")
    print("=" * 80)
    
    # 更大规模的测试
    large_sizes = [1000, 2000, 5000, 10000]
    
    for size in large_sizes:
        print(f"\n📈 超大规模测试: {size} 个bbox")
        print("-" * 40)
        
        # 只测试分块批量方法（适合大规模）
        start_time = time.time()
        try:
            result = chunked_batch_intersect(size, chunk_size=100)  # 增大chunk size
            elapsed = time.time() - start_time
            
            print(f"  ✅ 处理{size}个bbox")
            print(f"  ⏱️  总耗时: {elapsed:.2f}秒")
            print(f"  📊 结果数: {len(result)}条")
            print(f"  ⚡ 处理速度: {size/elapsed:.1f} bbox/秒")
            print(f"  💡 平均每bbox: {elapsed/size*1000:.0f}毫秒")
            
            # 添加到测试结果
            test_results.append({
                'bbox_count': size,
                'method1_time': None,
                'method2_time': None, 
                'method3_time': elapsed,
                'result_count': len(result)
            })
            
            # 如果时间太长就停止
            if elapsed > 120:  # 超过2分钟
                print(f"  ⚠️  耗时超过2分钟，停止更大规模测试")
                break
                
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            break

def multiple_rounds_test():
    """多轮次稳定性测试"""
    print("\n🔄 多轮次稳定性测试")
    print("=" * 80)
    
    test_size = 50  # 中等规模
    rounds = 5      # 测试轮次
    
    round_results = []
    
    for i in range(rounds):
        print(f"\n第{i+1}轮测试 ({test_size} bbox)")
        
        # 测试分块批量方法
        start_time = time.time()
        try:
            result = chunked_batch_intersect(test_size, chunk_size=25)
            elapsed = time.time() - start_time
            round_results.append({
                'round': i+1,
                'time': elapsed,
                'count': len(result),
                'speed': len(result)/elapsed
            })
            print(f"  耗时: {elapsed:.2f}秒 | 结果: {len(result)}条 | 速度: {len(result)/elapsed:.1f} bbox/秒")
        except Exception as e:
            print(f"  ❌ 第{i+1}轮失败: {e}")
    
    # 计算统计数据
    if round_results:
        times = [r['time'] for r in round_results]
        speeds = [r['speed'] for r in round_results]
        
        print(f"\n📊 {rounds}轮测试统计:")
        print(f"  平均耗时: {sum(times)/len(times):.2f}秒")
        print(f"  最快耗时: {min(times):.2f}秒")
        print(f"  最慢耗时: {max(times):.2f}秒")
        print(f"  平均速度: {sum(speeds)/len(speeds):.1f} bbox/秒")
        print(f"  速度稳定性: {(max(speeds)-min(speeds))/sum(speeds)*len(speeds):.1%}")

def print_summary():
    """打印测试总结 - 方便拷贝分析"""
    print("\n" + "="*100)
    print("📋 测试结果总结 (可拷贝给助手分析)")
    print("="*100)
    
    # 表格标题
    print(f"{'规模':<8} {'逐个查询':<12} {'批量查询':<12} {'分块查询':<12} {'结果数':<8} {'最优方法':<12}")
    print("-" * 80)
    
    for result in test_results:
        bbox_count = result['bbox_count']
        method1 = f"{result['method1_time']:.2f}s" if result['method1_time'] else "N/A"
        method2 = f"{result['method2_time']:.2f}s" if result['method2_time'] else "N/A"
        method3 = f"{result['method3_time']:.2f}s" if result['method3_time'] else "N/A"
        count = result['result_count']
        
        # 找出最优方法
        times = []
        if result['method1_time']: times.append(('逐个', result['method1_time']))
        if result['method2_time']: times.append(('批量', result['method2_time']))
        if result['method3_time']: times.append(('分块', result['method3_time']))
        
        best_method = min(times, key=lambda x: x[1])[0] if times else "N/A"
        
        print(f"{bbox_count:<8} {method1:<12} {method2:<12} {method3:<12} {count:<8} {best_method:<12}")
    
    print("\n🔍 关键观察:")
    print("1. 性能趋势: 随着规模增大，哪种方法性能最稳定？")
    print("2. 临界点: 在什么规模下，批量查询开始优于逐个查询？")
    print("3. 扩展性: 分块查询在大规模数据下的表现如何？")
    print("4. 推荐策略: 基于不同规模推荐最优方法")
    
    print(f"\n📊 性能数据 (CSV格式):")
    print("bbox_count,individual_time,batch_time,chunked_time,result_count")
    for result in test_results:
        print(f"{result['bbox_count']},{result['method1_time'] or 'N/A'},{result['method2_time'] or 'N/A'},{result['method3_time'] or 'N/A'},{result['result_count']}")

if __name__ == "__main__":
    test_performance_comparison()
    test_large_scale()
    multiple_rounds_test()
    print_summary() 