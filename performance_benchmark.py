#!/usr/bin/env python3
"""
高性能Polygon轨迹查询性能基准测试

对比不同配置参数下的性能表现，验证优化效果
"""

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import List, Dict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_benchmark_polygons(count: int) -> str:
    """创建性能测试用的polygon集合"""
    # 创建北京地区的多个测试polygon
    base_coords = [
        [116.3, 39.9], [116.4, 39.9], [116.4, 40.0], [116.3, 40.0], [116.3, 39.9]
    ]
    
    features = []
    for i in range(count):
        # 在基础坐标上添加小偏移，创建不同的polygon
        offset_x = (i % 10) * 0.01  # 经度偏移
        offset_y = (i // 10) * 0.01  # 纬度偏移
        
        coords = [[x + offset_x, y + offset_y] for x, y in base_coords]
        
        feature = {
            "type": "Feature",
            "properties": {
                "id": f"benchmark_area_{i}",
                "name": f"测试区域{i}"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords]
            }
        }
        features.append(feature)
    
    polygon_collection = {
        "type": "FeatureCollection",
        "features": features
    }
    
    # 保存到临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as f:
        json.dump(polygon_collection, f, ensure_ascii=False, indent=2)
        temp_file = f.name
    
    logger.info(f"创建了包含 {count} 个polygon的测试文件: {temp_file}")
    return temp_file

def run_performance_test(geojson_file: str, config_name: str, config: 'PolygonTrajectoryConfig') -> Dict:
    """运行单个性能测试"""
    from src.spdatalab.dataset.polygon_trajectory_query import process_polygon_trajectory_query
    
    logger.info(f"开始性能测试: {config_name}")
    logger.info(f"配置: batch_threshold={config.batch_threshold}, chunk_size={config.chunk_size}")
    
    start_time = time.time()
    
    # 只写入数据库，不导出文件
    table_name = f"benchmark_{config_name.lower().replace(' ', '_')}"
    
    stats = process_polygon_trajectory_query(
        geojson_file=geojson_file,
        output_table=table_name,
        config=config
    )
    
    total_time = time.time() - start_time
    
    return {
        'config_name': config_name,
        'total_time': total_time,
        'success': stats.get('success', False),
        'query_stats': stats.get('query_stats', {}),
        'build_stats': stats.get('build_stats', {}),
        'save_stats': stats.get('save_stats', {}),
        'polygon_count': stats.get('polygon_count', 0)
    }

def run_benchmark_suite():
    """运行完整的性能基准测试套件"""
    from src.spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig
    
    logger.info("=" * 80)
    logger.info("🚀 开始高性能Polygon轨迹查询基准测试")
    logger.info("=" * 80)
    
    # 测试不同数量的polygon
    polygon_counts = [5, 25, 100]  # 小、中、大规模测试
    
    # 测试配置组合
    test_configs = [
        {
            'name': '默认配置',
            'config': PolygonTrajectoryConfig()
        },
        {
            'name': '小批量高频',
            'config': PolygonTrajectoryConfig(
                batch_threshold=10,
                chunk_size=5,
                batch_insert_size=200
            )
        },
        {
            'name': '大批量低频',
            'config': PolygonTrajectoryConfig(
                batch_threshold=100,
                chunk_size=50,
                batch_insert_size=2000
            )
        },
        {
            'name': '超高性能',
            'config': PolygonTrajectoryConfig(
                batch_threshold=200,
                chunk_size=100,
                batch_insert_size=5000,
                limit_per_polygon=50000
            )
        }
    ]
    
    all_results = []
    
    for polygon_count in polygon_counts:
        logger.info(f"\n🔍 测试规模: {polygon_count} 个polygon")
        logger.info("-" * 60)
        
        # 创建测试数据
        geojson_file = create_benchmark_polygons(polygon_count)
        
        try:
            scale_results = []
            
            for test_config in test_configs:
                try:
                    result = run_performance_test(
                        geojson_file=geojson_file,
                        config_name=test_config['name'],
                        config=test_config['config']
                    )
                    result['polygon_count'] = polygon_count
                    scale_results.append(result)
                    
                    # 输出单个测试结果
                    if result['success']:
                        query_time = result['query_stats'].get('query_time', 0)
                        total_points = result['query_stats'].get('total_points', 0)
                        strategy = result['query_stats'].get('strategy', 'unknown')
                        
                        logger.info(f"✅ {test_config['name']}: {result['total_time']:.2f}s "
                                   f"(查询: {query_time:.2f}s, {total_points:,} 点, {strategy})")
                    else:
                        logger.error(f"❌ {test_config['name']}: 测试失败")
                        
                except Exception as e:
                    logger.error(f"❌ {test_config['name']} 测试出错: {e}")
                    scale_results.append({
                        'config_name': test_config['name'],
                        'polygon_count': polygon_count,
                        'success': False,
                        'error': str(e)
                    })
            
            all_results.extend(scale_results)
            
        finally:
            # 清理测试文件
            Path(geojson_file).unlink(missing_ok=True)
    
    # 输出综合分析
    print_benchmark_analysis(all_results)
    
    return all_results

def print_benchmark_analysis(results: List[Dict]):
    """输出基准测试分析报告"""
    logger.info("\n" + "=" * 80)
    logger.info("📊 性能基准测试分析报告")
    logger.info("=" * 80)
    
    # 按polygon数量分组
    by_scale = {}
    for result in results:
        if result.get('success', False):
            scale = result['polygon_count']
            if scale not in by_scale:
                by_scale[scale] = []
            by_scale[scale].append(result)
    
    # 各规模性能对比
    for scale in sorted(by_scale.keys()):
        scale_results = by_scale[scale]
        logger.info(f"\n🎯 {scale} 个polygon性能对比:")
        
        # 按总时间排序
        scale_results.sort(key=lambda x: x['total_time'])
        
        best_time = scale_results[0]['total_time']
        
        for i, result in enumerate(scale_results):
            config_name = result['config_name']
            total_time = result['total_time']
            speedup = best_time / total_time
            
            query_stats = result.get('query_stats', {})
            strategy = query_stats.get('strategy', 'unknown')
            total_points = query_stats.get('total_points', 0)
            
            rank_emoji = ["🥇", "🥈", "🥉", "📊"][min(i, 3)]
            
            logger.info(f"   {rank_emoji} {config_name}: {total_time:.2f}s "
                       f"({speedup:.1f}x) - {strategy}, {total_points:,} 点")
    
    # 策略效果分析
    logger.info(f"\n📈 查询策略效果分析:")
    
    batch_results = [r for r in results if r.get('success') and 
                    r.get('query_stats', {}).get('strategy') == 'batch_query']
    chunked_results = [r for r in results if r.get('success') and 
                      r.get('query_stats', {}).get('strategy') == 'chunked_query']
    
    if batch_results:
        avg_batch_time = sum(r['query_stats']['query_time'] for r in batch_results) / len(batch_results)
        logger.info(f"   🔗 批量查询平均用时: {avg_batch_time:.2f}s ({len(batch_results)} 次测试)")
    
    if chunked_results:
        avg_chunked_time = sum(r['query_stats']['query_time'] for r in chunked_results) / len(chunked_results)
        logger.info(f"   📦 分块查询平均用时: {avg_chunked_time:.2f}s ({len(chunked_results)} 次测试)")
    
    # 最佳配置推荐
    logger.info(f"\n🏆 最佳配置推荐:")
    
    for scale in sorted(by_scale.keys()):
        best_result = min(by_scale[scale], key=lambda x: x['total_time'])
        logger.info(f"   • {scale} 个polygon: {best_result['config_name']} "
                   f"({best_result['total_time']:.2f}s)")
    
    logger.info("\n" + "=" * 80)

def main():
    """主函数"""
    try:
        # 检查依赖
        try:
            from src.spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig
            logger.info("✅ 模块导入成功")
        except ImportError as e:
            logger.error(f"❌ 模块导入失败: {e}")
            return 1
        
        print("🧪 高性能Polygon轨迹查询基准测试")
        print("\n注意事项:")
        print("• 此测试将创建多个数据库表用于性能测试")
        print("• 测试可能需要几分钟时间，请耐心等待")
        print("• 确保数据库连接正常且有足够空间")
        
        user_input = input("\n是否开始基准测试？(y/N): ")
        
        if user_input.lower() not in ['y', 'yes']:
            print("取消基准测试")
            return 0
        
        # 运行基准测试
        results = run_benchmark_suite()
        
        # 保存结果
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        results_file = f"benchmark_results_{timestamp}.json"
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"📋 详细结果已保存到: {results_file}")
        
        logger.info("🎉 基准测试完成！")
        return 0
        
    except KeyboardInterrupt:
        logger.info("用户中断测试")
        return 1
    except Exception as e:
        logger.error(f"基准测试失败: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 