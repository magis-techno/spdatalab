#!/usr/bin/env python3
"""
生产环境性能测试脚本

测试分表模式在生产环境下的查询性能和稳定性
"""

import argparse
import time
import statistics
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

class PerformanceTester:
    def __init__(self, dsn, view_name='clips_bbox_unified'):
        self.dsn = dsn
        self.view_name = view_name
        self.eng = create_engine(dsn, future=True)
        self.test_results = []
    
    def execute_query_with_timing(self, query_name, sql_query, iterations=3):
        """执行查询并记录性能"""
        print(f"\n🔍 测试查询: {query_name}")
        
        times = []
        results = []
        
        for i in range(iterations):
            try:
                start_time = time.time()
                
                with self.eng.connect() as conn:
                    result = conn.execute(text(sql_query))
                    rows = result.fetchall()
                
                end_time = time.time()
                execution_time = end_time - start_time
                times.append(execution_time)
                results.append(len(rows))
                
                print(f"  第{i+1}次: {execution_time:.2f}秒 ({len(rows)}条记录)")
                
            except Exception as e:
                print(f"  第{i+1}次: 失败 - {str(e)}")
                times.append(None)
                results.append(0)
        
        # 计算统计信息
        valid_times = [t for t in times if t is not None]
        if valid_times:
            avg_time = statistics.mean(valid_times)
            min_time = min(valid_times)
            max_time = max(valid_times)
            
            print(f"  📊 平均: {avg_time:.2f}秒, 最快: {min_time:.2f}秒, 最慢: {max_time:.2f}秒")
            
            self.test_results.append({
                'query_name': query_name,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'success_rate': len(valid_times) / iterations,
                'avg_records': statistics.mean([r for r in results if r > 0]) if results else 0
            })
            
            return True
        else:
            print(f"  ❌ 所有查询都失败了")
            return False
    
    def test_basic_queries(self):
        """测试基础查询性能"""
        print("🎯 基础查询性能测试")
        print("=" * 60)
        
        queries = [
            {
                "name": "总记录数统计",
                "sql": f"SELECT COUNT(*) FROM {self.view_name};"
            },
            {
                "name": "子数据集统计",
                "sql": f"""
                    SELECT subdataset_name, COUNT(*) as count 
                    FROM {self.view_name} 
                    GROUP BY subdataset_name 
                    ORDER BY count DESC;
                """
            },
            {
                "name": "有效数据统计",
                "sql": f"""
                    SELECT all_good, COUNT(*) as count 
                    FROM {self.view_name} 
                    GROUP BY all_good;
                """
            },
            {
                "name": "几何数据统计",
                "sql": f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(geometry) as with_geometry
                    FROM {self.view_name};
                """
            }
        ]
        
        for query in queries:
            self.execute_query_with_timing(query["name"], query["sql"])
    
    def test_spatial_queries(self):
        """测试空间查询性能"""
        print("\n🗺️  空间查询性能测试")
        print("=" * 60)
        
        # 先获取数据范围
        bbox_query = f"""
            SELECT 
                ST_XMin(ST_Extent(geometry)) as min_x,
                ST_YMin(ST_Extent(geometry)) as min_y,
                ST_XMax(ST_Extent(geometry)) as max_x,
                ST_YMax(ST_Extent(geometry)) as max_y
            FROM {self.view_name} 
            WHERE geometry IS NOT NULL;
        """
        
        try:
            with self.eng.connect() as conn:
                result = conn.execute(text(bbox_query))
                bbox = result.fetchone()
                if bbox:
                    min_x, min_y, max_x, max_y = bbox
                    print(f"📍 数据范围: X({min_x:.6f}, {max_x:.6f}) Y({min_y:.6f}, {max_y:.6f})")
                    
                    # 计算测试区域（中心区域的1/4）
                    center_x = (min_x + max_x) / 2
                    center_y = (min_y + max_y) / 2
                    quarter_width = (max_x - min_x) / 4
                    quarter_height = (max_y - min_y) / 4
                    
                    test_queries = [
                        {
                            "name": "空间范围查询（中心1/4区域）",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE ST_Intersects(
                                    geometry, 
                                    ST_MakeEnvelope({center_x - quarter_width}, {center_y - quarter_height}, 
                                                   {center_x + quarter_width}, {center_y + quarter_height}, 4326)
                                );
                            """
                        },
                        {
                            "name": "空间距离查询（500米缓冲区）",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE ST_DWithin(
                                    geometry::geography, 
                                    ST_Point({center_x}, {center_y})::geography, 
                                    500
                                );
                            """
                        },
                        {
                            "name": "边界框提取",
                            "sql": f"""
                                SELECT 
                                    ST_XMin(geometry) as min_x,
                                    ST_YMin(geometry) as min_y,
                                    ST_XMax(geometry) as max_x,
                                    ST_YMax(geometry) as max_y
                                FROM {self.view_name} 
                                WHERE geometry IS NOT NULL 
                                LIMIT 1000;
                            """
                        }
                    ]
                    
                    for query in test_queries:
                        self.execute_query_with_timing(query["name"], query["sql"])
                        
        except Exception as e:
            print(f"❌ 获取数据范围失败: {str(e)}")
    
    def test_filtering_queries(self):
        """测试过滤查询性能"""
        print("\n🔍 过滤查询性能测试")
        print("=" * 60)
        
        # 先获取一些子数据集名称
        try:
            with self.eng.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT subdataset_name, COUNT(*) as count
                    FROM {self.view_name} 
                    GROUP BY subdataset_name 
                    ORDER BY count DESC 
                    LIMIT 5;
                """))
                top_subdatasets = result.fetchall()
                
                if top_subdatasets:
                    # 使用第一个子数据集进行测试
                    test_subdataset = top_subdatasets[0][0]
                    
                    filter_queries = [
                        {
                            "name": f"按子数据集过滤 ({test_subdataset})",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE subdataset_name = '{test_subdataset}';
                            """
                        },
                        {
                            "name": "按有效性过滤 (all_good=true)",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE all_good = true;
                            """
                        },
                        {
                            "name": "组合过滤 (子数据集+有效性)",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE subdataset_name = '{test_subdataset}' 
                                AND all_good = true;
                            """
                        },
                        {
                            "name": "模糊匹配过滤",
                            "sql": f"""
                                SELECT COUNT(*) 
                                FROM {self.view_name} 
                                WHERE subdataset_name LIKE '%{test_subdataset[:10]}%';
                            """
                        }
                    ]
                    
                    for query in filter_queries:
                        self.execute_query_with_timing(query["name"], query["sql"])
                        
        except Exception as e:
            print(f"❌ 过滤查询测试失败: {str(e)}")
    
    def test_concurrent_queries(self, max_workers=4):
        """测试并发查询性能"""
        print(f"\n🚀 并发查询性能测试 ({max_workers} workers)")
        print("=" * 60)
        
        # 准备并发查询
        concurrent_queries = [
            f"SELECT COUNT(*) FROM {self.view_name} WHERE all_good = true;",
            f"SELECT COUNT(*) FROM {self.view_name} WHERE all_good = false;",
            f"SELECT COUNT(DISTINCT subdataset_name) FROM {self.view_name};",
            f"SELECT COUNT(*) FROM {self.view_name} WHERE geometry IS NOT NULL;"
        ]
        
        def execute_single_query(query_idx, sql):
            try:
                start_time = time.time()
                eng = create_engine(self.dsn, future=True)
                with eng.connect() as conn:
                    result = conn.execute(text(sql))
                    rows = result.fetchall()
                end_time = time.time()
                return {
                    'query_idx': query_idx,
                    'execution_time': end_time - start_time,
                    'success': True,
                    'record_count': len(rows)
                }
            except Exception as e:
                return {
                    'query_idx': query_idx,
                    'execution_time': None,
                    'success': False,
                    'error': str(e)
                }
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, query in enumerate(concurrent_queries):
                future = executor.submit(execute_single_query, i, query)
                futures.append(future)
            
            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
        
        total_time = time.time() - start_time
        
        successful_results = [r for r in results if r['success']]
        failed_results = [r for r in results if not r['success']]
        
        print(f"  📊 总体时间: {total_time:.2f}秒")
        print(f"  ✅ 成功查询: {len(successful_results)}/{len(concurrent_queries)}")
        
        if successful_results:
            avg_time = statistics.mean([r['execution_time'] for r in successful_results])
            print(f"  ⏱️  平均查询时间: {avg_time:.2f}秒")
        
        if failed_results:
            print(f"  ❌ 失败查询: {len(failed_results)}")
            for result in failed_results:
                print(f"    查询{result['query_idx']}: {result.get('error', 'Unknown error')}")
    
    def generate_performance_report(self):
        """生成性能测试报告"""
        print("\n📊 性能测试报告")
        print("=" * 60)
        
        if not self.test_results:
            print("❌ 没有测试结果可报告")
            return
        
        # 按性能排序
        sorted_results = sorted(self.test_results, key=lambda x: x['avg_time'])
        
        print(f"{'查询名称':<30} {'平均时间':<10} {'成功率':<8} {'记录数':<10}")
        print("-" * 60)
        
        for result in sorted_results:
            avg_time = f"{result['avg_time']:.2f}s"
            success_rate = f"{result['success_rate']*100:.0f}%"
            avg_records = f"{result['avg_records']:.0f}"
            
            print(f"{result['query_name']:<30} {avg_time:<10} {success_rate:<8} {avg_records:<10}")
        
        # 性能评估
        print(f"\n🎯 性能评估:")
        fast_queries = [r for r in self.test_results if r['avg_time'] < 1.0]
        slow_queries = [r for r in self.test_results if r['avg_time'] > 5.0]
        
        print(f"  - 快速查询（<1秒）: {len(fast_queries)} 个")
        print(f"  - 慢查询（>5秒）: {len(slow_queries)} 个")
        
        if slow_queries:
            print(f"  ⚠️  需要优化的慢查询:")
            for query in slow_queries:
                print(f"    - {query['query_name']}: {query['avg_time']:.2f}秒")

def main():
    parser = argparse.ArgumentParser(description="生产环境性能测试")
    parser.add_argument('--dsn', default='postgresql+psycopg://postgres:postgres@local_pg:5432/postgres',
                       help='数据库连接字符串')
    parser.add_argument('--view-name', default='clips_bbox_unified', help='统一视图名称')
    parser.add_argument('--iterations', type=int, default=3, help='每个查询的重复次数')
    parser.add_argument('--concurrent-workers', type=int, default=4, help='并发查询worker数量')
    parser.add_argument('--skip-spatial', action='store_true', help='跳过空间查询测试')
    parser.add_argument('--skip-concurrent', action='store_true', help='跳过并发查询测试')
    
    args = parser.parse_args()
    
    print("🏭 生产环境性能测试开始")
    print("=" * 60)
    
    tester = PerformanceTester(args.dsn, args.view_name)
    
    # 基础查询测试
    tester.test_basic_queries()
    
    # 空间查询测试
    if not args.skip_spatial:
        tester.test_spatial_queries()
    
    # 过滤查询测试
    tester.test_filtering_queries()
    
    # 并发查询测试
    if not args.skip_concurrent:
        tester.test_concurrent_queries(args.concurrent_workers)
    
    # 生成报告
    tester.generate_performance_report()
    
    print("\n✅ 生产环境性能测试完成")

if __name__ == "__main__":
    main() 