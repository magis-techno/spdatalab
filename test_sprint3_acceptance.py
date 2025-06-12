#!/usr/bin/env python3
"""
Sprint 3 验收测试脚本

验证生产环境功能的完整性和可靠性
"""

import argparse
import sys
import subprocess
from pathlib import Path
from sqlalchemy import create_engine, text
import time

class Sprint3AcceptanceTester:
    def __init__(self, dsn, dataset_file=None):
        self.dsn = dsn
        self.dataset_file = dataset_file
        self.eng = create_engine(dsn, future=True)
        self.test_results = {}
    
    def test_database_environment(self):
        """测试数据库环境和扩展"""
        print("🔍 测试数据库环境...")
        
        try:
            with self.eng.connect() as conn:
                # 检查PostgreSQL版本
                result = conn.execute(text("SELECT version();"))
                pg_version = result.scalar()
                print(f"  ✅ PostgreSQL: {pg_version.split(',')[0]}")
                
                # 检查PostGIS扩展
                result = conn.execute(text("SELECT PostGIS_Version();"))
                postgis_version = result.scalar()
                print(f"  ✅ PostGIS: {postgis_version}")
                
                # 检查连接数限制
                result = conn.execute(text("SHOW max_connections;"))
                max_connections = result.scalar()
                print(f"  ✅ 最大连接数: {max_connections}")
                
                # 检查当前连接数
                result = conn.execute(text("SELECT count(*) FROM pg_stat_activity;"))
                current_connections = result.scalar()
                print(f"  📊 当前连接数: {current_connections}")
                
                self.test_results['database_environment'] = True
                return True
                
        except Exception as e:
            print(f"  ❌ 数据库环境测试失败: {str(e)}")
            self.test_results['database_environment'] = False
            return False
    
    def test_partitioned_tables_structure(self):
        """测试分表结构和数据"""
        print("\n📊 测试分表结构...")
        
        try:
            # 统计分表数量
            tables_sql = text("""
                SELECT COUNT(*) as table_count
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'clips_bbox_%'
                AND table_type = 'BASE TABLE';
            """)
            
            with self.eng.connect() as conn:
                result = conn.execute(tables_sql)
                table_count = result.scalar()
                print(f"  📋 分表数量: {table_count}")
                
                if table_count == 0:
                    print("  ❌ 没有发现分表")
                    self.test_results['partitioned_tables'] = False
                    return False
                
                # 统计总记录数
                unified_view_sql = text("""
                    SELECT COUNT(*) as total_records
                    FROM clips_bbox_unified;
                """)
                
                result = conn.execute(unified_view_sql)
                total_records = result.scalar()
                print(f"  📊 总记录数: {total_records:,}")
                
                # 检查数据分布
                distribution_sql = text("""
                    SELECT 
                        COUNT(DISTINCT source_table) as unique_tables,
                        COUNT(DISTINCT subdataset_name) as unique_subdatasets
                    FROM clips_bbox_unified;
                """)
                
                result = conn.execute(distribution_sql)
                unique_tables, unique_subdatasets = result.fetchone()
                print(f"  🗂️  分布统计: {unique_tables} 个表，{unique_subdatasets} 个子数据集")
                
                self.test_results['partitioned_tables'] = True
                return True
                
        except Exception as e:
            print(f"  ❌ 分表结构测试失败: {str(e)}")
            self.test_results['partitioned_tables'] = False
            return False
    
    def test_unified_view_functionality(self):
        """测试统一视图功能"""
        print("\n🔍 测试统一视图功能...")
        
        try:
            # 检查统一视图是否存在
            view_sql = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = 'clips_bbox_unified'
                );
            """)
            
            with self.eng.connect() as conn:
                result = conn.execute(view_sql)
                view_exists = result.scalar()
                
                if not view_exists:
                    print("  ❌ 统一视图不存在")
                    self.test_results['unified_view'] = False
                    return False
                
                print("  ✅ 统一视图存在")
                
                # 测试基本查询
                basic_queries = [
                    ("记录计数", "SELECT COUNT(*) FROM clips_bbox_unified;"),
                    ("几何数据", "SELECT COUNT(*) FROM clips_bbox_unified WHERE geometry IS NOT NULL;"),
                    ("有效数据", "SELECT COUNT(*) FROM clips_bbox_unified WHERE all_good = true;"),
                    ("子数据集", "SELECT COUNT(DISTINCT subdataset_name) FROM clips_bbox_unified;")
                ]
                
                for query_name, sql in basic_queries:
                    result = conn.execute(text(sql))
                    count = result.scalar()
                    print(f"  📊 {query_name}: {count:,}")
                
                self.test_results['unified_view'] = True
                return True
                
        except Exception as e:
            print(f"  ❌ 统一视图测试失败: {str(e)}")
            self.test_results['unified_view'] = False
            return False
    
    def test_query_performance(self):
        """测试查询性能"""
        print("\n⏱️  测试查询性能...")
        
        performance_queries = [
            {
                "name": "快速计数查询",
                "sql": "SELECT COUNT(*) FROM clips_bbox_unified;",
                "expected_time": 2.0  # 秒
            },
            {
                "name": "子数据集分组",
                "sql": """
                    SELECT subdataset_name, COUNT(*) 
                    FROM clips_bbox_unified 
                    GROUP BY subdataset_name 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 10;
                """,
                "expected_time": 5.0
            },
            {
                "name": "几何数据过滤",
                "sql": """
                    SELECT COUNT(*) 
                    FROM clips_bbox_unified 
                    WHERE geometry IS NOT NULL 
                    AND all_good = true;
                """,
                "expected_time": 3.0
            }
        ]
        
        performance_passed = 0
        
        for query in performance_queries:
            try:
                start_time = time.time()
                with self.eng.connect() as conn:
                    result = conn.execute(text(query["sql"]))
                    rows = result.fetchall()
                end_time = time.time()
                
                execution_time = end_time - start_time
                
                if execution_time <= query["expected_time"]:
                    print(f"  ✅ {query['name']}: {execution_time:.2f}s (目标: <{query['expected_time']}s)")
                    performance_passed += 1
                else:
                    print(f"  ⚠️  {query['name']}: {execution_time:.2f}s (超过目标: {query['expected_time']}s)")
                    
            except Exception as e:
                print(f"  ❌ {query['name']}: 查询失败 - {str(e)}")
        
        performance_ratio = performance_passed / len(performance_queries)
        if performance_ratio >= 0.8:
            print(f"  ✅ 性能测试通过: {performance_passed}/{len(performance_queries)}")
            self.test_results['query_performance'] = True
            return True
        else:
            print(f"  ❌ 性能测试未达标: {performance_passed}/{len(performance_queries)}")
            self.test_results['query_performance'] = False
            return False
    
    def test_spatial_functionality(self):
        """测试空间功能"""
        print("\n🗺️  测试空间功能...")
        
        try:
            spatial_queries = [
                {
                    "name": "空间索引检查",
                    "sql": """
                        SELECT COUNT(*) 
                        FROM pg_indexes 
                        WHERE indexname LIKE '%geometry%' 
                        AND tablename LIKE 'clips_bbox%';
                    """
                },
                {
                    "name": "边界框计算",
                    "sql": """
                        SELECT 
                            ST_XMin(ST_Extent(geometry)) as min_x,
                            ST_YMin(ST_Extent(geometry)) as min_y,
                            ST_XMax(ST_Extent(geometry)) as max_x,
                            ST_YMax(ST_Extent(geometry)) as max_y
                        FROM clips_bbox_unified 
                        WHERE geometry IS NOT NULL;
                    """
                },
                {
                    "name": "几何类型检查",
                    "sql": """
                        SELECT ST_GeometryType(geometry) as geom_type, COUNT(*) 
                        FROM clips_bbox_unified 
                        WHERE geometry IS NOT NULL 
                        GROUP BY ST_GeometryType(geometry);
                    """
                }
            ]
            
            spatial_passed = 0
            
            for query in spatial_queries:
                try:
                    with self.eng.connect() as conn:
                        result = conn.execute(text(query["sql"]))
                        rows = result.fetchall()
                        
                        if query["name"] == "空间索引检查":
                            index_count = rows[0][0] if rows else 0
                            print(f"  📊 {query['name']}: {index_count} 个空间索引")
                            if index_count > 0:
                                spatial_passed += 1
                        elif query["name"] == "边界框计算":
                            if rows and rows[0][0] is not None:
                                min_x, min_y, max_x, max_y = rows[0]
                                print(f"  📍 {query['name']}: X({min_x:.6f}, {max_x:.6f}) Y({min_y:.6f}, {max_y:.6f})")
                                spatial_passed += 1
                        elif query["name"] == "几何类型检查":
                            geom_types = [(row[0], row[1]) for row in rows]
                            print(f"  🔍 {query['name']}: {geom_types}")
                            if geom_types:
                                spatial_passed += 1
                        
                except Exception as e:
                    print(f"  ❌ {query['name']}: {str(e)}")
            
            if spatial_passed >= 2:
                print(f"  ✅ 空间功能测试通过: {spatial_passed}/3")
                self.test_results['spatial_functionality'] = True
                return True
            else:
                print(f"  ❌ 空间功能测试失败: {spatial_passed}/3")
                self.test_results['spatial_functionality'] = False
                return False
                
        except Exception as e:
            print(f"  ❌ 空间功能测试异常: {str(e)}")
            self.test_results['spatial_functionality'] = False
            return False
    
    def test_data_integrity(self):
        """测试数据完整性"""
        print("\n🔒 测试数据完整性...")
        
        try:
            integrity_queries = [
                {
                    "name": "重复数据检查",
                    "sql": """
                        SELECT COUNT(*) as duplicates
                        FROM (
                            SELECT scene_token, COUNT(*) 
                            FROM clips_bbox_unified 
                            GROUP BY scene_token 
                            HAVING COUNT(*) > 1
                        ) t;
                    """
                },
                {
                    "name": "空值检查",
                    "sql": """
                        SELECT 
                            COUNT(*) - COUNT(scene_token) as null_scene_tokens,
                            COUNT(*) - COUNT(data_name) as null_data_names,
                            COUNT(*) - COUNT(subdataset_name) as null_subdatasets
                        FROM clips_bbox_unified;
                    """
                },
                {
                    "name": "数据一致性检查",
                    "sql": """
                        SELECT COUNT(*) as inconsistent_records
                        FROM clips_bbox_unified 
                        WHERE (geometry IS NOT NULL AND all_good IS NULL)
                        OR (scene_token IS NULL OR scene_token = '');
                    """
                }
            ]
            
            integrity_passed = 0
            
            for query in integrity_queries:
                try:
                    with self.eng.connect() as conn:
                        result = conn.execute(text(query["sql"]))
                        row = result.fetchone()
                        
                        if query["name"] == "重复数据检查":
                            duplicates = row[0]
                            if duplicates == 0:
                                print(f"  ✅ {query['name']}: 无重复数据")
                                integrity_passed += 1
                            else:
                                print(f"  ⚠️  {query['name']}: 发现 {duplicates} 条重复数据")
                                
                        elif query["name"] == "空值检查":
                            null_tokens, null_names, null_subdatasets = row
                            total_nulls = null_tokens + null_names + null_subdatasets
                            if total_nulls == 0:
                                print(f"  ✅ {query['name']}: 无关键字段空值")
                                integrity_passed += 1
                            else:
                                print(f"  ⚠️  {query['name']}: tokens:{null_tokens}, names:{null_names}, subdatasets:{null_subdatasets}")
                                
                        elif query["name"] == "数据一致性检查":
                            inconsistent = row[0]
                            if inconsistent == 0:
                                print(f"  ✅ {query['name']}: 数据一致性良好")
                                integrity_passed += 1
                            else:
                                print(f"  ⚠️  {query['name']}: {inconsistent} 条不一致记录")
                        
                except Exception as e:
                    print(f"  ❌ {query['name']}: {str(e)}")
            
            if integrity_passed >= 2:
                print(f"  ✅ 数据完整性测试通过: {integrity_passed}/3")
                self.test_results['data_integrity'] = True
                return True
            else:
                print(f"  ❌ 数据完整性测试未达标: {integrity_passed}/3")
                self.test_results['data_integrity'] = False
                return False
                
        except Exception as e:
            print(f"  ❌ 数据完整性测试异常: {str(e)}")
            self.test_results['data_integrity'] = False
            return False
    
    def generate_final_report(self):
        """生成最终验收报告"""
        print("\n" + "="*60)
        print("📋 Sprint 3 验收测试报告")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result)
        
        print(f"📊 测试结果汇总: {passed_tests}/{total_tests} 通过")
        print()
        
        test_names = {
            'database_environment': '数据库环境',
            'partitioned_tables': '分表结构',
            'unified_view': '统一视图',
            'query_performance': '查询性能',
            'spatial_functionality': '空间功能',
            'data_integrity': '数据完整性'
        }
        
        for test_key, result in self.test_results.items():
            test_name = test_names.get(test_key, test_key)
            status = "✅ 通过" if result else "❌ 失败"
            print(f"  {test_name}: {status}")
        
        print()
        
        if passed_tests == total_tests:
            print("🎉 Sprint 3 验收测试全部通过！")
            print("✅ 生产环境已就绪")
            return True
        elif passed_tests >= total_tests * 0.8:
            print("⚠️  Sprint 3 验收测试基本通过")
            print("💡 建议解决剩余问题后正式发布")
            return True
        else:
            print("❌ Sprint 3 验收测试未通过")
            print("🔧 需要修复关键问题")
            return False

def main():
    parser = argparse.ArgumentParser(description="Sprint 3 验收测试")
    parser.add_argument('--dsn', default='postgresql+psycopg://postgres:postgres@local_pg:5432/postgres',
                       help='数据库连接字符串')
    parser.add_argument('--dataset-file', help='数据集文件路径（可选）')
    
    args = parser.parse_args()
    
    print("🎯 Sprint 3 验收测试开始")
    print("🚀 验证生产环境功能完整性")
    print("="*60)
    
    tester = Sprint3AcceptanceTester(args.dsn, args.dataset_file)
    
    # 执行所有测试
    test_methods = [
        tester.test_database_environment,
        tester.test_partitioned_tables_structure,
        tester.test_unified_view_functionality,
        tester.test_query_performance,
        tester.test_spatial_functionality,
        tester.test_data_integrity
    ]
    
    for test_method in test_methods:
        try:
            test_method()
        except Exception as e:
            print(f"❌ 测试执行异常: {str(e)}")
    
    # 生成最终报告
    success = tester.generate_final_report()
    
    if success:
        print("\n🎊 恭喜！Sprint 3 验收成功！")
        print("🎯 可以开始 QGIS 集成测试了")
    else:
        print("\n🔧 需要修复问题后重新测试")
        sys.exit(1)

if __name__ == "__main__":
    main() 