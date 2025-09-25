#!/usr/bin/env python3
"""
数据库迁移测试验证脚本
用于验证迁移后的数据完整性和一致性
"""

import os
import sys
import psycopg2
import argparse
import json
from datetime import datetime
from collections import defaultdict

class MigrationValidator:
    def __init__(self, source_config, target_config):
        self.source_config = source_config
        self.target_config = target_config
        self.test_results = []
        
    def log_result(self, test_name, status, message, details=None):
        """记录测试结果"""
        result = {
            'timestamp': datetime.now().isoformat(),
            'test': test_name,
            'status': status,  # PASS, FAIL, SKIP
            'message': message,
            'details': details
        }
        self.test_results.append(result)
        
        status_icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⏭️"
        print(f"{status_icon} [{status}] {test_name}: {message}")
        
        if details:
            for key, value in details.items():
                print(f"    {key}: {value}")
    
    def connect_database(self, config, database='postgres'):
        """连接数据库"""
        try:
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                user=config['username'],
                password=config.get('password', ''),
                database=database
            )
            return conn
        except Exception as e:
            return None
    
    def test_connection(self):
        """测试数据库连接"""
        print("\n=== 连接测试 ===")
        
        # 测试源数据库连接
        source_conn = self.connect_database(self.source_config)
        if source_conn:
            self.log_result("source_connection", "PASS", "源数据库连接成功")
            source_conn.close()
        else:
            self.log_result("source_connection", "FAIL", "源数据库连接失败")
            return False
        
        # 测试目标数据库连接
        target_conn = self.connect_database(self.target_config)
        if target_conn:
            self.log_result("target_connection", "PASS", "目标数据库连接成功")
            target_conn.close()
        else:
            self.log_result("target_connection", "FAIL", "目标数据库连接失败")
            return False
        
        return True
    
    def get_database_list(self, config):
        """获取数据库列表"""
        conn = self.connect_database(config)
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT datname FROM pg_database 
                WHERE datistemplate = false 
                AND datname != 'postgres'
                ORDER BY datname
            """)
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return databases
        except Exception as e:
            conn.close()
            return []
    
    def test_database_existence(self):
        """测试数据库是否存在"""
        print("\n=== 数据库存在性测试 ===")
        
        source_databases = self.get_database_list(self.source_config)
        target_databases = self.get_database_list(self.target_config)
        
        if not source_databases:
            self.log_result("database_existence", "SKIP", "无法获取源数据库列表")
            return False
        
        missing_databases = set(source_databases) - set(target_databases)
        extra_databases = set(target_databases) - set(source_databases)
        
        if not missing_databases and not extra_databases:
            self.log_result("database_existence", "PASS", 
                          f"所有数据库都已迁移 ({len(source_databases)} 个数据库)",
                          {"databases": ", ".join(source_databases)})
        else:
            details = {}
            if missing_databases:
                details["missing"] = ", ".join(missing_databases)
            if extra_databases:
                details["extra"] = ", ".join(extra_databases)
            
            self.log_result("database_existence", "FAIL", 
                          "数据库列表不匹配", details)
        
        return len(missing_databases) == 0
    
    def get_table_info(self, config, database):
        """获取表信息"""
        conn = self.connect_database(config, database)
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            # 获取表基本信息
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    hasindexes,
                    hasrules,
                    hastriggers
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schemaname, tablename
            """)
            
            tables = {}
            for row in cursor.fetchall():
                schema, table, has_indexes, has_rules, has_triggers = row
                table_key = f"{schema}.{table}"
                tables[table_key] = {
                    'schema': schema,
                    'table': table,
                    'has_indexes': has_indexes,
                    'has_rules': has_rules,
                    'has_triggers': has_triggers
                }
            
            cursor.close()
            conn.close()
            return tables
            
        except Exception as e:
            conn.close()
            return {}
    
    def get_table_row_count(self, config, database, schema, table):
        """获取表行数"""
        conn = self.connect_database(config, database)
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            conn.close()
            return None
    
    def test_table_structure(self, database_list=None):
        """测试表结构"""
        print("\n=== 表结构测试 ===")
        
        if not database_list:
            database_list = self.get_database_list(self.source_config)
        
        overall_pass = True
        
        for database in database_list:
            print(f"\n检查数据库: {database}")
            
            source_tables = self.get_table_info(self.source_config, database)
            target_tables = self.get_table_info(self.target_config, database)
            
            if not source_tables and not target_tables:
                self.log_result(f"table_structure_{database}", "SKIP", 
                              f"数据库 {database} 中没有用户表")
                continue
            
            missing_tables = set(source_tables.keys()) - set(target_tables.keys())
            extra_tables = set(target_tables.keys()) - set(source_tables.keys())
            
            if missing_tables or extra_tables:
                details = {}
                if missing_tables:
                    details["missing_tables"] = ", ".join(missing_tables)
                if extra_tables:
                    details["extra_tables"] = ", ".join(extra_tables)
                
                self.log_result(f"table_structure_{database}", "FAIL",
                              f"数据库 {database} 表结构不匹配", details)
                overall_pass = False
            else:
                self.log_result(f"table_structure_{database}", "PASS",
                              f"数据库 {database} 表结构匹配 ({len(source_tables)} 个表)")
        
        return overall_pass
    
    def test_row_counts(self, database_list=None, sample_tables=5):
        """测试行数一致性"""
        print("\n=== 行数一致性测试 ===")
        
        if not database_list:
            database_list = self.get_database_list(self.source_config)
        
        overall_pass = True
        
        for database in database_list:
            print(f"\n检查数据库: {database}")
            
            source_tables = self.get_table_info(self.source_config, database)
            target_tables = self.get_table_info(self.target_config, database)
            
            # 取样本表进行检查
            common_tables = list(set(source_tables.keys()) & set(target_tables.keys()))
            
            if not common_tables:
                self.log_result(f"row_count_{database}", "SKIP",
                              f"数据库 {database} 中没有公共表")
                continue
            
            # 限制检查的表数量
            tables_to_check = common_tables[:sample_tables] if len(common_tables) > sample_tables else common_tables
            
            mismatched_tables = []
            
            for table_key in tables_to_check:
                schema, table = table_key.split('.')
                
                source_count = self.get_table_row_count(self.source_config, database, schema, table)
                target_count = self.get_table_row_count(self.target_config, database, schema, table)
                
                if source_count is None or target_count is None:
                    mismatched_tables.append(f"{table_key}(无法获取行数)")
                elif source_count != target_count:
                    mismatched_tables.append(f"{table_key}({source_count} vs {target_count})")
            
            if mismatched_tables:
                self.log_result(f"row_count_{database}", "FAIL",
                              f"数据库 {database} 行数不匹配",
                              {"mismatched_tables": ", ".join(mismatched_tables)})
                overall_pass = False
            else:
                self.log_result(f"row_count_{database}", "PASS",
                              f"数据库 {database} 行数匹配 (检查了 {len(tables_to_check)} 个表)")
        
        return overall_pass
    
    def test_sequences(self, database_list=None):
        """测试序列值"""
        print("\n=== 序列测试 ===")
        
        if not database_list:
            database_list = self.get_database_list(self.source_config)
        
        overall_pass = True
        
        for database in database_list:
            print(f"\n检查数据库: {database}")
            
            # 获取序列信息
            source_sequences = self.get_sequences(self.source_config, database)
            target_sequences = self.get_sequences(self.target_config, database)
            
            if not source_sequences and not target_sequences:
                self.log_result(f"sequences_{database}", "SKIP",
                              f"数据库 {database} 中没有序列")
                continue
            
            missing_sequences = set(source_sequences.keys()) - set(target_sequences.keys())
            
            if missing_sequences:
                self.log_result(f"sequences_{database}", "FAIL",
                              f"数据库 {database} 缺少序列",
                              {"missing": ", ".join(missing_sequences)})
                overall_pass = False
            else:
                self.log_result(f"sequences_{database}", "PASS",
                              f"数据库 {database} 序列完整 ({len(source_sequences)} 个序列)")
        
        return overall_pass
    
    def get_sequences(self, config, database):
        """获取序列信息"""
        conn = self.connect_database(config, database)
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sequence_schema, sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            
            sequences = {}
            for row in cursor.fetchall():
                schema, name = row
                sequences[f"{schema}.{name}"] = {'schema': schema, 'name': name}
            
            cursor.close()
            conn.close()
            return sequences
            
        except Exception as e:
            conn.close()
            return {}
    
    def test_user_functions(self, database_list=None):
        """测试用户定义函数"""
        print("\n=== 用户函数测试 ===")
        
        if not database_list:
            database_list = self.get_database_list(self.source_config)
        
        overall_pass = True
        
        for database in database_list:
            print(f"\n检查数据库: {database}")
            
            source_functions = self.get_user_functions(self.source_config, database)
            target_functions = self.get_user_functions(self.target_config, database)
            
            if not source_functions and not target_functions:
                self.log_result(f"functions_{database}", "SKIP",
                              f"数据库 {database} 中没有用户函数")
                continue
            
            missing_functions = set(source_functions.keys()) - set(target_functions.keys())
            
            if missing_functions:
                self.log_result(f"functions_{database}", "FAIL",
                              f"数据库 {database} 缺少函数",
                              {"missing": ", ".join(missing_functions)})
                overall_pass = False
            else:
                self.log_result(f"functions_{database}", "PASS",
                              f"数据库 {database} 函数完整 ({len(source_functions)} 个函数)")
        
        return overall_pass
    
    def get_user_functions(self, config, database):
        """获取用户定义函数"""
        conn = self.connect_database(config, database)
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT routine_schema, routine_name, routine_type
                FROM information_schema.routines
                WHERE routine_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            
            functions = {}
            for row in cursor.fetchall():
                schema, name, type_ = row
                functions[f"{schema}.{name}"] = {
                    'schema': schema, 
                    'name': name, 
                    'type': type_
                }
            
            cursor.close()
            conn.close()
            return functions
            
        except Exception as e:
            conn.close()
            return {}
    
    def run_all_tests(self, database_list=None):
        """运行所有测试"""
        print("=" * 80)
        print("PostgreSQL迁移验证测试")
        print("=" * 80)
        
        # 1. 连接测试
        if not self.test_connection():
            print("\n❌ 连接测试失败，无法继续后续测试")
            return False
        
        # 2. 数据库存在性测试
        db_exists = self.test_database_existence()
        
        # 3. 表结构测试
        table_structure = self.test_table_structure(database_list)
        
        # 4. 行数一致性测试
        row_counts = self.test_row_counts(database_list)
        
        # 5. 序列测试
        sequences = self.test_sequences(database_list)
        
        # 6. 用户函数测试
        functions = self.test_user_functions(database_list)
        
        # 生成总结报告
        self.generate_summary_report()
        
        # 返回总体结果
        all_passed = all([db_exists, table_structure, row_counts, sequences, functions])
        return all_passed
    
    def generate_summary_report(self):
        """生成总结报告"""
        print("\n" + "=" * 80)
        print("测试总结报告")
        print("=" * 80)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['status'] == 'PASS'])
        failed_tests = len([r for r in self.test_results if r['status'] == 'FAIL'])
        skipped_tests = len([r for r in self.test_results if r['status'] == 'SKIP'])
        
        print(f"总测试数: {total_tests}")
        print(f"✅ 通过: {passed_tests}")
        print(f"❌ 失败: {failed_tests}")
        print(f"⏭️ 跳过: {skipped_tests}")
        
        if failed_tests > 0:
            print(f"\n失败的测试:")
            for result in self.test_results:
                if result['status'] == 'FAIL':
                    print(f"  - {result['test']}: {result['message']}")
        
        # 保存详细报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"migration_test_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, indent=2, ensure_ascii=False)
        
        print(f"\n详细报告已保存: {report_file}")
        
        success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        print(f"成功率: {success_rate:.1f}%")
        
        if success_rate >= 90:
            print("🎉 迁移质量: 优秀")
        elif success_rate >= 75:
            print("👍 迁移质量: 良好")
        elif success_rate >= 50:
            print("⚠️ 迁移质量: 需要关注")
        else:
            print("🚨 迁移质量: 需要修复")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PostgreSQL迁移验证测试工具')
    
    # 源数据库配置
    parser.add_argument('--source-host', required=True, help='源数据库主机')
    parser.add_argument('--source-port', default='5432', help='源数据库端口')
    parser.add_argument('--source-username', required=True, help='源数据库用户名')
    parser.add_argument('--source-password', help='源数据库密码')
    
    # 目标数据库配置
    parser.add_argument('--target-host', required=True, help='目标数据库主机')
    parser.add_argument('--target-port', default='5432', help='目标数据库端口')
    parser.add_argument('--target-username', required=True, help='目标数据库用户名')
    parser.add_argument('--target-password', help='目标数据库密码')
    
    # 测试选项
    parser.add_argument('--databases', nargs='+', help='要测试的数据库列表')
    parser.add_argument('--config-file', help='从JSON配置文件读取参数')
    
    args = parser.parse_args()
    
    # 从配置文件读取参数
    if args.config_file:
        with open(args.config_file, 'r') as f:
            config = json.load(f)
        
        source_config = config.get('source', {})
        target_config = config.get('target', {})
        
        # 命令行参数覆盖配置文件
        for key, value in vars(args).items():
            if value is not None and key.startswith('source_'):
                source_config[key.replace('source_', '')] = value
            elif value is not None and key.startswith('target_'):
                target_config[key.replace('target_', '')] = value
    else:
        source_config = {
            'host': args.source_host,
            'port': args.source_port,
            'username': args.source_username,
            'password': args.source_password
        }
        
        target_config = {
            'host': args.target_host,
            'port': args.target_port,
            'username': args.target_username,
            'password': args.target_password
        }
    
    # 创建验证器
    validator = MigrationValidator(source_config, target_config)
    
    # 运行测试
    success = validator.run_all_tests(args.databases)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()


