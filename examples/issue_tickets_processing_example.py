#!/usr/bin/env python3
"""
问题单URL处理示例脚本

本示例演示如何使用新的问题单URL处理功能，包括：
1. 处理包含额外属性的URL文件
2. 创建专门的问题单bbox表
3. 查询和分析问题单数据

使用方法：
    python examples/issue_tickets_processing_example.py
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from spdatalab.dataset.bbox import (
    run_issue_tickets_processing,
    create_issue_bbox_table_if_not_exists
)
from spdatalab.dataset.dataset_manager import DatasetManager
from sqlalchemy import create_engine, text
import tempfile

def create_sample_url_file():
    """创建示例URL文件"""
    sample_content = """https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535	基础巡航	"左转窄桥，内切验证有剐蹭护栏风险"
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=90000_ddi-application-667754027299119536	智能泊车	"自动泊车精度不足，需要优化算法"
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537	基础巡航	"右转时识别盲区车辆延迟"
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=20000_ddi-application-667754027299119538	车道变更	"变道决策过于保守，影响通行效率"
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119539	基础巡航
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=30000_ddi-application-667754027299119540"""
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(sample_content)
        return f.name

def demo_url_parsing():
    """演示URL解析功能"""
    print("=== URL解析功能演示 ===")
    
    dataset_manager = DatasetManager()
    
    # 测试不同格式的URL行
    test_lines = [
        'https://example.com/detail?dataName=10000_test	基础巡航	"测试问题描述"',
        'https://example.com/detail?dataName=20000_test	智能泊车',
        'https://example.com/detail?dataName=30000_test'
    ]
    
    for i, line in enumerate(test_lines, 1):
        print(f"\n测试行 {i}: {line}")
        result = dataset_manager.parse_url_line_with_attributes(line)
        if result:
            print(f"  URL: {result['url']}")
            print(f"  模块: {result['module']}")
            print(f"  描述: {result['description']}")
            
            # 提取dataName
            dataname = dataset_manager.extract_dataname_from_url(result['url'])
            print(f"  dataName: {dataname}")
        else:
            print("  解析失败")

def demo_table_creation():
    """演示表创建功能"""
    print("\n=== 表创建功能演示 ===")
    
    try:
        from spdatalab.dataset.bbox import LOCAL_DSN
        eng = create_engine(LOCAL_DSN, future=True)
        
        # 创建测试表
        test_table_name = "clips_bbox_issues_demo"
        success = create_issue_bbox_table_if_not_exists(eng, test_table_name)
        
        if success:
            print(f"✅ 成功创建测试表: {test_table_name}")
            
            # 查看表结构
            with eng.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '{test_table_name}'
                    AND table_schema = 'public'
                    ORDER BY ordinal_position;
                """))
                
                print(f"\n表结构 ({test_table_name}):")
                for row in result:
                    print(f"  {row.column_name}: {row.data_type} ({'NULL' if row.is_nullable == 'YES' else 'NOT NULL'})")
        else:
            print("❌ 表创建失败")
            
    except Exception as e:
        print(f"❌ 表创建演示失败: {str(e)}")

def demo_file_format_detection():
    """演示文件格式检测功能"""
    print("\n=== 文件格式检测演示 ===")
    
    dataset_manager = DatasetManager()
    
    # 创建示例文件
    url_file = create_sample_url_file()
    
    try:
        # 检测文件格式
        file_format = dataset_manager.detect_file_format(url_file)
        print(f"检测到的文件格式: {file_format}")
        
        if file_format == 'url_with_attributes':
            print("✅ 正确识别为带属性的URL格式")
            
            # 解析文件内容（仅作演示，不实际查询数据库）
            print("\n文件内容预览:")
            with open(url_file, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    if i <= 3:  # 只显示前3行
                        result = dataset_manager.parse_url_line_with_attributes(line)
                        if result:
                            print(f"  行 {i}: {result['module']} - {result['description'][:30]}{'...' if len(result['description']) > 30 else ''}")
        else:
            print(f"检测到格式: {file_format}")
            
    finally:
        # 清理临时文件
        os.unlink(url_file)

def demo_query_examples():
    """演示查询示例"""
    print("\n=== SQL查询示例 ===")
    
    queries = [
        {
            "name": "查看所有问题单数据",
            "sql": """
            SELECT 
                scene_token,
                url,
                module,
                description,
                dataname,
                all_good,
                ST_AsText(geometry) as geometry_wkt
            FROM clips_bbox_issues
            ORDER BY id
            LIMIT 10;
            """
        },
        {
            "name": "按责任模块分组统计",
            "sql": """
            SELECT 
                module,
                COUNT(*) as count,
                COUNT(CASE WHEN all_good THEN 1 END) as good_count,
                ROUND(COUNT(CASE WHEN all_good THEN 1 END) * 100.0 / COUNT(*), 2) as good_rate
            FROM clips_bbox_issues
            WHERE module != ''
            GROUP BY module
            ORDER BY count DESC;
            """
        },
        {
            "name": "查询指定区域内的问题单",
            "sql": """
            SELECT 
                url,
                module,
                description,
                scene_token
            FROM clips_bbox_issues
            WHERE ST_Intersects(
                geometry,
                ST_MakeEnvelope(116.3, 39.9, 116.4, 40.0, 4326)
            )
            LIMIT 5;
            """
        }
    ]
    
    for query in queries:
        print(f"\n【{query['name']}】")
        print("```sql")
        print(query['sql'].strip())
        print("```")

def main():
    """主函数"""
    print("问题单URL处理功能演示")
    print("=" * 50)
    
    # 1. URL解析演示
    demo_url_parsing()
    
    # 2. 表创建演示
    demo_table_creation()
    
    # 3. 文件格式检测演示
    demo_file_format_detection()
    
    # 4. 查询示例演示
    demo_query_examples()
    
    print("\n=== 使用说明 ===")
    print("1. 准备URL文件（参考 examples/issue_tickets_example.txt）")
    print("2. 运行处理命令：")
    print("   python -m spdatalab.dataset.bbox \\")
    print("       --input data/issue_tickets.txt \\")
    print("       --issue-tickets \\")
    print("       --create-table \\")
    print("       --batch 100")
    print("3. 查询和分析数据（使用上面的SQL示例）")
    
    print("\n=== 文档参考 ===")
    print("- 详细使用指南: docs/issue_tickets_bbox_guide.md")
    print("- 示例文件: examples/issue_tickets_example.txt")

if __name__ == "__main__":
    main() 