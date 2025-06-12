#!/usr/bin/env python3
"""
并行处理功能演示脚本

演示新增的并行处理功能及其使用方法
"""

def demo_parallel_commands():
    """演示并行处理命令"""
    print("🚀 并行处理功能演示")
    print("=" * 80)
    
    commands = [
        {
            "title": "基础并行处理",
            "command": """python -m spdatalab process-bbox \\
    --input dataset.json \\
    --use-partitioning \\
    --use-parallel""",
            "description": "使用默认设置开启并行处理（自动检测CPU核心数）"
        },
        {
            "title": "指定并行worker数量",
            "command": """python -m spdatalab process-bbox \\
    --input dataset.json \\
    --use-partitioning \\
    --use-parallel \\
    --max-workers 4""",
            "description": "手动指定使用4个并行worker"
        },
        {
            "title": "并行处理完整配置",
            "command": """python -m spdatalab process-bbox \\
    --input dataset.json \\
    --use-partitioning \\
    --use-parallel \\
    --max-workers 6 \\
    --batch 2000 \\
    --insert-batch 1000 \\
    --create-unified-view \\
    --work-dir ./parallel_logs""",
            "description": "并行处理的完整参数配置"
        },
        {
            "title": "传统顺序处理（对比）",
            "command": """python -m spdatalab process-bbox \\
    --input dataset.json \\
    --use-partitioning""",
            "description": "不使用并行的分表模式（用于性能对比）"
        }
    ]
    
    for i, cmd_info in enumerate(commands, 1):
        print(f"\n{i}. {cmd_info['title']}")
        print("-" * 60)
        print(f"💡 {cmd_info['description']}")
        print(f"📝 命令:")
        print(f"   {cmd_info['command']}")

def demo_performance_comparison():
    """演示性能对比测试"""
    print("\n" + "="*80)
    print("📊 性能对比测试演示")
    print("="*80)
    
    print("""
1. 使用专用测试脚本进行性能对比:
   
   python test_parallel_performance.py \\
       --dataset-file dataset.json \\
       --test-mode both \\
       --batch-size 1000 \\
       --max-workers 4

2. 仅测试并行处理:
   
   python test_parallel_performance.py \\
       --dataset-file dataset.json \\
       --test-mode parallel \\
       --max-workers 6

3. 仅测试顺序处理:
   
   python test_parallel_performance.py \\
       --dataset-file dataset.json \\
       --test-mode sequential
    """)

def demo_best_practices():
    """演示最佳实践建议"""
    print("\n" + "="*80)
    print("💡 并行处理最佳实践")
    print("="*80)
    
    practices = [
        {
            "scenario": "小数据集（<50万记录）",
            "recommendation": "使用顺序处理，避免多进程开销",
            "command": "--use-partitioning（不使用--use-parallel）"
        },
        {
            "scenario": "中等数据集（50万-200万记录）",
            "recommendation": "使用并行处理，worker数量 = CPU核心数的一半",
            "command": "--use-partitioning --use-parallel --max-workers 4"
        },
        {
            "scenario": "大数据集（>200万记录）",
            "recommendation": "使用并行处理，worker数量 = CPU核心数",
            "command": "--use-partitioning --use-parallel（让系统自动检测）"
        },
        {
            "scenario": "服务器环境",
            "recommendation": "限制worker数量避免影响其他服务",
            "command": "--use-partitioning --use-parallel --max-workers 6"
        },
        {
            "scenario": "开发测试环境",
            "recommendation": "使用较少worker避免系统过载",
            "command": "--use-partitioning --use-parallel --max-workers 2"
        }
    ]
    
    for practice in practices:
        print(f"\n📋 {practice['scenario']}:")
        print(f"   建议: {practice['recommendation']}")
        print(f"   命令: {practice['command']}")

def demo_parallel_architecture():
    """演示并行架构说明"""
    print("\n" + "="*80)
    print("🔧 并行架构说明")
    print("="*80)
    
    print("""
🎯 并行化策略:
   - 按子数据集(subdataset)进行并行处理
   - 每个worker处理一个完整的子数据集
   - 避免数据库连接竞争和事务冲突

📊 性能优势:
   - CPU密集型任务并行化（数据处理、转换）
   - I/O操作并行化（数据库查询、插入）
   - 减少总体处理时间

⚠️  注意事项:
   - 数据库连接池限制（每个worker独立连接）
   - 内存使用量增加（多进程同时运行）
   - 不适合子数据集数量很少的场景

🔍 监控指标:
   - 每个worker的处理进度独立显示
   - 总体性能提升倍数计算
   - 处理时间和吞吐量对比
    """)

def demo_troubleshooting():
    """演示故障排除指南"""
    print("\n" + "="*80)
    print("🛠️  故障排除指南")
    print("="*80)
    
    issues = [
        {
            "problem": "并行处理比顺序处理慢",
            "causes": [
                "数据量太小，多进程开销大于收益",
                "worker数量设置过多，资源竞争激烈",
                "数据库连接限制"
            ],
            "solutions": [
                "减少max-workers数量",
                "对小数据集使用顺序处理",
                "检查数据库连接池配置"
            ]
        },
        {
            "problem": "内存使用过高",
            "causes": [
                "worker数量过多",
                "batch-size设置过大",
                "大量数据同时加载到内存"
            ],
            "solutions": [
                "减少max-workers",
                "减少batch-size",
                "增加系统内存或使用更小的批次处理"
            ]
        },
        {
            "problem": "某些worker进程卡住",
            "causes": [
                "数据库连接超时",
                "某个子数据集数据异常",
                "进程间死锁"
            ],
            "solutions": [
                "检查数据库连接状态",
                "查看worker进程日志",
                "重启处理，跳过问题数据集"
            ]
        }
    ]
    
    for issue in issues:
        print(f"\n❌ 问题: {issue['problem']}")
        print("   可能原因:")
        for cause in issue['causes']:
            print(f"     - {cause}")
        print("   解决方案:")
        for solution in issue['solutions']:
            print(f"     - {solution}")

def main():
    """主函数"""
    print("🎉 并行处理功能演示")
    print("🎯 本脚本演示新增的并行处理功能及使用方法")
    
    # 演示并行处理命令
    demo_parallel_commands()
    
    # 演示性能对比测试
    demo_performance_comparison()
    
    # 演示最佳实践
    demo_best_practices()
    
    # 演示并行架构
    demo_parallel_architecture()
    
    # 演示故障排除
    demo_troubleshooting()
    
    print("\n" + "="*80)
    print("✅ 并行处理功能演示完成")
    print("="*80)
    print("""
🚀 关键新功能:
   ✅ 多进程并行处理 (ProcessPoolExecutor)
   ✅ 自动CPU核心数检测
   ✅ 手动worker数量控制
   ✅ 独立进度跟踪
   ✅ 性能提升监控
   ✅ 智能错误处理

📚 相关文件:
   - 性能测试: python test_parallel_performance.py
   - 开发计划: DEVELOPMENT_PLAN.md
   - 核心实现: src/spdatalab/dataset/bbox.py

🎯 预期效果:
   - 大数据集处理速度提升 2-6倍
   - CPU资源充分利用
   - 总体处理时间显著减少
    """)

if __name__ == "__main__":
    main() 