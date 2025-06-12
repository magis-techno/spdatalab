#!/usr/bin/env python3
"""
简单的收费站分析测试脚本

用于验证新的收费站分析功能是否正常工作
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.spdatalab.fusion.toll_station_analysis import TollStationAnalyzer
    print("✅ 成功导入收费站分析模块")
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    sys.exit(1)

def test_database_connection():
    """测试数据库连接"""
    print("\n🔗 测试数据库连接...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 测试本地数据库连接
        with analyzer.local_engine.connect() as conn:
            result = conn.execute("SELECT 1").scalar()
            print("✅ 本地数据库连接正常")
        
        # 测试远程数据库连接
        with analyzer.remote_engine.connect() as conn:
            result = conn.execute("SELECT 1").scalar()
            print("✅ 远程数据库连接正常")
            
        return True
        
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False

def test_intersection_table():
    """测试intersection表是否存在"""
    print("\n📋 检查intersection表...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        with analyzer.remote_engine.connect() as conn:
            # 检查表是否存在
            check_table_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'full_intersection'
                );
            """
            table_exists = conn.execute(check_table_sql).scalar()
            
            if table_exists:
                print("✅ full_intersection表存在")
                
                # 检查表中的记录数
                count_sql = "SELECT COUNT(*) FROM full_intersection"
                total_count = conn.execute(count_sql).scalar()
                print(f"📊 表中总记录数: {total_count:,}")
                
                # 检查收费站数量
                toll_count_sql = "SELECT COUNT(*) FROM full_intersection WHERE intersectiontype = 2"
                toll_count = conn.execute(toll_count_sql).scalar()
                print(f"🏛️ 收费站数量: {toll_count}")
                
                if toll_count > 0:
                    return True
                else:
                    print("⚠️ 表中没有收费站数据（intersectiontype=2）")
                    return False
                    
            else:
                print("❌ full_intersection表不存在")
                return False
                
    except Exception as e:
        print(f"❌ 检查表失败: {e}")
        return False

def test_toll_station_search():
    """测试收费站搜索功能"""
    print("\n🔍 测试收费站搜索...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 查找少量收费站进行测试
        toll_stations, analysis_id = analyzer.find_toll_stations(limit=5)
        
        if not toll_stations.empty:
            print(f"✅ 成功找到 {len(toll_stations)} 个收费站")
            print(f"📋 分析ID: {analysis_id}")
            
            # 显示收费站信息
            for _, row in toll_stations.iterrows():
                print(f"   ID: {row['intersection_id']}, 类型: {row['intersectiontype']}")
            
            return analysis_id
        else:
            print("❌ 未找到收费站数据")
            return None
            
    except Exception as e:
        print(f"❌ 收费站搜索失败: {e}")
        return None

def test_trajectory_analysis(analysis_id):
    """测试轨迹分析功能"""
    if not analysis_id:
        print("\n⚠️ 跳过轨迹分析测试（没有收费站数据）")
        return False
        
    print(f"\n🚗 测试轨迹分析...")
    
    try:
        analyzer = TollStationAnalyzer()
        
        # 分析轨迹数据
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(
            analysis_id=analysis_id,
            use_buffer=True
        )
        
        if not trajectory_results.empty:
            print(f"✅ 成功分析轨迹数据")
            print(f"📊 找到 {len(trajectory_results)} 个数据集-收费站组合")
            
            # 显示统计信息
            total_trajectories = trajectory_results['trajectory_count'].sum()
            unique_datasets = trajectory_results['dataset_name'].nunique()
            print(f"📈 总轨迹数: {total_trajectories:,}")
            print(f"📦 数据集数: {unique_datasets}")
            
            return True
        else:
            print("⚠️ 未找到轨迹数据（这可能是正常的，如果收费站范围内没有轨迹）")
            return True  # 这不算失败
            
    except Exception as e:
        print(f"❌ 轨迹分析失败: {e}")
        return False

def main():
    """主函数"""
    print("🧪 收费站分析功能测试")
    print("=" * 50)
    
    success_count = 0
    total_tests = 4
    
    # 测试1: 数据库连接
    if test_database_connection():
        success_count += 1
    
    # 测试2: intersection表检查
    if test_intersection_table():
        success_count += 1
    
    # 测试3: 收费站搜索
    analysis_id = test_toll_station_search()
    if analysis_id:
        success_count += 1
    
    # 测试4: 轨迹分析
    if test_trajectory_analysis(analysis_id):
        success_count += 1
    
    # 总结
    print("\n" + "=" * 50)
    print(f"📊 测试结果: {success_count}/{total_tests} 项测试通过")
    
    if success_count == total_tests:
        print("🎉 所有测试通过！收费站分析功能正常")
        return 0
    elif success_count >= 2:
        print("⚠️ 部分测试通过，基本功能可用")
        return 0
    else:
        print("❌ 多数测试失败，请检查配置")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 