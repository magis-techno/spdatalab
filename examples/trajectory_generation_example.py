#!/usr/bin/env python3
"""
轨迹生成示例（简化版）

展示如何基于场景列表生成轨迹数据的各种用法。
推荐工作流程：
1. 生成bbox概览
2. 在QGIS中分析bbox分布
3. 导出感兴趣的场景列表
4. 基于场景列表生成轨迹
"""

from pathlib import Path
from spdatalab.dataset.bbox import run_trajectory_generation_from_scenes
from spdatalab.dataset.trajectory_generator import TrajectoryGenerator
from sqlalchemy import create_engine

# 数据库连接配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

def example_1_from_file():
    """示例1：从文件读取场景列表生成轨迹"""
    print("=== 示例1：从文件读取场景列表 ===")
    
    # 创建示例场景列表文件
    scene_list_file = "example_scenes.txt"
    example_scenes = [
        "# 示例场景列表",
        "# 这些是我们在QGIS中分析后感兴趣的场景",
        "scene_token_001",
        "scene_token_002", 
        "scene_token_003",
        "scene_token_004",
        "scene_token_005"
    ]
    
    with open(scene_list_file, 'w') as f:
        for scene in example_scenes:
            f.write(f"{scene}\n")
    
    print(f"创建了示例场景列表文件: {scene_list_file}")
    
    # 基于文件生成轨迹
    try:
        trajectory_count = run_trajectory_generation_from_scenes(
            scene_list=scene_list_file,
            trajectory_table="clips_trajectory_example_1",
            input_format="file"
        )
        print(f"成功生成 {trajectory_count} 条轨迹")
    except Exception as e:
        print(f"轨迹生成失败: {e}")
    
    # 清理临时文件
    Path(scene_list_file).unlink()

def example_2_from_list():
    """示例2：直接使用场景列表生成轨迹"""
    print("\n=== 示例2：直接使用场景列表 ===")
    
    # 假设这些是从QGIS中选择的场景
    selected_scenes = [
        "scene_token_101",
        "scene_token_102",
        "scene_token_103"
    ]
    
    print(f"处理 {len(selected_scenes)} 个选定场景")
    
    try:
        trajectory_count = run_trajectory_generation_from_scenes(
            scene_list=selected_scenes,
            trajectory_table="clips_trajectory_example_2",
            input_format="list"
        )
        print(f"成功生成 {trajectory_count} 条轨迹")
    except Exception as e:
        print(f"轨迹生成失败: {e}")

def example_3_qgis_workflow():
    """示例3：模拟QGIS工作流程"""
    print("\n=== 示例3：QGIS工作流程模拟 ===")
    
    # 模拟在QGIS中的操作步骤
    print("步骤1：假设已在QGIS中打开bbox图层")
    print("步骤2：使用空间查询选择北京市的高质量场景")
    
    # 模拟从QGIS属性表中复制的场景列表
    qgis_selected_scenes = [
        "beijing_scene_001",
        "beijing_scene_002", 
        "beijing_scene_003",
        "beijing_scene_004",
        "beijing_scene_005"
    ]
    
    print(f"步骤3：从QGIS中导出了 {len(qgis_selected_scenes)} 个场景")
    
    # 保存到文件（模拟QGIS导出）
    qgis_export_file = "qgis_selected_scenes.txt"
    with open(qgis_export_file, 'w') as f:
        f.write("# 从QGIS导出的北京高质量场景\n")
        f.write("# 筛选条件：city_id = 'beijing' AND all_good = true\n")
        for scene in qgis_selected_scenes:
            f.write(f"{scene}\n")
    
    print(f"步骤4：保存场景列表到文件 {qgis_export_file}")
    
    # 生成轨迹
    try:
        trajectory_count = run_trajectory_generation_from_scenes(
            scene_list=qgis_export_file,
            trajectory_table="clips_trajectory_beijing_selected",
            input_format="file"
        )
        print(f"步骤5：成功生成 {trajectory_count} 条轨迹")
    except Exception as e:
        print(f"轨迹生成失败: {e}")
    
    # 清理临时文件
    Path(qgis_export_file).unlink()

def example_4_batch_processing():
    """示例4：批量处理多个场景列表"""
    print("\n=== 示例4：批量处理多个场景列表 ===")
    
    # 创建多个场景列表文件
    scene_groups = {
        "beijing_accidents": [
            "beijing_accident_001",
            "beijing_accident_002",
            "beijing_accident_003"
        ],
        "shanghai_validation": [
            "shanghai_valid_001", 
            "shanghai_valid_002",
            "shanghai_valid_003"
        ],
        "guangzhou_edge_cases": [
            "guangzhou_edge_001",
            "guangzhou_edge_002"
        ]
    }
    
    for group_name, scenes in scene_groups.items():
        # 创建场景列表文件
        scene_file = f"{group_name}_scenes.txt"
        with open(scene_file, 'w') as f:
            f.write(f"# {group_name} 场景列表\n")
            for scene in scenes:
                f.write(f"{scene}\n")
        
        print(f"创建场景列表: {scene_file} ({len(scenes)} 个场景)")
        
        # 生成轨迹
        trajectory_table = f"clips_trajectory_{group_name}"
        try:
            trajectory_count = run_trajectory_generation_from_scenes(
                scene_list=scene_file,
                trajectory_table=trajectory_table,
                input_format="file"
            )
            print(f"  -> 生成 {trajectory_count} 条轨迹到表 {trajectory_table}")
        except Exception as e:
            print(f"  -> 轨迹生成失败: {e}")
        
        # 清理临时文件
        Path(scene_file).unlink()

def example_5_check_trajectory_data():
    """示例5：检查生成的轨迹数据"""
    print("\n=== 示例5：检查轨迹数据 ===")
    
    # 连接数据库查看轨迹数据
    eng = create_engine(LOCAL_DSN)
    
    # 查看轨迹表列表
    try:
        with eng.connect() as conn:
            result = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'clips_trajectory%'
                ORDER BY table_name
            """)
            
            trajectory_tables = [row[0] for row in result.fetchall()]
            print(f"发现 {len(trajectory_tables)} 个轨迹表:")
            for table in trajectory_tables:
                print(f"  - {table}")
    except Exception as e:
        print(f"查询轨迹表失败: {e}")

def example_6_scene_validation():
    """示例6：场景验证和错误处理"""
    print("\n=== 示例6：场景验证和错误处理 ===")
    
    # 包含一些可能不存在的场景
    mixed_scenes = [
        "valid_scene_001",      # 假设存在
        "invalid_scene_999",    # 假设不存在
        "valid_scene_002",      # 假设存在
        "missing_scene_888"     # 假设不存在
    ]
    
    print(f"测试场景列表（包含可能不存在的场景）:")
    for scene in mixed_scenes:
        print(f"  - {scene}")
    
    try:
        trajectory_count = run_trajectory_generation_from_scenes(
            scene_list=mixed_scenes,
            trajectory_table="clips_trajectory_validation_test",
            input_format="list"
        )
        print(f"实际生成了 {trajectory_count} 条轨迹")
        print("注意：系统自动跳过了没有轨迹数据的场景")
    except Exception as e:
        print(f"轨迹生成失败: {e}")

def example_7_direct_trajectory_generator():
    """示例7：直接使用TrajectoryGenerator类"""
    print("\n=== 示例7：直接使用TrajectoryGenerator类 ===")
    
    # 创建轨迹生成器
    generator = TrajectoryGenerator()
    eng = create_engine(LOCAL_DSN)
    
    # 测试场景列表
    test_scenes = ["test_scene_001", "test_scene_002"]
    
    try:
        # 创建轨迹表
        table_name = "clips_trajectory_direct_test"
        if generator.create_trajectory_table_if_not_exists(eng, table_name):
            print(f"轨迹表 {table_name} 已就绪")
        
        # 获取轨迹数据
        trajectory_df = generator.fetch_trajectory_data(test_scenes)
        print(f"获取到 {len(trajectory_df)} 个轨迹点")
        
        # 处理轨迹数据
        if not trajectory_df.empty:
            trajectory_gdf = generator.process_trajectory_data(trajectory_df)
            print(f"处理后得到 {len(trajectory_gdf)} 条轨迹")
            
            # 插入数据库
            if not trajectory_gdf.empty:
                trajectory_gdf.to_postgis(
                    table_name,
                    eng,
                    if_exists='append',
                    index=False
                )
                print(f"成功插入 {len(trajectory_gdf)} 条轨迹到数据库")
        else:
            print("没有找到轨迹数据")
            
    except Exception as e:
        print(f"直接使用TrajectoryGenerator失败: {e}")

if __name__ == "__main__":
    print("轨迹生成示例（简化版）")
    print("=" * 50)
    
    # 运行所有示例
    try:
        example_1_from_file()
        example_2_from_list()
        example_3_qgis_workflow()
        example_4_batch_processing()
        example_5_check_trajectory_data()
        example_6_scene_validation()
        example_7_direct_trajectory_generator()
        
        print("\n" + "=" * 50)
        print("✅ 所有示例运行完成")
        
    except Exception as e:
        print(f"❌ 示例运行失败: {e}")
        
    print("\n推荐的实际使用工作流程:")
    print("1. 使用 --mode bbox 生成概览数据")
    print("2. 在QGIS中加载bbox图层进行可视化分析")
    print("3. 在QGIS中选择感兴趣的场景，导出scene_token列表")
    print("4. 使用 --generate-trajectories --scenes 基于列表生成轨迹")
    print("5. 在QGIS中分析生成的轨迹数据") 