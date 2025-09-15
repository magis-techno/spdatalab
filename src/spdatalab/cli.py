"""命令行接口模块。"""

import click
import logging
from pathlib import Path

from .dataset.scene_list_generator import SceneListGenerator
from .dataset.dataset_manager import DatasetManager

logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Spatial-Data-Lab 工具集。"""
    pass


@cli.command()
@click.option('--index-file', required=True, help='索引文件路径')
@click.option('--output', required=True, help='输出文件路径')
def generate_scene_list(index_file: str, output: str):
    """生成场景数据列表。
    
    从索引文件读取数据源信息，生成场景数据列表并保存到输出文件。
    
    Args:
        index_file: 索引文件路径，每行格式为 obs_path@duplicateN
        output: 输出文件路径，保存为JSON格式
    """
    setup_logging()
    
    try:
        generator = SceneListGenerator()
        generator.generate_scene_list(index_file, output)
    except Exception as e:
        logger.error(f"生成场景数据列表失败: {str(e)}")
        raise

@cli.command()
@click.option('--index-file', help='索引文件路径（txt格式）')
@click.option('--training-dataset-json', help='训练数据集JSON文件路径')
@click.option('--dataset-name', help='数据集名称（使用JSON输入时可选，优先使用JSON中的名称）')
@click.option('--description', default='', help='数据集描述（使用JSON输入时可选，优先使用JSON中的描述）')
@click.option('--output', required=True, help='输出文件路径')
@click.option('--format', type=click.Choice(['json', 'parquet']), default='json', help='输出格式')
@click.option('--defect-mode', is_flag=True, help='启用问题单模式（处理问题单URL）')
def build_dataset(index_file: str, training_dataset_json: str, dataset_name: str, description: str, output: str, format: str, defect_mode: bool):
    """构建数据集结构。
    
    支持两种输入格式：
    1. 索引文件输入（txt格式）：从索引文件读取数据源信息
    2. 训练数据集JSON输入：从结构化JSON文件读取数据集信息（推荐）
    
    支持两种处理模式：
    1. 标准模式：处理OBS路径格式的训练数据（默认）
    2. 问题单模式：处理问题单URL数据（使用--defect-mode）
    
    Args:
        index_file: 索引文件路径（txt格式）
                   标准模式：每行格式为 obs_path@duplicateN
                   问题单模式：每行为问题单URL或URL|属性
        training_dataset_json: 训练数据集JSON文件路径，包含完整的数据集元信息
        dataset_name: 数据集名称（使用JSON输入时可选，优先使用JSON中的名称）
        description: 数据集描述（使用JSON输入时可选，优先使用JSON中的描述）
        output: 输出文件路径，保存为指定格式
        format: 输出格式，json 或 parquet
        defect_mode: 是否启用问题单处理模式
        
    Examples:
        # 使用JSON格式输入（推荐）
        python -m spdatalab.cli build-dataset \\
            --training-dataset-json training_dataset.json \\
            --output datasets/dataset.json
            
        # 使用传统txt格式输入
        python -m spdatalab.cli build-dataset \\
            --index-file data/index.txt \\
            --dataset-name "Dataset" \\
            --description "GOD E2E training dataset" \\
            --output datasets/dataset.json
    """
    setup_logging()
    
    # 验证输入参数
    if not index_file and not training_dataset_json:
        raise click.ClickException("必须提供 --index-file 或 --training-dataset-json 其中之一")
    
    if index_file and training_dataset_json:
        raise click.ClickException("--index-file 和 --training-dataset-json 不能同时使用")
    
    # 使用 txt 格式输入时，dataset_name 是必需的
    if index_file and not dataset_name:
        raise click.ClickException("使用 --index-file 时，--dataset-name 是必需的")
    
    try:
        manager = DatasetManager(defect_mode=defect_mode)
        
        if training_dataset_json:
            # 使用 JSON 格式输入
            dataset = manager.build_dataset_from_training_json(
                training_dataset_json, dataset_name, description
            )
            input_type = "JSON格式"
        else:
            # 使用传统 txt 格式输入
            dataset = manager.build_dataset_from_index(index_file, dataset_name, description)
            input_type = "txt格式"
        
        manager.save_dataset(dataset, output, format=format)
        
        click.echo(f"✅ 数据集构建完成: {dataset.name}")
        click.echo(f"   - 输入格式: {input_type}")
        click.echo(f"   - 处理模式: {'问题单模式' if defect_mode else '标准模式'}")
        click.echo(f"   - 子数据集数量: {len(dataset.subdatasets)}")
        click.echo(f"   - 总唯一场景数: {dataset.total_unique_scenes}")
        click.echo(f"   - 总场景数(含倍增): {dataset.total_scenes}")
        click.echo(f"   - 已保存到: {output} ({format}格式)")
        
        if defect_mode:
            # 显示问题单处理统计
            stats = manager.stats
            click.echo(f"   - 成功处理: {stats['processed_files']} 个URL")
            click.echo(f"   - 失败处理: {stats['failed_files']} 个URL")
            if stats['defect_query_failed'] > 0:
                click.echo(f"   - 数据库查询失败: {stats['defect_query_failed']} 个")
            if stats['defect_no_scene'] > 0:
                click.echo(f"   - 无scene_id: {stats['defect_no_scene']} 个")
        
        if format == 'parquet':
            click.echo(f"   - 推荐安装: pip install pandas pyarrow")
            
    except Exception as e:
        logger.error(f"构建数据集失败: {str(e)}")
        raise

@cli.command()
@click.option('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
@click.option('--batch', type=int, default=1000, help='处理批次大小')
@click.option('--insert-batch', type=int, default=1000, help='插入批次大小')
@click.option('--work-dir', help='工作目录，用于存储进度文件')
@click.option('--retry-failed', is_flag=True, help='只重试失败的数据')
@click.option('--show-stats', is_flag=True, help='显示处理统计信息并退出')
@click.option('--create-table', is_flag=True, default=True, help='是否创建表（如果不存在）')
@click.option('--use-partitioning', is_flag=True, help='使用分表模式处理（按子数据集分表存储）')
@click.option('--create-unified-view', is_flag=True, default=True, help='分表模式下是否创建统一视图')
@click.option('--maintain-view-only', is_flag=True, help='仅维护统一视图，不处理数据')
def process_bbox(input: str, batch: int, insert_batch: int, work_dir: str, retry_failed: bool, show_stats: bool, create_table: bool, use_partitioning: bool, create_unified_view: bool, maintain_view_only: bool):
    """处理边界框数据。
    
    从数据集文件中加载场景ID，获取边界框信息并插入到PostGIS数据库中。
    支持JSON、Parquet和文本格式的数据集文件。
    
    支持两种模式：
    1. 传统模式：插入到单表clips_bbox（默认）
    2. 分表模式：按子数据集分表存储，可选择创建统一视图
    
    Args:
        input: 输入文件路径，支持JSON/Parquet/文本格式
        batch: 处理批次大小，每批从数据库获取多少个场景的信息
        insert_batch: 插入批次大小，每批向数据库插入多少条记录
        work_dir: 工作目录，用于存储进度文件
        retry_failed: 是否只重试失败的数据
        show_stats: 是否显示统计信息并退出
        create_table: 是否创建表（如果不存在）
        use_partitioning: 是否使用分表模式处理
        create_unified_view: 分表模式下是否创建统一视图
        maintain_view_only: 是否仅维护统一视图，不处理数据
    """
    setup_logging()
    
    try:
        # 根据模式选择不同的运行函数
        if use_partitioning:
            from .dataset.bbox import run_with_partitioning
            
            click.echo(f"🎯 开始分表模式处理边界框数据:")
            click.echo(f"  - 输入文件: {input}")
            click.echo(f"  - 处理批次: {batch}")
            click.echo(f"  - 插入批次: {insert_batch}")
            click.echo(f"  - 工作目录: {work_dir or './bbox_import_logs'}")
            click.echo(f"  - 创建统一视图: {'是' if create_unified_view else '否'}")
            click.echo(f"  - 仅维护视图: {'是' if maintain_view_only else '否'}")
            
            if show_stats:
                click.echo("分表模式下显示统计信息功能暂未实现")
                return
            
            run_with_partitioning(
                input_path=input,
                batch=batch,
                insert_batch=insert_batch,
                work_dir=work_dir or "./bbox_import_logs",
                create_unified_view_flag=create_unified_view,
                maintain_view_only=maintain_view_only
            )
            
            click.echo("✅ 分表模式边界框处理完成")
            
        else:
            from .dataset.bbox import run as bbox_run
            
            if show_stats:
                click.echo("显示处理统计信息功能暂未实现")
                return
            
            click.echo(f"📝 开始传统模式处理边界框数据:")
            click.echo(f"  - 输入文件: {input}")
            click.echo(f"  - 处理批次: {batch}")
            click.echo(f"  - 插入批次: {insert_batch}")
            if work_dir:
                click.echo(f"  - 工作目录: {work_dir}")
            if retry_failed:
                click.echo(f"  - 重试模式: 仅处理失败的数据")
            
            bbox_run(
                input_path=input,
                batch=batch,
                insert_batch=insert_batch,
                work_dir=work_dir or "./bbox_import_logs",
                retry_failed=retry_failed,
                create_table=create_table
            )
            
            click.echo("✅ 传统模式边界框处理完成")
        
    except Exception as e:
        logger.error(f"处理边界框失败: {str(e)}")
        raise

@cli.command()
@click.option('--view-name', default='clips_bbox_unified', help='统一视图名称')
def create_unified_view(view_name: str):
    """创建或更新bbox分表的统一视图。
    
    自动发现所有bbox分表并创建统一视图，便于跨表查询。
    
    Args:
        view_name: 统一视图名称
    """
    setup_logging()
    
    try:
        from .dataset.bbox import create_unified_view as create_view_func
        from sqlalchemy import create_engine
        
        # 这里需要导入数据库连接配置
        LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        eng = create_engine(LOCAL_DSN, future=True)
        
        click.echo(f"🔧 创建统一视图: {view_name}")
        
        success = create_view_func(eng, view_name)
        
        if success:
            click.echo(f"✅ 统一视图 {view_name} 创建成功")
        else:
            click.echo(f"❌ 统一视图 {view_name} 创建失败")
            
    except Exception as e:
        logger.error(f"创建统一视图失败: {str(e)}")
        raise

@cli.command()
@click.option('--view-name', default='clips_bbox_unified', help='统一视图名称')
def maintain_unified_view(view_name: str):
    """维护bbox分表的统一视图。
    
    检查并更新统一视图以包含所有当前的分表。
    
    Args:
        view_name: 统一视图名称
    """
    setup_logging()
    
    try:
        from .dataset.bbox import maintain_unified_view as maintain_view_func
        from sqlalchemy import create_engine
        
        # 这里需要导入数据库连接配置
        LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        eng = create_engine(LOCAL_DSN, future=True)
        
        click.echo(f"🔧 维护统一视图: {view_name}")
        
        success = maintain_view_func(eng, view_name)
        
        if success:
            click.echo(f"✅ 统一视图 {view_name} 维护成功")
        else:
            click.echo(f"❌ 统一视图 {view_name} 维护失败")
            
    except Exception as e:
        logger.error(f"维护统一视图失败: {str(e)}")
        raise

@cli.command()
def list_bbox_tables():
    """列出所有bbox相关的数据表。
    
    显示数据库中所有bbox分表的信息。
    """
    setup_logging()
    
    try:
        from .dataset.bbox import list_bbox_tables as list_tables_func
        from sqlalchemy import create_engine
        
        # 这里需要导入数据库连接配置
        LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        eng = create_engine(LOCAL_DSN, future=True)
        
        click.echo("📋 查询bbox数据表...")
        
        tables = list_tables_func(eng)
        
        if tables:
            click.echo(f"找到 {len(tables)} 个bbox表:")
            for i, table in enumerate(tables, 1):
                click.echo(f"  {i:2d}. {table}")
        else:
            click.echo("没有找到任何bbox表")
            
    except Exception as e:
        logger.error(f"列出bbox表失败: {str(e)}")
        raise

@cli.command()
@click.option('--index-file', required=True, help='索引文件路径')
@click.option('--dataset-name', required=True, help='数据集名称')
@click.option('--description', default='', help='数据集描述')
@click.option('--output', required=True, help='输出数据集文件路径')
@click.option('--format', type=click.Choice(['json', 'parquet']), default='json', help='数据集保存格式')
@click.option('--batch', type=int, default=1000, help='边界框处理批次大小')
@click.option('--insert-batch', type=int, default=1000, help='边界框插入批次大小')
@click.option('--buffer-meters', type=int, default=50, help='缓冲区大小（米）')
@click.option('--precise-buffer', is_flag=True, help='使用精确的米级缓冲区')
@click.option('--skip-bbox', is_flag=True, help='跳过边界框处理')
@click.option('--defect-mode', is_flag=True, help='启用问题单模式（处理问题单URL）')
def build_dataset_with_bbox(index_file: str, dataset_name: str, description: str, output: str, 
                           format: str, batch: int, insert_batch: int, buffer_meters: int, 
                           precise_buffer: bool, skip_bbox: bool, defect_mode: bool):
    """构建数据集并处理边界框（完整工作流程）。
    
    从索引文件构建数据集，保存后自动处理边界框数据，提供一键式完整工作流程。
    
    支持两种模式：
    1. 标准模式：处理OBS路径格式的训练数据（默认）
    2. 问题单模式：处理问题单URL数据（使用--defect-mode）
    
    Args:
        index_file: 索引文件路径
                   标准模式：每行格式为 obs_path@duplicateN
                   问题单模式：每行为问题单URL或URL|属性
        dataset_name: 数据集名称
        description: 数据集描述
        output: 输出数据集文件路径
        format: 数据集保存格式，json 或 parquet
        batch: 边界框处理批次大小
        insert_batch: 边界框插入批次大小
        buffer_meters: 缓冲区大小（米）
        precise_buffer: 是否使用精确的米级缓冲区
        skip_bbox: 是否跳过边界框处理
        defect_mode: 是否启用问题单处理模式
    """
    setup_logging()
    
    try:
        # 步骤1：构建数据集
        click.echo("=== 步骤1: 构建数据集 ===")
        click.echo(f"   - 处理模式: {'问题单模式' if defect_mode else '标准模式'}")
        manager = DatasetManager(defect_mode=defect_mode)
        dataset = manager.build_dataset_from_index(index_file, dataset_name, description)
        
        # 显示数据集统计信息
        stats = manager.get_dataset_stats(dataset)
        click.echo(f"数据集统计信息:")
        click.echo(f"  - 数据集名称: {stats['dataset_name']}")
        click.echo(f"  - 子数据集数量: {stats['subdataset_count']}")
        click.echo(f"  - 唯一场景数: {stats['total_unique_scenes']}")
        click.echo(f"  - 总场景数(含倍增): {stats['total_scenes_with_duplicates']}")
        
        # 步骤2：保存数据集
        click.echo("=== 步骤2: 保存数据集 ===")
        # 确保输出目录存在
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        manager.save_dataset(dataset, output, format=format)
        click.echo(f"✅ 数据集已保存到: {output} ({format}格式)")
        
        # 步骤3：处理边界框（如果需要）
        if not skip_bbox:
            click.echo("=== 步骤3: 处理边界框 ===")
            
            from .dataset.bbox import run as bbox_run
            
            click.echo(f"边界框处理配置:")
            click.echo(f"  - 处理批次: {batch}")
            click.echo(f"  - 插入批次: {insert_batch}")
            click.echo(f"  - 缓冲区: {buffer_meters}米")
            click.echo(f"  - 精确模式: {'是' if precise_buffer else '否'}")
            
            bbox_run(
                input_path=output,
                batch=batch,
                insert_batch=insert_batch,
                buffer_meters=buffer_meters,
                use_precise_buffer=precise_buffer
            )
            
            click.echo("✅ 边界框处理完成")
        else:
            click.echo("=== 步骤3: 跳过边界框处理 ===")
        
        click.echo("🎉 完整工作流程完成！")
        
    except Exception as e:
        logger.error(f"完整工作流程失败: {str(e)}")
        raise

@cli.command()
@click.option('--dataset-file', required=True, help='数据集文件路径')
@click.option('--subdataset', default=None, help='子数据集名称（可选）')
@click.option('--output', default=None, help='输出文件路径（可选）')
def list_scenes(dataset_file: str, subdataset: str, output: str):
    """列出数据集中的场景ID。
    
    Args:
        dataset_file: 数据集文件路径
        subdataset: 子数据集名称，如果不指定则列出所有场景
        output: 输出文件路径，如果不指定则输出到控制台
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(dataset_file)
        scene_ids = manager.list_scene_ids(dataset, subdataset)
        
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                for scene_id in scene_ids:
                    f.write(f"{scene_id}\n")
            click.echo(f"✅ 场景ID列表已保存到: {output}")
        else:
            click.echo(f"场景ID列表 ({'子数据集: ' + subdataset if subdataset else '全部'}):")
            for scene_id in scene_ids:
                click.echo(f"  {scene_id}")
        
        click.echo(f"总共 {len(scene_ids)} 个场景")
        
    except Exception as e:
        logger.error(f"列出场景失败: {str(e)}")
        raise

@cli.command()
@click.option('--dataset-file', required=True, help='数据集文件路径')
@click.option('--subdataset', default=None, help='子数据集名称（可选）')
@click.option('--output', required=True, help='输出文件路径')
def generate_scene_ids(dataset_file: str, subdataset: str, output: str):
    """生成包含倍增的场景ID列表。
    
    Args:
        dataset_file: 数据集文件路径
        subdataset: 子数据集名称，如果不指定则处理所有子数据集
        output: 输出文件路径
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(dataset_file)
        
        count = 0
        with open(output, 'w', encoding='utf-8') as f:
            for scene_id in manager.generate_scene_list_with_duplication(dataset, subdataset):
                f.write(f"{scene_id}\n")
                count += 1
        
        click.echo(f"✅ 场景ID列表（含倍增）已保存到: {output}")
        click.echo(f"总共 {count} 个场景（含倍增）")
        
    except Exception as e:
        logger.error(f"生成场景ID列表失败: {str(e)}")
        raise

@cli.command()
@click.option('--dataset-file', required=True, help='数据集文件路径')
def dataset_info(dataset_file: str):
    """显示数据集信息。
    
    Args:
        dataset_file: 数据集文件路径
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(dataset_file)
        
        click.echo(f"数据集信息:")
        click.echo(f"  名称: {dataset.name}")
        click.echo(f"  描述: {dataset.description}")
        click.echo(f"  创建时间: {dataset.created_at}")
        click.echo(f"  子数据集数量: {len(dataset.subdatasets)}")
        click.echo(f"  总唯一场景数: {dataset.total_unique_scenes}")
        click.echo(f"  总场景数(含倍增): {dataset.total_scenes}")
        
        if dataset.subdatasets:
            click.echo(f"\n子数据集详情:")
            for i, subdataset in enumerate(dataset.subdatasets, 1):
                click.echo(f"  {i}. {subdataset.name}")
                click.echo(f"     - OBS路径: {subdataset.obs_path}")
                click.echo(f"     - 场景数: {subdataset.scene_count}")
                click.echo(f"     - 倍增因子: {subdataset.duplication_factor}")
                click.echo(f"     - 倍增后场景数: {subdataset.scene_count * subdataset.duplication_factor}")
        
    except Exception as e:
        logger.error(f"显示数据集信息失败: {str(e)}")
        raise

@cli.command()
@click.option('--dataset-file', required=True, help='数据集文件路径')
@click.option('--output', required=True, help='输出文件路径')
@click.option('--include-duplicates', is_flag=True, help='是否包含倍增的场景ID')
def export_scene_ids(dataset_file: str, output: str, include_duplicates: bool):
    """导出场景ID为Parquet格式。
    
    Args:
        dataset_file: 数据集文件路径
        output: 输出文件路径（parquet格式）
        include_duplicates: 是否包含倍增的场景ID
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(dataset_file)
        manager.export_scene_ids_parquet(dataset, output, include_duplicates)
        
        click.echo(f"✅ 场景ID已导出到: {output}")
        if include_duplicates:
            click.echo(f"   - 包含倍增，总记录数: {dataset.total_scenes}")
        else:
            click.echo(f"   - 不含倍增，总记录数: {dataset.total_unique_scenes}")
            
    except Exception as e:
        logger.error(f"导出场景ID失败: {str(e)}")
        raise

@cli.command()
@click.option('--parquet-file', required=True, help='Parquet数据集文件路径')
@click.option('--subdataset', default=None, help='按子数据集过滤')
@click.option('--duplication-factor', type=int, default=None, help='按倍增因子过滤')
@click.option('--output', default=None, help='输出文件路径（可选）')
def query_parquet(parquet_file: str, subdataset: str, duplication_factor: int, output: str):
    """查询Parquet格式数据集。
    
    Args:
        parquet_file: Parquet数据集文件路径
        subdataset: 按子数据集名称过滤
        duplication_factor: 按倍增因子过滤
        output: 输出文件路径，如果不指定则显示统计信息
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        
        # 构建过滤条件
        filters = {}
        if subdataset:
            filters['subdataset_name'] = subdataset
        if duplication_factor:
            filters['duplication_factor'] = duplication_factor
            
        df = manager.query_scenes_parquet(parquet_file, **filters)
        
        if output:
            df.to_parquet(output, index=False)
            click.echo(f"✅ 查询结果已保存到: {output}")
        else:
            click.echo(f"查询结果:")
            click.echo(f"  - 匹配记录数: {len(df)}")
            if len(df) > 0:
                click.echo(f"  - 唯一子数据集: {df['subdataset_name'].nunique()}")
                click.echo(f"  - 子数据集列表:")
                for name in df['subdataset_name'].unique():
                    count = len(df[df['subdataset_name'] == name])
                    click.echo(f"    * {name}: {count} 个场景")
        
        click.echo(f"总记录数: {len(df)}")
        
    except Exception as e:
        logger.error(f"查询Parquet数据集失败: {str(e)}")
        raise

@cli.command()
@click.option('--dataset-file', required=True, help='数据集文件路径')
@click.option('--output-format', type=click.Choice(['json', 'parquet']), default='json', help='输出格式')
def dataset_stats(dataset_file: str, output_format: str):
    """显示数据集统计信息。
    
    Args:
        dataset_file: 数据集文件路径
        output_format: 输出格式
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.load_dataset(dataset_file)
        stats = manager.get_dataset_stats(dataset)
        
        if output_format == 'json':
            click.echo(json.dumps(stats, ensure_ascii=False, indent=2))
        else:
            click.echo(f"数据集统计信息:")
            click.echo(f"  数据集名称: {stats['dataset_name']}")
            click.echo(f"  子数据集数量: {stats['subdataset_count']}")
            click.echo(f"  总唯一场景数: {stats['total_unique_scenes']}")
            click.echo(f"  总场景数(含倍增): {stats['total_scenes_with_duplicates']}")
            
            click.echo(f"\n子数据集详情:")
            for i, sub_stats in enumerate(stats['subdatasets'], 1):
                click.echo(f"  {i}. {sub_stats['name']}")
                click.echo(f"     - 场景数: {sub_stats['scene_count']}")
                click.echo(f"     - 倍增因子: {sub_stats['duplication_factor']}")
                click.echo(f"     - 倍增后场景数: {sub_stats['total_scenes_with_duplicates']}")
        
    except Exception as e:
        logger.error(f"获取数据集统计信息失败: {str(e)}")
        raise

@cli.command()
@click.option('--left-table', default='clips_bbox', help='左表名（如clips_bbox）')
@click.option('--right-table', required=True, help='右表名（如intersections）')
@click.option('--num-bbox', type=int, default=1000, help='要处理的bbox数量')
@click.option('--city-filter', help='城市过滤条件')
@click.option('--chunk-size', type=int, help='分块大小（用于大规模数据处理）')
@click.option('--spatial-relation', type=click.Choice([
    'intersects', 'within', 'contains', 'touches', 'crosses', 'overlaps', 'dwithin'
]), default='intersects', help='空间关系')
@click.option('--distance-meters', type=float, help='距离阈值（仅用于dwithin关系）')
@click.option('--buffer-meters', type=float, default=0.0, help='缓冲区半径（米）')
@click.option('--output-table', help='输出数据库表名')
@click.option('--output-file', help='输出文件路径（CSV格式）')
@click.option('--fields-to-add', help='要添加的字段列表，逗号分隔（如: field1,field2）')
@click.option('--discard-nonmatching', is_flag=True, help='丢弃未匹配的记录（INNER JOIN）')
@click.option('--summarize', is_flag=True, help='开启统计汇总模式')
@click.option('--summary-fields', help='统计字段，格式: field1:method1,field2:method2')
def spatial_join(left_table: str, right_table: str, num_bbox: int, city_filter: str, chunk_size: int,
                spatial_relation: str, distance_meters: float, buffer_meters: float, output_table: str,
                output_file: str, fields_to_add: str, discard_nonmatching: bool,
                summarize: bool, summary_fields: str):
    """执行空间连接分析 - 类似QGIS的join attributes by location。
    
    Examples:
        # 基础相交分析
        spdatalab spatial-join --right-table intersections
        
        # 距离范围内连接
        spdatalab spatial-join --right-table intersections --spatial-relation dwithin --distance-meters 50
        
        # 选择特定字段
        spdatalab spatial-join --right-table intersections --fields-to-add "intersection_id,intersection_type"
        
        # 统计汇总
        spdatalab spatial-join --right-table intersections --buffer-meters 50 --summarize --summary-fields "count:count,distance:distance"
    """
    setup_logging()
    
    try:
        from .fusion import SpatialJoin
        
        click.echo(f"执行空间连接:")
        click.echo(f"  - 左表: {left_table}")
        click.echo(f"  - 右表: {right_table}")
        click.echo(f"  - 处理数量: {num_bbox} 个bbox")
        if city_filter:
            click.echo(f"  - 城市过滤: {city_filter}")
        if chunk_size:
            click.echo(f"  - 分块大小: {chunk_size}")
        click.echo(f"  - 空间关系: {spatial_relation}")
        if distance_meters:
            click.echo(f"  - 距离阈值: {distance_meters}米")
        if buffer_meters > 0:
            click.echo(f"  - 缓冲区: {buffer_meters}米")
        if discard_nonmatching:
            click.echo(f"  - 连接类型: INNER JOIN (丢弃未匹配)")
        else:
            click.echo(f"  - 连接类型: LEFT JOIN (保留所有左表记录)")
        
        # 解析字段选择
        parsed_fields_to_add = None
        if fields_to_add:
            parsed_fields_to_add = [f.strip() for f in fields_to_add.split(',')]
        
        # 解析统计字段
        parsed_summary_fields = None
        if summary_fields:
            parsed_summary_fields = {}
            for field_spec in summary_fields.split(','):
                if ':' in field_spec:
                    field, method = field_spec.split(':', 1)
                    parsed_summary_fields[field.strip()] = method.strip()
                else:
                    parsed_summary_fields[field_spec.strip()] = "count"
        
        # 注意：当前版本主要支持polygon相交查询
        # 其他空间关系和字段选择功能正在开发中
        
        click.echo("⚠️  当前版本专注于高性能polygon相交查询")
        click.echo("   复杂的空间关系和字段选择功能将在后续版本提供")
        
        # 使用生产级空间连接
        spatial_joiner = SpatialJoin()
        
        try:
            # 当前版本只支持基本的polygon相交
            result, stats = spatial_joiner.polygon_intersect(
                num_bbox=num_bbox,
                city_filter=city_filter,
                chunk_size=chunk_size
            )
            
            # 将结果转换为DataFrame格式（兼容性）
            if len(result) > 0:
                # 为了兼容后续处理，确保有scene_token列
                result = result.rename(columns={'scene_token': 'scene_token'})
            else:
                import pandas as pd
                result = pd.DataFrame()
        except Exception as join_error:
            # 提供更友好的错误信息和建议
            error_msg = str(join_error)
            if "does not exist" in error_msg and "relation" in error_msg:
                click.echo(f"❌ 表 '{right_table}' 不存在")
                click.echo("\n💡 请检查以下事项:")
                click.echo("1. 确保数据库连接正常:")
                click.echo("   make psql")
                click.echo("\n2. 检查clips_bbox表是否存在:")
                click.echo("   SELECT count(*) FROM clips_bbox;")
                click.echo("\n3. 检查远端数据库连接:")
                click.echo("   当前版本使用内置的远端连接配置")
                click.echo("\n4. 如需处理其他表，请使用完整的API接口:")
            raise
        
        click.echo(f"✅ 空间连接完成，共 {len(result)} 条记录")
        click.echo(f"  - 使用策略: {stats['strategy']}")
        click.echo(f"  - 处理速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")
        click.echo(f"  - 总耗时: {stats['total_time']:.2f}秒")
        
        # 显示基础统计
        if len(result) > 0:
            if 'scene_token' in result.columns:
                click.echo(f"  - 唯一场景数: {result['scene_token'].nunique()}")
        
        # 导出文件
        if output_file and len(result) > 0:
            # 移除几何列并保存为CSV
            df = result.drop(columns=['geometry']) if 'geometry' in result.columns else result
            df.to_csv(output_file, index=False, encoding='utf-8')
            click.echo(f"  - 结果已导出到: {output_file}")
        
    except Exception as e:
        logger.error(f"空间连接失败: {str(e)}")
        raise

@cli.command()
def list_layers():
    """查看可用的标准化图层信息。
    
    显示当前FDW配置中可用的标准化图层列表和基本信息。
    """
    setup_logging()
    
    try:
        click.echo("⚠️  当前版本主要专注于polygon相交查询")
        click.echo("图层管理功能将在后续版本提供")
        click.echo()
        
        click.echo("📋 当前支持的功能:")
        click.echo("  - clips_bbox 与 full_intersection 的高性能相交查询")
        click.echo("  - 自动策略选择（批量查询 vs 分块查询）")
        click.echo("  - 详细的性能统计")
        click.echo()
        
        click.echo("💡 使用示例:")
        click.echo("  # Python API")
        click.echo("  from spdatalab.fusion import quick_spatial_join")
        click.echo("  result, stats = quick_spatial_join(num_bbox=100)")
        click.echo()
        click.echo("  # 命令行（基础功能）")
        click.echo("  spdatalab spatial-join --right-table intersections")
        
        return
        
        # 以下代码保留以便将来启用
        # from .fusion import SpatialJoin
        # spatial_joiner = SpatialJoin()
        
        # 查询可用图层信息
        try:
            import pandas as pd
            # layers_df = pd.read_sql(
            #     "SELECT * FROM available_layers ORDER BY layer_name",
            #     spatial_joiner.engine
            # )
            
            if len(layers_df) == 0:
                click.echo("❌ 没有找到可用的图层")
                click.echo("请确保已正确配置FDW：")
                click.echo("  psql -h local_pg -U postgres -f sql/01_fdw_remote.sql")
                return
            
            click.echo("📋 可用的标准化图层:")
            click.echo("=" * 80)
            
            for _, layer in layers_df.iterrows():
                click.echo(f"🗂️  {layer['layer_name']}")
                click.echo(f"   描述: {layer['description']}")
                click.echo(f"   源表: {layer['source_table']}")
                click.echo(f"   几何类型: {layer['geometry_type']}")
                click.echo(f"   记录数: {layer['record_count']:,}")
                click.echo()
            
            click.echo("💡 使用示例:")
            for _, layer in layers_df.iterrows():
                click.echo(f"  spdatalab spatial-join --right-table {layer['layer_name']} --buffer-meters 50")
            
        except Exception as e:
            if "available_layers" in str(e):
                click.echo("❌ available_layers 视图不存在")
                click.echo("请重新配置FDW以创建标准化视图：")
                click.echo("  psql -h local_pg -U postgres -f sql/01_fdw_remote.sql")
            else:
                click.echo(f"❌ 查询图层信息失败: {str(e)}")
            
    except Exception as e:
        logger.error(f"列出图层失败: {str(e)}")
        raise

@cli.command()
@click.option('--view-name', default='clips_bbox_unified_qgis', help='QGIS兼容视图名称')
def create_qgis_view(view_name: str):
    """创建QGIS兼容的统一视图。
    
    创建带有全局唯一ID的统一视图，解决QGIS加载PostgreSQL视图的兼容性问题。
    
    Args:
        view_name: QGIS兼容视图名称
    """
    setup_logging()
    
    try:
        from .dataset.bbox import create_qgis_compatible_unified_view
        from .db import get_psql_engine
        
        click.echo(f"🔧 创建QGIS兼容统一视图: {view_name}")
        
        eng = get_psql_engine()
        success = create_qgis_compatible_unified_view(eng, view_name)
        
        if success:
            click.echo(f"✅ QGIS兼容视图 {view_name} 创建成功")
            click.echo(f"📝 在QGIS中连接PostgreSQL数据库时：")
            click.echo(f"   1. 选择视图: {view_name}")
            click.echo(f"   2. 主键列选择: qgis_id")
            click.echo(f"   3. 几何列选择: geometry")
        else:
            click.echo(f"❌ QGIS兼容视图 {view_name} 创建失败")
            
    except Exception as e:
        logger.error(f"创建QGIS兼容视图失败: {str(e)}")
        raise

@cli.command()
@click.option('--view-name', default='clips_bbox_unified_mat', help='物化视图名称')
def create_materialized_view(view_name: str):
    """创建物化统一视图。
    
    创建物化视图以提供更好的QGIS性能，适合大数据量场景。
    物化视图将查询结果物理存储，查询速度快但需要手动刷新。
    
    Args:
        view_name: 物化视图名称
    """
    setup_logging()
    
    try:
        from .dataset.bbox import create_materialized_unified_view
        from .db import get_psql_engine
        
        click.echo(f"🔧 创建物化统一视图: {view_name}")
        
        eng = get_psql_engine()
        success = create_materialized_unified_view(eng, view_name)
        
        if success:
            click.echo(f"✅ 物化视图 {view_name} 创建成功")
            click.echo(f"📝 在QGIS中连接PostgreSQL数据库时：")
            click.echo(f"   1. 选择物化视图: {view_name}")
            click.echo(f"   2. 主键列选择: qgis_id")
            click.echo(f"   3. 几何列选择: geometry")
            click.echo(f"⚠️  提醒：数据更新后记得刷新物化视图")
        else:
            click.echo(f"❌ 物化视图 {view_name} 创建失败")
            
    except Exception as e:
        logger.error(f"创建物化视图失败: {str(e)}")
        raise

@cli.command()
@click.option('--view-name', default='clips_bbox_unified_mat', help='物化视图名称')
def refresh_materialized_view(view_name: str):
    """刷新物化视图。
    
    更新物化视图的数据，使其包含最新的分表数据。
    在分表数据有更新时需要运行此命令。
    
    Args:
        view_name: 要刷新的物化视图名称
    """
    setup_logging()
    
    try:
        from .dataset.bbox import refresh_materialized_view as refresh_func
        from .db import get_psql_engine
        
        click.echo(f"🔄 刷新物化视图: {view_name}")
        
        eng = get_psql_engine()
        success = refresh_func(eng, view_name)
        
        if success:
            click.echo(f"✅ 物化视图 {view_name} 刷新完成")
            click.echo(f"🎯 新数据已可在QGIS中使用")
        else:
            click.echo(f"❌ 物化视图 {view_name} 刷新失败")
            
    except Exception as e:
        logger.error(f"刷新物化视图失败: {str(e)}")
        raise

@cli.command()
@click.option('--limit', type=int, help='限制分析的收费站数量（可选）')
@click.option('--analysis-id', help='自定义分析ID')
@click.option('--export-qgis', is_flag=True, help='导出QGIS可视化视图')
@click.option('--max-trajectory-records', type=int, default=10000, help='最大轨迹记录数')
def analyze_toll_stations(limit: int, analysis_id: str, export_qgis: bool,
                         max_trajectory_records: int):
    """
    分析收费站（intersectiontype=2）及其范围内的轨迹数据
    
    功能：
    1. 直接查找intersectiontype=2的收费站数据（不依赖bbox）
    2. 使用收费站原始几何与轨迹数据进行空间相交分析
    3. 按dataset_name聚合轨迹统计
    4. 可选导出QGIS可视化视图
    
    示例：
        # 基础分析
        spdatalab analyze-toll-stations
        
        # 限制分析数量
        spdatalab analyze-toll-stations --limit 100
        
        # 导出QGIS视图
        spdatalab analyze-toll-stations --export-qgis
    """
    try:
        from .fusion.toll_station_analysis import (
            TollStationAnalyzer,
            TollStationAnalysisConfig,
            analyze_toll_station_trajectories
        )
    except ImportError as e:
        click.echo(f"❌ 导入模块失败: {e}")
        click.echo("请确保已正确安装所有依赖")
        return
    
    click.echo("🚀 开始收费站轨迹分析...")
    click.echo(f"📋 分析参数:")
    click.echo(f"   - 收费站限制: {limit or '无限制'}")
    click.echo(f"   - 空间关系: 直接几何相交（无缓冲区）")
    click.echo(f"   - 最大轨迹记录: {max_trajectory_records}")
    
    try:
        # 配置分析参数
        config = TollStationAnalysisConfig(
            max_trajectory_records=max_trajectory_records
        )
        
        # 执行分析
        toll_stations, trajectory_results, final_analysis_id = analyze_toll_station_trajectories(
            limit=limit,
            config=config
        )
        
        if analysis_id:
            final_analysis_id = analysis_id
        
        # 显示结果
        if toll_stations.empty:
            click.echo("⚠️ 未找到收费站数据")
            click.echo("\n💡 可能的原因:")
            click.echo("   - intersection表中没有intersectiontype=2的数据")
            click.echo("   - 远程数据库连接问题")
            click.echo("   - full_intersection表不存在或为空")
            return
        
        click.echo(f"\n✅ 分析完成！")
        click.echo(f"📊 分析ID: {final_analysis_id}")
        click.echo(f"📍 找到收费站: {len(toll_stations)} 个")
        
        # 显示收费站统计
        if 'intersectionsubtype' in toll_stations.columns:
            subtype_stats = toll_stations['intersectionsubtype'].value_counts()
            click.echo(f"\n🏛️ 收费站子类型分布:")
            for subtype, count in subtype_stats.head(10).items():
                click.echo(f"   子类型{subtype}: {count} 个收费站")
        
        # 显示轨迹分析结果
        if not trajectory_results.empty:
            total_trajectories = trajectory_results['trajectory_count'].sum()
            total_datasets = trajectory_results['dataset_name'].nunique()
            avg_workstage_2 = trajectory_results['workstage_2_ratio'].mean()
            
            click.echo(f"\n🚗 轨迹数据统计:")
            click.echo(f"   - 总轨迹数: {total_trajectories:,}")
            click.echo(f"   - 数据集数: {total_datasets}")
            click.echo(f"   - 平均工作阶段2比例: {avg_workstage_2:.1f}%")
            
            # 显示Top数据集
            click.echo(f"\n🔝 Top 10 数据集:")
            top_datasets = trajectory_results.groupby('dataset_name')['trajectory_count'].sum().sort_values(ascending=False).head(10)
            for i, (dataset, count) in enumerate(top_datasets.items(), 1):
                click.echo(f"   {i:2d}. {dataset}: {count:,} 条轨迹")
        else:
            click.echo(f"\n⚠️ 未找到轨迹数据")
        
        # 导出QGIS视图
        if export_qgis:
            click.echo(f"\n🗺️ 导出QGIS可视化视图...")
            try:
                from .fusion.toll_station_analysis import export_toll_station_results_for_qgis
                export_info = export_toll_station_results_for_qgis(final_analysis_id, config)
                
                click.echo(f"✅ QGIS视图创建成功:")
                for view_type, view_name in export_info.items():
                    click.echo(f"   - {view_name}")
                
                click.echo(f"\n💡 QGIS使用说明:")
                click.echo(f"   1. 连接到local_pg数据库 (localhost:5432/postgres)")
                click.echo(f"   2. 添加上述视图作为图层")
                click.echo(f"   3. 收费站视图显示位置，轨迹视图显示统计密度")
                
            except Exception as qgis_error:
                click.echo(f"❌ QGIS视图导出失败: {qgis_error}")
        
        # 保存分析信息
        click.echo(f"\n💾 分析数据已保存到本地数据库")
        click.echo(f"   - 收费站表: toll_station_analysis")
        click.echo(f"   - 轨迹结果表: toll_station_trajectories")
        
        # 给出后续操作建议
        click.echo(f"\n🎯 后续操作建议:")
        click.echo(f"   - 查看分析汇总: 使用 --analysis-id {final_analysis_id}")
        click.echo(f"   - QGIS可视化: 连接到local_pg数据库查看视图")
        click.echo(f"   - 数据导出: 可从数据库表中导出CSV或其他格式")
        
    except Exception as e:
        click.echo(f"❌ 分析失败: {e}")
        import traceback
        if click.get_current_context().obj.get('debug', False):
            click.echo(f"\n🐛 详细错误信息:")
            click.echo(traceback.format_exc())
        else:
            click.echo(f"\n💡 使用 --debug 参数查看详细错误信息")

@cli.command()
@click.option('--analysis-id', required=True, help='分析ID')
def toll_stations_summary(analysis_id: str):
    """
    查看收费站分析的汇总信息
    
    示例：
        spdatalab toll-stations-summary --analysis-id toll_station_20231201_143022
    """
    try:
        from .fusion.toll_station_analysis import get_toll_station_analysis_summary
    except ImportError as e:
        click.echo(f"❌ 导入模块失败: {e}")
        return
    
    try:
        summary = get_toll_station_analysis_summary(analysis_id)
        
        if 'error' in summary:
            click.echo(f"❌ 获取汇总失败: {summary['error']}")
            return
        
        click.echo(f"📊 收费站分析汇总 - {analysis_id}")
        click.echo("=" * 60)
        
        # 收费站统计
        click.echo(f"🏛️ 收费站统计:")
        click.echo(f"   - 总收费站数: {summary.get('total_toll_stations', 0)}")
        click.echo(f"   - 涉及城市数: {summary.get('cities_count', 0)}")
        click.echo(f"   - 涉及场景数: {summary.get('scenes_count', 0)}")
        
        # 轨迹统计
        click.echo(f"\n🚗 轨迹统计:")
        click.echo(f"   - 唯一数据集: {summary.get('unique_datasets', 0)}")
        click.echo(f"   - 总轨迹数: {summary.get('total_trajectories', 0):,}")
        click.echo(f"   - 总数据点: {summary.get('total_points', 0):,}")
        click.echo(f"   - 平均工作阶段2比例: {summary.get('avg_workstage_2_ratio', 0)}%")
        
        # 分析信息  
        click.echo(f"\n📋 分析信息:")
        click.echo(f"   - 分析ID: {summary.get('analysis_id', 'N/A')}")
        click.echo(f"   - 分析时间: {summary.get('analysis_time', 'N/A')}")
        
    except Exception as e:
        click.echo(f"❌ 获取汇总失败: {e}")

def setup_logging():
    """设置日志配置。"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
        ]
    )

def main():
    """主函数。"""
    cli()

if __name__ == '__main__':
    cli()