"""命令行接口模块。"""

import click
from spdatalab.common.db import get_conn
import argparse
import json
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
@click.option('--name', required=True)
@click.option('--desc', default='')
@click.option('--jsonl', type=click.Path(exists=True), required=True, help='Path to JSONL containing scene_token')
def create(name, desc, jsonl):
    """Create a new scene set from JSONL file"""
    import json
    conn = get_conn()
    with conn, conn.cursor() as cur:
        cur.execute(
            'INSERT INTO scene_sets(name, description) VALUES(%s,%s) RETURNING set_id',
            (name, desc)
        )
        set_id = cur.fetchone()[0]
        rows = []
        with open(jsonl, 'r', encoding='utf-8') as fh:
            for line in fh:
                try:
                    token = json.loads(line)['scene_token']
                    rows.append((set_id, token))
                except (KeyError, json.JSONDecodeError):
                    continue
        cur.executemany(
            'INSERT INTO scene_set_members(set_id, scene_token) VALUES(%s,%s) ON CONFLICT DO NOTHING',
            rows
        )
    click.echo(f'✅ Created set {set_id} with {len(rows)} scenes')

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
@click.option('--index-file', required=True, help='索引文件路径')
@click.option('--dataset-name', required=True, help='数据集名称')
@click.option('--description', default='', help='数据集描述')
@click.option('--output', required=True, help='输出文件路径')
@click.option('--format', type=click.Choice(['json', 'parquet']), default='json', help='输出格式')
def build_dataset(index_file: str, dataset_name: str, description: str, output: str, format: str):
    """构建数据集结构。
    
    从索引文件读取数据源信息，构建数据集结构并保存。
    
    Args:
        index_file: 索引文件路径，每行格式为 obs_path@duplicateN
        dataset_name: 数据集名称
        description: 数据集描述
        output: 输出文件路径，保存为指定格式
        format: 输出格式，json 或 parquet
    """
    setup_logging()
    
    try:
        manager = DatasetManager()
        dataset = manager.build_dataset_from_index(index_file, dataset_name, description)
        manager.save_dataset(dataset, output, format=format)
        
        click.echo(f"✅ 数据集构建完成: {dataset_name}")
        click.echo(f"   - 子数据集数量: {len(dataset.subdatasets)}")
        click.echo(f"   - 总唯一场景数: {dataset.total_unique_scenes}")
        click.echo(f"   - 总场景数(含倍增): {dataset.total_scenes}")
        click.echo(f"   - 已保存到: {output} ({format}格式)")
        
        if format == 'parquet':
            click.echo(f"   - 推荐安装: pip install pandas pyarrow")
            
    except Exception as e:
        logger.error(f"构建数据集失败: {str(e)}")
        raise

@cli.command()
@click.option('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
@click.option('--batch', type=int, default=1000, help='处理批次大小')
@click.option('--insert-batch', type=int, default=1000, help='插入批次大小')
@click.option('--buffer-meters', type=int, default=50, help='缓冲区大小（米）')
@click.option('--precise-buffer', is_flag=True, help='使用精确的米级缓冲区（需要投影转换）')
def process_bbox(input: str, batch: int, insert_batch: int, buffer_meters: int, precise_buffer: bool):
    """处理边界框数据。
    
    从数据集文件中加载场景ID，获取边界框信息并插入到PostGIS数据库中。
    支持JSON、Parquet和文本格式的数据集文件。
    
    Args:
        input: 输入文件路径，支持JSON/Parquet/文本格式
        batch: 处理批次大小，每批从数据库获取多少个场景的信息
        insert_batch: 插入批次大小，每批向数据库插入多少条记录
        buffer_meters: 缓冲区大小（米），用于点数据的边界框扩展
        precise_buffer: 是否使用精确的米级缓冲区（通过投影转换实现）
    """
    setup_logging()
    
    try:
        from .dataset.bbox import run as bbox_run
        
        click.echo(f"开始处理边界框数据:")
        click.echo(f"  - 输入文件: {input}")
        click.echo(f"  - 处理批次: {batch}")
        click.echo(f"  - 插入批次: {insert_batch}")
        click.echo(f"  - 缓冲区: {buffer_meters}米")
        click.echo(f"  - 精确模式: {'是' if precise_buffer else '否'}")
        
        bbox_run(
            input_path=input,
            batch=batch,
            insert_batch=insert_batch,
            buffer_meters=buffer_meters,
            use_precise_buffer=precise_buffer
        )
        
        click.echo("✅ 边界框处理完成")
        
    except Exception as e:
        logger.error(f"处理边界框失败: {str(e)}")
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
def build_dataset_with_bbox(index_file: str, dataset_name: str, description: str, output: str, 
                           format: str, batch: int, insert_batch: int, buffer_meters: int, 
                           precise_buffer: bool, skip_bbox: bool):
    """构建数据集并处理边界框（完整工作流程）。
    
    从索引文件构建数据集，保存后自动处理边界框数据，提供一键式完整工作流程。
    
    Args:
        index_file: 索引文件路径，每行格式为 obs_path@duplicateN
        dataset_name: 数据集名称
        description: 数据集描述
        output: 输出数据集文件路径
        format: 数据集保存格式，json 或 parquet
        batch: 边界框处理批次大小
        insert_batch: 边界框插入批次大小
        buffer_meters: 缓冲区大小（米）
        precise_buffer: 是否使用精确的米级缓冲区
        skip_bbox: 是否跳过边界框处理
    """
    setup_logging()
    
    try:
        # 步骤1：构建数据集
        click.echo("=== 步骤1: 构建数据集 ===")
        manager = DatasetManager()
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
    parser = argparse.ArgumentParser(description='Spatial-Data-Lab 工具集')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 添加场景数据列表生成命令
    scene_list_parser = subparsers.add_parser('generate-scene-list',
                                            help='生成场景数据列表')
    scene_list_parser.add_argument('--index-file', required=True,
                                 help='索引文件路径')
    scene_list_parser.add_argument('--output', required=True,
                                 help='输出文件路径')
    scene_list_parser.set_defaults(func=generate_scene_list)
    
    # 添加数据集构建命令
    build_dataset_parser = subparsers.add_parser('build-dataset',
                                                help='构建数据集结构')
    build_dataset_parser.add_argument('--index-file', required=True,
                                     help='索引文件路径')
    build_dataset_parser.add_argument('--dataset-name', required=True,
                                     help='数据集名称')
    build_dataset_parser.add_argument('--description', default='',
                                     help='数据集描述')
    build_dataset_parser.add_argument('--output', required=True,
                                     help='输出文件路径')
    build_dataset_parser.add_argument('--format', type=click.Choice(['json', 'parquet']), default='json',
                                     help='输出格式，json 或 parquet')
    build_dataset_parser.set_defaults(func=build_dataset)
    
    args = parser.parse_args()
    if args.command:
        if args.command == 'generate-scene-list':
            args.func(args.index_file, args.output)
        elif args.command == 'build-dataset':
            args.func(args.index_file, args.dataset_name, args.description, args.output, args.format)
    else:
        parser.print_help()

if __name__ == '__main__':
    cli()