"""命令行接口模块。"""

import click
from spdatalab.common.db import get_conn
import argparse
import json
import logging
from pathlib import Path

from .dataset.scene_list_generator import SceneListGenerator

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

def setup_logging():
    """设置日志配置。"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    
    args = parser.parse_args()
    if args.command:
        args.func(args.index_file, args.output)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()