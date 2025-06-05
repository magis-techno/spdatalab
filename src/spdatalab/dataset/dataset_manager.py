"""数据集管理器，提供数据集的组织和管理功能。"""

import logging
from typing import Dict, List, Optional, Iterator, Tuple, Union, TYPE_CHECKING
from pathlib import Path
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    pd = None
    pa = None
    pq = None

# 只在类型检查时导入pandas类型
if TYPE_CHECKING and PARQUET_AVAILABLE:
    import pandas as pd

from ..common.file_utils import open_file, ensure_dir

logger = logging.getLogger(__name__)

@dataclass
class SubDataset:
    """子数据集信息。"""
    name: str  # 子数据集名称，如 lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59
    obs_path: str  # OBS路径
    duplication_factor: int  # 倍增因子
    scene_count: int = 0  # 场景数量
    scene_ids: List[str] = None  # 场景ID列表
    metadata: Dict = None  # 额外元数据
    
    def __post_init__(self):
        if self.scene_ids is None:
            self.scene_ids = []
        if self.metadata is None:
            self.metadata = {}

@dataclass
class Dataset:
    """数据集信息。"""
    name: str  # 数据集名称
    description: str = ""  # 数据集描述
    subdatasets: List[SubDataset] = None  # 子数据集列表
    created_at: str = ""  # 创建时间
    total_scenes: int = 0  # 总场景数（包含倍增后的）
    total_unique_scenes: int = 0  # 唯一场景数（不含倍增）
    metadata: Dict = None  # 额外元数据
    
    def __post_init__(self):
        if self.subdatasets is None:
            self.subdatasets = []
        if self.metadata is None:
            self.metadata = {}
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

class DatasetManager:
    """数据集管理器。"""
    
    def __init__(self):
        """初始化数据集管理器。"""
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'total_scenes': 0,
            'failed_scenes': 0,
            'total_subdatasets': 0
        }
        
    def extract_subdataset_name(self, obs_path: str) -> str:
        """从OBS路径中提取子数据集名称。
        
        Args:
            obs_path: OBS路径
            
        Returns:
            子数据集名称
        """
        try:
            # 使用正则表达式提取子数据集名称
            # 匹配模式：/god/(子数据集名称)/train_god_...
            pattern = r'/god/([^/]+)/train_god_'
            match = re.search(pattern, obs_path)
            if match:
                return match.group(1)
            else:
                # 如果正则匹配失败，尝试从路径结构中提取
                parts = obs_path.split('/')
                for i, part in enumerate(parts):
                    if part == 'god' and i + 1 < len(parts):
                        candidate = parts[i + 1]
                        if 'GOD_E2E' in candidate:
                            return candidate
                # 最后的备选方案
                return Path(obs_path).parent.name
        except Exception as e:
            logger.warning(f"提取子数据集名称失败: {obs_path}, 错误: {str(e)}")
            return Path(obs_path).parent.name
            
    def parse_index_line(self, line: str) -> Optional[Tuple[str, int]]:
        """解析索引文件中的一行。
        
        Args:
            line: 索引文件中的一行，格式为 obs_path@duplicateN
            
        Returns:
            包含 obs_path 和 duplication_factor 的元组，如果解析失败则返回 None
        """
        line = line.strip()
        if not line:
            return None
            
        try:
            obs_path, factor_str = line.split('@')
            # 处理 duplicateN 格式
            factor = int(factor_str.replace('duplicate', ''))
            return obs_path, factor
        except Exception as e:
            logger.error(f"解析索引行失败: {line}, 错误: {str(e)}")
            return None
            
    def extract_scene_ids_from_file(self, file_path: str) -> List[str]:
        """从shrink文件中提取scene_id列表。
        
        Args:
            file_path: shrink文件路径
            
        Returns:
            scene_id列表
        """
        from .scene_list_generator import SceneListGenerator
        
        scene_ids = []
        generator = SceneListGenerator()
        
        try:
            for scene in generator.iter_scenes_from_file(file_path):
                scene_id = scene.get('scene_id')
                if scene_id:
                    scene_ids.append(scene_id)
                    
            logger.info(f"从 {file_path} 提取到 {len(scene_ids)} 个scene_id")
            
        except Exception as e:
            logger.error(f"提取scene_id失败: {file_path}, 错误: {str(e)}")
            self.stats['failed_files'] += 1
            
        return scene_ids
        
    def build_dataset_from_index(self, index_file: str, dataset_name: str, 
                                description: str = "") -> Dataset:
        """从索引文件构建数据集。
        
        Args:
            index_file: 索引文件路径
            dataset_name: 数据集名称
            description: 数据集描述
            
        Returns:
            Dataset对象
        """
        dataset = Dataset(name=dataset_name, description=description)
        
        try:
            with open_file(index_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    result = self.parse_index_line(line)
                    if result is None:
                        logger.warning(f"索引文件第 {line_num} 行解析失败")
                        continue
                        
                    obs_path, factor = result
                    self.stats['total_files'] += 1
                    
                    # 提取子数据集名称
                    subdataset_name = self.extract_subdataset_name(obs_path)
                    
                    # 提取场景ID
                    scene_ids = self.extract_scene_ids_from_file(obs_path)
                    scene_count = len(scene_ids)
                    
                    if scene_count > 0:
                        self.stats['processed_files'] += 1
                        self.stats['total_scenes'] += scene_count
                        self.stats['total_subdatasets'] += 1
                        
                        # 创建子数据集
                        subdataset = SubDataset(
                            name=subdataset_name,
                            obs_path=obs_path,
                            duplication_factor=factor,
                            scene_count=scene_count,
                            scene_ids=scene_ids
                        )
                        
                        dataset.subdatasets.append(subdataset)
                        dataset.total_unique_scenes += scene_count
                        dataset.total_scenes += scene_count * factor
                        
                        logger.info(f"添加子数据集: {subdataset_name}, 场景数: {scene_count}, 倍增因子: {factor}")
                    else:
                        self.stats['failed_files'] += 1
                        logger.warning(f"子数据集 {subdataset_name} 没有有效场景")
                        
        except Exception as e:
            logger.error(f"处理索引文件 {index_file} 失败: {str(e)}")
            raise
            
        # 输出统计信息
        logger.info(f"数据集构建完成:")
        logger.info(f"- 数据集名称: {dataset.name}")
        logger.info(f"- 子数据集数量: {len(dataset.subdatasets)}")
        logger.info(f"- 总唯一场景数: {dataset.total_unique_scenes}")
        logger.info(f"- 总场景数(含倍增): {dataset.total_scenes}")
        logger.info(f"- 成功处理文件数: {self.stats['processed_files']}")
        logger.info(f"- 失败文件数: {self.stats['failed_files']}")
        
        return dataset
        
    def save_dataset(self, dataset: Dataset, output_file: str, format: str = 'json'):
        """保存数据集到文件。
        
        Args:
            dataset: Dataset对象
            output_file: 输出文件路径
            format: 保存格式，'json' 或 'parquet'
        """
        ensure_dir(Path(output_file).parent)
        
        if format.lower() == 'json':
            self._save_dataset_json(dataset, output_file)
        elif format.lower() == 'parquet':
            self._save_dataset_parquet(dataset, output_file)
        else:
            raise ValueError(f"不支持的格式: {format}，支持的格式: 'json', 'parquet'")
            
    def _save_dataset_json(self, dataset: Dataset, output_file: str):
        """保存数据集为JSON格式。"""
        # 转换为可序列化的字典
        dataset_dict = asdict(dataset)
        
        with open_file(output_file, 'w') as f:
            json.dump(dataset_dict, f, ensure_ascii=False, indent=2)
            
        logger.info(f"数据集已保存到 {output_file} (JSON格式)")
        
    def _save_dataset_parquet(self, dataset: Dataset, output_file: str):
        """保存数据集为Parquet格式。"""
        if not PARQUET_AVAILABLE:
            raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式: pip install pandas pyarrow")
            
        # 准备数据
        data_rows = []
        
        for subdataset in dataset.subdatasets:
            for scene_id in subdataset.scene_ids:
                data_rows.append({
                    'dataset_name': dataset.name,
                    'dataset_description': dataset.description,
                    'dataset_created_at': dataset.created_at,
                    'subdataset_name': subdataset.name,
                    'obs_path': subdataset.obs_path,
                    'duplication_factor': subdataset.duplication_factor,
                    'scene_id': scene_id,
                    'metadata': json.dumps(subdataset.metadata)
                })
        
        # 创建DataFrame
        df = pd.DataFrame(data_rows)
        
        # 保存为parquet
        df.to_parquet(output_file, index=False, compression='snappy')
        
        # 同时保存数据集元信息为JSON
        meta_file = Path(output_file).with_suffix('.meta.json')
        meta_info = {
            'name': dataset.name,
            'description': dataset.description,
            'created_at': dataset.created_at,
            'total_scenes': dataset.total_scenes,
            'total_unique_scenes': dataset.total_unique_scenes,
            'subdataset_count': len(dataset.subdatasets),
            'metadata': dataset.metadata,
            'format_version': '1.0'
        }
        
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(meta_info, f, ensure_ascii=False, indent=2)
            
        logger.info(f"数据集已保存到 {output_file} (Parquet格式)")
        logger.info(f"元信息已保存到 {meta_file}")
        
    def load_dataset(self, dataset_file: str) -> Dataset:
        """从文件加载数据集。
        
        Args:
            dataset_file: 数据集文件路径
            
        Returns:
            Dataset对象
        """
        dataset_path = Path(dataset_file)
        
        if dataset_path.suffix.lower() == '.json':
            return self._load_dataset_json(dataset_file)
        elif dataset_path.suffix.lower() == '.parquet':
            return self._load_dataset_parquet(dataset_file)
        else:
            # 尝试自动检测格式
            if dataset_path.exists():
                # 如果是JSON文件
                try:
                    return self._load_dataset_json(dataset_file)
                except:
                    pass
            
            # 尝试parquet格式
            try:
                return self._load_dataset_parquet(dataset_file)
            except:
                pass
                
            raise ValueError(f"无法识别数据集文件格式: {dataset_file}")
            
    def _load_dataset_json(self, dataset_file: str) -> Dataset:
        """从JSON文件加载数据集。"""
        with open_file(dataset_file, 'r') as f:
            dataset_dict = json.load(f)
            
        # 重构子数据集对象
        subdatasets = []
        for sub_dict in dataset_dict.get('subdatasets', []):
            subdataset = SubDataset(**sub_dict)
            subdatasets.append(subdataset)
            
        dataset_dict['subdatasets'] = subdatasets
        dataset = Dataset(**dataset_dict)
        
        logger.info(f"数据集已从 {dataset_file} 加载 (JSON格式)")
        return dataset
        
    def _load_dataset_parquet(self, dataset_file: str) -> Dataset:
        """从Parquet文件加载数据集。"""
        if not PARQUET_AVAILABLE:
            raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式: pip install pandas pyarrow")
            
        # 加载parquet数据
        df = pd.read_parquet(dataset_file)
        
        # 检查是否有对应的元信息文件
        meta_file = Path(dataset_file).with_suffix('.meta.json')
        meta_info = {}
        
        if meta_file.exists():
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_info = json.load(f)
        
        # 从数据中提取信息
        first_row = df.iloc[0] if len(df) > 0 else {}
        
        dataset_name = meta_info.get('name', first_row.get('dataset_name', 'Unknown'))
        dataset_description = meta_info.get('description', first_row.get('dataset_description', ''))
        dataset_created_at = meta_info.get('created_at', first_row.get('dataset_created_at', ''))
        dataset_metadata = meta_info.get('metadata', {})
        
        # 按子数据集分组
        subdatasets = []
        grouped = df.groupby(['subdataset_name', 'obs_path', 'duplication_factor'])
        
        for (subdataset_name, obs_path, duplication_factor), group in grouped:
            scene_ids = group['scene_id'].tolist()
            
            # 解析metadata
            metadata = {}
            if 'metadata' in group.columns and len(group) > 0:
                metadata_str = group['metadata'].iloc[0]
                if metadata_str:
                    try:
                        metadata = json.loads(metadata_str)
                    except:
                        pass
            
            subdataset = SubDataset(
                name=subdataset_name,
                obs_path=obs_path,
                duplication_factor=int(duplication_factor),
                scene_count=len(scene_ids),
                scene_ids=scene_ids,
                metadata=metadata
            )
            subdatasets.append(subdataset)
        
        # 计算统计信息
        total_unique_scenes = len(df)
        total_scenes = sum(sub.scene_count * sub.duplication_factor for sub in subdatasets)
        
        dataset = Dataset(
            name=dataset_name,
            description=dataset_description,
            subdatasets=subdatasets,
            created_at=dataset_created_at,
            total_scenes=total_scenes,
            total_unique_scenes=total_unique_scenes,
            metadata=dataset_metadata
        )
        
        logger.info(f"数据集已从 {dataset_file} 加载 (Parquet格式)")
        return dataset
        
    def get_subdataset_info(self, dataset: Dataset, subdataset_name: str) -> Optional[SubDataset]:
        """获取指定子数据集信息。
        
        Args:
            dataset: Dataset对象
            subdataset_name: 子数据集名称
            
        Returns:
            SubDataset对象，如果不存在则返回None
        """
        for subdataset in dataset.subdatasets:
            if subdataset.name == subdataset_name:
                return subdataset
        return None
        
    def list_scene_ids(self, dataset: Dataset, subdataset_name: Optional[str] = None) -> List[str]:
        """列出场景ID。
        
        Args:
            dataset: Dataset对象
            subdataset_name: 子数据集名称，如果为None则返回所有场景ID
            
        Returns:
            场景ID列表
        """
        if subdataset_name:
            subdataset = self.get_subdataset_info(dataset, subdataset_name)
            if subdataset:
                return subdataset.scene_ids
            else:
                logger.warning(f"子数据集 {subdataset_name} 不存在")
                return []
        else:
            # 返回所有场景ID
            all_scene_ids = []
            for subdataset in dataset.subdatasets:
                all_scene_ids.extend(subdataset.scene_ids)
            return all_scene_ids
            
    def generate_scene_list_with_duplication(self, dataset: Dataset, 
                                           subdataset_name: Optional[str] = None) -> Iterator[str]:
        """生成包含倍增的场景ID列表。
        
        Args:
            dataset: Dataset对象
            subdataset_name: 子数据集名称，如果为None则处理所有子数据集
            
        Yields:
            场景ID（包含倍增）
        """
        subdatasets_to_process = []
        
        if subdataset_name:
            subdataset = self.get_subdataset_info(dataset, subdataset_name)
            if subdataset:
                subdatasets_to_process.append(subdataset)
            else:
                logger.warning(f"子数据集 {subdataset_name} 不存在")
                return
        else:
            subdatasets_to_process = dataset.subdatasets
            
        for subdataset in subdatasets_to_process:
            for scene_id in subdataset.scene_ids:
                for _ in range(subdataset.duplication_factor):
                    yield scene_id
                    
    def query_scenes_parquet(self, parquet_file: str, **filters):
        """查询Parquet格式数据集。
        
        Args:
            parquet_file: Parquet文件路径
            **filters: 过滤条件，例如 subdataset_name='xxx', duplication_factor=20
            
        Returns:
            过滤后的DataFrame
        """
        if not PARQUET_AVAILABLE:
            raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式")
            
        df = pd.read_parquet(parquet_file)
        
        # 应用过滤条件
        for key, value in filters.items():
            if key in df.columns:
                df = df[df[key] == value]
        
        return df
        
    def export_scene_ids_parquet(self, dataset: Dataset, output_file: str, 
                                include_duplicates: bool = False):
        """导出场景ID为Parquet格式。
        
        Args:
            dataset: Dataset对象
            output_file: 输出文件路径
            include_duplicates: 是否包含倍增的场景ID
        """
        if not PARQUET_AVAILABLE:
            raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式")
            
        data_rows = []
        
        for subdataset in dataset.subdatasets:
            if include_duplicates:
                # 包含倍增
                for scene_id in subdataset.scene_ids:
                    for i in range(subdataset.duplication_factor):
                        data_rows.append({
                            'subdataset_name': subdataset.name,
                            'scene_id': scene_id,
                            'duplicate_index': i
                        })
            else:
                # 不包含倍增
                for scene_id in subdataset.scene_ids:
                    data_rows.append({
                        'subdataset_name': subdataset.name,
                        'scene_id': scene_id,
                        'duplication_factor': subdataset.duplication_factor
                    })
        
        df = pd.DataFrame(data_rows)
        df.to_parquet(output_file, index=False, compression='snappy')
        
        logger.info(f"场景ID已导出到 {output_file} (Parquet格式)")
        logger.info(f"总记录数: {len(df)}")
        
    def get_dataset_stats(self, dataset: Dataset) -> Dict:
        """获取数据集统计信息。
        
        Args:
            dataset: Dataset对象
            
        Returns:
            统计信息字典
        """
        stats = {
            'dataset_name': dataset.name,
            'subdataset_count': len(dataset.subdatasets),
            'total_unique_scenes': dataset.total_unique_scenes,
            'total_scenes_with_duplicates': dataset.total_scenes,
            'subdatasets': []
        }
        
        for subdataset in dataset.subdatasets:
            sub_stats = {
                'name': subdataset.name,
                'scene_count': subdataset.scene_count,
                'duplication_factor': subdataset.duplication_factor,
                'total_scenes_with_duplicates': subdataset.scene_count * subdataset.duplication_factor
            }
            stats['subdatasets'].append(sub_stats)
        
        return stats 