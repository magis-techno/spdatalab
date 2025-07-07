"""数据集管理器，提供数据集的组织和管理功能。"""

import logging
from typing import Dict, List, Optional, Iterator, Tuple, Union, TYPE_CHECKING
from pathlib import Path
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from urllib.parse import urlparse, parse_qs

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
from ..common.io_hive import hive_cursor

logger = logging.getLogger(__name__)

@dataclass
class SubDataset:
    """子数据集信息。"""
    name: str  # 子数据集名称，如 lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59
    obs_path: str  # OBS路径或URL
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
        
    def extract_dataname_from_url(self, url: str) -> Optional[str]:
        """从URL中提取dataName部分。
        
        Args:
            url: 问题单URL，例如：
                
        Returns:
            dataName值，如 '10000_ddi-application-667754027299119535'，失败时返回None
        """
        try:
            # 解析URL，处理fragment中的查询参数
            parsed = urlparse(url)
            
            # 检查fragment部分是否包含查询参数
            if parsed.fragment and '?' in parsed.fragment:
                # 提取fragment中的查询字符串部分
                fragment_query = parsed.fragment.split('?', 1)[1]
                query_params = parse_qs(fragment_query)
                
                # 获取dataName参数
                if 'dataName' in query_params:
                    data_name = query_params['dataName'][0]
                    logger.debug(f"从URL提取dataName: {data_name}")
                    return data_name
            
            # 如果fragment中没有找到，检查普通查询参数
            query_params = parse_qs(parsed.query)
            if 'dataName' in query_params:
                data_name = query_params['dataName'][0]
                logger.debug(f"从URL查询参数提取dataName: {data_name}")
                return data_name
                
            logger.warning(f"URL中未找到dataName参数: {url}")
            return None
            
        except Exception as e:
            logger.error(f"解析URL失败: {url}, 错误: {str(e)}")
            return None
            
    def query_defect_id_by_dataname(self, dataname: str, original_url: str = None) -> Optional[str]:
        """通过dataName查询defect_id。
        
        Args:
            dataname: dataName值，如 '10000_ddi-application-667754027299119535'
            original_url: 原始URL（用于错误信息显示）
            
        Returns:
            defect_id值，如 'DI20250116151107D21104779'，失败时返回None
        """
        sql = "SELECT defect_id FROM elasticsearch_ros.ods_ddi_index002_datalake WHERE id = %(dataname)s"
        
        try:
            with hive_cursor() as cur:
                cur.execute(sql, {"dataname": dataname})
                result = cur.fetchone()
                
                if result:
                    defect_id = result[0]
                    logger.debug(f"dataName {dataname} -> defect_id {defect_id}")
                    return defect_id
                else:
                    error_msg = f"未找到dataName对应的defect_id: {dataname}"
                    if original_url:
                        error_msg += f"\n  原始URL: {original_url}"
                    logger.warning(error_msg)
                    return None
                    
        except Exception as e:
            error_msg = f"查询defect_id失败，dataName: {dataname}, 错误: {str(e)}"
            if original_url:
                error_msg += f"\n  原始URL: {original_url}"
            logger.error(error_msg)
            return None
            
    def query_scene_ids_by_defect_id(self, defect_id: str, dataname: str = None, original_url: str = None) -> List[str]:
        """通过defect_id查询scene_id列表。
        
        Args:
            defect_id: defect_id值，如 'DI20250116151107D21104779'
            dataname: dataName值（用于错误信息显示）
            original_url: 原始URL（用于错误信息显示）
            
        Returns:
            scene_id列表
        """
        sql = "SELECT id FROM transform.ods_t_data_fragment_datalake WHERE origin_source_id = %(defect_id)s"
        
        try:
            with hive_cursor() as cur:
                cur.execute(sql, {"defect_id": defect_id})
                results = cur.fetchall()
                
                scene_ids = [result[0] for result in results]
                if scene_ids:
                    logger.info(f"defect_id {defect_id} -> {len(scene_ids)} scene_ids")
                else:
                    error_msg = f"defect_id未找到对应scene_id: {defect_id}"
                    if dataname:
                        error_msg += f"\n  dataName: {dataname}"
                    if original_url:
                        error_msg += f"\n  原始URL: {original_url}"
                    logger.warning(error_msg)
                return scene_ids
                
        except Exception as e:
            error_msg = f"查询scene_id失败，defect_id: {defect_id}, 错误: {str(e)}"
            if dataname:
                error_msg += f"\n  dataName: {dataname}"
            if original_url:
                error_msg += f"\n  原始URL: {original_url}"
            logger.error(error_msg)
            return []
            
    def parse_url_line_with_attributes(self, line: str) -> Optional[Dict[str, str]]:
        """解析包含额外属性的URL行。
        
        支持的格式：
        1. URL\t责任模块\t问题描述
        2. URL (兼容原格式)
        
        Args:
            line: URL行，可能包含tab分隔的额外属性
            
        Returns:
            包含url, module, description等字段的字典，失败时返回None
        """
        line = line.strip()
        if not line:
            return None
            
        try:
            # 按tab分割
            parts = line.split('\t')
            
            url = parts[0].strip()
            if not url.startswith(('http://', 'https://')):
                logger.warning(f"无效的URL格式: {url}")
                return None
            
            # 提取基本信息
            result = {
                'url': url,
                'module': '',
                'description': ''
            }
            
            # 解析额外属性
            if len(parts) >= 2:
                result['module'] = parts[1].strip()
                
            if len(parts) >= 3:
                description = parts[2].strip()
                # 去除描述中的引号（如果有）
                if description.startswith('"') and description.endswith('"'):
                    description = description[1:-1]
                result['description'] = description
            
            logger.debug(f"解析URL行: {result}")
            return result
            
        except Exception as e:
            logger.error(f"解析URL行失败: {line}, 错误: {str(e)}")
            return None

    def extract_scene_ids_from_urls_with_attributes(self, file_path: str) -> List[Dict[str, str]]:
        """从URL文件中提取scene_id列表以及相关属性。
        
        Args:
            file_path: URL文件路径，每行可能包含URL和额外属性
            
        Returns:
            包含scene_id和属性信息的字典列表
        """
        result_data = []
        
        try:
            with open_file(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    self.stats['total_files'] += 1
                    
                    # 解析URL行和属性
                    url_data = self.parse_url_line_with_attributes(line)
                    if not url_data:
                        logger.warning(f"第 {line_num} 行解析失败: {line}")
                        self.stats['failed_files'] += 1
                        continue
                    
                    url = url_data['url']
                    
                    # 提取dataName
                    dataname = self.extract_dataname_from_url(url)
                    if not dataname:
                        logger.warning(f"第 {line_num} 行URL解析失败: {url}")
                        self.stats['failed_files'] += 1
                        continue
                    
                    # 查询defect_id
                    defect_id = self.query_defect_id_by_dataname(dataname, original_url=url)
                    if not defect_id:
                        logger.warning(f"第 {line_num} 行dataName未找到对应defect_id: {dataname}")
                        self.stats['failed_files'] += 1
                        continue
                    
                    # 查询scene_ids
                    scene_ids = self.query_scene_ids_by_defect_id(defect_id, dataname=dataname, original_url=url)
                    if scene_ids:
                        # 为每个scene_id创建记录，包含属性信息
                        for scene_id in scene_ids:
                            result_data.append({
                                'scene_id': scene_id,
                                'url': url,
                                'dataname': dataname,
                                'defect_id': defect_id,
                                'module': url_data['module'],
                                'description': url_data['description']
                            })
                        
                        self.stats['processed_files'] += 1
                        self.stats['total_scenes'] += len(scene_ids)
                        logger.info(f"第 {line_num} 行: URL -> {len(scene_ids)} scene_ids, 模块: {url_data['module']}")
                    else:
                        logger.warning(f"第 {line_num} 行defect_id未找到对应scene_id: {defect_id}")
                        self.stats['failed_files'] += 1
                        
            logger.info(f"URL文件处理完成: 总计提取 {len(result_data)} 条记录")
            
        except Exception as e:
            logger.error(f"处理URL文件失败: {file_path}, 错误: {str(e)}")
            raise
            
        return result_data

    def extract_scene_ids_from_urls(self, file_path: str) -> List[str]:
        """从URL文件中提取scene_id列表（原有方法，保持兼容性）。
        
        Args:
            file_path: URL文件路径，每行一个URL
            
        Returns:
            scene_id列表
        """
        scene_ids = []
        
        try:
            with open_file(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    self.stats['total_files'] += 1
                    
                    # 提取dataName
                    dataname = self.extract_dataname_from_url(line)
                    if not dataname:
                        logger.warning(f"第 {line_num} 行URL解析失败: {line}")
                        self.stats['failed_files'] += 1
                        continue
                    
                    # 查询defect_id
                    defect_id = self.query_defect_id_by_dataname(dataname, original_url=line)
                    if not defect_id:
                        logger.warning(f"第 {line_num} 行dataName未找到对应defect_id: {dataname}")
                        self.stats['failed_files'] += 1
                        continue
                    
                    # 查询scene_ids
                    line_scene_ids = self.query_scene_ids_by_defect_id(defect_id, dataname=dataname, original_url=line)
                    if line_scene_ids:
                        scene_ids.extend(line_scene_ids)
                        self.stats['processed_files'] += 1
                        self.stats['total_scenes'] += len(line_scene_ids)
                        logger.info(f"第 {line_num} 行: URL -> {len(line_scene_ids)} scene_ids")
                    else:
                        logger.warning(f"第 {line_num} 行defect_id未找到对应scene_id: {defect_id}")
                        self.stats['failed_files'] += 1
                        
            logger.info(f"URL文件处理完成: 总计提取 {len(scene_ids)} 个scene_id")
            
        except Exception as e:
            logger.error(f"处理URL文件失败: {file_path}, 错误: {str(e)}")
            raise
            
        return scene_ids
        
    def detect_file_format(self, file_path: str) -> str:
        """检测文件格式类型。
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件格式：'url', 'url_with_attributes', 'jsonl_path', 'unknown'
        """
        try:
            with open_file(file_path, 'r') as f:
                # 读取前几行进行格式检测
                sample_lines = []
                for i, line in enumerate(f):
                    line = line.strip()
                    if line:
                        sample_lines.append(line)
                    if i >= 10:  # 只检查前10行
                        break
                        
            if not sample_lines:
                return 'unknown'
                
            # 检测是否为URL格式
            url_count = 0
            url_with_attributes_count = 0
            jsonl_path_count = 0
            
            for line in sample_lines:
                # URL格式检测
                if line.startswith(('http://', 'https://')) and 'dataName=' in line:
                    # 检查是否包含tab分隔的额外属性
                    if '\t' in line:
                        url_with_attributes_count += 1
                    else:
                        url_count += 1
                # JSONL路径格式检测（现有格式）
                elif ('obs://' in line or '/god/' in line) and '@duplicate' in line:
                    jsonl_path_count += 1
                    
            # 判断主要格式
            if url_with_attributes_count > len(sample_lines) * 0.7:  # 70%以上是带属性的URL
                return 'url_with_attributes'
            elif url_count > len(sample_lines) * 0.7:  # 70%以上是URL
                return 'url'
            elif jsonl_path_count > len(sample_lines) * 0.7:  # 70%以上是jsonl路径
                return 'jsonl_path'
            else:
                return 'unknown'
                
        except Exception as e:
            logger.error(f"检测文件格式失败: {file_path}, 错误: {str(e)}")
            return 'unknown'
        
    def extract_subdataset_name(self, obs_path: str) -> str:
        """从OBS路径或URL中提取子数据集名称。
        
        Args:
            obs_path: OBS路径或URL
            
        Returns:
            子数据集名称
        """
        try:
            # 如果是URL，提取dataName作为子数据集名称
            if obs_path.startswith(('http://', 'https://')):
                dataname = self.extract_dataname_from_url(obs_path)
                if dataname:
                    return f"url_{dataname}"
                else:
                    return "url_unknown"
            
            # 原有的OBS路径处理逻辑
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
        """解析索引文件中的一行，支持多种格式。
        
        Args:
            line: 索引文件中的一行，支持以下格式：
                - obs_path@duplicateN (现有格式)
                - https://example.com/... (URL格式，自动识别)
            
        Returns:
            包含 obs_path/url 和 duplication_factor 的元组，如果解析失败则返回 None
        """
        line = line.strip()
        if not line:
            return None
            
        try:
            # 检查是否为URL格式（直接以http/https开头）
            if line.startswith(('http://', 'https://')):
                # URL格式，duplication_factor默认为1
                return line, 1
            
            # 传统格式：obs_path@duplicateN
            if '@' in line:
                obs_path, factor_str = line.split('@', 1)
                # 处理 duplicateN 格式
                factor = int(factor_str.replace('duplicate', ''))
                return obs_path, factor
            else:
                # 如果没有@符号，可能是纯路径，默认duplication_factor为1
                return line, 1
                
        except Exception as e:
            logger.error(f"解析索引行失败: {line}, 错误: {str(e)}")
            return None
            
    def extract_scene_ids_from_file(self, file_path: str) -> List[str]:
        """从文件中提取scene_id列表，自动检测文件格式。
        
        Args:
            file_path: 文件路径
            
        Returns:
            scene_id列表
        """
        # 检测文件格式
        file_format = self.detect_file_format(file_path)
        logger.info(f"检测到文件格式: {file_format} ({file_path})")
        
        if file_format == 'url':
            # URL格式处理
            return self.extract_scene_ids_from_urls(file_path)
        elif file_format == 'url_with_attributes':
            # 带属性的URL格式，只返回scene_id列表以保持兼容性
            result_data = self.extract_scene_ids_from_urls_with_attributes(file_path)
            return [item['scene_id'] for item in result_data]
        elif file_format == 'jsonl_path':
            # 现有JSONL路径格式处理
            return self._extract_scene_ids_from_jsonl_path(file_path)
        else:
            # 尝试两种格式，优先尝试现有格式
            logger.warning(f"未能识别文件格式，尝试现有JSONL路径格式: {file_path}")
            try:
                return self._extract_scene_ids_from_jsonl_path(file_path)
            except Exception as e1:
                logger.warning(f"JSONL路径格式失败: {str(e1)}，尝试URL格式")
                try:
                    return self.extract_scene_ids_from_urls(file_path)
                except Exception as e2:
                    logger.error(f"所有格式尝试失败: JSONL={str(e1)}, URL={str(e2)}")
                    raise Exception(f"无法处理文件格式: {file_path}")
                    
    def _extract_scene_ids_from_jsonl_path(self, file_path: str) -> List[str]:
        """从shrink文件路径中提取scene_id列表（原有逻辑）。
        
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
        """从索引文件构建数据集，支持多种格式。
        
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
                        
                    obs_path_or_url, factor = result
                    self.stats['total_files'] += 1
                    
                    # 提取子数据集名称
                    subdataset_name = self.extract_subdataset_name(obs_path_or_url)
                    
                    # 提取场景ID（支持自动格式检测）
                    scene_ids = self.extract_scene_ids_from_file(obs_path_or_url)
                    scene_count = len(scene_ids)
                    
                    if scene_count > 0:
                        self.stats['processed_files'] += 1
                        self.stats['total_scenes'] += scene_count
                        self.stats['total_subdatasets'] += 1
                        
                        # 创建子数据集
                        subdataset = SubDataset(
                            name=subdataset_name,
                            obs_path=obs_path_or_url,  # 可能是URL
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