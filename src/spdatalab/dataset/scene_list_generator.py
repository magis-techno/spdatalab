"""场景数据列表生成器模块，用于生成场景数据列表。"""

import logging
from typing import Iterator, Dict, List, Optional, Tuple
from pathlib import Path
import json

from ..common.file_utils import open_file, ensure_dir
from ..common.decoder import decode_shrink_line

logger = logging.getLogger(__name__)

class SceneListGenerator:
    """场景数据列表生成器类。"""
    
    def __init__(self):
        """初始化场景数据列表生成器。"""
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'total_scenes': 0,
            'failed_scenes': 0
        }
        
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
            
    def iter_scenes_from_file(self, file_path: str) -> Iterator[Dict]:
        """从文件中迭代读取场景数据。
        
        Args:
            file_path: 文件路径，可以是本地路径或OBS路径
            
        Yields:
            解码后的场景数据字典
        """
        # ============ 临时调试代码 START ============
        logger.info(f"[调试] SceneListGenerator.iter_scenes_from_file 开始处理: {file_path[:100]}...")
        # ============ 临时调试代码 END ============
        
        try:
            with open_file(file_path, 'r') as f:
                # ============ 临时调试代码 START ============
                logger.info(f"[调试] 文件已打开，开始逐行读取")
                line_count = 0
                # ============ 临时调试代码 END ============
                
                for line_num, line in enumerate(f, 1):
                    scene = decode_shrink_line(line)
                    if scene is not None:
                        # ============ 临时调试代码 START ============
                        line_count += 1
                        if line_count <= 3:  # 只打印前3行
                            logger.info(f"[调试] 成功解码第 {line_num} 行，scene_id: {scene.get('scene_id', 'N/A')[:30]}...")
                        # ============ 临时调试代码 END ============
                        yield scene
                    else:
                        self.stats['failed_scenes'] += 1
                        logger.warning(f"文件 {file_path} 第 {line_num} 行解码失败")
                
                # ============ 临时调试代码 START ============
                logger.info(f"[调试] 文件读取完成，共处理 {line_count} 行")
                # ============ 临时调试代码 END ============
        except Exception as e:
            # ============ 临时调试代码 START ============
            logger.error(f"[调试] 读取文件异常！")
            logger.error(f"[调试] 异常类型: {type(e).__name__}")
            logger.error(f"[调试] 异常详情: {str(e)}")
            import traceback
            logger.error(f"[调试] 堆栈跟踪:\n{traceback.format_exc()}")
            # ============ 临时调试代码 END ============
            logger.error(f"读取文件 {file_path} 失败: {str(e)}")
            self.stats['failed_files'] += 1
            return
            
        self.stats['processed_files'] += 1
        
    def iter_scene_list(self, index_file: str) -> Iterator[Dict]:
        """迭代生成场景数据列表。
        
        Args:
            index_file: 索引文件路径
            
        Yields:
            场景数据字典
        """
        try:
            with open_file(index_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    result = self.parse_index_line(line)
                    if result is None:
                        logger.warning(f"索引文件第 {line_num} 行解析失败")
                        continue
                        
                    obs_path, factor = result
                    self.stats['total_files'] += 1
                    logger.info(f"处理文件 {obs_path}, 复制因子: {factor}")
                    
                    # 读取并解码文件
                    for scene in self.iter_scenes_from_file(obs_path):
                        self.stats['total_scenes'] += 1
                        # 数据倍增
                        for _ in range(factor):
                            yield scene
                            
        except Exception as e:
            logger.error(f"处理索引文件 {index_file} 失败: {str(e)}")
            raise
            
    def generate_scene_list(self, index_file: str, output_file: Optional[str] = None) -> List[Dict]:
        """生成场景数据列表。
        
        Args:
            index_file: 索引文件路径
            output_file: 输出文件路径，如果为None则只返回列表
            
        Returns:
            场景数据列表
        """
        scene_list = []
        
        # 生成场景列表
        for scene in self.iter_scene_list(index_file):
            scene_list.append(scene)
            
        # 保存到文件
        if output_file:
            ensure_dir(Path(output_file).parent)
            with open_file(output_file, 'w') as f:
                json.dump(scene_list, f, ensure_ascii=False, indent=2)
            logger.info(f"场景数据列表已保存到 {output_file}")
            
        # 输出统计信息
        logger.info(f"处理完成:")
        logger.info(f"- 总文件数: {self.stats['total_files']}")
        logger.info(f"- 成功处理文件数: {self.stats['processed_files']}")
        logger.info(f"- 失败文件数: {self.stats['failed_files']}")
        logger.info(f"- 总场景数: {self.stats['total_scenes']}")
        logger.info(f"- 失败场景数: {self.stats['failed_scenes']}")
        
        return scene_list 