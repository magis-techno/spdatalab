"""
场景图片检索模块

从OBS存储的parquet文件中获取和解码autoscenes相机图片。

主要功能：
- 从数据库查询场景OBS路径
- 读取parquet格式的图片数据
- 支持帧过滤和批量加载
- 支持多相机类型（架构预留）

作者：spdatalab
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from PIL import Image
import io

from spdatalab.common.io_hive import hive_cursor
from spdatalab.common.file_utils import open_file

logger = logging.getLogger(__name__)


@dataclass
class ImageFrame:
    """图片帧数据结构"""
    scene_id: str
    frame_index: int
    timestamp: int
    image_data: bytes  # PNG/JPEG二进制数据
    image_format: str  # 'png' 或 'jpeg'
    filename: Optional[str] = None  # 原始文件名（可选）
    
    def to_pil_image(self) -> Image.Image:
        """转换为PIL Image对象"""
        return Image.open(io.BytesIO(self.image_data))
    
    def __repr__(self) -> str:
        size_kb = len(self.image_data) / 1024
        return f"ImageFrame(scene={self.scene_id}, frame={self.frame_index}, size={size_kb:.1f}KB)"


class SceneImageRetriever:
    """场景图片检索器
    
    从OBS存储的parquet文件中检索autoscenes相机图片。
    
    Args:
        camera_type: 相机类型，默认 "CAM_FRONT_WIDE_ANGLE"
        
    Example:
        >>> retriever = SceneImageRetriever()
        >>> images = retriever.load_images_from_scene("scene_abc123", max_frames=5)
        >>> print(f"加载了 {len(images)} 帧图片")
    """
    
    def __init__(self, camera_type: str = "CAM_FRONT_WIDE_ANGLE"):
        self.camera_type = camera_type
        logger.info(f"初始化SceneImageRetriever，相机类型: {camera_type}")
    
    def get_scene_obs_paths(self, scene_ids: List[str]) -> pd.DataFrame:
        """批量查询场景的OBS路径
        
        Args:
            scene_ids: 场景ID列表
            
        Returns:
            包含scene_id, data_name, scene_obs_path, timestamp的DataFrame
            
        Raises:
            ValueError: 如果scene_ids为空
            Exception: 数据库查询失败
        """
        if not scene_ids:
            raise ValueError("scene_ids不能为空")
        
        logger.info(f"查询 {len(scene_ids)} 个场景的OBS路径...")
        
        sql = (
            "SELECT id AS scene_id, origin_name AS data_name, "
            "scene_obs_path, timestamp "
            "FROM transform.ods_t_data_fragment_datalake "
            "WHERE id IN %(scene_ids)s"
        )
        
        try:
            with hive_cursor() as cur:
                cur.execute(sql, {"scene_ids": tuple(scene_ids)})
                columns = [d[0] for d in cur.description]
                results = cur.fetchall()
                
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ 成功查询到 {len(df)} 个场景的OBS路径")
            
            # 检查缺失的场景
            missing = set(scene_ids) - set(df['scene_id'].tolist())
            if missing:
                logger.warning(f"⚠️ 有 {len(missing)} 个场景未找到OBS路径: {list(missing)[:5]}...")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 查询场景OBS路径失败: {e}")
            raise
    
    def _parse_camera_parquet_path(self, scene_obs_path: str) -> str:
        """构建相机parquet文件路径
        
        Args:
            scene_obs_path: 场景OBS根路径
            
        Returns:
            相机parquet文件的完整OBS路径
        """
        # 确保路径格式正确
        if not scene_obs_path.startswith('obs://'):
            scene_obs_path = f'obs://{scene_obs_path}'
        
        # 去除尾部斜杠
        scene_obs_path = scene_obs_path.rstrip('/')
        
        # 构建路径：{scene_obs_path}/samples/{camera_type}/*.parquet
        # 注意：这里假设只有一个parquet文件，实际可能需要列出目录
        camera_dir = f"{scene_obs_path}/samples/{self.camera_type}"
        
        return camera_dir
    
    def _list_obs_directory(self, obs_dir: str) -> List[str]:
        """列出OBS目录中的文件
        
        Args:
            obs_dir: OBS目录路径
            
        Returns:
            文件路径列表
        """
        try:
            import moxing as mox
            from spdatalab.common.io_obs import init_moxing
            
            init_moxing()
            
            # 列出目录
            if not obs_dir.endswith('/'):
                obs_dir += '/'
            
            files = mox.file.list_directory(obs_dir, recursive=False)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            
            return [obs_dir + f for f in parquet_files]
            
        except Exception as e:
            logger.error(f"列出OBS目录失败 {obs_dir}: {e}")
            return []
    
    def _load_parquet_from_obs(self, obs_path: str) -> Optional[pd.DataFrame]:
        """从OBS加载parquet文件
        
        Args:
            obs_path: parquet文件的OBS路径
            
        Returns:
            DataFrame或None（如果加载失败）
        """
        try:
            logger.debug(f"读取parquet文件: {obs_path}")
            
            # 使用open_file读取OBS文件
            with open_file(obs_path, 'rb') as f:
                # 读取parquet
                table = pq.read_table(f)
                df = table.to_pandas()
                
            logger.debug(f"成功读取parquet，形状: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取parquet文件失败 {obs_path}: {e}")
            return None
    
    def _detect_image_format(self, image_data: bytes) -> str:
        """检测图片格式
        
        Args:
            image_data: 图片二进制数据
            
        Returns:
            'png', 'jpeg' 或 'unknown'
        """
        if image_data[:8] == b'\x89PNG\r\n\x1a\n':
            return 'png'
        elif image_data[:2] == b'\xff\xd8':
            return 'jpeg'
        else:
            # 尝试用PIL检测
            try:
                img = Image.open(io.BytesIO(image_data))
                return img.format.lower() if img.format else 'unknown'
            except:
                return 'unknown'
    
    def _parse_parquet_to_frames(
        self, 
        df: pd.DataFrame, 
        scene_id: str,
        frame_indices: Optional[List[int]] = None,
        max_frames: Optional[int] = None
    ) -> List[ImageFrame]:
        """解析parquet DataFrame为ImageFrame列表
        
        Args:
            df: parquet数据的DataFrame
            scene_id: 场景ID
            frame_indices: 指定要提取的帧索引列表
            max_frames: 最大帧数限制
            
        Returns:
            ImageFrame对象列表
        """
        frames = []
        
        # 检测列名（适配不同命名方式）
        image_col = None
        timestamp_col = None
        filename_col = None
        
        # 查找图片数据列
        for col in ['image', 'img_data', 'image_data', 'data']:
            if col in df.columns:
                image_col = col
                break
        
        if image_col is None:
            logger.error(f"未找到图片数据列，可用列: {df.columns.tolist()}")
            return frames
        
        # 查找时间戳列
        for col in ['timestamp', 'time', 'ts']:
            if col in df.columns:
                timestamp_col = col
                break
        
        # 查找文件名列
        for col in ['filename', 'name', 'file']:
            if col in df.columns:
                filename_col = col
                break
        
        # 按时间戳排序（如果有）
        if timestamp_col:
            df = df.sort_values(by=timestamp_col).reset_index(drop=True)
        
        # 应用帧过滤
        if frame_indices is not None:
            # 只提取指定索引的帧
            df = df.iloc[frame_indices]
        elif max_frames is not None:
            # 限制最大帧数
            df = df.head(max_frames)
        
        # 解析每一帧
        for idx, row in df.iterrows():
            try:
                image_data = row[image_col]
                
                # 处理不同的数据类型
                if isinstance(image_data, bytes):
                    pass  # 已经是bytes
                elif hasattr(image_data, 'as_py'):
                    # PyArrow binary类型
                    image_data = image_data.as_py()
                else:
                    logger.warning(f"未知的图片数据类型: {type(image_data)}")
                    continue
                
                # 获取时间戳
                timestamp = int(row[timestamp_col]) if timestamp_col else 0
                
                # 获取文件名
                filename = row[filename_col] if filename_col else None
                if filename and hasattr(filename, 'as_py'):
                    filename = filename.as_py()
                
                # 检测图片格式
                image_format = self._detect_image_format(image_data)
                
                # 创建ImageFrame对象
                frame = ImageFrame(
                    scene_id=scene_id,
                    frame_index=int(idx),
                    timestamp=timestamp,
                    image_data=image_data,
                    image_format=image_format,
                    filename=filename
                )
                
                frames.append(frame)
                
            except Exception as e:
                logger.warning(f"解析第 {idx} 帧失败: {e}")
                continue
        
        logger.info(f"成功解析 {len(frames)} 帧图片")
        return frames
    
    def load_images_from_scene(
        self,
        scene_id: str,
        frame_indices: Optional[List[int]] = None,
        max_frames: Optional[int] = None
    ) -> List[ImageFrame]:
        """从单个场景加载图片
        
        Args:
            scene_id: 场景ID
            frame_indices: 指定要提取的帧索引列表（例如：[0, 5, 10]）
            max_frames: 最大帧数限制（如果不指定frame_indices）
            
        Returns:
            ImageFrame对象列表
            
        Example:
            >>> # 加载前3帧
            >>> frames = retriever.load_images_from_scene("scene_123", max_frames=3)
            >>> 
            >>> # 加载指定帧
            >>> frames = retriever.load_images_from_scene("scene_123", frame_indices=[0, 10, 20])
        """
        logger.info(f"开始加载场景 {scene_id} 的图片...")
        
        # 1. 查询OBS路径
        df_paths = self.get_scene_obs_paths([scene_id])
        if df_paths.empty:
            logger.error(f"未找到场景 {scene_id} 的OBS路径")
            return []
        
        scene_obs_path = df_paths.iloc[0]['scene_obs_path']
        if pd.isna(scene_obs_path) or not scene_obs_path:
            logger.error(f"场景 {scene_id} 的scene_obs_path为空")
            return []
        
        logger.info(f"场景OBS路径: {scene_obs_path}")
        
        # 2. 构建相机parquet路径
        camera_dir = self._parse_camera_parquet_path(scene_obs_path)
        
        # 3. 列出目录中的parquet文件
        parquet_files = self._list_obs_directory(camera_dir)
        if not parquet_files:
            logger.error(f"未找到parquet文件: {camera_dir}")
            return []
        
        logger.info(f"找到 {len(parquet_files)} 个parquet文件")
        
        # 4. 读取parquet文件（通常只有一个）
        all_frames = []
        for parquet_file in parquet_files:
            df = self._load_parquet_from_obs(parquet_file)
            if df is not None:
                frames = self._parse_parquet_to_frames(
                    df, scene_id, frame_indices, max_frames
                )
                all_frames.extend(frames)
        
        logger.info(f"✅ 场景 {scene_id} 共加载 {len(all_frames)} 帧图片")
        return all_frames
    
    def batch_load_images(
        self,
        scene_ids: List[str],
        frames_per_scene: int = 5
    ) -> Dict[str, List[ImageFrame]]:
        """批量加载多个场景的图片
        
        Args:
            scene_ids: 场景ID列表
            frames_per_scene: 每个场景加载的帧数
            
        Returns:
            字典，键为scene_id，值为ImageFrame列表
            
        Example:
            >>> scenes = ["scene_001", "scene_002", "scene_003"]
            >>> images = retriever.batch_load_images(scenes, frames_per_scene=3)
            >>> print(f"共加载 {len(images)} 个场景的图片")
        """
        logger.info(f"批量加载 {len(scene_ids)} 个场景的图片，每场景 {frames_per_scene} 帧...")
        
        results = {}
        success_count = 0
        fail_count = 0
        
        for i, scene_id in enumerate(scene_ids, 1):
            logger.info(f"[{i}/{len(scene_ids)}] 处理场景: {scene_id}")
            
            try:
                frames = self.load_images_from_scene(
                    scene_id, 
                    max_frames=frames_per_scene
                )
                
                if frames:
                    results[scene_id] = frames
                    success_count += 1
                else:
                    fail_count += 1
                    logger.warning(f"场景 {scene_id} 未加载到任何图片")
                    
            except Exception as e:
                fail_count += 1
                logger.error(f"加载场景 {scene_id} 失败: {e}")
                continue
        
        logger.info(f"✅ 批量加载完成: 成功 {success_count} 个，失败 {fail_count} 个")
        return results


# 便捷函数
def quick_load_scene_images(
    scene_id: str, 
    max_frames: int = 5,
    camera_type: str = "CAM_FRONT_WIDE_ANGLE"
) -> List[ImageFrame]:
    """快速加载场景图片的便捷函数
    
    Args:
        scene_id: 场景ID
        max_frames: 最大帧数
        camera_type: 相机类型
        
    Returns:
        ImageFrame列表
    """
    retriever = SceneImageRetriever(camera_type=camera_type)
    return retriever.load_images_from_scene(scene_id, max_frames=max_frames)

