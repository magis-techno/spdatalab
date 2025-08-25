"""多模态数据检索模块

基础数据处理功能：
1. MultimodalRetriever - 多模态API调用器，包含API限制控制
2. TrajectoryToPolygonConverter - 轨迹转Polygon转换器
3. 基础配置管理

复用现有模块的基础架构，专注于数据获取和基础转换。
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any
import warnings
import urllib3

import requests
from requests.exceptions import RequestException, Timeout
from shapely.geometry import LineString, Polygon

# 导入项目配置工具
from spdatalab.common.config import getenv

# 抑制警告
warnings.filterwarnings('ignore', category=UserWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 日志配置
logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """多模态API配置"""
    project: str
    api_key: str  
    username: str
    platform: str = "xmodalitys-external"
    region: str = "RaD-prod"
    entrypoint_version: str = "v2"
    api_base_url: str = "https://api.example.com"
    api_path: str = "/xmodalitys/retrieve"
    timeout: int = 30
    max_retries: int = 3
    
    @property
    def api_url(self) -> str:
        """完整的API URL"""
        return f"{self.api_base_url.rstrip('/')}{self.api_path}"
    
    @classmethod
    def from_env(cls) -> 'APIConfig':
        """从环境变量创建API配置
        
        需要的环境变量：
        - MULTIMODAL_PROJECT: 项目名称（默认：your_project）
        - MULTIMODAL_API_KEY: API密钥（必需）
        - MULTIMODAL_USERNAME: 用户名（必需）
        - MULTIMODAL_PLATFORM: 平台标识（默认：xmodalitys-external）
        - MULTIMODAL_REGION: 区域标识（默认：RaD-prod）
        - MULTIMODAL_ENTRYPOINT_VERSION: 入口版本（默认：v2）
        - MULTIMODAL_API_BASE_URL: API基础URL（必需，从环境变量获取）
        - MULTIMODAL_API_PATH: API路径（默认：/xmodalitys/retrieve）
        - MULTIMODAL_TIMEOUT: 超时时间（默认：30）
        - MULTIMODAL_MAX_RETRIES: 最大重试次数（默认：3）
        
        Returns:
            APIConfig实例
            
        Raises:
            RuntimeError: 当必需的环境变量缺失时
        """
        return cls(
            project=getenv('MULTIMODAL_PROJECT', 'your_project'),
            api_key=getenv('MULTIMODAL_API_KEY', required=True),
            username=getenv('MULTIMODAL_USERNAME', required=True),
            platform=getenv('MULTIMODAL_PLATFORM', 'xmodalitys-external'),
            region=getenv('MULTIMODAL_REGION', 'RaD-prod'),
            entrypoint_version=getenv('MULTIMODAL_ENTRYPOINT_VERSION', 'v2'),
            api_base_url=getenv('MULTIMODAL_API_BASE_URL', required=True),
            api_path=getenv('MULTIMODAL_API_PATH', '/xmodalitys/retrieve'),
            timeout=int(getenv('MULTIMODAL_TIMEOUT', '30')),
            max_retries=int(getenv('MULTIMODAL_MAX_RETRIES', '3'))
        )


class APIRetryStrategy:
    """API重试策略"""
    
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
    
    def execute_with_retry(self, api_call_func):
        """带重试的API调用"""
        for attempt in range(self.max_retries):
            try:
                return api_call_func()
            except (RequestException, Timeout) as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"API调用失败，已重试{self.max_retries}次: {e}")
                    raise
                wait_time = self.backoff_factor ** attempt
                logger.warning(f"API调用失败（第{attempt + 1}次），{wait_time}秒后重试: {e}")
                time.sleep(wait_time)


class MultimodalRetriever:
    """多模态API调用器
    
    专职功能：
    - 文本检索API调用（图片功能预留）
    - API限制处理：单次1万条，累计10万条控制
    - 相机自动匹配：从collection推导camera参数
    - 研发友好：简化参数，专注核心功能
    """
    
    def __init__(self, api_config: APIConfig):
        self.api_config = api_config
        self.retry_strategy = APIRetryStrategy(
            max_retries=api_config.max_retries
        )
        self.query_count = 0      # 累计查询计数
        self.max_total_count = 100000  # 累计限制10万条
        self.max_single_count = 10000  # 单次限制1万条
        
        # 构建请求头
        self.headers = self._build_headers()
    
    def _build_headers(self) -> Dict[str, str]:
        """构建API请求头"""
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": f"Bearer {self.api_config.api_key}",
            "Content-Type": "application/json",
            "Deepdata-Platform": self.api_config.platform,
            "Deepdata-Project": self.api_config.project,
            "Deepdata-Region": self.api_config.region,
            "Entrypoint-Version": self.api_config.entrypoint_version,
            "Host": self.api_config.api_base_url.replace("https://", "").replace("http://", ""),
            "User-Agent": "spdatalab-multimodal/1.0.0",
            "username": self.api_config.username
        }
    
    def _extract_camera_from_collection(self, collection: str) -> str:
        """从collection自动推导camera参数
        
        Args:
            collection: 如 "ddi_collection_camera_encoded_1"
            
        Returns:
            camera参数，如 "camera_1"
        """
        if "camera_encoded_" in collection:
            # 提取数字部分
            parts = collection.split("camera_encoded_")
            if len(parts) == 2:
                camera_id = parts[1]
                return f"camera_{camera_id}"
        
        # 默认返回
        logger.warning(f"无法从collection '{collection}' 推导camera参数，使用默认值")
        return "camera_1"
    
    def retrieve_by_text(self, text: str, collection: str, count: int = 5, 
                        start: int = 0, start_time: Optional[int] = None, 
                        end_time: Optional[int] = None) -> List[Dict]:
        """文本检索，包含API限制控制
        
        Args:
            text: 查询文本，如 "bicycle crossing intersection"
            collection: 相机表选择，如 "ddi_collection_camera_encoded_1"
            count: 返回数量，默认5，最大10000
            start: 起始偏移量，默认0
            start_time: 事件开始时间，13位时间戳（可选）
            end_time: 事件结束时间，13位时间戳（可选）
            
        Returns:
            检索结果列表
        """
        # 1. 验证单次查询限制（≤10000）
        if count > self.max_single_count:
            raise ValueError(f"单次查询数量不能超过{self.max_single_count}条，当前请求{count}条")
        
        # 2. 验证累计查询限制（≤100000）
        if self.query_count + count > self.max_total_count:
            remaining = self.max_total_count - self.query_count
            raise ValueError(f"累计查询数量不能超过{self.max_total_count}条，当前已查询{self.query_count}条，剩余{remaining}条")
        
        # 3. 自动推导camera参数
        camera = self._extract_camera_from_collection(collection)
        
        # 4. 构建请求参数（按照完整API格式）
        payload = {
            "text": text,
            "collection": collection,
            "camera": camera,
            "start": start,
            "count": count,
            "modality": 1  # 1表示文本检索
        }
        
        # 添加可选的时间范围参数
        if start_time is not None:
            payload["start_time"] = start_time
        if end_time is not None:
            payload["end_time"] = end_time
        
        logger.info(f"🔍 执行文本检索: '{text}', collection={collection}, camera={camera}, start={start}, count={count}")
        if start_time or end_time:
            logger.info(f"   时间范围: {start_time} - {end_time}")
        
        # 5. 执行API调用
        def api_call():
            response = requests.post(
                self.api_config.api_url,
                headers=self.headers,
                json=payload,
                timeout=self.api_config.timeout,
                verify=False  # 关闭SSL验证
            )
            response.raise_for_status()
            return response.json()
        
        try:
            result = self.retry_strategy.execute_with_retry(api_call)
            
            # 6. 更新查询计数
            actual_count = len(result) if isinstance(result, list) else 0
            self.query_count += actual_count
            
            logger.info(f"✅ 检索成功: 返回{actual_count}条结果，累计查询{self.query_count}条")
            
            return result if isinstance(result, list) else []
            
        except Exception as e:
            logger.error(f"❌ 文本检索失败: {e}")
            raise
    
    def retrieve_by_images(self, images: List[str], collection: str, count: int = 5,
                          start: int = 0, start_time: Optional[int] = None,
                          end_time: Optional[int] = None) -> List[Dict]:
        """图片检索，包含API限制控制
        
        Args:
            images: 图片base64编码后的字符串列表
            collection: 相机表选择，如 "ddi_collection_camera_encoded_1"
            count: 返回数量，默认5，最大10000
            start: 起始偏移量，默认0
            start_time: 事件开始时间，13位时间戳（可选）
            end_time: 事件结束时间，13位时间戳（可选）
            
        Returns:
            检索结果列表
        """
        # 1. 验证单次查询限制（≤10000）
        if count > self.max_single_count:
            raise ValueError(f"单次查询数量不能超过{self.max_single_count}条，当前请求{count}条")
        
        # 2. 验证累计查询限制（≤100000）
        if self.query_count + count > self.max_total_count:
            remaining = self.max_total_count - self.query_count
            raise ValueError(f"累计查询数量不能超过{self.max_total_count}条，当前已查询{self.query_count}条，剩余{remaining}条")
        
        # 3. 验证图片输入
        if not images or len(images) == 0:
            raise ValueError("图片列表不能为空")
        
        # 4. 自动推导camera参数
        camera = self._extract_camera_from_collection(collection)
        
        # 5. 构建请求参数（按照完整API格式）
        payload = {
            "images": images,
            "collection": collection,
            "camera": camera,
            "start": start,
            "count": count,
            "modality": 2  # 2表示图片检索
        }
        
        # 添加可选的时间范围参数
        if start_time is not None:
            payload["start_time"] = start_time
        if end_time is not None:
            payload["end_time"] = end_time
        
        logger.info(f"🔍 执行图片检索: {len(images)}张图片, collection={collection}, camera={camera}, start={start}, count={count}")
        if start_time or end_time:
            logger.info(f"   时间范围: {start_time} - {end_time}")
        
        # 6. 执行API调用
        def api_call():
            response = requests.post(
                self.api_config.api_url,
                headers=self.headers,
                json=payload,
                timeout=self.api_config.timeout,
                verify=False  # 关闭SSL验证
            )
            response.raise_for_status()
            return response.json()
        
        try:
            result = self.retry_strategy.execute_with_retry(api_call)
            
            # 7. 更新查询计数
            actual_count = len(result) if isinstance(result, list) else 0
            self.query_count += actual_count
            
            logger.info(f"✅ 图片检索成功: 返回{actual_count}条结果，累计查询{self.query_count}条")
            
            return result if isinstance(result, list) else []
            
        except Exception as e:
            logger.error(f"❌ 图片检索失败: {e}")
            raise
    
    def get_query_stats(self) -> Dict[str, Any]:
        """获取查询统计信息"""
        return {
            "total_queries": self.query_count,
            "remaining_queries": self.max_total_count - self.query_count,
            "max_single_query": self.max_single_count,
            "max_total_query": self.max_total_count
        }


class TrajectoryToPolygonConverter:
    """轨迹转Polygon转换器
    
    极简设计（研发分析专用）：
    - 10米固定缓冲区，适合精确分析
    - 基础几何验证和优化
    - 并行批量处理支持
    """
    
    def __init__(self, buffer_distance: float = 10.0):
        self.buffer_distance = buffer_distance  # 默认10米，适合精确分析
        self.simplify_tolerance = 2.0           # 固定简化容差
        self.min_area = 50.0                    # 固定最小面积（平方米）
    
    def convert_trajectory_to_polygon(self, trajectory_linestring: LineString) -> Optional[Polygon]:
        """轨迹转polygon的简化算法（研发分析专用）
        
        核心逻辑：10米缓冲区 + 基础验证
        
        Args:
            trajectory_linestring: 轨迹线几何
            
        Returns:
            转换后的Polygon，失败返回None
        """
        try:
            if not isinstance(trajectory_linestring, LineString) or trajectory_linestring.is_empty:
                return None
            
            # 1. 创建固定缓冲区
            polygon = trajectory_linestring.buffer(self.buffer_distance)
            
            # 2. 基础验证
            if not polygon.is_valid or polygon.area < self.min_area:
                return None
            
            # 3. 简单优化
            optimized_polygon = polygon.simplify(self.simplify_tolerance, preserve_topology=True)
            
            # 4. 再次验证
            if not optimized_polygon.is_valid or optimized_polygon.area < self.min_area:
                return polygon  # 返回未优化版本
            
            return optimized_polygon
            
        except Exception as e:
            logger.warning(f"轨迹转换失败: {e}")
            return None  # 研发场景下，简单跳过异常情况
    
    def batch_convert(self, trajectory_data: List[Dict]) -> List[Dict]:
        """批量转换，支持并行处理
        
        Args:
            trajectory_data: 轨迹数据列表，每项包含 linestring 字段
            
        Returns:
            转换结果列表，包含polygon和源数据映射
        """
        if not trajectory_data:
            return []
        
        logger.info(f"🔄 批量转换{len(trajectory_data)}条轨迹为Polygon...")
        
        # 并行处理
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(self.convert_trajectory_to_polygon, traj.get('linestring'))
                for traj in trajectory_data
            ]
            
            results = []
            for i, future in enumerate(futures):
                try:
                    polygon = future.result()
                    if polygon:  # 过滤无效结果
                        results.append({
                            'id': f"traj_polygon_{i}",
                            'geometry': polygon,
                            'properties': {
                                'source_dataset': trajectory_data[i].get('dataset_name', f'dataset_{i}'),
                                'source_timestamp': trajectory_data[i].get('timestamp', 0),
                                'buffer_distance': self.buffer_distance,
                                'original_length': trajectory_data[i].get('linestring', LineString()).length if trajectory_data[i].get('linestring') else 0
                            }
                        })
                except Exception as e:
                    logger.warning(f"转换第{i}条轨迹失败: {e}")
        
        logger.info(f"✅ 批量转换完成: {len(results)}/{len(trajectory_data)} 条成功")
        return results


# 导出主要类
__all__ = [
    'APIConfig',
    'MultimodalRetriever', 
    'TrajectoryToPolygonConverter',
    'APIRetryStrategy'
]
