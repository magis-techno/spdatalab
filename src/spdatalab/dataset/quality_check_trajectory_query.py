"""质检轨迹查询模块

基于Excel质检结果查询轨迹数据，支持时间分段和结果合并功能：
- 批量读取多个Excel质检结果文件
- 智能过滤有效数据（有result或other_scenario的记录）
- 根据autoscene_id查找对应的完整轨迹
- 根据时间区间description进行轨迹分段
- 合并处理result和other_scenario字段
- 统一输出MultiLineString几何格式
- 高性能并行处理和批量保存

功能特性：
1. 多Excel文件批量处理和智能数据过滤
2. 万级数据高效并行处理
3. 高效的scene_id到dataset_name映射查询
4. 智能轨迹时间分段算法
5. 结果字段合并去重处理
6. 统一MultiLineString几何格式
7. 批量数据库写入和文件导出
8. 内存优化和并行查询支持
"""

import argparse
import json
import logging
import sys
import time
import ast
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Union
from dataclasses import dataclass
import warnings
import concurrent.futures
from itertools import islice
import gc

import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, Point
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor

# 抑制警告
warnings.filterwarnings('ignore', category=UserWarning)

# 数据库配置
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 日志配置
logger = logging.getLogger(__name__)

@dataclass
class QualityCheckRecord:
    """质检记录数据结构"""
    task_name: str
    annotator: str
    autoscene_id: str
    result: Union[str, List[str]]
    description: List[List[float]]  # [[start, end], ...]
    other_scenario: Union[str, List[str]]

@dataclass
class SegmentedTrajectory:
    """分段轨迹输出数据结构"""
    task_name: str
    annotator: str
    scene_id: str
    dataset_name: str
    segment_count: int
    merged_results: List[str]
    geometry: MultiLineString
    total_duration: float
    start_time: int
    end_time: int
    total_points: int

@dataclass
class QualityCheckConfig:
    """质检轨迹查询配置"""
    local_dsn: str = LOCAL_DSN
    point_table: str = POINT_TABLE
    
    # Excel处理配置
    excel_batch_size: int = 2000
    required_columns: List[str] = None
    filter_invalid_records: bool = True  # 过滤无result和other_scenario的记录
    
    # 轨迹分段配置
    min_points_per_segment: int = 2
    time_tolerance: float = 0.1  # 时间匹配容差（秒）
    
    # 几何配置
    force_multilinestring: bool = True
    simplify_geometry: bool = False
    simplify_tolerance: float = 0.00001
    
    # 批量处理配置
    scene_id_batch_size: int = 200  # 增大批量大小
    trajectory_batch_size: int = 100  # 增大批量大小
    batch_insert_size: int = 2000  # 增大插入批量
    
    # 查询优化配置
    query_timeout: int = 300
    cache_scene_mappings: bool = True
    
    # 并行处理配置
    max_workers: int = 4  # 最大并行工作线程数
    enable_parallel_trajectory_query: bool = True  # 启用并行轨迹查询
    enable_parallel_processing: bool = True  # 启用并行记录处理
    memory_optimization: bool = True  # 启用内存优化
    
    # 万级数据处理配置
    large_data_threshold: int = 5000  # 大数据阈值
    chunk_processing_size: int = 1000  # 分块处理大小
    progress_report_interval: int = 500  # 进度报告间隔
    
    def __post_init__(self):
        if self.required_columns is None:
            self.required_columns = [
                'task_name', 'annotator', 'autoscene_id', 
                'result', 'description', 'other_scenario'
            ]

class ExcelDataParser:
    """Excel数据解析器"""
    
    def __init__(self, config: QualityCheckConfig):
        self.config = config
    
    def load_multiple_excel_files(self, file_paths: List[str]) -> List[QualityCheckRecord]:
        """批量加载并解析多个Excel文件
        
        Args:
            file_paths: Excel文件路径列表
            
        Returns:
            质检记录列表
        """
        all_records = []
        total_files = len(file_paths)
        
        logger.info(f"📖 开始批量加载 {total_files} 个Excel文件")
        
        for i, file_path in enumerate(file_paths, 1):
            try:
                logger.info(f"📊 处理文件 {i}/{total_files}: {Path(file_path).name}")
                records = self.load_excel_data(file_path)
                all_records.extend(records)
                
                # 内存优化：定期清理
                if self.config.memory_optimization and i % 5 == 0:
                    gc.collect()
                    
            except Exception as e:
                logger.error(f"❌ 文件处理失败 {file_path}: {str(e)}")
                continue
        
        logger.info(f"✅ 批量加载完成: 总计 {len(all_records)} 条有效记录")
        return all_records
    
    def load_excel_data(self, file_path: str) -> List[QualityCheckRecord]:
        """加载并解析Excel文件
        
        Args:
            file_path: Excel文件路径
            
        Returns:
            质检记录列表
        """
        try:
            logger.info(f"📖 加载Excel文件: {Path(file_path).name}")
            
            # 读取Excel文件（指定编码）
            df = pd.read_excel(file_path, engine='openpyxl')
            logger.info(f"📊 原始数据: {len(df)} 行, {len(df.columns)} 列")
            
            # 检查必需列
            missing_cols = [col for col in self.config.required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Excel文件缺少必需列: {missing_cols}")
            
            # 清理数据
            df = self._clean_dataframe(df)
            
            # 过滤无效记录（提高效率）
            if self.config.filter_invalid_records:
                df = self._filter_valid_records(df)
                logger.info(f"📋 过滤后有效数据: {len(df)} 行")
            
            # 转换为QualityCheckRecord列表
            records = []
            for index, row in df.iterrows():
                try:
                    record = self._parse_record(row)
                    if record:
                        records.append(record)
                except Exception as e:
                    logger.debug(f"解析第 {index+1} 行数据失败: {str(e)}")
                    continue
            
            logger.info(f"✅ 解析完成: {len(records)} 条有效质检记录")
            return records
            
        except Exception as e:
            logger.error(f"加载Excel文件失败: {str(e)}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理DataFrame数据"""
        # 移除空行
        df = df.dropna(subset=['autoscene_id'])
        
        # 清理字符串字段（确保UTF-8编码）
        string_cols = ['task_name', 'annotator', 'autoscene_id', 'result', 'other_scenario']
        for col in string_cols:
            if col in df.columns:
                # 确保字符串编码正确
                df[col] = df[col].astype(str).str.strip()
                # 处理编码问题：如果存在编码错误，尝试修复
                df[col] = df[col].apply(self._fix_encoding)
                df[col] = df[col].replace(['nan', 'None', ''], None)
        
        # 处理description字段
        if 'description' in df.columns:
            df['description'] = df['description'].astype(str).str.strip()
            df['description'] = df['description'].replace(['nan', 'None', ''], None)
        
        logger.debug(f"数据清理后: {len(df)} 行有效数据")
        return df
    
    def _filter_valid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """过滤有效记录：至少有result或other_scenario的数据，且排除result为'good'的记录"""
        # 检查result字段是否有效
        result_valid = df['result'].notna() & (df['result'] != '') & (df['result'] != 'nan')
        
        # 检查other_scenario字段是否有效
        other_scenario_valid = df['other_scenario'].notna() & (df['other_scenario'] != '') & (df['other_scenario'] != 'nan')
        
        # 至少有一个字段有效
        valid_mask = result_valid | other_scenario_valid
        
        # 过滤掉result为'good'的记录
        def is_good_result(value):
            """检查结果是否为good（处理编码问题）"""
            if pd.isna(value) or value == '' or value == 'nan':
                return False
            
            # 先修复编码
            fixed_value = self._fix_encoding(value)
            
            # 处理字符串格式的列表
            if isinstance(fixed_value, str):
                if fixed_value.lower().strip() == 'good':
                    return True
                # 处理字符串格式的列表，如 "['good']"
                try:
                    if fixed_value.startswith('[') and fixed_value.endswith(']'):
                        import ast
                        parsed_list = ast.literal_eval(fixed_value)
                        if isinstance(parsed_list, list):
                            # 检查列表中是否只有'good'
                            cleaned_list = []
                            for item in parsed_list:
                                if str(item).strip():
                                    fixed_item = self._fix_encoding(str(item)).lower().strip()
                                    cleaned_list.append(fixed_item)
                            return len(cleaned_list) == 1 and cleaned_list[0] == 'good'
                except:
                    pass
            
            # 处理列表类型
            elif isinstance(fixed_value, list):
                cleaned_list = []
                for item in fixed_value:
                    if str(item).strip():
                        fixed_item = self._fix_encoding(str(item)).lower().strip()
                        cleaned_list.append(fixed_item)
                return len(cleaned_list) == 1 and cleaned_list[0] == 'good'
            
            return False
        
        # 过滤掉result为good的记录
        good_mask = df['result'].apply(is_good_result)
        logger.debug(f"发现 {good_mask.sum()} 条result为'good'的记录，将被过滤")
        
        # 最终过滤条件：有效记录 且 不是good结果
        final_mask = valid_mask & ~good_mask
        
        filtered_df = df[final_mask].copy()
        
        logger.info(f"数据过滤: {len(df)} -> {len(filtered_df)} 行")
        logger.info(f"  过滤掉无效数据: {(~valid_mask).sum()} 行")
        logger.info(f"  过滤掉'good'结果: {good_mask.sum()} 行")
        logger.info(f"  最终有效数据: {len(filtered_df)} 行")
        
        return filtered_df
    
    def _fix_encoding(self, text):
        """修复字符串编码问题"""
        if pd.isna(text) or text in ['nan', 'None', '']:
            return text
        
        try:
            # 转换为字符串
            text_str = str(text)
            
            # 如果是正常的字符串，直接返回
            if all(ord(char) < 128 for char in text_str if char.isprintable()):
                # 纯ASCII字符，可能包含中文的编码表示
                pass
            elif any('\u4e00' <= char <= '\u9fff' for char in text_str):
                # 包含中文字符，直接返回
                return text_str
            
            # 尝试各种编码修复
            encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'latin1']
            
            for encoding in encodings_to_try:
                try:
                    # 如果文本已经是字符串，尝试先编码再解码
                    if isinstance(text_str, str):
                        # 尝试不同的解码方式
                        fixed_text = text_str.encode('latin1').decode(encoding)
                        # 检查是否包含中文字符
                        if any('\u4e00' <= char <= '\u9fff' for char in fixed_text):
                            logger.debug(f"编码修复成功: '{text_str}' -> '{fixed_text}' (使用 {encoding})")
                            return fixed_text
                except:
                    continue
            
            # 如果无法修复，返回原文本
            return text_str
            
        except Exception as e:
            logger.debug(f"编码修复失败: {text}, 错误: {e}")
            return str(text)
    
    def _parse_record(self, row: pd.Series) -> Optional[QualityCheckRecord]:
        """解析单条记录"""
        try:
            # 基础字段
            task_name = row.get('task_name', '').strip()
            annotator = row.get('annotator', '').strip()
            autoscene_id = row.get('autoscene_id', '').strip()
            
            if not autoscene_id:
                return None
            
            # 解析result字段
            result = self._parse_result_field(row.get('result'))
            
            # 解析other_scenario字段
            other_scenario = self._parse_result_field(row.get('other_scenario'))
            
            # 解析description字段
            description = self._parse_description_field(row.get('description'))
            
            return QualityCheckRecord(
                task_name=task_name,
                annotator=annotator,
                autoscene_id=autoscene_id,
                result=result,
                description=description,
                other_scenario=other_scenario
            )
            
        except Exception as e:
            logger.debug(f"解析记录失败: {str(e)}")
            return None
    
    def _parse_result_field(self, value) -> Union[str, List[str]]:
        """解析result或other_scenario字段（处理编码问题）"""
        if pd.isna(value) or value in ['', 'nan', 'None']:
            return []
        
        # 先修复编码问题
        value_fixed = self._fix_encoding(value)
        value_str = str(value_fixed).strip()
        
        # 尝试解析为列表
        if value_str.startswith('[') and value_str.endswith(']'):
            try:
                parsed = ast.literal_eval(value_str)
                if isinstance(parsed, list):
                    # 对列表中的每个元素也进行编码修复
                    result = []
                    for item in parsed:
                        if str(item).strip():
                            fixed_item = self._fix_encoding(str(item).strip().strip("'\""))
                            result.append(fixed_item)
                    return result
                else:
                    fixed_item = self._fix_encoding(str(parsed).strip().strip("'\""))
                    return [fixed_item]
            except:
                # 解析失败，当作字符串处理
                fixed_item = self._fix_encoding(value_str.strip().strip("'\""))
                return [fixed_item]
        else:
            # 单个字符串
            fixed_item = self._fix_encoding(value_str.strip().strip("'\""))
            return [fixed_item]
    
    def _parse_description_field(self, value) -> List[List[float]]:
        """解析description时间区间字段"""
        logger.debug(f"🔍 解析description字段: {value} (类型: {type(value)})")
        
        if pd.isna(value) or value in ['', 'nan', 'None']:
            logger.debug("   结果: 空值，返回空列表")
            return []
        
        value_str = str(value).strip()
        logger.debug(f"   字符串化后: '{value_str}'")
        
        try:
            # 尝试解析为嵌套列表
            parsed = ast.literal_eval(value_str)
            logger.debug(f"   ast解析结果: {parsed} (类型: {type(parsed)})")
            
            if isinstance(parsed, list):
                result = []
                for i, item in enumerate(parsed):
                    logger.debug(f"   处理第{i+1}个区间: {item} (类型: {type(item)})")
                    
                    if isinstance(item, list) and len(item) == 2:
                        try:
                            start_time = float(item[0])
                            end_time = float(item[1])
                            
                            if start_time < end_time:  # 验证时间区间有效性
                                result.append([start_time, end_time])
                                logger.debug(f"     ✅ 有效区间: [{start_time}, {end_time}]")
                            else:
                                logger.debug(f"     ❌ 无效区间: [{start_time}, {end_time}] (开始>=结束)")
                        except Exception as e:
                            logger.debug(f"     ❌ 区间转换失败: {e}")
                            continue
                    else:
                        logger.debug(f"     ❌ 区间格式错误: 不是长度为2的列表")
                
                logger.debug(f"   解析结果: {result} ({len(result)} 个有效区间)")
                return result
            else:
                logger.debug(f"   ❌ 解析结果不是列表: {type(parsed)}")
        except Exception as e:
            logger.debug(f"   ❌ ast解析失败: {e}")
        
        logger.debug("   返回空列表")
        return []

class ResultFieldProcessor:
    """结果字段处理器"""
    
    @staticmethod
    def merge_and_clean_results(result: Union[str, List[str]], 
                               other_scenario: Union[str, List[str]]) -> List[str]:
        """合并并清理结果字段
        
        Args:
            result: 主要结果字段
            other_scenario: 其他场景字段
            
        Returns:
            合并去重后的结果列表
        """
        all_results = []
        
        # 处理result字段
        if isinstance(result, str):
            if result.strip():
                all_results.append(result.strip())
        elif isinstance(result, list):
            for item in result:
                if isinstance(item, str) and item.strip():
                    all_results.append(item.strip())
        
        # 处理other_scenario字段
        if isinstance(other_scenario, str):
            if other_scenario.strip():
                all_results.append(other_scenario.strip())
        elif isinstance(other_scenario, list):
            for item in other_scenario:
                if isinstance(item, str) and item.strip():
                    all_results.append(item.strip())
        
        # 去重并排序
        unique_results = list(set(all_results))
        unique_results.sort()
        
        return unique_results

class SceneIdMapper:
    """场景ID映射器"""
    
    def __init__(self, config: QualityCheckConfig):
        self.config = config
        self._cache = {} if config.cache_scene_mappings else None
    
    def batch_query_scene_mappings(self, autoscene_ids: List[str]) -> Dict[str, Dict]:
        """高效批量查询autoscene_id到数据集信息的映射
        
        Args:
            autoscene_ids: autoscene_id列表
            
        Returns:
            映射字典 {autoscene_id: {'dataset_name': str, 'event_id': int, 'event_name': str}}
        """
        if not autoscene_ids:
            return {}
        
        # 去重
        unique_ids = list(set(autoscene_ids))
        
        # 检查缓存
        if self._cache:
            uncached_ids = [aid for aid in unique_ids if aid not in self._cache]
            cached_results = {aid: self._cache[aid] for aid in unique_ids if aid in self._cache}
        else:
            uncached_ids = unique_ids
            cached_results = {}
        
        if not uncached_ids:
            logger.info(f"✅ 从缓存获取 {len(cached_results)} 个scene映射")
            return cached_results
        
        logger.info(f"🔍 批量查询scene映射: {len(uncached_ids)} 个新autoscene_id（缓存: {len(cached_results)} 个）")
        
        # 分批查询（避免单次查询过大）
        new_mappings = {}
        batch_size = self.config.scene_id_batch_size
        
        for i in range(0, len(uncached_ids), batch_size):
            batch_ids = uncached_ids[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(uncached_ids) + batch_size - 1) // batch_size
            
            logger.info(f"🔍 查询批次 {batch_num}/{total_batches}: {len(batch_ids)} 个ID")
            
            batch_mappings = self._query_scene_mapping_batch(batch_ids)
            new_mappings.update(batch_mappings)
            
            # 内存优化：定期清理
            if self.config.memory_optimization and batch_num % 10 == 0:
                gc.collect()
        
        # 合并结果
        all_mappings = {**cached_results, **new_mappings}
        
        logger.info(f"✅ 查询完成: {len(new_mappings)} 个新映射, 总计 {len(all_mappings)} 个映射")
        
        return all_mappings
    
    def _query_scene_mapping_batch(self, scene_ids: List[str]) -> Dict[str, Dict]:
        """查询单批scene映射"""
        try:
            sql = """
                SELECT id AS scene_id,
                       origin_name AS dataset_name,
                       event_id,
                       event_name
                FROM (
                    SELECT id, 
                           origin_name,
                           event_id,
                           event_name,
                           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
                    FROM transform.ods_t_data_fragment_datalake 
                    WHERE id IN %(scene_ids)s
                ) ranked
                WHERE rn = 1
            """
            
            mappings = {}
            
            with hive_cursor() as cur:
                cur.execute(sql, {"scene_ids": tuple(scene_ids)})
                cols = [d[0] for d in cur.description]
                results = cur.fetchall()
                
                for row in results:
                    row_dict = dict(zip(cols, row))
                    scene_id = row_dict['scene_id']
                    
                    # 处理event_id（避免浮点数问题）
                    event_id = row_dict.get('event_id')
                    if event_id is not None and event_id != '':
                        try:
                            event_id = int(float(event_id))
                        except:
                            event_id = None
                    else:
                        event_id = None
                    
                    mapping = {
                        'dataset_name': row_dict.get('dataset_name', ''),
                        'event_id': event_id,
                        'event_name': row_dict.get('event_name', '')
                    }
                    
                    mappings[scene_id] = mapping
                    
                    # 更新缓存
                    if self._cache is not None:
                        self._cache[scene_id] = mapping
            
            return mappings
            
        except Exception as e:
            logger.error(f"查询scene映射批次失败: {str(e)}")
            return {}

class TrajectorySegmenter:
    """轨迹分段器"""
    
    def __init__(self, config: QualityCheckConfig):
        self.config = config
    
    def query_complete_trajectory(self, dataset_name: str) -> pd.DataFrame:
        """查询完整轨迹数据
        
        Args:
            dataset_name: 数据集名称
            
        Returns:
            轨迹点DataFrame
        """
        logger.debug(f"🔍 开始查询轨迹数据: {dataset_name}")
        
        try:
            sql = f"""
                SELECT 
                    timestamp,
                    ST_X(point_lla) as longitude,
                    ST_Y(point_lla) as latitude,
                    twist_linear,
                    avp_flag,
                    workstage
                FROM {self.config.point_table}
                WHERE dataset_name = %(dataset_name)s
                AND point_lla IS NOT NULL
                AND timestamp IS NOT NULL
                ORDER BY timestamp
            """
            
            logger.debug(f"📊 执行SQL查询: {self.config.point_table}")
            
            with hive_cursor("dataset_gy1") as cur:
                cur.execute(sql, {"dataset_name": dataset_name})
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                
                logger.debug(f"📋 SQL查询结果: {len(rows)} 行数据")
                
                if rows:
                    df = pd.DataFrame(rows, columns=cols)
                    
                    # 检查数据质量
                    null_coords = df[['longitude', 'latitude']].isnull().any(axis=1).sum()
                    null_timestamps = df['timestamp'].isnull().sum()
                    
                    logger.debug(f"✅ 查询成功 {dataset_name}: {len(df)} 个点")
                    logger.debug(f"   空坐标: {null_coords} 个")
                    logger.debug(f"   空时间戳: {null_timestamps} 个")
                    
                    if len(df) > 0:
                        logger.debug(f"   时间范围: {df['timestamp'].min()} - {df['timestamp'].max()}")
                        logger.debug(f"   坐标范围: lon[{df['longitude'].min():.6f}, {df['longitude'].max():.6f}], "
                                   f"lat[{df['latitude'].min():.6f}, {df['latitude'].max():.6f}]")
                    
                    return df
                else:
                    logger.warning(f"⚠️ 未查询到轨迹数据: {dataset_name}")
                    logger.debug(f"   SQL: {sql}")
                    logger.debug(f"   参数: dataset_name={dataset_name}")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"❌ 查询轨迹失败 {dataset_name}: {str(e)}")
            logger.error(f"   SQL: {sql}")
            logger.error(f"   表名: {self.config.point_table}")
            return pd.DataFrame()
    
    def segment_trajectory_by_time_ranges(self, 
                                        trajectory_df: pd.DataFrame, 
                                        time_ranges: List[List[float]]) -> Tuple[MultiLineString, int]:
        """根据时间区间分段轨迹
        
        Args:
            trajectory_df: 完整轨迹点DataFrame
            time_ranges: 时间区间列表 [[start, end], ...]
            
        Returns:
            (MultiLineString几何, 有效分段数量)
        """
        logger.debug(f"🔧 开始轨迹分段: 轨迹点数={len(trajectory_df)}, 时间区间数={len(time_ranges)}")
        
        if trajectory_df.empty:
            logger.warning("⚠️ 轨迹DataFrame为空，无法进行分段")
            return MultiLineString([]), 0
        
        # 计算相对时间
        start_timestamp = trajectory_df['timestamp'].min()
        end_timestamp = trajectory_df['timestamp'].max()
        total_duration = end_timestamp - start_timestamp
        
        logger.debug(f"📊 轨迹时间范围: {start_timestamp} - {end_timestamp} (时长: {total_duration}s)")
        
        trajectory_df = trajectory_df.copy()
        trajectory_df['relative_time'] = trajectory_df['timestamp'] - start_timestamp
        
        segments = []
        valid_segments = 0
        skipped_segments = 0
        
        for i, time_range in enumerate(time_ranges):
            start_time, end_time = time_range
            logger.debug(f"🔍 处理时间区间 {i+1}/{len(time_ranges)}: [{start_time}, {end_time}]s")
            
            # 检查时间区间是否合理
            if start_time >= end_time:
                logger.warning(f"⚠️ 无效时间区间: [{start_time}, {end_time}] (开始时间 >= 结束时间)")
                skipped_segments += 1
                continue
            
            if end_time < 0 or start_time > total_duration:
                logger.warning(f"⚠️ 时间区间超出轨迹范围: [{start_time}, {end_time}]s, 轨迹时长: {total_duration}s")
                skipped_segments += 1
                continue
            
            # 筛选时间区间内的点
            mask = (
                (trajectory_df['relative_time'] >= start_time - self.config.time_tolerance) &
                (trajectory_df['relative_time'] <= end_time + self.config.time_tolerance)
            )
            
            segment_points = trajectory_df[mask]
            logger.debug(f"📍 时间区间 [{start_time}, {end_time}]s 筛选到 {len(segment_points)} 个点")
            
            if len(segment_points) < self.config.min_points_per_segment:
                logger.warning(f"⚠️ 分段点数不足: {len(segment_points)} < {self.config.min_points_per_segment}")
                skipped_segments += 1
                continue
            
            try:
                coordinates = list(zip(segment_points['longitude'], segment_points['latitude']))
                
                # 检查坐标有效性
                valid_coords = [(lon, lat) for lon, lat in coordinates if pd.notna(lon) and pd.notna(lat)]
                if len(valid_coords) < self.config.min_points_per_segment:
                    logger.warning(f"⚠️ 有效坐标不足: {len(valid_coords)} < {self.config.min_points_per_segment}")
                    skipped_segments += 1
                    continue
                
                segment_geom = LineString(valid_coords)
                
                # 可选的几何简化
                if self.config.simplify_geometry:
                    original_coords = len(segment_geom.coords)
                    segment_geom = segment_geom.simplify(self.config.simplify_tolerance)
                    simplified_coords = len(segment_geom.coords)
                    logger.debug(f"🔧 几何简化: {original_coords} -> {simplified_coords} 个坐标点")
                
                segments.append(segment_geom)
                valid_segments += 1
                logger.debug(f"✅ 成功创建分段 {valid_segments}: {start_time}-{end_time}s, {len(segment_points)} 个点")
                
            except Exception as e:
                logger.error(f"❌ 创建分段几何失败: {str(e)}")
                skipped_segments += 1
                continue
        
        logger.info(f"📊 分段结果: 成功={valid_segments}, 跳过={skipped_segments}, 总数={len(time_ranges)}")
        
        if segments:
            try:
                multi_geom = MultiLineString(segments)
                logger.debug(f"✅ 成功创建MultiLineString: {len(segments)} 个分段")
                return multi_geom, len(segments)
            except Exception as e:
                logger.error(f"❌ 创建MultiLineString失败: {str(e)}")
                return MultiLineString([]), 0
        else:
            logger.warning("⚠️ 没有有效的分段，返回空几何")
            return MultiLineString([]), 0
    
    def create_complete_trajectory(self, trajectory_df: pd.DataFrame) -> Tuple[MultiLineString, float]:
        """创建完整轨迹（无分段情况）
        
        Args:
            trajectory_df: 轨迹点DataFrame
            
        Returns:
            (MultiLineString几何, 总时长)
        """
        logger.debug(f"🔧 开始创建完整轨迹: 轨迹点数={len(trajectory_df)}")
        
        if trajectory_df.empty:
            logger.warning("⚠️ 轨迹DataFrame为空，无法创建完整轨迹")
            return MultiLineString([]), 0.0
        
        if len(trajectory_df) < self.config.min_points_per_segment:
            logger.warning(f"⚠️ 轨迹点数不足: {len(trajectory_df)} < {self.config.min_points_per_segment}")
            return MultiLineString([]), 0.0
        
        try:
            # 检查必要字段
            required_columns = ['longitude', 'latitude', 'timestamp']
            missing_columns = [col for col in required_columns if col not in trajectory_df.columns]
            if missing_columns:
                logger.error(f"❌ 缺少必要字段: {missing_columns}")
                return MultiLineString([]), 0.0
            
            # 过滤有效坐标
            coordinates = list(zip(trajectory_df['longitude'], trajectory_df['latitude']))
            valid_coords = [(lon, lat) for lon, lat in coordinates if pd.notna(lon) and pd.notna(lat)]
            
            logger.debug(f"📍 有效坐标数量: {len(valid_coords)}/{len(coordinates)}")
            
            if len(valid_coords) < self.config.min_points_per_segment:
                logger.warning(f"⚠️ 有效坐标不足: {len(valid_coords)} < {self.config.min_points_per_segment}")
                return MultiLineString([]), 0.0
            
            # 创建轨迹几何
            trajectory_geom = LineString(valid_coords)
            logger.debug(f"✅ 成功创建LineString: {len(trajectory_geom.coords)} 个坐标点")
            
            # 可选的几何简化
            if self.config.simplify_geometry:
                original_coords = len(trajectory_geom.coords)
                trajectory_geom = trajectory_geom.simplify(self.config.simplify_tolerance)
                simplified_coords = len(trajectory_geom.coords)
                logger.debug(f"🔧 几何简化: {original_coords} -> {simplified_coords} 个坐标点")
            
            # 计算总时长
            min_timestamp = trajectory_df['timestamp'].min()
            max_timestamp = trajectory_df['timestamp'].max()
            duration = float(max_timestamp - min_timestamp)
            
            logger.debug(f"📊 轨迹时长: {duration}s ({min_timestamp} - {max_timestamp})")
            
            # 转换为MultiLineString
            multi_geom = MultiLineString([trajectory_geom])
            logger.debug(f"✅ 成功创建完整轨迹MultiLineString")
            
            return multi_geom, duration
            
        except Exception as e:
            logger.error(f"❌ 创建完整轨迹失败: {str(e)}")
            logger.error(f"   轨迹数据样本: {trajectory_df.head() if not trajectory_df.empty else 'Empty'}")
            return MultiLineString([]), 0.0

class QualityCheckTrajectoryQuery:
    """质检轨迹查询主类"""
    
    def __init__(self, config: Optional[QualityCheckConfig] = None):
        self.config = config or QualityCheckConfig()
        self.engine = create_engine(
            self.config.local_dsn, 
            future=True,
            connect_args={
                "client_encoding": "utf8",
                "options": "-c timezone=UTC"
            }
        )
        
        # 初始化组件
        self.excel_parser = ExcelDataParser(self.config)
        self.result_processor = ResultFieldProcessor()
        self.scene_mapper = SceneIdMapper(self.config)
        self.trajectory_segmenter = TrajectorySegmenter(self.config)
        
        logger.info("🔧 质检轨迹查询器初始化完成")
    
    def process_excel_files(self, 
                           excel_files: Union[str, List[str]],
                           output_table: Optional[str] = None,
                           output_geojson: Optional[str] = None) -> Dict:
        """处理Excel质检文件完整工作流（支持多文件和并行处理）
        
        Args:
            excel_files: Excel文件路径或文件路径列表
            output_table: 输出数据库表名（可选）
            output_geojson: 输出GeoJSON文件路径（可选）
            
        Returns:
            处理统计信息
        """
        workflow_start = time.time()
        
        # 标准化文件路径
        if isinstance(excel_files, str):
            file_paths = [excel_files]
        else:
            file_paths = excel_files
        
        stats = {
            'start_time': datetime.now(),
            'excel_files': file_paths,
            'file_count': len(file_paths),
            'output_table': output_table,
            'output_geojson': output_geojson,
            'total_records': 0,
            'valid_trajectories': 0,
            'failed_records': 0,
            'processing_mode': 'parallel' if self.config.enable_parallel_processing else 'sequential'
        }
        
        try:
            logger.info("=" * 60)
            logger.info("🚀 开始质检轨迹查询工作流（万级数据优化版）")
            logger.info("=" * 60)
            
            # 阶段1: 解析Excel文件
            logger.info(f"📖 阶段1: 解析 {len(file_paths)} 个Excel文件")
            if len(file_paths) == 1:
                records = self.excel_parser.load_excel_data(file_paths[0])
            else:
                records = self.excel_parser.load_multiple_excel_files(file_paths)
            
            stats['total_records'] = len(records)
            
            if not records:
                logger.error("❌ 未解析到任何有效记录")
                stats['error'] = "No valid records parsed"
                return stats
            
            # 阶段2: 批量查询场景映射
            logger.info(f"🔍 阶段2: 查询场景映射信息")
            autoscene_ids = [record.autoscene_id for record in records]
            scene_mappings = self.scene_mapper.batch_query_scene_mappings(autoscene_ids)
            
            # 阶段3: 高效批量处理轨迹
            logger.info(f"🔧 阶段3: 高效处理 {len(records)} 条轨迹数据")
            
            if len(records) >= self.config.large_data_threshold:
                logger.info(f"📊 大数据模式：启用分块并行处理（阈值: {self.config.large_data_threshold}）")
                trajectories, failed_count = self._process_large_dataset(records, scene_mappings)
            else:
                logger.info(f"📊 标准模式：顺序处理")
                trajectories, failed_count = self._process_standard_dataset(records, scene_mappings)
            
            stats['valid_trajectories'] = len(trajectories)
            stats['failed_records'] = failed_count
            
            if not trajectories:
                logger.warning("⚠️ 未生成任何有效轨迹")
                stats['warning'] = "No valid trajectories generated"
                return stats
            
            logger.info(f"✅ 成功处理 {len(trajectories)} 条轨迹")
            
            # 阶段4: 保存到数据库（可选）
            if output_table:
                logger.info(f"💾 阶段4: 保存到数据库表: {output_table}")
                inserted_count, save_stats = self._save_trajectories_to_table(trajectories, output_table)
                stats['save_stats'] = save_stats
                logger.info(f"✅ 成功保存 {inserted_count} 条轨迹到数据库")
            
            # 阶段5: 导出到GeoJSON（可选）
            if output_geojson:
                logger.info(f"📄 阶段5: 导出到GeoJSON文件: {output_geojson}")
                if self._export_trajectories_to_geojson(trajectories, output_geojson):
                    stats['geojson_exported'] = True
                    logger.info(f"✅ 成功导出轨迹到GeoJSON文件")
                else:
                    stats['geojson_export_failed'] = True
                    logger.warning("⚠️ GeoJSON导出失败")
            
            # 最终统计
            stats['workflow_duration'] = time.time() - workflow_start
            stats['end_time'] = datetime.now()
            stats['success'] = True
            
            logger.info("=" * 60)
            logger.info("🎉 质检轨迹查询工作流完成!")
            logger.info(f"⏱️ 总耗时: {stats['workflow_duration']:.2f}s")
            logger.info(f"📊 成功处理: {stats['valid_trajectories']}/{stats['total_records']} 条记录")
            logger.info("=" * 60)
            
            return stats
            
        except Exception as e:
            stats['error'] = str(e)
            stats['workflow_duration'] = time.time() - workflow_start
            stats['end_time'] = datetime.now()
            stats['success'] = False
            logger.error(f"❌ 工作流执行失败: {str(e)}")
            return stats
    
    def _process_large_dataset(self, 
                              records: List[QualityCheckRecord], 
                              scene_mappings: Dict[str, Dict]) -> Tuple[List[SegmentedTrajectory], int]:
        """大数据集并行处理"""
        trajectories = []
        failed_count = 0
        total_records = len(records)
        
        # 分块处理
        chunk_size = self.config.chunk_processing_size
        total_chunks = (total_records + chunk_size - 1) // chunk_size
        
        logger.info(f"🔄 分块并行处理: {total_chunks} 个块，每块 {chunk_size} 条记录")
        
        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_records)
            chunk_records = records[start_idx:end_idx]
            
            logger.info(f"🔄 处理块 {chunk_idx + 1}/{total_chunks}: {len(chunk_records)} 条记录")
            
            if self.config.enable_parallel_processing:
                # 并行处理当前块
                chunk_trajectories, chunk_failed = self._process_chunk_parallel(chunk_records, scene_mappings)
            else:
                # 顺序处理当前块
                chunk_trajectories, chunk_failed = self._process_chunk_sequential(chunk_records, scene_mappings)
            
            trajectories.extend(chunk_trajectories)
            failed_count += chunk_failed
            
            # 进度报告
            processed_count = end_idx
            logger.info(f"📊 进度: {processed_count}/{total_records} ({processed_count/total_records*100:.1f}%), "
                       f"成功: {len(chunk_trajectories)}, 失败: {chunk_failed}")
            
            # 内存优化
            if self.config.memory_optimization and chunk_idx % 5 == 0:
                gc.collect()
        
        return trajectories, failed_count
    
    def _process_standard_dataset(self, 
                                 records: List[QualityCheckRecord], 
                                 scene_mappings: Dict[str, Dict]) -> Tuple[List[SegmentedTrajectory], int]:
        """标准数据集处理"""
        if self.config.enable_parallel_processing and len(records) > 50:
            logger.info("🔄 启用并行处理")
            return self._process_chunk_parallel(records, scene_mappings)
        else:
            logger.info("🔄 使用顺序处理")
            return self._process_chunk_sequential(records, scene_mappings)
    
    def _process_chunk_parallel(self, 
                               chunk_records: List[QualityCheckRecord], 
                               scene_mappings: Dict[str, Dict]) -> Tuple[List[SegmentedTrajectory], int]:
        """并行处理记录块"""
        trajectories = []
        failed_count = 0
        
        def process_record_wrapper(record):
            """记录处理包装函数"""
            try:
                return self._process_single_record(record, scene_mappings)
            except Exception as e:
                logger.debug(f"处理记录失败 {record.autoscene_id}: {str(e)}")
                return None
        
        # 使用线程池并行处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # 提交所有任务
            future_to_record = {
                executor.submit(process_record_wrapper, record): record 
                for record in chunk_records
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_record):
                record = future_to_record[future]
                try:
                    trajectory = future.result()
                    if trajectory:
                        trajectories.append(trajectory)
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.debug(f"并行处理异常 {record.autoscene_id}: {str(e)}")
                    failed_count += 1
        
        return trajectories, failed_count
    
    def _process_chunk_sequential(self, 
                                 chunk_records: List[QualityCheckRecord], 
                                 scene_mappings: Dict[str, Dict]) -> Tuple[List[SegmentedTrajectory], int]:
        """顺序处理记录块"""
        trajectories = []
        failed_count = 0
        
        for i, record in enumerate(chunk_records, 1):
            try:
                # 进度报告
                if i % self.config.progress_report_interval == 0:
                    logger.info(f"📊 块内进度: {i}/{len(chunk_records)} ({i/len(chunk_records)*100:.1f}%)")
                
                trajectory = self._process_single_record(record, scene_mappings)
                if trajectory:
                    trajectories.append(trajectory)
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.debug(f"处理记录失败 {record.autoscene_id}: {str(e)}")
                failed_count += 1
                continue
        
        return trajectories, failed_count
    
    def _process_single_record(self, 
                             record: QualityCheckRecord, 
                             scene_mappings: Dict[str, Dict]) -> Optional[SegmentedTrajectory]:
        """处理单条质检记录
        
        Args:
            record: 质检记录
            scene_mappings: 场景映射字典
            
        Returns:
            分段轨迹对象或None
        """
        logger.debug(f"🔧 开始处理记录: {record.autoscene_id}")
        logger.debug(f"   task_name: {record.task_name}")
        logger.debug(f"   annotator: {record.annotator}")
        logger.debug(f"   result: {record.result}")
        logger.debug(f"   other_scenario: {record.other_scenario}")
        logger.debug(f"   description: {record.description}")
        
        # 获取场景映射信息
        scene_info = scene_mappings.get(record.autoscene_id)
        if not scene_info:
            logger.warning(f"❌ 未找到场景映射: {record.autoscene_id}")
            logger.debug(f"   可用的场景ID: {list(scene_mappings.keys())[:5]}...")
            return None
        
        dataset_name = scene_info.get('dataset_name')
        if not dataset_name:
            logger.warning(f"❌ 场景映射缺少dataset_name: {record.autoscene_id}")
            logger.debug(f"   场景信息: {scene_info}")
            return None
        
        logger.debug(f"✅ 获得场景映射: {record.autoscene_id} -> {dataset_name}")
        
        # 查询完整轨迹
        logger.debug(f"🔍 查询轨迹数据: {dataset_name}")
        trajectory_df = self.trajectory_segmenter.query_complete_trajectory(dataset_name)
        if trajectory_df.empty:
            logger.warning(f"❌ 未查询到轨迹数据: {dataset_name}")
            return None
        
        logger.debug(f"✅ 查询到轨迹数据: {len(trajectory_df)} 个点")
        
        # 合并结果字段
        merged_results = self.result_processor.merge_and_clean_results(
            record.result, record.other_scenario
        )
        logger.debug(f"📋 合并结果字段: {merged_results}")
        
        # 计算基础统计
        start_time = int(trajectory_df['timestamp'].min())
        end_time = int(trajectory_df['timestamp'].max())
        total_duration = float(end_time - start_time)
        total_points = len(trajectory_df)
        
        logger.debug(f"📊 轨迹基础统计: 点数={total_points}, 时长={total_duration}s, 时间范围=[{start_time}, {end_time}]")
        
        # 处理轨迹分段
        if record.description and len(record.description) > 0:
            # 有时间区间描述，进行分段
            logger.debug(f"🔄 开始时间分段处理: {len(record.description)} 个时间区间")
            logger.debug(f"   时间区间详情: {record.description}")
            logger.debug(f"   轨迹时长: {total_duration}s")
            
            geometry, segment_count = self.trajectory_segmenter.segment_trajectory_by_time_ranges(
                trajectory_df, record.description
            )
            
            if geometry.is_empty:
                logger.warning(f"❌ 分段轨迹生成失败: {record.autoscene_id}")
                logger.warning(f"   时间区间: {record.description}")
                logger.warning(f"   轨迹时长: {total_duration}s")
                # 如果分段失败，尝试创建完整轨迹作为备选
                logger.info(f"🔄 分段失败，尝试创建完整轨迹作为备选...")
                geometry, _ = self.trajectory_segmenter.create_complete_trajectory(trajectory_df)
                segment_count = 0
                if geometry.is_empty:
                    logger.warning(f"❌ 备选完整轨迹也失败: {record.autoscene_id}")
                    return None
                else:
                    logger.info(f"✅ 备选完整轨迹创建成功")
            else:
                logger.debug(f"✅ 分段轨迹创建成功: {segment_count} 个分段")
        else:
            # 无描述，保留完整轨迹
            logger.debug(f"🔄 创建完整轨迹（无时间区间描述）")
            geometry, _ = self.trajectory_segmenter.create_complete_trajectory(trajectory_df)
            segment_count = 0
            
            if geometry.is_empty:
                logger.warning(f"❌ 完整轨迹生成失败: {record.autoscene_id}")
                logger.warning(f"   轨迹点数: {total_points}")
                logger.warning(f"   最小点数要求: {self.config.min_points_per_segment}")
                return None
                
            logger.debug(f"✅ 完整轨迹创建成功")
        
        return SegmentedTrajectory(
            task_name=record.task_name,
            annotator=record.annotator,
            scene_id=record.autoscene_id,
            dataset_name=dataset_name,
            segment_count=segment_count,
            merged_results=merged_results,
            geometry=geometry,
            total_duration=total_duration,
            start_time=start_time,
            end_time=end_time,
            total_points=total_points
        )
    
    def _save_trajectories_to_table(self, 
                                   trajectories: List[SegmentedTrajectory], 
                                   table_name: str) -> Tuple[int, Dict]:
        """保存轨迹数据到数据库表
        
        Args:
            trajectories: 轨迹数据列表
            table_name: 目标表名
            
        Returns:
            (保存成功的记录数, 保存统计)
        """
        start_time = time.time()
        
        save_stats = {
            'total_trajectories': len(trajectories),
            'saved_records': 0,
            'save_time': 0,
            'table_created': False,
            'batch_count': 0
        }
        
        if not trajectories:
            logger.warning("没有轨迹数据需要保存")
            return 0, save_stats
        
        try:
            # 创建表
            if not self._create_trajectory_table(table_name):
                logger.error("创建表失败")
                return 0, save_stats
            
            save_stats['table_created'] = True
            
            # 批量插入
            total_saved = 0
            for i in range(0, len(trajectories), self.config.batch_insert_size):
                batch = trajectories[i:i+self.config.batch_insert_size]
                batch_num = i // self.config.batch_insert_size + 1
                
                logger.info(f"保存第 {batch_num} 批: {len(batch)} 条轨迹")
                
                # 准备GeoDataFrame数据
                gdf_data = []
                geometries = []
                
                for j, traj in enumerate(batch):
                    # 转换PostgreSQL数组格式（确保UTF-8编码）
                    try:
                        # 确保每个结果都是正确编码的字符串
                        cleaned_results = []
                        for result in traj.merged_results:
                            if isinstance(result, str):
                                # 确保字符串是UTF-8编码
                                cleaned_result = result.encode('utf-8', errors='ignore').decode('utf-8')
                                # 转义双引号
                                cleaned_result = cleaned_result.replace('"', '""')
                                cleaned_results.append(cleaned_result)
                            else:
                                cleaned_results.append(str(result))
                        
                        merged_results_pg = '{' + ','.join(f'"{result}"' for result in cleaned_results) + '}'
                    except Exception as e:
                        logger.warning(f"PostgreSQL数组格式转换失败: {e}, 使用默认格式")
                        merged_results_pg = '{' + ','.join(f'"{str(result)}"' for result in traj.merged_results) + '}'
                    
                    row = {
                        'task_name': traj.task_name,
                        'annotator': traj.annotator,
                        'scene_id': traj.scene_id,
                        'dataset_name': traj.dataset_name,
                        'segment_count': traj.segment_count,
                        'merged_results': merged_results_pg,
                        'total_duration': traj.total_duration,
                        'start_time': traj.start_time,
                        'end_time': traj.end_time,
                        'total_points': traj.total_points
                    }
                    
                    # 调试信息：显示第一条记录的详细信息
                    if batch_num == 1 and j == 0:
                        logger.debug(f"💾 第一条记录保存信息:")
                        logger.debug(f"   task_name: '{traj.task_name}'")
                        logger.debug(f"   annotator: '{traj.annotator}'")
                        logger.debug(f"   scene_id: '{traj.scene_id}'")
                        logger.debug(f"   dataset_name: '{traj.dataset_name}'")
                        logger.debug(f"   segment_count: {traj.segment_count}")
                        logger.debug(f"   merged_results: {traj.merged_results}")
                        logger.debug(f"   merged_results_pg: '{merged_results_pg}'")
                        # 显示编码信息
                        for idx, result in enumerate(traj.merged_results):
                            try:
                                encoded_check = result.encode('utf-8').decode('utf-8')
                                logger.debug(f"   结果{idx+1} 编码检查: '{result}' -> UTF-8正常")
                            except:
                                logger.debug(f"   结果{idx+1} 编码检查: '{result}' -> 可能有编码问题")
                        logger.debug(f"   total_duration: {traj.total_duration}")
                        logger.debug(f"   几何类型: {traj.geometry.geom_type}")
                        logger.debug(f"   几何是否为空: {traj.geometry.is_empty}")
                    
                    gdf_data.append(row)
                    geometries.append(traj.geometry)
                
                # 创建GeoDataFrame
                gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
                
                # 批量插入到数据库
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False
                )
                
                total_saved += len(gdf)
                save_stats['batch_count'] += 1
                
                logger.debug(f"批次 {batch_num} 保存完成: {len(gdf)} 条记录")
            
            save_stats['saved_records'] = total_saved
            save_stats['save_time'] = time.time() - start_time
            
            logger.info(f"✅ 数据库保存完成: {save_stats['saved_records']} 条轨迹记录, "
                       f"{save_stats['batch_count']} 个批次, "
                       f"表: {table_name}, "
                       f"用时: {save_stats['save_time']:.2f}s")
            
            return total_saved, save_stats
            
        except Exception as e:
            logger.error(f"保存轨迹数据失败: {str(e)}")
            return 0, save_stats
    
    def _create_trajectory_table(self, table_name: str) -> bool:
        """创建质检轨迹结果表"""
        try:
            # 检查表是否已存在
            check_table_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(check_table_sql)
                table_exists = result.scalar()
                
                if table_exists:
                    logger.info(f"表 {table_name} 已存在，跳过创建")
                    return True
            
            logger.info(f"创建质检轨迹表: {table_name}")
            
            # 创建表结构
            create_sql = text(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    task_name text NOT NULL,
                    annotator text NOT NULL,
                    scene_id text NOT NULL,
                    dataset_name text NOT NULL,
                    segment_count integer DEFAULT 0,
                    merged_results text[],
                    total_duration numeric(10,2),
                    start_time bigint,
                    end_time bigint,
                    total_points integer,
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 添加几何列
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'MULTILINESTRING', 2);
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_scene_id ON {table_name}(scene_id);
                CREATE INDEX idx_{table_name}_task_name ON {table_name}(task_name);
                CREATE INDEX idx_{table_name}_annotator ON {table_name}(annotator);
                CREATE INDEX idx_{table_name}_dataset_name ON {table_name}(dataset_name);
                CREATE INDEX idx_{table_name}_merged_results ON {table_name} USING GIN(merged_results);
                CREATE INDEX idx_{table_name}_created_at ON {table_name}(created_at);
            """)
            
            # 分步执行SQL
            with self.engine.connect() as conn:
                conn.execute(create_sql)
                conn.commit()
            logger.debug("✅ 表结构创建完成")
            
            with self.engine.connect() as conn:
                conn.execute(add_geom_sql)
                conn.commit()
            logger.debug("✅ 几何列添加完成")
            
            with self.engine.connect() as conn:
                conn.execute(index_sql)
                conn.commit()
            logger.debug("✅ 索引创建完成")
            
            logger.info(f"✅ 质检轨迹表创建成功: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建质检轨迹表失败: {table_name}, 错误: {str(e)}")
            return False
    
    def _export_trajectories_to_geojson(self, 
                                       trajectories: List[SegmentedTrajectory], 
                                       output_file: str) -> bool:
        """导出轨迹数据到GeoJSON文件
        
        Args:
            trajectories: 轨迹数据列表
            output_file: 输出文件路径
            
        Returns:
            导出是否成功
        """
        if not trajectories:
            logger.warning("没有轨迹数据需要导出")
            return False
        
        try:
            # 准备GeoDataFrame数据
            gdf_data = []
            geometries = []
            
            for traj in trajectories:
                # 确保字符串字段编码正确
                try:
                    merged_results_str = ','.join(
                        result.encode('utf-8', errors='ignore').decode('utf-8') 
                        if isinstance(result, str) else str(result)
                        for result in traj.merged_results
                    )
                except:
                    merged_results_str = ','.join(str(result) for result in traj.merged_results)
                
                row = {
                    'task_name': traj.task_name,
                    'annotator': traj.annotator,
                    'scene_id': traj.scene_id,
                    'dataset_name': traj.dataset_name,
                    'segment_count': traj.segment_count,
                    'merged_results': merged_results_str,
                    'total_duration': traj.total_duration,
                    'start_time': traj.start_time,
                    'end_time': traj.end_time,
                    'total_points': traj.total_points
                }
                gdf_data.append(row)
                geometries.append(traj.geometry)
            
            # 创建GeoDataFrame
            gdf = gpd.GeoDataFrame(gdf_data, geometry=geometries, crs=4326)
            
            # 导出到GeoJSON
            gdf.to_file(output_file, driver='GeoJSON', encoding='utf-8')
            
            logger.info(f"成功导出 {len(gdf)} 条轨迹到文件: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"导出轨迹数据失败: {str(e)}")
            return False

# 便捷函数
def process_quality_check_excel(
    excel_files: Union[str, List[str]],
    output_table: Optional[str] = None,
    output_geojson: Optional[str] = None,
    config: Optional[QualityCheckConfig] = None
) -> Dict:
    """处理质检Excel文件完整流程（支持多文件和万级数据）
    
    Args:
        excel_files: Excel文件路径或文件路径列表
        output_table: 输出数据库表名（可选）
        output_geojson: 输出GeoJSON文件路径（可选）
        config: 自定义配置（可选）
        
    Returns:
        详细的处理统计信息
    """
    query_config = config or QualityCheckConfig()
    processor = QualityCheckTrajectoryQuery(query_config)
    
    return processor.process_excel_files(
        excel_files=excel_files,
        output_table=output_table,
        output_geojson=output_geojson
    )

def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='质检轨迹查询模块 - 基于Excel质检结果查询和分段轨迹数据（万级数据优化版）',
        epilog="""
核心功能:
  • 多Excel文件批量处理：支持同时处理多个Excel文件
  • 智能数据过滤：自动过滤无result和other_scenario的无效记录
  • 万级数据并行处理：支持万级数据量的高效并行处理
  • 智能字段合并：自动合并result和other_scenario字段，去重处理
  • 时间分段轨迹：根据description时间区间进行轨迹智能分段
  • 统一几何格式：统一输出MultiLineString格式，支持完整轨迹和分段轨迹
  • 高性能优化：分块处理、内存优化、场景映射缓存

示例:
  # 基础用法：处理单个Excel文件
  python -m spdatalab.dataset.quality_check_trajectory_query --input quality_check.xlsx --table qc_trajectories
  
  # 多文件批量处理
  python -m spdatalab.dataset.quality_check_trajectory_query --input file1.xlsx file2.xlsx file3.xlsx --table qc_trajectories
  
  # 万级数据高性能处理
  python -m spdatalab.dataset.quality_check_trajectory_query --input large_data.xlsx \\
    --table qc_trajectories --max-workers 8 --chunk-size 2000 --batch-size 3000
  
  # 自定义配置和禁用并行处理
  python -m spdatalab.dataset.quality_check_trajectory_query --input quality_check.xlsx \\
    --table qc_trajectories --disable-parallel --large-data-threshold 10000 --verbose
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 基本参数
    parser.add_argument('--input', required=True, nargs='+', help='输入Excel文件路径（支持多个文件）')
    parser.add_argument('--table', help='输出数据库表名（可选）')
    parser.add_argument('--output', help='输出GeoJSON文件路径（可选）')
    
    # 处理配置参数
    parser.add_argument('--batch-size', type=int, default=2000,
                       help='批量插入数据库的批次大小 (默认: 2000)')
    parser.add_argument('--min-points', type=int, default=2,
                       help='构建轨迹分段的最小点数 (默认: 2)')
    parser.add_argument('--time-tolerance', type=float, default=0.1,
                       help='时间匹配容差（秒）(默认: 0.1)')
    parser.add_argument('--simplify', action='store_true',
                       help='启用几何简化')
    parser.add_argument('--simplify-tolerance', type=float, default=0.00001,
                       help='几何简化容差 (默认: 0.00001)')
    
    # 并行处理和大数据配置
    parser.add_argument('--max-workers', type=int, default=4,
                       help='最大并行工作线程数 (默认: 4)')
    parser.add_argument('--disable-parallel', action='store_true',
                       help='禁用并行处理')
    parser.add_argument('--large-data-threshold', type=int, default=5000,
                       help='大数据处理阈值 (默认: 5000)')
    parser.add_argument('--chunk-size', type=int, default=1000,
                       help='分块处理大小 (默认: 1000)')
    parser.add_argument('--disable-filter', action='store_true',
                       help='禁用无效数据过滤')
    
    # 其他参数
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 验证参数
    if not args.table and not args.output:
        parser.error("必须指定 --table 或 --output 中的至少一个")
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 检查输入文件
        input_files = args.input
        for file_path in input_files:
            if not Path(file_path).exists():
                logger.error(f"输入文件不存在: {file_path}")
                return 1
        
        # 构建配置
        config = QualityCheckConfig(
            batch_insert_size=args.batch_size,
            min_points_per_segment=args.min_points,
            time_tolerance=args.time_tolerance,
            simplify_geometry=args.simplify,
            simplify_tolerance=args.simplify_tolerance,
            max_workers=args.max_workers,
            enable_parallel_processing=not args.disable_parallel,
            large_data_threshold=args.large_data_threshold,
            chunk_processing_size=args.chunk_size,
            filter_invalid_records=not args.disable_filter
        )
        
        # 输出配置信息
        logger.info("🔧 配置参数:")
        logger.info(f"   • 输入文件数量: {len(input_files)}")
        logger.info(f"   • 批量插入大小: {config.batch_insert_size}")
        logger.info(f"   • 最小分段点数: {config.min_points_per_segment}")
        logger.info(f"   • 时间匹配容差: {config.time_tolerance}s")
        logger.info(f"   • 几何简化: {'启用' if config.simplify_geometry else '禁用'}")
        logger.info(f"   • 并行处理: {'启用' if config.enable_parallel_processing else '禁用'}")
        logger.info(f"   • 最大工作线程: {config.max_workers}")
        logger.info(f"   • 大数据阈值: {config.large_data_threshold}")
        logger.info(f"   • 分块大小: {config.chunk_processing_size}")
        logger.info(f"   • 数据过滤: {'启用' if config.filter_invalid_records else '禁用'}")
        
        # 执行处理
        stats = process_quality_check_excel(
            excel_files=input_files,
            output_table=args.table,
            output_geojson=args.output,
            config=config
        )
        
        # 检查处理结果
        if 'error' in stats:
            logger.error(f"❌ 处理错误: {stats['error']}")
            return 1
        
        if not stats.get('success', False):
            logger.error("❌ 处理未成功完成")
            return 1
        
        # 成功完成
        logger.info("🎉 所有处理成功完成！")
        
        # 确定返回代码
        has_results = stats.get('valid_trajectories', 0) > 0
        return 0 if has_results else 1
        
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 