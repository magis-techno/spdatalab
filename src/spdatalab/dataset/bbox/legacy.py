from __future__ import annotations
import argparse
import json
import signal
import sys
import re
from pathlib import Path
from datetime import datetime
import geopandas as gpd, pandas as pd
from sqlalchemy import text, create_engine
from spdatalab.common.io_hive import hive_cursor
from typing import List, Dict
import multiprocessing as mp
from multiprocessing import Pool, Manager
from concurrent.futures import ProcessPoolExecutor, as_completed
import threading
import time
import os

# 检查是否有parquet支持
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
POINT_TABLE = "public.ddi_data_points"

# 全局变量用于优雅退出
interrupted = False

def signal_handler(signum, frame):
    """信号处理函数，用于优雅退出"""
    global interrupted
    print(f"\n接收到中断信号 ({signum})，正在优雅退出...")
    print("等待当前批次处理完成，请稍候...")
    interrupted = True

def setup_signal_handlers():
    """设置信号处理器"""
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号

class LightweightProgressTracker:
    """轻量级进度跟踪器，使用Parquet文件存储状态，针对大规模数据优化"""
    
    def __init__(self, work_dir="./bbox_import_logs"):
        self.work_dir = Path(work_dir).resolve()  # 使用绝对路径
        try:
            self.work_dir.mkdir(exist_ok=True, parents=True)
        except PermissionError:
            # 如果当前目录权限不足，尝试使用临时目录
            import tempfile
            self.work_dir = Path(tempfile.gettempdir()) / "bbox_import_logs"
            self.work_dir.mkdir(exist_ok=True, parents=True)
            print(f"权限不足，使用临时目录: {self.work_dir}")
        
        # 状态文件路径
        self.success_file = self.work_dir / "successful_tokens.parquet"
        self.failed_file = self.work_dir / "failed_tokens.parquet"
        self.progress_file = self.work_dir / "progress.json"
        
        # 内存缓存（用于批量操作）
        self._success_cache = self._load_success_cache()
        self._failed_buffer = []  # 失败记录缓冲区
        self._success_buffer = []  # 成功记录缓冲区
        self._buffer_size = 1000  # 缓冲区大小
        
    def _load_success_cache(self):
        """加载成功token的缓存"""
        if self.success_file.exists() and PARQUET_AVAILABLE:
            try:
                df = pd.read_parquet(self.success_file)
                cache = set(df['scene_token'].tolist())
                print(f"已加载 {len(cache)} 个成功处理的scene_token")
                return cache
            except Exception as e:
                print(f"加载成功记录失败: {e}，将创建新文件")
                return set()
        else:
            return set()
    
    def save_successful_batch(self, scene_tokens, batch_num=None):
        """批量保存成功处理的token（使用缓冲区优化）"""
        if not scene_tokens:
            return
            
        # 添加到缓冲区
        timestamp = datetime.now()
        for token in scene_tokens:
            if token not in self._success_cache:
                self._success_buffer.append({
                    'scene_token': token,
                    'processed_at': timestamp,
                    'batch_num': batch_num
                })
                self._success_cache.add(token)
        
        # 如果缓冲区达到阈值，则写入文件
        if len(self._success_buffer) >= self._buffer_size:
            self._flush_success_buffer()
    
    def _flush_success_buffer(self):
        """将成功记录缓冲区写入文件"""
        if not self._success_buffer or not PARQUET_AVAILABLE:
            return
            
        new_df = pd.DataFrame(self._success_buffer)
        
        try:
            if self.success_file.exists():
                # 追加到现有文件
                existing_df = pd.read_parquet(self.success_file)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                # 去重（以防万一）
                combined_df = combined_df.drop_duplicates(subset=['scene_token'], keep='last')
            else:
                combined_df = new_df
            
            # 写入文件
            combined_df.to_parquet(self.success_file, index=False)
            print(f"已保存 {len(self._success_buffer)} 个成功记录到文件")
            
        except Exception as e:
            print(f"保存成功记录失败: {e}")
        
        # 清空缓冲区
        self._success_buffer = []
    
    def save_failed_record(self, scene_token, error_msg, batch_num=None, step="unknown"):
        """保存失败记录（使用缓冲区优化）"""
        self._failed_buffer.append({
            'scene_token': scene_token,
            'error_msg': str(error_msg),
            'batch_num': batch_num,
            'step': step,
            'failed_at': datetime.now()
        })
        
        # 如果缓冲区达到阈值，则写入文件
        if len(self._failed_buffer) >= self._buffer_size:
            self._flush_failed_buffer()
    
    def _flush_failed_buffer(self):
        """将失败记录缓冲区写入文件"""
        if not self._failed_buffer or not PARQUET_AVAILABLE:
            return
            
        new_df = pd.DataFrame(self._failed_buffer)
        
        try:
            if self.failed_file.exists():
                # 追加到现有文件
                existing_df = pd.read_parquet(self.failed_file)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # 写入文件
            combined_df.to_parquet(self.failed_file, index=False)
            print(f"已保存 {len(self._failed_buffer)} 个失败记录到文件")
            
        except Exception as e:
            print(f"保存失败记录失败: {e}")
        
        # 清空缓冲区
        self._failed_buffer = []
    
    def get_remaining_tokens(self, all_tokens):
        """获取还需要处理的token"""
        remaining = [token for token in all_tokens if token not in self._success_cache]
        print(f"总计 {len(all_tokens)} 个场景，已成功处理 {len(self._success_cache)} 个，剩余 {len(remaining)} 个")
        return remaining
    
    def check_tokens_exist(self, tokens):
        """批量检查tokens是否已存在"""
        return set(tokens) & self._success_cache
    
    def load_failed_tokens(self):
        """加载失败的tokens，用于重试"""
        if not self.failed_file.exists() or not PARQUET_AVAILABLE:
            return []
        
        try:
            failed_df = pd.read_parquet(self.failed_file)
            # 排除已成功处理的tokens
            failed_tokens = failed_df[~failed_df['scene_token'].isin(self._success_cache)]['scene_token'].unique().tolist()
            print(f"加载了 {len(failed_tokens)} 个失败的scene_token")
            return failed_tokens
        except Exception as e:
            print(f"加载失败记录失败: {e}")
            return []
    
    def save_progress(self, total_scenes, processed_scenes, inserted_records, current_batch):
        """保存总体进度"""
        progress = {
            "total_scenes": total_scenes,
            "processed_scenes": processed_scenes,
            "inserted_records": inserted_records,
            "current_batch": current_batch,
            "timestamp": datetime.now().isoformat(),
            "successful_count": len(self._success_cache),
            "failed_count": len(self._failed_buffer) + (
                len(pd.read_parquet(self.failed_file)) if self.failed_file.exists() and PARQUET_AVAILABLE else 0
            )
        }
        
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存进度失败: {e}")
    
    def get_statistics(self):
        """获取统计信息"""
        success_count = len(self._success_cache)
        
        failed_count = 0
        failed_by_step = {}
        
        if self.failed_file.exists() and PARQUET_AVAILABLE:
            try:
                failed_df = pd.read_parquet(self.failed_file)
                # 排除已成功处理的
                active_failed = failed_df[~failed_df['scene_token'].isin(self._success_cache)]
                failed_count = len(active_failed['scene_token'].unique())
                
                # 按步骤统计
                if not active_failed.empty:
                    failed_by_step = active_failed['step'].value_counts().to_dict()
                    
            except Exception as e:
                print(f"统计失败记录时出错: {e}")
        
        return {
            'success_count': success_count,
            'failed_count': failed_count,
            'failed_by_step': failed_by_step
        }
    
    def finalize(self):
        """完成处理，刷新所有缓冲区"""
        self._flush_success_buffer()
        self._flush_failed_buffer()

def create_table_if_not_exists(eng, table_name='clips_bbox'):
    """如果表不存在则创建表 - 与cleanup_clips_bbox.sql保持一致"""
    # 检查表是否已存在
    check_table_sql = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(check_table_sql)
            table_exists = result.scalar()
            
            if table_exists:
                print(f"表 {table_name} 已存在，跳过创建")
                return True
                
            print(f"表 {table_name} 不存在，开始创建...")
            
            # 与cleanup_clips_bbox.sql保持一致的表结构
            create_sql = text(f"""
                CREATE TABLE {table_name}(
                    id serial PRIMARY KEY,
                    scene_token text,
                    data_name text UNIQUE,
                    event_id text,
                    city_id text,
                    "timestamp" bigint,
                    all_good boolean
                );
            """)
            
            # 使用PostGIS添加几何列
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POLYGON', 2);
            """)
            
            # 添加几何约束
            constraint_sql = text(f"""
                ALTER TABLE {table_name} ADD CONSTRAINT check_geom_type 
                    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_scene_token ON {table_name}(scene_token);
            """)
            
            # 执行SQL语句，需要分步提交以确保PostGIS函数能找到表
            conn.execute(create_sql)
            conn.commit()  # 先提交表创建
            
            # 执行PostGIS相关操作
            conn.execute(add_geom_sql)
            conn.execute(constraint_sql)
            conn.commit()  # 提交几何列和约束
            
            # 创建索引
            conn.execute(index_sql)
            conn.commit()  # 最后提交索引
            
            print(f"成功创建表 {table_name} 及相关索引")
            return True
            
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        print("建议：如果表已通过cleanup_clips_bbox.sql创建，请使用 --no-create-table 选项")
        return False

def chunk(lst, n):
    """将列表分块处理"""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def load_scene_ids_from_json(file_path):
    """从JSON格式的数据集文件中加载scene_id列表"""
    with open(file_path, 'r', encoding='utf-8') as f:
        dataset_data = json.load(f)
    
    scene_ids = []
    subdatasets = dataset_data.get('subdatasets', [])
    
    for subdataset in subdatasets:
        scene_ids.extend(subdataset.get('scene_ids', []))
    
    print(f"从JSON文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids_from_parquet(file_path):
    """从Parquet格式的数据集文件中加载scene_id列表"""
    if not PARQUET_AVAILABLE:
        raise ImportError("需要安装 pandas 和 pyarrow 才能使用 parquet 格式: pip install pandas pyarrow")
    
    df = pd.read_parquet(file_path)
    scene_ids = df['scene_id'].unique().tolist()
    
    print(f"从Parquet文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids_from_text(file_path):
    """从文本文件中加载scene_id列表（兼容原格式）"""
    scene_ids = [t.strip() for t in Path(file_path).read_text().splitlines() if t.strip()]
    print(f"从文本文件加载了 {len(scene_ids)} 个scene_id")
    return scene_ids

def load_scene_ids(file_path):
    """智能加载scene_id列表，自动检测文件格式"""
    file_path = Path(file_path)
    
    if file_path.suffix.lower() == '.json':
        return load_scene_ids_from_json(file_path)
    elif file_path.suffix.lower() == '.parquet':
        return load_scene_ids_from_parquet(file_path)
    else:
        # 默认按文本格式处理
        return load_scene_ids_from_text(file_path)

def fetch_meta(tokens):
    """批量获取场景元数据"""
    sql = ("SELECT id AS scene_token,origin_name AS data_name,event_id,city_id,timestamp "
           "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s")
    with hive_cursor() as cur:
        cur.execute(sql, {"tok": tuple(tokens)})
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)

def fetch_bbox_with_geometry(names, eng):
    """批量获取边界框信息并直接在PostGIS中构建几何对象"""
    sql_query = text(f"""
        WITH bbox_data AS (
            SELECT 
                dataset_name,
                ST_XMin(ST_Extent(point_lla)) AS xmin,
                ST_YMin(ST_Extent(point_lla)) AS ymin,
                ST_XMax(ST_Extent(point_lla)) AS xmax,
                ST_YMax(ST_Extent(point_lla)) AS ymax,
                bool_and(workstage = 2) AS all_good
            FROM {POINT_TABLE}
            WHERE dataset_name = ANY(:names_param)
            GROUP BY dataset_name
        )
        SELECT 
            dataset_name,
            all_good,
            CASE 
                WHEN xmin = xmax OR ymin = ymax THEN
                    -- 对于点数据，直接创建点几何对象
                    ST_Point((xmin + xmax) / 2, (ymin + ymax) / 2)
                ELSE
                    -- 对于已有边界框的数据，直接使用边界框
                    ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
            END AS geometry
        FROM bbox_data;""")
    
    return gpd.read_postgis(
        sql_query, 
        eng, 
        params={"names_param": names},
        geom_col='geometry'
    )

def batch_insert_to_postgis(gdf, eng, table_name='clips_bbox', batch_size=1000, tracker=None, batch_num=None):
    """批量插入到PostGIS，依赖数据库约束处理重复数据"""
    total_rows = len(gdf)
    inserted_rows = 0
    successful_tokens = []
    
    # 分批插入
    for i in range(0, total_rows, batch_size):
        batch_gdf = gdf.iloc[i:i+batch_size]
        batch_tokens = batch_gdf['scene_token'].tolist()
        
        try:
            # 直接插入，让数据库处理重复
            batch_gdf.to_postgis(
                table_name, 
                eng, 
                if_exists='append', 
                index=False
            )
            inserted_rows += len(batch_gdf)
            successful_tokens.extend(batch_tokens)
            print(f'[批量插入] 已插入: {inserted_rows}/{total_rows} 行')
            
        except Exception as e:
            error_str = str(e).lower()
            
            # 如果是重复键违反约束，尝试逐行插入以识别具体的重复记录
            if 'unique' in error_str or 'duplicate' in error_str or 'constraint' in error_str:
                print(f'[批量插入] 批次 {i//batch_size + 1} 遇到重复数据，进行逐行插入')
                
                for idx, row in batch_gdf.iterrows():
                    scene_token = row['scene_token']
                    try:
                        # 创建单行GeoDataFrame
                        single_gdf = gpd.GeoDataFrame(
                            [row.drop('geometry')], 
                            geometry=[row.geometry], 
                            crs=4326
                        )
                        single_gdf.to_postgis(table_name, eng, if_exists='append', index=False)
                        inserted_rows += 1
                        successful_tokens.append(scene_token)
                    except Exception as row_e:
                        row_error_str = str(row_e).lower()
                        if 'unique' in row_error_str or 'duplicate' in row_error_str:
                            # 重复数据，不记录为失败，只是跳过
                            print(f'[跳过重复] scene_token: {scene_token}')
                            successful_tokens.append(scene_token)  # 视为成功（已存在）
                        else:
                            # 其他类型的错误才记录为失败
                            error_msg = f'插入失败: {str(row_e)}'
                            print(f'[插入错误] scene_token: {scene_token}: {error_msg}')
                            if tracker:
                                tracker.save_failed_record(scene_token, error_msg, batch_num, "database_insert")
            else:
                # 非重复数据问题，记录为失败
                print(f'[批量插入错误] 批次 {i//batch_size + 1}: {str(e)}')
                for token in batch_tokens:
                    if tracker:
                        tracker.save_failed_record(token, f"批量插入异常: {str(e)}", batch_num, "database_insert")
    
    # 批量保存成功的tokens（包括重复跳过的）
    if tracker and successful_tokens:
        tracker.save_successful_batch(successful_tokens, batch_num)
    
    return inserted_rows

def normalize_subdataset_name(subdataset_name: str) -> str:
    """规范化子数据集名称
    
    规则：
    1. 去掉开头的 "GOD_E2E_"
    2. 如果有 "_sub_ddi_" 则截断到这里（不包含_sub_ddi_）
    3. 如果中间出现类似 "_277736e2e_"（277打头，e2e结尾）的字符串，这部分及之后都不要
    4. 移除结尾的时间戳格式 "_YYYY_MM_DD_HH_MM_SS"
    
    Args:
        subdataset_name: 原始子数据集名称
        
    Returns:
        规范化后的名称
    """
    original_name = subdataset_name
    
    # 1. 去掉开头的 GOD_E2E_
    if subdataset_name.startswith("GOD_E2E_"):
        subdataset_name = subdataset_name[8:]  # len("GOD_E2E_") = 8
    
    # 2. 处理 _sub_ddi_ 截断
    sub_ddi_pos = subdataset_name.find("_sub_ddi_")
    if sub_ddi_pos != -1:
        subdataset_name = subdataset_name[:sub_ddi_pos]
    
    # 3. 处理类似 _277736e2e_ 的模式截断（277打头，e2e结尾）
    # 匹配模式：_277开头，任意字符，e2e结尾的字符串
    hash_pattern = r'_277[^_]*e2e_'
    hash_match = re.search(hash_pattern, subdataset_name)
    if hash_match:
        # 截断到匹配位置之前
        subdataset_name = subdataset_name[:hash_match.start()]
    
    # 4. 移除结尾的时间戳格式 "_YYYY_MM_DD_HH_MM_SS"
    timestamp_pattern = r'_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}$'
    subdataset_name = re.sub(timestamp_pattern, '', subdataset_name)
    
    # 5. 清理和验证结果
    subdataset_name = subdataset_name.strip('_')
    
    # 确保名称不为空
    if not subdataset_name:
        subdataset_name = "unnamed_dataset"
    
    print(f"名称规范化: '{original_name}' -> '{subdataset_name}'")
    return subdataset_name

def get_table_name_for_subdataset(subdataset_name: str) -> str:
    """为子数据集生成合法的PostgreSQL表名"""
    # 先规范化名称
    normalized_name = normalize_subdataset_name(subdataset_name)
    
    # 清理特殊字符，确保表名合法
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', normalized_name)
    
    # 转换为小写（PostgreSQL表名最佳实践）
    clean_name = clean_name.lower()
    
    # 处理连续下划线问题
    original_underscores = len(re.findall(r'_{2,}', clean_name))
    clean_name = re.sub(r'_+', '_', clean_name)  # 多个下划线合并为一个
    clean_name = clean_name.strip('_')  # 去除首尾下划线
    
    if original_underscores > 0:
        print(f"警告: 发现 {original_underscores} 处连续下划线，已自动合并")
        
    # 确保没有空的表名部分（由连续下划线导致）
    name_parts = clean_name.split('_')
    # 过滤掉空字符串部分
    valid_parts = [part for part in name_parts if part.strip()]
    if len(valid_parts) != len(name_parts):
        print(f"警告: 清理了空的表名段，从 {len(name_parts)} 段减少到 {len(valid_parts)} 段")
        clean_name = '_'.join(valid_parts)
    
    # 使用保守的长度限制，为PostGIS兼容性预留空间
    # clips_bbox_ = 12字符，所以主体部分限制在 50-12=38 字符
    max_main_length = 38
    
    if len(clean_name) > max_main_length:
        # 直接截断（不需要处理时间戳，因为已经在normalize阶段移除了）
        clean_name = clean_name[:max_main_length]
    
    # 构建最终表名
    table_name = f"clips_bbox_{clean_name}"
    
    # 确保表名符合PostgreSQL规范（以字母开头）
    if table_name[0].isdigit():
        table_name = "t_" + table_name
        # 如果加前缀后超长，再次截断
        if len(table_name) > 50:  # 保守截断
            table_name = table_name[:50]
    
    # 最终安全检查
    if len(table_name) > 50:
        table_name = table_name[:50]
    
    # 最终清理：确保没有连续下划线和空段
    table_name = re.sub(r'_+', '_', table_name)  # 合并连续下划线
    table_name = table_name.strip('_')  # 去除首尾下划线
    
    # 清理空段
    name_parts = table_name.split('_')
    valid_parts = [part for part in name_parts if part.strip()]
    if len(valid_parts) != len(name_parts):
        print(f"警告: 最终清理了空的表名段，从 {len(name_parts)} 段减少到 {len(valid_parts)} 段")
        table_name = '_'.join(valid_parts)
    
    # 最终验证表名合法性
    validation_result = validate_table_name(table_name)
    if not validation_result['valid']:
        print(f"警告: 表名验证失败: {validation_result['issues']}")
    
    print(f"表名生成: '{subdataset_name}' -> '{table_name}' (长度: {len(table_name)})")
    return table_name

def validate_table_name(table_name: str) -> dict:
    """验证表名的合法性
    
    Args:
        table_name: 要验证的表名
        
    Returns:
        包含验证结果的字典
    """
    issues = []
    
    # 检查长度
    if len(table_name) > 63:
        issues.append(f"表名过长: {len(table_name)} > 63")
    
    # 检查字符
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
        issues.append("表名包含非法字符或不以字母开头")
    
    # 检查大写字母（PostgreSQL最佳实践是小写）
    if re.search(r'[A-Z]', table_name):
        issues.append("表名包含大写字母，建议使用小写")
    
    # 检查连续下划线
    if re.search(r'_{2,}', table_name):
        issues.append("表名包含连续下划线")
    
    # 检查首尾下划线
    if table_name.startswith('_') or table_name.endswith('_'):
        issues.append("表名以下划线开头或结尾")
    
    # 检查空的段
    parts = table_name.split('_')
    empty_parts = [i for i, part in enumerate(parts) if not part.strip()]
    if empty_parts:
        issues.append(f"表名包含空段: 位置 {empty_parts}")
    
    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'length': len(table_name)
    }

def create_table_for_subdataset(eng, subdataset_name, subdataset_metadata=None, base_table_name='clips_bbox'):
    """为特定子数据集创建分表，支持根据metadata动态添加字段"""
    table_name = get_table_name_for_subdataset(subdataset_name)
    
    # 检查表是否已存在
    check_table_sql = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(check_table_sql)
            table_exists = result.scalar()
            
            if table_exists:
                print(f"分表 {table_name} 已存在，跳过创建")
                return True, table_name
                
            print(f"创建子数据集分表: {table_name}")
            
            # 基础字段（保持向后兼容）
            base_fields = [
                "id serial PRIMARY KEY",
                "scene_token text",
                "data_name text UNIQUE", 
                "event_id text",
                "city_id text",
                '"timestamp" bigint',
                "all_good boolean"
            ]
            
            # 根据metadata动态添加字段
            dynamic_fields = []
            if subdataset_metadata:
                # 检测是否为问题单数据集
                data_type = subdataset_metadata.get('data_type')
                if data_type == 'defect':
                    # 为问题单数据集添加特殊字段
                    dynamic_fields.append("data_type text DEFAULT 'defect'")
                    dynamic_fields.append("original_url text")
                    
                    # 分析scene_attributes中的所有字段类型
                    scene_attributes = subdataset_metadata.get('scene_attributes', {})
                    all_custom_fields = set()
                    field_types = {}
                    
                    for scene_attrs in scene_attributes.values():
                        for key, value in scene_attrs.items():
                            if key not in {'original_url', 'data_name'}:
                                all_custom_fields.add(key)
                                # 推断字段类型（取最宽泛的类型）
                                inferred_type = infer_field_type(value)
                                if key not in field_types:
                                    field_types[key] = inferred_type
                                else:
                                    # 如果已有类型，选择更宽泛的类型
                                    current_type = field_types[key]
                                    new_type = merge_field_types(current_type, inferred_type)
                                    field_types[key] = new_type
                    
                    # 添加动态字段
                    for field_name in sorted(all_custom_fields):
                        field_type = field_types.get(field_name, 'text')
                        dynamic_fields.append(f"{field_name} {field_type}")
                        print(f"  添加动态字段: {field_name} ({field_type})")
                else:
                    # 标准数据集，添加data_type标识
                    dynamic_fields.append("data_type text DEFAULT 'standard'")
            else:
                # 向后兼容：没有metadata的情况
                dynamic_fields.append("data_type text DEFAULT 'standard'")
            
            # 组合所有字段
            all_fields = base_fields + dynamic_fields
            fields_sql = ",\n                ".join(all_fields)
            
            # 创建表的SQL
            create_sql = text(f"""
                CREATE TABLE {table_name}(
                {fields_sql}
                );
            """)
            
            # 使用PostGIS添加几何列
            add_geom_sql = text(f"""
                SELECT AddGeometryColumn('public', '{table_name}', 'geometry', 4326, 'POLYGON', 2);
            """)
            
            # 添加几何约束
            constraint_sql = text(f"""
                ALTER TABLE {table_name} ADD CONSTRAINT check_{table_name}_geom_type 
                    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_scene_token ON {table_name}(scene_token);
                CREATE INDEX idx_{table_name}_data_type ON {table_name}(data_type);
            """)
            
            # 执行SQL语句，需要分步提交以确保PostGIS函数能找到表
            conn.execute(create_sql)
            conn.commit()  # 先提交表创建
            
            # 执行PostGIS相关操作
            conn.execute(add_geom_sql)
            conn.execute(constraint_sql)
            conn.commit()  # 提交几何列和约束
            
            # 创建索引
            conn.execute(index_sql)
            conn.commit()  # 最后提交索引
            
            print(f"成功创建分表 {table_name} 及相关索引")
            if dynamic_fields:
                print(f"  包含 {len(dynamic_fields)} 个动态字段")
            
            return True, table_name
            
    except Exception as e:
        print(f"创建分表 {table_name} 时出错: {str(e)}")
        return False, table_name

def infer_field_type(value):
    """推断字段类型"""
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "numeric"
    elif isinstance(value, str):
        # 尝试检测特殊格式
        if value.lower() in ('true', 'false'):
            return "boolean"
        try:
            int(value)
            return "integer"
        except ValueError:
            try:
                float(value)
                return "numeric"
            except ValueError:
                return "text"
    else:
        return "text"

def merge_field_types(type1: str, type2: str) -> str:
    """合并两个字段类型，选择更宽泛的类型
    
    类型优先级（从窄到宽）：boolean < integer < numeric < text
    
    Args:
        type1: 第一个类型
        type2: 第二个类型
        
    Returns:
        合并后的类型
    """
    # 定义类型优先级
    type_priority = {
        'boolean': 1,
        'integer': 2, 
        'numeric': 3,
        'text': 4
    }
    
    # 获取优先级，未知类型默认为text
    priority1 = type_priority.get(type1, 4)
    priority2 = type_priority.get(type2, 4)
    
    # 返回优先级更高（更宽泛）的类型
    if priority1 >= priority2:
        return type1
    else:
        return type2

def convert_value_to_expected_type(field_name: str, value):
    """根据字段名和值，转换为合适的数据类型
    
    Args:
        field_name: 字段名称
        value: 原始值
        
    Returns:
        转换后的值
    """
    if value is None:
        return None
    
    # 特殊字段的类型处理可以在这里添加
    
    # 推断字段类型并转换
    inferred_type = infer_field_type(value)
    
    try:
        if inferred_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            else:
                return bool(value)
        elif inferred_type == "integer":
            if isinstance(value, str):
                return int(float(value))  # 处理"15.0"这种情况
            elif isinstance(value, float):
                return int(value)
            else:
                return int(value)
        elif inferred_type == "numeric":
            return float(value)
        else:
            return str(value)
    except (ValueError, TypeError) as e:
        print(f"警告: 字段 {field_name} 的值 {value} 类型转换失败: {e}，使用字符串类型")
        return str(value)

def group_scenes_by_subdataset(dataset_file: str) -> Dict[str, Dict]:
    """按子数据集分组scene_ids，包含metadata信息
    
    Args:
        dataset_file: dataset文件路径（JSON/Parquet格式）
        
    Returns:
        字典，key为子数据集名称，value为包含scene_ids和metadata的字典
        格式：{subdataset_name: {'scene_ids': [...], 'metadata': {...}}}
    """
    from ..dataset.dataset_manager import DatasetManager
    
    try:
        dataset_manager = DatasetManager()
        dataset = dataset_manager.load_dataset(dataset_file)
        
        groups = {}
        total_scenes = 0
        
        print(f"从数据集文件加载: {dataset_file}")
        print(f"数据集名称: {dataset.name}")
        print(f"子数据集数量: {len(dataset.subdatasets)}")
        
        for subdataset in dataset.subdatasets:
            subdataset_name = subdataset.name
            scene_ids = subdataset.scene_ids
            metadata = subdataset.metadata or {}
            
            if scene_ids:
                groups[subdataset_name] = {
                    'scene_ids': scene_ids,
                    'metadata': metadata
                }
                total_scenes += len(scene_ids)
                
                # 显示数据集类型
                data_type = metadata.get('data_type', 'standard')
                type_info = f" (类型: {data_type})"
                if data_type == 'defect':
                    dynamic_fields = [k for k in metadata.keys() 
                                    if k not in {'data_type', 'original_url', 'data_name', 'line_number'} 
                                    and not k.startswith('data_')]
                    if dynamic_fields:
                        type_info += f", 动态字段: {len(dynamic_fields)}个"
                
                print(f"  {subdataset_name}: {len(scene_ids)} 个场景{type_info}")
            else:
                print(f"  {subdataset_name}: 无场景数据，跳过")
        
        print(f"总计: {len(groups)} 个有效子数据集，{total_scenes} 个场景")
        return groups
        
    except Exception as e:
        print(f"分组scene_ids失败: {str(e)}")
        raise

def batch_create_tables_for_subdatasets(eng, subdataset_groups: Dict[str, Dict]) -> Dict[str, str]:
    """批量为子数据集创建分表，支持动态字段
    
    Args:
        eng: 数据库引擎
        subdataset_groups: 子数据集分组信息，包含scene_ids和metadata
                          格式：{subdataset_name: {'scene_ids': [...], 'metadata': {...}}}
        
    Returns:
        字典，key为原始子数据集名称，value为创建的表名
    """
    table_mapping = {}
    success_count = 0
    
    print(f"开始批量创建 {len(subdataset_groups)} 个分表...")
    
    for i, (subdataset_name, subdataset_info) in enumerate(subdataset_groups.items(), 1):
        metadata = subdataset_info.get('metadata', {})
        data_type = metadata.get('data_type', 'standard')
        
        print(f"[{i}/{len(subdataset_groups)}] 处理: {subdataset_name} (类型: {data_type})")
        
        success, table_name = create_table_for_subdataset(eng, subdataset_name, metadata)
        table_mapping[subdataset_name] = table_name
        
        if success:
            success_count += 1
        else:
            print(f"警告: 子数据集 {subdataset_name} 的分表创建失败")
    
    print(f"批量创建完成: 成功 {success_count}/{len(subdataset_groups)} 个分表")
    return table_mapping

def filter_partition_tables(tables: List[str], exclude_view: str = None, exclude_defect_tables: bool = True) -> List[str]:
    """过滤出真正的分表，排除主表、视图、临时表等
    
    Args:
        tables: 表名列表
        exclude_view: 要排除的视图名称
        exclude_defect_tables: 是否排除问题单数据表
        
    Returns:
        过滤后的分表列表
    """
    filtered = []
    
    for table in tables:
        # 排除主表
        if table == 'clips_bbox':
            continue
            
        # 排除指定的视图（避免循环引用）
        if exclude_view and table == exclude_view:
            continue
            
        # 排除包含特定关键词的表
        exclude_keywords = ['unified', 'temp', 'backup', 'test', 'tmp']
        if any(keyword in table.lower() for keyword in exclude_keywords):
            continue
            
        # 只包含分表格式的表（必须以clips_bbox_开头）
        if not (table.startswith('clips_bbox_') and table != 'clips_bbox'):
            continue
        
        # 检查是否为问题单数据表（简化实现，基于表名推断）
        if exclude_defect_tables:
            try:
                from sqlalchemy import create_engine, text
                eng = create_engine(LOCAL_DSN, future=True)
                with eng.connect() as conn:
                    # 检查表是否包含data_type字段且值为'defect'
                    check_defect_sql = text(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{table}' 
                            AND column_name = 'data_type'
                        );
                    """)
                    
                    has_data_type = conn.execute(check_defect_sql).scalar()
                    
                    if has_data_type:
                        # 检查是否包含问题单数据
                        check_defect_data_sql = text(f"""
                            SELECT EXISTS (
                                SELECT 1 FROM {table} 
                                WHERE data_type = 'defect' 
                                LIMIT 1
                            );
                        """)
                        
                        has_defect_data = conn.execute(check_defect_data_sql).scalar()
                        
                        if has_defect_data:
                            print(f"排除问题单数据表: {table}")
                            continue
                            
            except Exception as e:
                # 如果检查失败，记录但不影响过滤
                print(f"检查表 {table} 的数据类型时出错: {str(e)}")
        
        filtered.append(table)
    
    return filtered

def list_bbox_tables(eng) -> List[str]:
    """列出所有bbox相关的表
    
    Args:
        eng: 数据库引擎
        
    Returns:
        bbox表名列表
    """
    list_tables_sql = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'clips_bbox%'
        ORDER BY table_name;
    """)
    
    try:
        with eng.connect() as conn:
            result = conn.execute(list_tables_sql)
            tables = [row[0] for row in result.fetchall()]
            return tables
    except Exception as e:
        print(f"列出bbox表失败: {str(e)}")
        return []

def create_unified_view(eng, view_name: str = 'clips_bbox_unified') -> bool:
    """创建统一视图，聚合所有分表数据
    
    Args:
        eng: 数据库引擎
        view_name: 统一视图名称
        
    Returns:
        创建是否成功
    """
    try:
        # 获取所有bbox相关表
        all_tables = list_bbox_tables(eng)
        if not all_tables:
            print("没有找到任何bbox表，无法创建统一视图")
            return False
        
        # 过滤出真正的分表，排除视图、主表等
        bbox_tables = filter_partition_tables(all_tables, exclude_view=view_name)
        if not bbox_tables:
            print("没有找到任何分表，无法创建统一视图")
            print(f"可用的表: {all_tables}")
            return False
        
        # 构建UNION ALL查询
        union_parts = []
        for table_name in bbox_tables:
            # 提取子数据集名称（去掉clips_bbox_前缀）
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
        
        # 组合完整的视图查询
        view_query = "UNION ALL\n".join(union_parts)
        
        # 先删除现有视图（如果存在）
        drop_view_sql = text(f"DROP VIEW IF EXISTS {view_name};")
        
        # 创建新视图
        create_view_sql = text(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            {view_query};
        """)
        
        with eng.connect() as conn:
            conn.execute(drop_view_sql)
            conn.execute(create_view_sql)
            conn.commit()
        
        print(f"成功创建统一视图 {view_name}，包含 {len(bbox_tables)} 个分表:")
        for table in bbox_tables:
            print(f"  - {table}")
        
        return True
        
    except Exception as e:
        print(f"创建统一视图失败: {str(e)}")
        
        # 提供调试信息
        try:
            print(f"调试信息:")
            print(f"  - 找到的表数量: {len(bbox_tables) if 'bbox_tables' in locals() else 'N/A'}")
            if 'bbox_tables' in locals() and bbox_tables:
                print(f"  - 表列表: {', '.join(bbox_tables)}")
            
            # 显示生成的查询（前100个字符）
            if 'view_query' in locals():
                query_preview = view_query[:200] + "..." if len(view_query) > 200 else view_query
                print(f"  - 生成的查询预览: {query_preview}")
                
        except Exception as debug_e:
            print(f"  - 无法显示调试信息: {str(debug_e)}")
        
        return False

def create_qgis_compatible_unified_view(eng, view_name: str = 'clips_bbox_unified_qgis') -> bool:
    """
    创建QGIS兼容的统一视图，带全局唯一ID
    
    Args:
        eng: SQLAlchemy engine
        view_name: 视图名称
        
    Returns:
        bool: 创建是否成功
    """
    try:
        # 获取分表列表
        all_tables = list_bbox_tables(eng)
        bbox_tables = filter_partition_tables(all_tables, exclude_view=view_name)
        
        if not bbox_tables:
            print("没有找到任何分表，无法创建QGIS兼容的统一视图")
            return False
        
        print(f"正在为 {len(bbox_tables)} 个分表创建QGIS兼容的统一视图...")
        
        # 构建带ROW_NUMBER的UNION查询
        union_parts = []
        for table_name in bbox_tables:
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id as original_id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
        
        # 包装在ROW_NUMBER中创建全局唯一ID
        inner_query = "UNION ALL\n".join(union_parts)
        
        view_query = f"""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY source_table, original_id) as qgis_id,
            original_id,
            scene_token,
            data_name,
            event_id,
            city_id,
            timestamp,
            all_good,
            geometry,
            subdataset_name,
            source_table
        FROM (
            {inner_query}
        ) as unified_data
        """
        
        # 创建视图
        drop_view_sql = text(f"DROP VIEW IF EXISTS {view_name};")
        create_view_sql = text(f"CREATE OR REPLACE VIEW {view_name} AS {view_query};")
        
        with eng.connect() as conn:
            conn.execute(drop_view_sql)
            conn.execute(create_view_sql)
            conn.commit()
        
        print(f"✅ 成功创建QGIS兼容的统一视图 {view_name}")
        print(f"📋 在QGIS中加载时，请选择 'qgis_id' 作为主键列")
        print(f"🔍 视图包含以下分表: {', '.join(bbox_tables)}")
        
        return True
        
    except Exception as e:
        print(f"创建QGIS兼容统一视图失败: {str(e)}")
        return False

def create_materialized_unified_view(eng, view_name: str = 'clips_bbox_unified_mat') -> bool:
    """
    创建物化视图，提供更好的QGIS性能
    
    Args:
        eng: SQLAlchemy engine
        view_name: 物化视图名称
        
    Returns:
        bool: 创建是否成功
    """
    try:
        # 获取分表列表
        all_tables = list_bbox_tables(eng)
        bbox_tables = filter_partition_tables(all_tables, exclude_view=view_name)
        
        if not bbox_tables:
            print("没有找到任何分表，无法创建物化视图")
            return False
        
        print(f"正在为 {len(bbox_tables)} 个分表创建物化视图...")
        
        # 构建UNION查询
        union_parts = []
        for table_name in bbox_tables:
            subdataset_name = table_name.replace('clips_bbox_', '') if table_name.startswith('clips_bbox_') else table_name
            
            union_part = f"""
            SELECT 
                {table_name}.id as original_id,
                {table_name}.scene_token,
                {table_name}.data_name,
                {table_name}.event_id,
                {table_name}.city_id,
                {table_name}.timestamp,
                {table_name}.all_good,
                {table_name}.geometry,
                '{subdataset_name}' as subdataset_name,
                '{table_name}' as source_table
            FROM {table_name}
            """
            union_parts.append(union_part)
        
        inner_query = "UNION ALL\n".join(union_parts)
        
        # 创建物化视图SQL
        drop_view_sql = text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
        
        create_mat_view_sql = text(f"""
            CREATE MATERIALIZED VIEW {view_name} AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY source_table, original_id) as qgis_id,
                original_id,
                scene_token,
                data_name,
                event_id,
                city_id,
                timestamp,
                all_good,
                geometry,
                subdataset_name,
                source_table
            FROM (
                {inner_query}
            ) as unified_data;
        """)
        
        # 创建索引
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {view_name}_qgis_id_idx 
            ON {view_name} (qgis_id);
        """)
        
        create_spatial_index_sql = text(f"""
            CREATE INDEX IF NOT EXISTS {view_name}_geom_idx 
            ON {view_name} USING GIST (geometry);
        """)
        
        with eng.connect() as conn:
            conn.execute(drop_view_sql)
            conn.execute(create_mat_view_sql)
            conn.execute(create_index_sql)
            conn.execute(create_spatial_index_sql)
            conn.commit()
        
        print(f"✅ 成功创建物化视图 {view_name}")
        print(f"📋 在QGIS中使用 'qgis_id' 作为主键列")
        print(f"💡 提示：数据更新后需要刷新物化视图：REFRESH MATERIALIZED VIEW {view_name};")
        print(f"🔍 物化视图包含以下分表: {', '.join(bbox_tables)}")
        
        return True
        
    except Exception as e:
        print(f"创建物化视图失败: {str(e)}")
        return False

def refresh_materialized_view(eng, view_name: str = 'clips_bbox_unified_mat') -> bool:
    """
    刷新物化视图
    
    Args:
        eng: SQLAlchemy engine
        view_name: 物化视图名称
        
    Returns:
        bool: 刷新是否成功
    """
    try:
        refresh_sql = text(f"REFRESH MATERIALIZED VIEW {view_name};")
        
        with eng.connect() as conn:
            print(f"正在刷新物化视图 {view_name}...")
            conn.execute(refresh_sql)
            conn.commit()
        
        print(f"✅ 物化视图 {view_name} 刷新完成")
        return True
        
    except Exception as e:
        print(f"刷新物化视图失败: {str(e)}")
        return False

def maintain_unified_view(eng, view_name: str = 'clips_bbox_unified') -> bool:
    """维护统一视图，确保包含所有当前的分表
    
    Args:
        eng: 数据库引擎  
        view_name: 统一视图名称
        
    Returns:
        维护是否成功
    """
    try:
        # 检查视图是否存在
        check_view_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = '{view_name}'
            );
        """)
        
        with eng.connect() as conn:
            result = conn.execute(check_view_sql)
            view_exists = result.scalar()
        
        if not view_exists:
            print(f"视图 {view_name} 不存在，将创建新视图")
            return create_unified_view(eng, view_name)
        else:
            print(f"视图 {view_name} 已存在，重新创建以包含最新的分表")
            return create_unified_view(eng, view_name)
        
    except Exception as e:
        print(f"维护统一视图失败: {str(e)}")
        return False

def process_subdataset_parallel(args):
    """并行处理单个子数据集的包装函数
    
    Args:
        args: (subdataset_name, scene_ids, table_name, batch_size, insert_batch_size, work_dir, dsn, metadata)
        
    Returns:
        (subdataset_name, processed_count, inserted_count, success)
    """
    subdataset_name, scene_ids, table_name, batch_size, insert_batch_size, work_dir, dsn, metadata = args
    
    try:
        # 为每个进程创建独立的数据库连接
        from sqlalchemy import create_engine
        eng = create_engine(dsn, future=True)
        
        # 创建独立的进度跟踪器
        sub_work_dir = f"{work_dir}/{subdataset_name}"
        sub_tracker = LightweightProgressTracker(sub_work_dir)
        
        # 获取需要处理的场景ID
        remaining_scene_ids = sub_tracker.get_remaining_tokens(scene_ids)
        
        if not remaining_scene_ids:
            print(f"  🔄 [{subdataset_name}] 所有场景已处理完成，跳过")
            return subdataset_name, 0, 0, True
        
        print(f"  🚀 [{subdataset_name}] 开始处理 {len(remaining_scene_ids)} 个场景")
        
        # 处理当前子数据集的数据
        processed_count, inserted_count = process_subdataset_scenes(
            eng, remaining_scene_ids, table_name, batch_size, insert_batch_size, sub_tracker, metadata
        )
        
        print(f"  ✅ [{subdataset_name}] 完成: 处理 {processed_count} 个，插入 {inserted_count} 条记录")
        
        return subdataset_name, processed_count, inserted_count, True
        
    except Exception as e:
        print(f"  ❌ [{subdataset_name}] 处理失败: {str(e)}")
        return subdataset_name, 0, 0, False

def run_with_partitioning_parallel(input_path, batch=1000, insert_batch=1000, work_dir="./bbox_import_logs", 
                                 create_unified_view_flag=True, maintain_view_only=False, max_workers=None):
    """使用并行分表模式运行边界框处理
    
    Args:
        input_path: 输入数据集文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小  
        work_dir: 工作目录
        create_unified_view_flag: 是否创建统一视图
        maintain_view_only: 是否只维护视图（不处理数据）
        max_workers: 最大并行worker数量，None为自动检测CPU核心数
    """
    global interrupted
    
    # 设置信号处理器
    setup_signal_handlers()
    
    # 确定并行worker数量
    if max_workers is None:
        # 智能默认值：CPU核心数 * 1.5，但不超过16（可通过参数覆盖）
        cpu_count = mp.cpu_count()
        max_workers = min(int(cpu_count * 1.5), 16)
        print(f"🔍 检测到 {cpu_count} 个CPU核心，默认使用 {max_workers} 个workers")
    else:
        print(f"🎯 用户指定使用 {max_workers} 个workers")
    
    print(f"=== 并行分表模式处理开始 ===")
    print(f"输入文件: {input_path}")
    print(f"工作目录: {work_dir}")
    print(f"批次大小: {batch}")
    print(f"插入批次大小: {insert_batch}")
    print(f"并行worker数: {max_workers}")
    print(f"创建统一视图: {create_unified_view_flag}")
    print(f"仅维护视图: {maintain_view_only}")
    
    eng = create_engine(LOCAL_DSN, future=True)
    
    # 如果只是维护视图
    if maintain_view_only:
        print("\n=== 维护统一视图模式 ===")
        success = maintain_unified_view(eng)
        if success:
            print("✅ 统一视图维护完成")
        else:
            print("❌ 统一视图维护失败")
        return
    
    try:
        # 步骤1: 按子数据集分组场景
        print("\n=== 步骤1: 分组场景数据 ===")
        scene_groups = group_scenes_by_subdataset(input_path)
        
        if not scene_groups:
            print("没有找到有效的场景分组数据")
            return
        
        print(f"找到 {len(scene_groups)} 个子数据集")
        
        # 步骤2: 批量创建分表
        print("\n=== 步骤2: 创建分表 ===")
        table_mapping = batch_create_tables_for_subdatasets(eng, scene_groups)
        
        # 步骤3: 并行处理每个子数据集
        print(f"\n=== 步骤3: 并行分表数据处理 ({max_workers} workers) ===")
        
        # 准备并行任务参数
        task_args = []
        for subdataset_name, subdataset_info in scene_groups.items():
            scene_ids = subdataset_info['scene_ids']
            metadata = subdataset_info.get('metadata', {})
            table_name = table_mapping[subdataset_name]
            task_args.append((
                subdataset_name, scene_ids, table_name, 
                batch, insert_batch, work_dir, LOCAL_DSN, metadata
            ))
        
        # 执行并行处理
        total_processed = 0
        total_inserted = 0
        completed_count = 0
        failed_count = 0
        
        print(f"启动 {len(task_args)} 个并行任务...")
        
        start_time = time.time()
        
        # 使用ProcessPoolExecutor进行并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_subdataset = {
                executor.submit(process_subdataset_parallel, args): args[0] 
                for args in task_args
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_subdataset):
                if interrupted:
                    print("\n⚠️  检测到中断信号，正在停止剩余任务...")
                    executor.shutdown(wait=False)
                    break
                
                subdataset_name = future_to_subdataset[future]
                try:
                    result_name, processed, inserted, success = future.result()
                    completed_count += 1
                    
                    if success:
                        total_processed += processed
                        total_inserted += inserted
                        print(f"✅ [{completed_count}/{len(task_args)}] {subdataset_name}: {processed}处理/{inserted}插入")
                    else:
                        failed_count += 1
                        print(f"❌ [{completed_count}/{len(task_args)}] {subdataset_name}: 处理失败")
                        
                except Exception as e:
                    failed_count += 1
                    print(f"❌ [{completed_count}/{len(task_args)}] {subdataset_name}: 异常 - {str(e)}")
        
        processing_time = time.time() - start_time
        
        # 步骤4: 创建统一视图（如果需要）
        if create_unified_view_flag and not interrupted:
            print("\n=== 步骤4: 创建统一视图 ===")
            success = create_unified_view(eng)
            if success:
                print("✅ 统一视图创建完成")
            else:
                print("❌ 统一视图创建失败")
        
        # 输出最终统计
        print(f"\n=== 并行分表处理完成 ===")
        print(f"处理时间: {processing_time:.2f} 秒")
        print(f"总计处理: {total_processed} 条记录")
        print(f"总计插入: {total_inserted} 条记录")
        print(f"成功子数据集: {completed_count - failed_count}/{len(scene_groups)}")
        if failed_count > 0:
            print(f"失败子数据集: {failed_count}")
        
        if interrupted:
            print("⚠️  处理被中断，部分数据可能未完成")
        else:
            print("✅ 并行分表处理完成")
            
        # 计算性能提升估算
        if processing_time > 0:
            estimated_sequential_time = processing_time * max_workers
            speedup = estimated_sequential_time / processing_time
            print(f"🚀 预计性能提升: {speedup:.1f}x (相比顺序处理)")
            
    except KeyboardInterrupt:
        print(f"\n程序被用户中断")
        interrupted = True
    except Exception as e:
        print(f"\n并行分表处理遇到错误: {str(e)}")
    finally:
        print(f"\n日志和进度文件保存在: {work_dir}")

def run_with_partitioning(input_path, batch=1000, insert_batch=1000, work_dir="./bbox_import_logs", 
                         create_unified_view_flag=True, maintain_view_only=False, use_parallel=False, 
                         max_workers=None):
    """使用分表模式运行边界框处理（支持并行和顺序模式）
    
    Args:
        input_path: 输入数据集文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小  
        work_dir: 工作目录
        create_unified_view_flag: 是否创建统一视图
        maintain_view_only: 是否只维护视图（不处理数据）
        use_parallel: 是否使用并行处理模式
        max_workers: 最大并行worker数量，None为自动检测CPU核心数
    """
    if use_parallel:
        # 使用并行模式
        return run_with_partitioning_parallel(
            input_path, batch, insert_batch, work_dir, 
            create_unified_view_flag, maintain_view_only, max_workers
        )
    else:
        # 使用顺序模式（原始实现）
        return run_with_partitioning_sequential(
            input_path, batch, insert_batch, work_dir, 
            create_unified_view_flag, maintain_view_only
        )

def run_with_partitioning_sequential(input_path, batch=1000, insert_batch=1000, work_dir="./bbox_import_logs", 
                                   create_unified_view_flag=True, maintain_view_only=False):
    """使用顺序分表模式运行边界框处理（原始实现）
    
    Args:
        input_path: 输入数据集文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小  
        work_dir: 工作目录
        create_unified_view_flag: 是否创建统一视图
        maintain_view_only: 是否只维护视图（不处理数据）
    """
    global interrupted
    
    # 设置信号处理器
    setup_signal_handlers()
    
    print(f"=== 分表模式处理开始 ===")
    print(f"输入文件: {input_path}")
    print(f"工作目录: {work_dir}")
    print(f"批次大小: {batch}")
    print(f"插入批次大小: {insert_batch}")
    print(f"创建统一视图: {create_unified_view_flag}")
    print(f"仅维护视图: {maintain_view_only}")
    
    eng = create_engine(LOCAL_DSN, future=True)
    
    # 如果只是维护视图
    if maintain_view_only:
        print("\n=== 维护统一视图模式 ===")
        success = maintain_unified_view(eng)
        if success:
            print("✅ 统一视图维护完成")
        else:
            print("❌ 统一视图维护失败")
        return
    
    try:
        # 步骤1: 按子数据集分组场景
        print("\n=== 步骤1: 分组场景数据 ===")
        scene_groups = group_scenes_by_subdataset(input_path)
        
        if not scene_groups:
            print("没有找到有效的场景分组数据")
            return
        
        # 步骤2: 批量创建分表
        print("\n=== 步骤2: 创建分表 ===")
        table_mapping = batch_create_tables_for_subdatasets(eng, scene_groups)
        
        # 步骤3: 分别处理每个子数据集
        print("\n=== 步骤3: 分表数据处理 ===")
        total_processed = 0
        total_inserted = 0
        
        for i, (subdataset_name, subdataset_info) in enumerate(scene_groups.items(), 1):
            scene_ids = subdataset_info['scene_ids']
            metadata = subdataset_info.get('metadata', {})
            if interrupted:
                print(f"\n程序被中断，已处理 {i-1}/{len(scene_groups)} 个子数据集")
                break
                
            table_name = table_mapping[subdataset_name]
            print(f"\n[{i}/{len(scene_groups)}] 处理子数据集: {subdataset_name}")
            print(f"  - 目标表: {table_name}")
            print(f"  - 场景数: {len(scene_ids)}")
            
            # 为每个子数据集创建独立的进度跟踪器
            sub_work_dir = f"{work_dir}/{subdataset_name}"
            sub_tracker = LightweightProgressTracker(sub_work_dir)
            
            try:
                # 获取需要处理的场景ID
                remaining_scene_ids = sub_tracker.get_remaining_tokens(scene_ids)
                
                if not remaining_scene_ids:
                    print(f"  - 子数据集 {subdataset_name} 所有场景已处理完成，跳过")
                    continue
                
                print(f"  - 需要处理: {len(remaining_scene_ids)} 个场景")
                
                # 处理当前子数据集的数据
                sub_processed, sub_inserted = process_subdataset_scenes(
                    eng, remaining_scene_ids, table_name, batch, insert_batch, sub_tracker, metadata
                )
                
                total_processed += sub_processed
                total_inserted += sub_inserted
                
                print(f"  - 完成: 处理 {sub_processed} 个，插入 {sub_inserted} 条记录")
                
            except Exception as e:
                print(f"  - 处理子数据集 {subdataset_name} 失败: {str(e)}")
                continue
        
        # 步骤4: 创建统一视图（如果需要）
        if create_unified_view_flag and not interrupted:
            print("\n=== 步骤4: 创建统一视图 ===")
            success = create_unified_view(eng)
            if success:
                print("✅ 统一视图创建完成")
            else:
                print("❌ 统一视图创建失败")
        
        # 输出最终统计
        print(f"\n=== 分表处理完成 ===")
        print(f"总计处理: {total_processed} 条记录")
        print(f"总计插入: {total_inserted} 条记录")
        print(f"处理子数据集: {len(scene_groups)} 个")
        
        if interrupted:
            print("⚠️  处理被中断，部分数据可能未完成")
        else:
            print("✅ 分表处理全部成功完成")
            
    except KeyboardInterrupt:
        print(f"\n程序被用户中断")
        interrupted = True
    except Exception as e:
        print(f"\n分表处理遇到错误: {str(e)}")
    finally:
        print(f"\n日志和进度文件保存在: {work_dir}")

def process_subdataset_scenes(eng, scene_ids, table_name, batch_size, insert_batch_size, tracker, metadata=None):
    """处理单个子数据集的场景数据
    
    Args:
        eng: 数据库引擎
        scene_ids: 场景ID列表
        table_name: 目标表名
        batch_size: 处理批次大小
        insert_batch_size: 插入批次大小
        tracker: 进度跟踪器
        metadata: 子数据集元数据，用于添加额外字段
        
    Returns:
        (processed_count, inserted_count) 元组
    """
    processed_count = 0
    inserted_count = 0
    
    try:
        for batch_num, token_batch in enumerate(chunk(scene_ids, batch_size), 1):
            # 检查中断信号
            if interrupted:
                print(f"    批次处理被中断，已处理 {batch_num-1} 个批次")
                break
            
            print(f"    [批次 {batch_num}] 处理 {len(token_batch)} 个场景")
            
            # 过滤已处理的记录
            existing_in_progress = tracker.check_tokens_exist(token_batch)
            token_batch = [token for token in token_batch if token not in existing_in_progress]
            
            if not token_batch:
                print(f"    [批次 {batch_num}] 所有数据已处理，跳过")
                continue
            
            if existing_in_progress:
                print(f"    [批次 {batch_num}] 跳过 {len(existing_in_progress)} 个已处理的记录")
            
            # 获取元数据
            try:
                meta = fetch_meta(token_batch)
                if meta.empty:
                    print(f"    [批次 {batch_num}] 没有找到元数据，跳过")
                    for token in token_batch:
                        tracker.save_failed_record(token, "无法获取元数据", batch_num, "fetch_meta")
                    continue
                
                print(f"    [批次 {batch_num}] 获取到 {len(meta)} 条元数据")
                
            except Exception as e:
                print(f"    [批次 {batch_num}] 获取元数据失败: {str(e)}")
                for token in token_batch:
                    tracker.save_failed_record(token, f"获取元数据异常: {str(e)}", batch_num, "fetch_meta")
                continue
            
            # 检查中断信号
            if interrupted:
                break
            
            # 获取边界框和几何对象
            try:
                bbox_gdf = fetch_bbox_with_geometry(meta.data_name.tolist(), eng)
                if bbox_gdf.empty:
                    print(f"    [批次 {batch_num}] 没有找到边界框数据，跳过")
                    for token in meta.scene_token:
                        tracker.save_failed_record(token, "无法获取边界框数据", batch_num, "fetch_bbox")
                    continue
                
                print(f"    [批次 {batch_num}] 获取到 {len(bbox_gdf)} 条边界框数据")
                
            except Exception as e:
                print(f"    [批次 {batch_num}] 获取边界框失败: {str(e)}")
                for token in meta.scene_token:
                    tracker.save_failed_record(token, f"获取边界框异常: {str(e)}", batch_num, "fetch_bbox")
                continue
            
            # 检查中断信号
            if interrupted:
                break
            
                        # 合并数据
            try:
                merged = meta.merge(bbox_gdf, left_on='data_name', right_on='dataset_name', how='inner')
                if merged.empty:
                    print(f"    [批次 {batch_num}] 合并后数据为空，跳过")
                    for token in meta.scene_token:
                        tracker.save_failed_record(token, "元数据与边界框数据无法匹配", batch_num, "data_merge")
                    continue
                    
                print(f"    [批次 {batch_num}] 合并后得到 {len(merged)} 条记录")
                
                # 创建基础字段的数据
                base_columns = ['scene_token', 'data_name', 'event_id', 'city_id', 'timestamp', 'all_good']
                final_data = merged[base_columns].copy()
                
                # 添加额外字段（如果有metadata）
                if metadata:
                    data_type = metadata.get('data_type', 'standard')
                    final_data['data_type'] = data_type
                    
                    if data_type == 'defect':
                        # 获取scene_attributes
                        scene_attributes = metadata.get('scene_attributes', {})
                        
                        # 为每个场景添加特定属性
                        for idx, scene_token in enumerate(final_data['scene_token']):
                            scene_attrs = scene_attributes.get(scene_token, {})
                            
                            # 添加基础问题单字段（带类型转换）
                            final_data.loc[idx, 'original_url'] = str(scene_attrs.get('original_url', ''))
                            
                            # 添加其他自定义字段（带类型转换）
                            system_fields = {'data_type', 'original_url', 'data_name'}
                            for key, value in scene_attrs.items():
                                if key not in system_fields and not key.startswith('data_'):
                                    # 根据字段名推断预期类型并转换
                                    converted_value = convert_value_to_expected_type(key, value)
                                    final_data.loc[idx, key] = converted_value
                                    
                        print(f"    [批次 {batch_num}] 添加了问题单特定字段，包含 {len(scene_attributes)} 个场景的属性")
                else:
                    # 向后兼容：添加默认data_type
                    final_data['data_type'] = 'standard'
                
                # 创建最终的GeoDataFrame
                final_gdf = gpd.GeoDataFrame(
                    final_data, 
                    geometry=merged['geometry'], 
                    crs=4326
                )
                
            except Exception as e:
                print(f"    [批次 {batch_num}] 数据合并失败: {str(e)}")
                for token in meta.scene_token:
                    tracker.save_failed_record(token, f"数据合并异常: {str(e)}", batch_num, "data_merge")
                continue
            
            # 检查中断信号
            if interrupted:
                break
            
            # 批量插入到指定表
            try:
                batch_inserted = batch_insert_to_postgis(
                    final_gdf, eng, 
                    table_name=table_name,  # 使用指定的分表名称
                    batch_size=insert_batch_size, 
                    tracker=tracker, 
                    batch_num=batch_num
                )
                inserted_count += batch_inserted
                processed_count += len(final_gdf)
                
                print(f"    [批次 {batch_num}] 完成，插入 {batch_inserted} 条记录到 {table_name}")
                
            except Exception as e:
                print(f"    [批次 {batch_num}] 插入数据库失败: {str(e)}")
                for token in final_gdf.scene_token:
                    tracker.save_failed_record(token, f"批量插入异常: {str(e)}", batch_num, "batch_insert")
                continue
        
        return processed_count, inserted_count
        
    except Exception as e:
        print(f"    处理子数据集场景失败: {str(e)}")
        return processed_count, inserted_count
    finally:
        # 保存进度和统计信息
        tracker.finalize()

def run(input_path, batch=1000, insert_batch=1000, create_table=False, retry_failed=False, work_dir="./bbox_import_logs", show_stats=False):
    """主运行函数
    
    Args:
        input_path: 输入文件路径
        batch: 处理批次大小
        insert_batch: 插入批次大小
        create_table: 是否创建表（如果不存在）
        retry_failed: 是否只重试失败的数据
        work_dir: 工作目录，用于存储日志和进度文件
        show_stats: 是否显示统计信息并退出
    """
    global interrupted
    
    # 设置信号处理器
    setup_signal_handlers()
    
    # 检查Parquet支持
    if not PARQUET_AVAILABLE:
        print("警告: 未安装pyarrow，将使用降级的文本文件模式")
    
    # 初始化进度跟踪器
    tracker = LightweightProgressTracker(work_dir)
    
    # 如果只是查看统计信息
    if show_stats:
        stats = tracker.get_statistics()
        print("\n=== 处理统计信息 ===")
        print(f"成功处理: {stats['success_count']} 个场景")
        print(f"失败场景: {stats['failed_count']} 个")
        
        if stats['failed_by_step']:
            print("\n按步骤分类的失败统计:")
            for step, count in stats['failed_by_step'].items():
                print(f"  {step}: {count} 个")
        
        return
    
    print(f"开始处理输入文件: {input_path}")
    print(f"工作目录: {work_dir}")
    
    # 智能加载scene_id列表
    try:
        if retry_failed:
            scene_ids = tracker.load_failed_tokens()
            print(f"重试模式：加载了 {len(scene_ids)} 个失败的scene_id")
        else:
            all_scene_ids = load_scene_ids(input_path)
            scene_ids = tracker.get_remaining_tokens(all_scene_ids)
    except Exception as e:
        print(f"加载输入文件失败: {str(e)}")
        return
    
    if not scene_ids:
        print("没有找到需要处理的scene_id")
        return
    
    eng = create_engine(LOCAL_DSN, future=True)
    
    # 创建表（如果需要）
    if create_table:
        if not create_table_if_not_exists(eng):
            print("创建表失败，退出")
            return
    
    total_processed = 0
    total_inserted = 0
    
    print(f"开始处理 {len(scene_ids)} 个场景，批次大小: {batch}")
    print("使用原始边界框数据，不进行缓冲区扩展")
    
    try:
        for batch_num, token_batch in enumerate(chunk(scene_ids, batch), 1):
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，已处理 {batch_num-1} 个批次")
                break
                
            print(f"[批次 {batch_num}] 处理 {len(token_batch)} 个场景")
            
            # 只检查进度跟踪器中的记录（内存中的成功缓存）
            existing_in_progress = tracker.check_tokens_exist(token_batch)
            token_batch = [token for token in token_batch if token not in existing_in_progress]
            
            if not token_batch:
                print(f"[批次 {batch_num}] 所有数据已在进度中标记为处理过，跳过")
                continue
            
            if existing_in_progress:
                print(f"[批次 {batch_num}] 跳过 {len(existing_in_progress)} 个已处理的记录")
            
            # 获取元数据
            try:
                meta = fetch_meta(token_batch)
                if meta.empty:
                    print(f"[批次 {batch_num}] 没有找到元数据，跳过")
                    # 记录获取元数据失败的tokens
                    for token in token_batch:
                        tracker.save_failed_record(token, "无法获取元数据", batch_num, "fetch_meta")
                    continue
                    
                print(f"[批次 {batch_num}] 获取到 {len(meta)} 条元数据")
                
            except Exception as e:
                print(f"[批次 {batch_num}] 获取元数据失败: {str(e)}")
                for token in token_batch:
                    tracker.save_failed_record(token, f"获取元数据异常: {str(e)}", batch_num, "fetch_meta")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 获取边界框和几何对象（直接从PostGIS获取原始几何）
            try:
                bbox_gdf = fetch_bbox_with_geometry(meta.data_name.tolist(), eng)
                if bbox_gdf.empty:
                    print(f"[批次 {batch_num}] 没有找到边界框数据，跳过")
                    # 记录获取边界框失败的tokens
                    for token in meta.scene_token:
                        tracker.save_failed_record(token, "无法获取边界框数据", batch_num, "fetch_bbox")
                    continue
                    
                print(f"[批次 {batch_num}] 获取到 {len(bbox_gdf)} 条边界框数据")
                
            except Exception as e:
                print(f"[批次 {batch_num}] 获取边界框失败: {str(e)}")
                for token in meta.scene_token:
                    tracker.save_failed_record(token, f"获取边界框异常: {str(e)}", batch_num, "fetch_bbox")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 合并数据
            try:
                merged = meta.merge(bbox_gdf, left_on='data_name', right_on='dataset_name', how='inner')
                if merged.empty:
                    print(f"[批次 {batch_num}] 合并后数据为空，跳过")
                    # 记录合并失败的tokens
                    for token in meta.scene_token:
                        tracker.save_failed_record(token, "元数据与边界框数据无法匹配", batch_num, "data_merge")
                    continue
                    
                print(f"[批次 {batch_num}] 合并后得到 {len(merged)} 条记录")
                
                # 创建最终的GeoDataFrame
                final_gdf = gpd.GeoDataFrame(
                    merged[['scene_token', 'data_name', 'event_id', 'city_id', 'timestamp', 'all_good']], 
                    geometry=merged['geometry'], 
                    crs=4326
                )
                
            except Exception as e:
                print(f"[批次 {batch_num}] 数据合并失败: {str(e)}")
                for token in meta.scene_token:
                    tracker.save_failed_record(token, f"数据合并异常: {str(e)}", batch_num, "data_merge")
                continue
            
            # 检查中断信号
            if interrupted:
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break
            
            # 批量插入数据库
            try:
                batch_inserted = batch_insert_to_postgis(
                    final_gdf, eng, 
                    batch_size=insert_batch, 
                    tracker=tracker, 
                    batch_num=batch_num
                )
                total_inserted += batch_inserted
                total_processed += len(final_gdf)
                
                print(f"[批次 {batch_num}] 完成，插入 {batch_inserted} 条记录")
                print(f"[累计进度] 已处理: {total_processed}, 已插入: {total_inserted}")
                
                # 每10个批次保存一次进度
                if batch_num % 10 == 0:
                    tracker.save_progress(len(scene_ids), total_processed, total_inserted, batch_num)
                
            except Exception as e:
                print(f"[批次 {batch_num}] 插入数据库失败: {str(e)}")
                for token in final_gdf.scene_token:
                    tracker.save_failed_record(token, f"批量插入异常: {str(e)}", batch_num, "batch_insert")
                continue
                
    except KeyboardInterrupt:
        print(f"\n程序被用户中断")
        interrupted = True
    except Exception as e:
        print(f"\n程序遇到未预期的错误: {str(e)}")
    finally:
        # 最终保存进度和刷新缓冲区
        tracker.finalize()
        tracker.save_progress(len(scene_ids), total_processed, total_inserted, batch_num if 'batch_num' in locals() else 0)
        
        # 显示最终统计
        stats = tracker.get_statistics()
        
        if interrupted:
            print(f"程序优雅退出！已处理: {total_processed} 条记录，成功插入: {total_inserted} 条记录")
        else:
            print(f"处理完成！总计处理: {total_processed} 条记录，成功插入: {total_inserted} 条记录")
        
        print(f"\n=== 最终统计 ===")
        print(f"成功处理: {stats['success_count']} 个场景")
        print(f"失败场景: {stats['failed_count']} 个")
        
        print(f"\n状态文件位置:")
        print(f"- 成功记录: {tracker.success_file}")
        print(f"- 失败记录: {tracker.failed_file}")
        print(f"- 进度文件: {tracker.progress_file}")

def build_parser() -> argparse.ArgumentParser:
    """兼容旧脚本的命令行解析器。"""

    parser = argparse.ArgumentParser(description='从数据集文件生成边界框数据')
    parser.add_argument('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
    parser.add_argument('--batch', type=int, default=1000, help='处理批次大小')
    parser.add_argument('--insert-batch', type=int, default=1000, help='插入批次大小')
    parser.add_argument('--create-table', action='store_true', help='创建表（如果不存在）。默认假设表已通过SQL脚本创建')
    parser.add_argument('--retry-failed', action='store_true', help='是否只重试失败的数据')
    parser.add_argument('--work-dir', default='./bbox_import_logs', help='工作目录，用于存储日志和进度文件')
    parser.add_argument('--show-stats', action='store_true', help='显示处理统计信息并退出')
    return parser


def main(argv: list[str] | None = None) -> int:
    """暴露一个入口以供新的 CLI 代理调用。"""

    parser = build_parser()
    args = parser.parse_args(argv)
    run(
        args.input,
        args.batch,
        args.insert_batch,
        args.create_table,
        args.retry_failed,
        args.work_dir,
        args.show_stats,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
