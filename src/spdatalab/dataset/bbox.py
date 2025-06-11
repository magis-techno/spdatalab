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
            
            # 执行所有SQL语句
            conn.execute(create_sql)
            conn.execute(add_geom_sql)
            conn.execute(constraint_sql)
            conn.execute(index_sql)
            conn.commit()
            
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
    3. 如果中间出现类似 "_277736e2e_"（2777打头，e2e结尾）的字符串，这部分及之后都不要
    
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
    
    # 3. 处理 _2777xxxe2e_ 模式截断（2777打头，e2e结尾的字符串）
    # 匹配模式：_2777开头，中间任意字符，e2e结尾，后面可能还有其他内容
    hash_pattern = r'_2777[^_]*e2e_?.*$'
    hash_match = re.search(hash_pattern, subdataset_name)
    if hash_match:
        # 截断到匹配位置之前
        subdataset_name = subdataset_name[:hash_match.start()]
    
    # 4. 清理和验证结果
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
    clean_name = re.sub(r'_+', '_', clean_name)  # 多个下划线合并
    clean_name = clean_name.strip('_')  # 去除首尾下划线
    
    # PostgreSQL表名限制63字符
    if len(clean_name) > 50:  # 留出前缀空间
        # 保留开头和结尾，中间用省略
        clean_name = clean_name[:20] + "_" + clean_name[-25:]
    
    table_name = f"clips_bbox_{clean_name}"
    
    # 确保表名符合PostgreSQL规范（以字母开头）
    if table_name[0].isdigit():
        table_name = "t_" + table_name
    
    return table_name

def create_table_for_subdataset(eng, subdataset_name, base_table_name='clips_bbox'):
    """为特定子数据集创建分表"""
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
            
            # 创建与主表相同结构的分表
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
                ALTER TABLE {table_name} ADD CONSTRAINT check_{table_name}_geom_type 
                    CHECK (ST_GeometryType(geometry) IN ('ST_Polygon', 'ST_Point'));
            """)
            
            # 创建索引
            index_sql = text(f"""
                CREATE INDEX idx_{table_name}_geometry ON {table_name} USING GIST(geometry);
                CREATE INDEX idx_{table_name}_data_name ON {table_name}(data_name);
                CREATE INDEX idx_{table_name}_scene_token ON {table_name}(scene_token);
            """)
            
            # 执行所有SQL语句
            conn.execute(create_sql)
            conn.execute(add_geom_sql)
            conn.execute(constraint_sql)
            conn.execute(index_sql)
            conn.commit()
            
            print(f"成功创建分表 {table_name} 及相关索引")
            return True, table_name
            
    except Exception as e:
        print(f"创建分表 {table_name} 时出错: {str(e)}")
        return False, table_name

def group_scenes_by_subdataset(dataset_file: str) -> Dict[str, List[str]]:
    """按子数据集分组scene_ids
    
    Args:
        dataset_file: dataset文件路径（JSON/Parquet格式）
        
    Returns:
        字典，key为子数据集名称，value为scene_ids列表
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
            
            if scene_ids:
                groups[subdataset_name] = scene_ids
                total_scenes += len(scene_ids)
                print(f"  {subdataset_name}: {len(scene_ids)} 个场景")
            else:
                print(f"  {subdataset_name}: 无场景数据，跳过")
        
        print(f"总计: {len(groups)} 个有效子数据集，{total_scenes} 个场景")
        return groups
        
    except Exception as e:
        print(f"分组scene_ids失败: {str(e)}")
        raise

def batch_create_tables_for_subdatasets(eng, subdataset_names: List[str]) -> Dict[str, str]:
    """批量为子数据集创建分表
    
    Args:
        eng: 数据库引擎
        subdataset_names: 子数据集名称列表
        
    Returns:
        字典，key为原始子数据集名称，value为创建的表名
    """
    table_mapping = {}
    success_count = 0
    
    print(f"开始批量创建 {len(subdataset_names)} 个分表...")
    
    for i, subdataset_name in enumerate(subdataset_names, 1):
        print(f"[{i}/{len(subdataset_names)}] 处理: {subdataset_name}")
        
        success, table_name = create_table_for_subdataset(eng, subdataset_name)
        table_mapping[subdataset_name] = table_name
        
        if success:
            success_count += 1
        else:
            print(f"警告: 子数据集 {subdataset_name} 的分表创建失败")
    
    print(f"批量创建完成: 成功 {success_count}/{len(subdataset_names)} 个分表")
    return table_mapping

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

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='从数据集文件生成边界框数据')
    ap.add_argument('--input', required=True, help='输入文件路径（支持JSON/Parquet/文本格式）')
    ap.add_argument('--batch', type=int, default=1000, help='处理批次大小')
    ap.add_argument('--insert-batch', type=int, default=1000, help='插入批次大小')
    ap.add_argument('--create-table', action='store_true', help='创建表（如果不存在）。默认假设表已通过SQL脚本创建')
    ap.add_argument('--retry-failed', action='store_true', help='是否只重试失败的数据')
    ap.add_argument('--work-dir', default='./bbox_import_logs', help='工作目录，用于存储日志和进度文件')
    ap.add_argument('--show-stats', action='store_true', help='显示处理统计信息并退出')
    
    args = ap.parse_args()
    run(args.input, args.batch, args.insert_batch, args.create_table, args.retry_failed, args.work_dir, args.show_stats)