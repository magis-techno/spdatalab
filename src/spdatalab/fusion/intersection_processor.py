"""
高级交集处理器

整合轨迹交集分析和叠置分析功能，提供：
1. 批量交集分析工作流程
2. 结果可视化和导出
3. 性能优化和并行处理
4. 结果质量评估
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json

from .trajectory_intersection import TrajectoryIntersectionAnalyzer
from .overlay_analysis import OverlayAnalyzer
from ..common.config import LOCAL_DSN

logger = logging.getLogger(__name__)

class IntersectionProcessor:
    """高级交集处理器"""
    
    def __init__(self, engine=None, max_workers: int = 4):
        """
        初始化处理器
        
        Args:
            engine: SQLAlchemy引擎
            max_workers: 最大并行工作线程数
        """
        if engine is None:
            self.engine = create_engine(LOCAL_DSN, future=True)
        else:
            self.engine = engine
            
        self.trajectory_analyzer = TrajectoryIntersectionAnalyzer(self.engine)
        self.overlay_analyzer = OverlayAnalyzer(self.engine)
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
    
    def run_comprehensive_intersection_analysis(
        self,
        analysis_config: Dict[str, Any],
        output_dir: str,
        export_formats: List[str] = ['csv', 'geojson', 'gpkg']
    ) -> Dict[str, Any]:
        """
        运行综合交集分析
        
        Args:
            analysis_config: 分析配置字典
            output_dir: 输出目录
            export_formats: 导出格式列表
            
        Returns:
            分析结果汇总字典
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = {
            'analysis_time': datetime.now(),
            'config': analysis_config,
            'results': {},
            'statistics': {},
            'exported_files': []
        }
        
        # 轨迹与路口交集分析
        if analysis_config.get('trajectory_junction_analysis', {}).get('enabled', False):
            junction_config = analysis_config['trajectory_junction_analysis']
            self.logger.info("开始轨迹与路口交集分析...")
            
            junction_results = self.trajectory_analyzer.analyze_trajectory_intersection_with_junctions(
                trajectory_table=junction_config.get('trajectory_table', 'clips_bbox'),
                junction_table=junction_config.get('junction_table', 'intersections'),
                buffer_meters=junction_config.get('buffer_meters', 20.0),
                output_table=junction_config.get('output_table')
            )
            
            results['results']['trajectory_junction'] = junction_results
            
            # 导出结果
            if len(junction_results) > 0:
                exported = self._export_results(
                    junction_results, 
                    output_path / 'trajectory_junction_analysis',
                    export_formats
                )
                results['exported_files'].extend(exported)
        
        # 轨迹与道路交集分析
        if analysis_config.get('trajectory_road_analysis', {}).get('enabled', False):
            road_config = analysis_config['trajectory_road_analysis']
            self.logger.info("开始轨迹与道路交集分析...")
            
            road_results = self.trajectory_analyzer.analyze_trajectory_intersection_with_roads(
                trajectory_table=road_config.get('trajectory_table', 'clips_bbox'),
                road_table=road_config.get('road_table', 'roads'),
                buffer_meters=road_config.get('buffer_meters', 10.0),
                output_table=road_config.get('output_table')
            )
            
            results['results']['trajectory_road'] = road_results
            
            if len(road_results) > 0:
                exported = self._export_results(
                    road_results,
                    output_path / 'trajectory_road_analysis',
                    export_formats
                )
                results['exported_files'].extend(exported)
        
        # 轨迹与区域交集分析
        if analysis_config.get('trajectory_region_analysis', {}).get('enabled', False):
            region_config = analysis_config['trajectory_region_analysis']
            self.logger.info("开始轨迹与区域交集分析...")
            
            region_results = self.trajectory_analyzer.analyze_trajectory_intersection_with_regions(
                trajectory_table=region_config.get('trajectory_table', 'clips_bbox'),
                region_table=region_config.get('region_table', 'regions'),
                region_type=region_config.get('region_type'),
                buffer_meters=region_config.get('buffer_meters', 0.0),
                output_table=region_config.get('output_table')
            )
            
            results['results']['trajectory_region'] = region_results
            
            if len(region_results) > 0:
                exported = self._export_results(
                    region_results,
                    output_path / 'trajectory_region_analysis',
                    export_formats
                )
                results['exported_files'].extend(exported)
        
        # 轨迹间交集分析
        if analysis_config.get('trajectory_to_trajectory_analysis', {}).get('enabled', False):
            traj_config = analysis_config['trajectory_to_trajectory_analysis']
            self.logger.info("开始轨迹间交集分析...")
            
            traj_results = self.trajectory_analyzer.analyze_trajectory_to_trajectory_intersection(
                trajectory_table1=traj_config.get('trajectory_table1', 'clips_bbox'),
                trajectory_table2=traj_config.get('trajectory_table2'),
                buffer_meters=traj_config.get('buffer_meters', 5.0),
                time_tolerance_seconds=traj_config.get('time_tolerance_seconds'),
                output_table=traj_config.get('output_table')
            )
            
            results['results']['trajectory_to_trajectory'] = traj_results
            
            if len(traj_results) > 0:
                exported = self._export_results(
                    traj_results,
                    output_path / 'trajectory_to_trajectory_analysis',
                    export_formats
                )
                results['exported_files'].extend(exported)
        
        # 生成统计报告
        all_results = [gdf for gdf in results['results'].values() if len(gdf) > 0]
        if all_results:
            summary_stats = self.trajectory_analyzer.generate_intersection_summary(
                all_results,
                output_path / 'intersection_summary.csv'
            )
            results['statistics']['summary'] = summary_stats
        
        # 生成可视化报告
        if analysis_config.get('generate_visualizations', True):
            viz_files = self._generate_visualization_reports(
                results['results'],
                output_path / 'visualizations'
            )
            results['exported_files'].extend(viz_files)
        
        # 保存配置和结果元数据
        metadata_file = output_path / 'analysis_metadata.json'
        with open(metadata_file, 'w', encoding='utf-8') as f:
            # 序列化时排除不能JSON化的对象
            serializable_results = {
                'analysis_time': results['analysis_time'].isoformat(),
                'config': results['config'],
                'statistics': {k: v.to_dict('records') if hasattr(v, 'to_dict') else v 
                             for k, v in results['statistics'].items()},
                'result_counts': {k: len(v) for k, v in results['results'].items()},
                'exported_files': results['exported_files']
            }
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        results['exported_files'].append(str(metadata_file))
        
        self.logger.info(f"综合交集分析完成，结果保存到: {output_dir}")
        return results
    
    def run_parallel_intersection_analysis(
        self,
        city_ids: List[str],
        analysis_type: str,
        analysis_params: Dict[str, Any],
        output_dir: str
    ) -> Dict[str, gpd.GeoDataFrame]:
        """
        并行运行多城市交集分析
        
        Args:
            city_ids: 城市ID列表
            analysis_type: 分析类型
            analysis_params: 分析参数
            output_dir: 输出目录
            
        Returns:
            每个城市的分析结果字典
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_city = {}
            for city_id in city_ids:
                future = executor.submit(
                    self._run_single_city_analysis,
                    city_id,
                    analysis_type,
                    analysis_params,
                    output_path
                )
                future_to_city[future] = city_id
            
            # 收集结果
            for future in as_completed(future_to_city):
                city_id = future_to_city[future]
                try:
                    city_result = future.result()
                    results[city_id] = city_result
                    self.logger.info(f"城市 {city_id} 分析完成")
                except Exception as e:
                    self.logger.error(f"城市 {city_id} 分析失败: {str(e)}")
                    results[city_id] = gpd.GeoDataFrame()
        
        self.logger.info(f"并行分析完成，共处理 {len(city_ids)} 个城市")
        return results
    
    def evaluate_intersection_quality(
        self,
        intersection_results: gpd.GeoDataFrame,
        quality_thresholds: Dict[str, float] = None
    ) -> Dict[str, Any]:
        """
        评估交集分析结果质量
        
        Args:
            intersection_results: 交集分析结果
            quality_thresholds: 质量阈值字典
            
        Returns:
            质量评估结果
        """
        if quality_thresholds is None:
            quality_thresholds = {
                'min_intersection_area_m2': 1.0,
                'max_distance_meters': 1000.0,
                'min_intersection_count': 5
            }
        
        quality_report = {
            'total_intersections': len(intersection_results),
            'quality_flags': {},
            'recommendations': []
        }
        
        if len(intersection_results) == 0:
            quality_report['quality_flags']['no_results'] = True
            quality_report['recommendations'].append("检查输入数据是否存在以及参数设置是否合理")
            return quality_report
        
        # 检查交集面积
        if 'intersection_area_m2' in intersection_results.columns:
            small_areas = intersection_results[
                intersection_results['intersection_area_m2'] < quality_thresholds['min_intersection_area_m2']
            ]
            if len(small_areas) > 0:
                quality_report['quality_flags']['small_intersection_areas'] = len(small_areas)
                quality_report['recommendations'].append(
                    f"发现 {len(small_areas)} 个面积过小的交集，建议检查数据精度或调整缓冲区参数"
                )
        
        # 检查距离
        if 'distance_meters' in intersection_results.columns:
            large_distances = intersection_results[
                intersection_results['distance_meters'] > quality_thresholds['max_distance_meters']
            ]
            if len(large_distances) > 0:
                quality_report['quality_flags']['large_distances'] = len(large_distances)
                quality_report['recommendations'].append(
                    f"发现 {len(large_distances)} 个距离过大的交集，建议检查坐标系统或数据投影"
                )
        
        # 检查交集数量
        if len(intersection_results) < quality_thresholds['min_intersection_count']:
            quality_report['quality_flags']['few_intersections'] = True
            quality_report['recommendations'].append(
                "交集数量偏少，建议检查输入数据的空间范围或调整分析参数"
            )
        
        # 检查空间分布
        if 'city_id' in intersection_results.columns:
            city_counts = intersection_results['city_id'].value_counts()
            if city_counts.std() / city_counts.mean() > 0.5:  # 变异系数大于0.5
                quality_report['quality_flags']['uneven_distribution'] = True
                quality_report['recommendations'].append(
                    "交集在不同城市的分布不均匀，建议检查数据完整性"
                )
        
        # 检查几何有效性
        invalid_geoms = intersection_results[
            ~intersection_results.geometry.is_valid
        ]
        if len(invalid_geoms) > 0:
            quality_report['quality_flags']['invalid_geometries'] = len(invalid_geoms)
            quality_report['recommendations'].append(
                f"发现 {len(invalid_geoms)} 个无效几何对象，建议进行几何修复"
            )
        
        # 计算质量得分
        flag_count = len([k for k, v in quality_report['quality_flags'].items() if v])
        quality_score = max(0, 100 - flag_count * 15)  # 每个问题扣15分
        quality_report['quality_score'] = quality_score
        
        if quality_score >= 85:
            quality_report['quality_level'] = 'excellent'
        elif quality_score >= 70:
            quality_report['quality_level'] = 'good'
        elif quality_score >= 50:
            quality_report['quality_level'] = 'fair'
        else:
            quality_report['quality_level'] = 'poor'
        
        return quality_report
    
    def _run_single_city_analysis(
        self,
        city_id: str,
        analysis_type: str,
        analysis_params: Dict[str, Any],
        output_path: Path
    ) -> gpd.GeoDataFrame:
        """运行单个城市的分析"""
        # 添加城市过滤条件
        city_params = analysis_params.copy()
        
        if analysis_type == 'trajectory_junction':
            # 创建带城市过滤的临时视图
            temp_view = f"temp_trajectory_{city_id.replace('-', '_')}"
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE TEMP VIEW {temp_view} AS 
                    SELECT * FROM {city_params.get('trajectory_table', 'clips_bbox')}
                    WHERE city_id = '{city_id}'
                """))
                conn.commit()
            
            result = self.trajectory_analyzer.analyze_trajectory_intersection_with_junctions(
                trajectory_table=temp_view,
                **{k: v for k, v in city_params.items() if k != 'trajectory_table'}
            )
        elif analysis_type == 'trajectory_road':
            temp_view = f"temp_trajectory_{city_id.replace('-', '_')}"
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE TEMP VIEW {temp_view} AS 
                    SELECT * FROM {city_params.get('trajectory_table', 'clips_bbox')}
                    WHERE city_id = '{city_id}'
                """))
                conn.commit()
                
            result = self.trajectory_analyzer.analyze_trajectory_intersection_with_roads(
                trajectory_table=temp_view,
                **{k: v for k, v in city_params.items() if k != 'trajectory_table'}
            )
        else:
            raise ValueError(f"不支持的分析类型: {analysis_type}")
        
        # 保存城市结果
        if len(result) > 0:
            city_output_path = output_path / f'{analysis_type}_{city_id}'
            self._export_results(result, city_output_path, ['csv', 'geojson'])
        
        return result
    
    def _export_results(
        self,
        gdf: gpd.GeoDataFrame,
        base_path: Path,
        formats: List[str]
    ) -> List[str]:
        """导出结果到多种格式"""
        exported_files = []
        
        if len(gdf) == 0:
            self.logger.warning("没有数据可导出")
            return exported_files
        
        for fmt in formats:
            try:
                if fmt == 'csv':
                    # 导出为CSV，不包含几何列
                    df = pd.DataFrame(gdf.drop(columns=['geometry']))
                    output_file = f"{base_path}.csv"
                    df.to_csv(output_file, index=False, encoding='utf-8')
                    exported_files.append(output_file)
                    
                elif fmt == 'geojson':
                    output_file = f"{base_path}.geojson"
                    gdf.to_file(output_file, driver='GeoJSON', encoding='utf-8')
                    exported_files.append(output_file)
                    
                elif fmt == 'gpkg':
                    output_file = f"{base_path}.gpkg"
                    gdf.to_file(output_file, driver='GPKG')
                    exported_files.append(output_file)
                    
                elif fmt == 'shp':
                    output_file = f"{base_path}.shp"
                    gdf.to_file(output_file, driver='ESRI Shapefile', encoding='utf-8')
                    exported_files.append(output_file)
                    
            except Exception as e:
                self.logger.error(f"导出 {fmt} 格式失败: {str(e)}")
        
        return exported_files
    
    def _generate_visualization_reports(
        self,
        results: Dict[str, gpd.GeoDataFrame],
        output_path: Path
    ) -> List[str]:
        """生成可视化报告"""
        output_path.mkdir(parents=True, exist_ok=True)
        viz_files = []
        
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        for analysis_name, gdf in results.items():
            if len(gdf) == 0:
                continue
                
            try:
                # 统计图表
                fig, axes = plt.subplots(2, 2, figsize=(15, 12))
                fig.suptitle(f'{analysis_name} 分析结果统计', fontsize=16)
                
                # 图1: 交集数量分布
                if 'city_id' in gdf.columns:
                    city_counts = gdf['city_id'].value_counts()
                    axes[0, 0].bar(range(len(city_counts)), city_counts.values)
                    axes[0, 0].set_title('各城市交集数量分布')
                    axes[0, 0].set_xlabel('城市')
                    axes[0, 0].set_ylabel('交集数量')
                    axes[0, 0].tick_params(axis='x', rotation=45)
                
                # 图2: 面积分布
                if 'intersection_area_m2' in gdf.columns:
                    axes[0, 1].hist(gdf['intersection_area_m2'], bins=30, alpha=0.7)
                    axes[0, 1].set_title('交集面积分布')
                    axes[0, 1].set_xlabel('面积 (m²)')
                    axes[0, 1].set_ylabel('频次')
                
                # 图3: 距离分布
                if 'distance_meters' in gdf.columns:
                    axes[1, 0].hist(gdf['distance_meters'], bins=30, alpha=0.7)
                    axes[1, 0].set_title('距离分布')
                    axes[1, 0].set_xlabel('距离 (m)')
                    axes[1, 0].set_ylabel('频次')
                
                # 图4: 交集类型分布
                if 'intersection_type' in gdf.columns:
                    type_counts = gdf['intersection_type'].value_counts()
                    axes[1, 1].pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%')
                    axes[1, 1].set_title('交集类型分布')
                
                plt.tight_layout()
                
                viz_file = output_path / f'{analysis_name}_statistics.png'
                plt.savefig(viz_file, dpi=300, bbox_inches='tight')
                plt.close()
                
                viz_files.append(str(viz_file))
                
            except Exception as e:
                self.logger.error(f"生成 {analysis_name} 可视化失败: {str(e)}")
        
        return viz_files 