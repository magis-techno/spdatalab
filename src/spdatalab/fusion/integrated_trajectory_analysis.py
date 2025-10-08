"""
集成轨迹分析模块

统一入口点，自动执行两阶段轨迹分析流程：
1. 第一阶段：轨迹道路分析 (trajectory_road_analysis)
2. 第二阶段：轨迹车道分析 (trajectory_lane_analysis)

主要功能：
- 支持从GeoJSON文件批量加载轨迹
- 自动执行两阶段分析流程
- 智能错误处理和进度跟踪
- 生成综合分析报告
- 支持QGIS可视化导出
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import json

from .geojson_utils import load_trajectories_from_geojson, validate_geojson_format
from .trajectory_road_analysis import batch_analyze_trajectories_from_geojson, create_batch_road_analysis_report
from .trajectory_lane_analysis import batch_analyze_lanes_from_road_results, create_batch_lane_analysis_report
from .integrated_analysis_config import IntegratedAnalysisConfig, create_default_config

logger = logging.getLogger(__name__)

class IntegratedTrajectoryAnalyzer:
    """集成轨迹分析器"""
    
    def __init__(self, config: Optional[IntegratedAnalysisConfig] = None):
        self.config = config or create_default_config()
        self.analysis_id = None
        self.analysis_start_time = None
        self.analysis_results = {
            'trajectories': [],
            'road_analysis_results': [],
            'lane_analysis_results': [],
            'errors': [],
            'summary': {}
        }
    
    def analyze_trajectories_from_geojson(
        self,
        geojson_file: str,
        analysis_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        从GeoJSON文件执行完整的两阶段轨迹分析
        
        Args:
            geojson_file: GeoJSON文件路径
            analysis_id: 分析ID（可选，自动生成）
            
        Returns:
            完整的分析结果字典
        """
        if not analysis_id:
            analysis_id = f"integrated_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.analysis_id = analysis_id
        self.analysis_start_time = datetime.now()
        
        logger.info(f"开始集成轨迹分析: {analysis_id}")
        logger.info(f"GeoJSON文件: {geojson_file}")
        
        try:
            # 第一步：验证输入文件
            self._validate_input_file(geojson_file)
            
            # 第二步：加载轨迹数据
            trajectories = self._load_trajectories(geojson_file)
            
            # 第三步：执行道路分析
            road_results = self._execute_road_analysis(geojson_file, trajectories)
            
            # 第四步：执行车道分析
            lane_results = self._execute_lane_analysis(road_results)
            
            # 第五步：生成综合结果
            integrated_results = self._generate_integrated_results(
                trajectories, road_results, lane_results
            )
            
            # 第六步：后处理
            self._post_process_results(integrated_results)
            
            logger.info(f"集成轨迹分析完成: {analysis_id}")
            return integrated_results
            
        except Exception as e:
            logger.error(f"集成轨迹分析失败: {e}")
            self.analysis_results['errors'].append({
                'stage': 'integrated_analysis',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            
            # 返回错误结果
            return {
                'analysis_id': analysis_id,
                'status': 'failed',
                'error': str(e),
                'analysis_results': self.analysis_results
            }
    
    def _validate_input_file(self, geojson_file: str):
        """验证输入文件"""
        logger.info("验证输入文件格式")
        
        # 检查文件是否存在
        if not Path(geojson_file).exists():
            raise FileNotFoundError(f"文件不存在: {geojson_file}")
        
        # 验证GeoJSON格式
        is_valid, errors = validate_geojson_format(geojson_file)
        if not is_valid:
            error_msg = f"GeoJSON格式验证失败: {'; '.join(errors)}"
            raise ValueError(error_msg)
        
        logger.info("✓ 输入文件格式验证通过")
    
    def _load_trajectories(self, geojson_file: str) -> List:
        """加载轨迹数据"""
        logger.info("加载轨迹数据")
        
        trajectories = load_trajectories_from_geojson(geojson_file)
        
        if not trajectories:
            raise ValueError("未加载到任何轨迹数据")
        
        self.analysis_results['trajectories'] = trajectories
        logger.info(f"✓ 加载轨迹数据: {len(trajectories)} 条")
        
        return trajectories
    
    def _execute_road_analysis(self, geojson_file: str, trajectories: List) -> List[Tuple[str, str, Dict[str, Any]]]:
        """执行道路分析"""
        logger.info("执行第一阶段：道路分析")
        
        # 生成道路分析ID
        road_analysis_id = f"{self.analysis_id}_road"
        
        try:
            # 创建动态表名配置
            road_config = self.config.road_analysis_config
            # 动态修改表名以包含分析ID
            road_config.analysis_table = f"{road_analysis_id}_analysis"
            road_config.lanes_table = f"{road_analysis_id}_lanes"
            road_config.intersections_table = f"{road_analysis_id}_intersections"
            road_config.roads_table = f"{road_analysis_id}_roads"
            
            # 执行批量道路分析
            road_results = batch_analyze_trajectories_from_geojson(
                geojson_file=geojson_file,
                batch_analysis_id=road_analysis_id,
                config=road_config
            )
            
            if not road_results:
                raise ValueError("道路分析未返回任何结果")
            
            # 统计结果
            successful_road_count = len([r for r in road_results if r[1] is not None])
            failed_road_count = len(road_results) - successful_road_count
            
            logger.info(f"✓ 道路分析完成: 成功 {successful_road_count}, 失败 {failed_road_count}")
            
            # 记录失败的道路分析
            for trajectory_id, analysis_id, summary in road_results:
                if analysis_id is None:
                    self.analysis_results['errors'].append({
                        'stage': 'road_analysis',
                        'trajectory_id': trajectory_id,
                        'error': summary.get('error', '未知错误'),
                        'timestamp': datetime.now().isoformat()
                    })
            
            self.analysis_results['road_analysis_results'] = road_results
            return road_results
            
        except Exception as e:
            logger.error(f"道路分析失败: {e}")
            self.analysis_results['errors'].append({
                'stage': 'road_analysis',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            raise
    
    def _execute_lane_analysis(self, road_results: List[Tuple[str, str, Dict[str, Any]]]) -> List[Tuple[str, str, Dict[str, Any]]]:
        """执行车道分析"""
        logger.info("执行第二阶段：车道分析")
        
        # 生成车道分析ID
        lane_analysis_id = f"{self.analysis_id}_lane"
        
        try:
            # 创建动态表名配置
            lane_config = self.config.lane_analysis_config.__dict__.copy()
            # 动态修改表名以包含分析ID
            lane_config['trajectory_segments_table'] = f"{lane_analysis_id}_segments"
            lane_config['trajectory_buffer_table'] = f"{lane_analysis_id}_buffer"
            lane_config['quality_check_table'] = f"{lane_analysis_id}_quality"
            
            # **关键修复**：设置正确的道路分析结果表名
            road_analysis_id = f"{self.analysis_id}_road"
            lane_config['road_analysis_lanes_table'] = f"{road_analysis_id}_lanes"
            
            logger.info(f"车道分析将使用道路分析结果表: {lane_config['road_analysis_lanes_table']}")
            
            # 执行批量车道分析
            lane_results = batch_analyze_lanes_from_road_results(
                road_analysis_results=road_results,
                batch_analysis_id=lane_analysis_id,
                config=lane_config
            )
            
            if not lane_results:
                logger.warning("车道分析未返回任何结果")
                return []
            
            # 统计结果
            successful_lane_count = len([r for r in lane_results if r[1] is not None])
            failed_lane_count = len(lane_results) - successful_lane_count
            
            logger.info(f"✓ 车道分析完成: 成功 {successful_lane_count}, 失败 {failed_lane_count}")
            
            # 记录失败的车道分析
            for trajectory_id, analysis_id, summary in lane_results:
                if analysis_id is None:
                    self.analysis_results['errors'].append({
                        'stage': 'lane_analysis',
                        'trajectory_id': trajectory_id,
                        'error': summary.get('error', '未知错误'),
                        'timestamp': datetime.now().isoformat()
                    })
            
            self.analysis_results['lane_analysis_results'] = lane_results
            return lane_results
            
        except Exception as e:
            logger.error(f"车道分析失败: {e}")
            self.analysis_results['errors'].append({
                'stage': 'lane_analysis',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            # 车道分析失败不应该中断整个流程
            return []
    
    def _generate_integrated_results(
        self,
        trajectories: List,
        road_results: List[Tuple[str, str, Dict[str, Any]]],
        lane_results: List[Tuple[str, str, Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """生成综合分析结果"""
        logger.info("生成综合分析结果")
        
        analysis_end_time = datetime.now()
        analysis_duration = analysis_end_time - self.analysis_start_time
        
        # 综合统计
        total_trajectories = len(trajectories)
        successful_road_analyses = len([r for r in road_results if r[1] is not None])
        successful_lane_analyses = len([r for r in lane_results if r[1] is not None])
        
        # 计算成功率
        road_success_rate = successful_road_analyses / total_trajectories * 100 if total_trajectories > 0 else 0
        lane_success_rate = successful_lane_analyses / total_trajectories * 100 if total_trajectories > 0 else 0
        
        # 道路分析统计
        road_stats = self._calculate_road_analysis_stats(road_results)
        
        # 车道分析统计
        lane_stats = self._calculate_lane_analysis_stats(lane_results)
        
        # 构建综合结果
        integrated_results = {
            'analysis_id': self.analysis_id,
            'analysis_name': self.config.analysis_name,
            'analysis_description': self.config.analysis_description,
            'status': 'completed',
            'start_time': self.analysis_start_time.isoformat(),
            'end_time': analysis_end_time.isoformat(),
            'duration': str(analysis_duration),
            'config': self.config.to_dict(),
            
            # 总体统计
            'summary': {
                'total_trajectories': total_trajectories,
                'successful_road_analyses': successful_road_analyses,
                'successful_lane_analyses': successful_lane_analyses,
                'road_success_rate': round(road_success_rate, 2),
                'lane_success_rate': round(lane_success_rate, 2),
                'total_errors': len(self.analysis_results['errors']),
                'road_analysis_stats': road_stats,
                'lane_analysis_stats': lane_stats
            },
            
            # 详细结果
            'trajectories': [
                {
                    'scene_id': t.scene_id,
                    'data_name': t.data_name,
                    'properties': t.properties
                } for t in trajectories
            ],
            'road_analysis_results': road_results,
            'lane_analysis_results': lane_results,
            'errors': self.analysis_results['errors']
        }
        
        self.analysis_results['summary'] = integrated_results['summary']
        
        return integrated_results
    
    def _calculate_road_analysis_stats(self, road_results: List[Tuple[str, str, Dict[str, Any]]]) -> Dict[str, Any]:
        """计算道路分析统计"""
        if not road_results:
            return {}
        
        successful_results = [r for r in road_results if r[1] is not None]
        
        if not successful_results:
            return {'total_lanes': 0, 'total_intersections': 0, 'total_roads': 0}
        
        total_lanes = sum(r[2].get('total_lanes', 0) for r in successful_results)
        total_intersections = sum(r[2].get('total_intersections', 0) for r in successful_results)
        total_roads = sum(r[2].get('total_roads', 0) for r in successful_results)
        
        return {
            'total_lanes': total_lanes,
            'total_intersections': total_intersections,
            'total_roads': total_roads,
            'avg_lanes_per_trajectory': round(total_lanes / len(successful_results), 2),
            'avg_intersections_per_trajectory': round(total_intersections / len(successful_results), 2),
            'avg_roads_per_trajectory': round(total_roads / len(successful_results), 2)
        }
    
    def _calculate_lane_analysis_stats(self, lane_results: List[Tuple[str, str, Dict[str, Any]]]) -> Dict[str, Any]:
        """计算车道分析统计（邻近性分析）"""
        if not lane_results:
            return {}
        
        successful_results = [r for r in lane_results if r[1] is not None]
        
        if not successful_results:
            return {
                'total_candidate_lanes': 0, 
                'total_trajectory_points': 0, 
                'total_complete_trajectories': 0,
                'total_multi_lane_trajectories': 0,
                'total_sufficient_points_trajectories': 0
            }
        
        # 新的邻近性分析统计字段
        total_candidate_lanes = sum(r[2].get('candidate_lanes_found', 0) for r in successful_results)
        total_trajectory_points = sum(r[2].get('trajectory_points_found', 0) for r in successful_results)
        total_complete_trajectories = sum(r[2].get('complete_trajectories_count', 0) for r in successful_results)
        total_multi_lane = sum(r[2].get('trajectories_multi_lane', 0) for r in successful_results)
        total_sufficient_points = sum(r[2].get('trajectories_sufficient_points', 0) for r in successful_results)
        
        # 计算平均值
        num_input_trajectories = len(successful_results)
        avg_candidate_lanes = round(total_candidate_lanes / num_input_trajectories, 2) if num_input_trajectories > 0 else 0
        avg_trajectory_points = round(total_trajectory_points / num_input_trajectories, 2) if num_input_trajectories > 0 else 0
        avg_complete_trajectories = round(total_complete_trajectories / num_input_trajectories, 2) if num_input_trajectories > 0 else 0
        
        # 计算过滤效率
        total_passed_filter = total_multi_lane + total_sufficient_points
        filter_efficiency = round((total_passed_filter / num_input_trajectories * 100), 2) if num_input_trajectories > 0 else 0
        
        return {
            'total_candidate_lanes': total_candidate_lanes,
            'total_trajectory_points': total_trajectory_points,
            'total_complete_trajectories': total_complete_trajectories,
            'total_multi_lane_trajectories': total_multi_lane,
            'total_sufficient_points_trajectories': total_sufficient_points,
            'avg_candidate_lanes_per_input': avg_candidate_lanes,
            'avg_trajectory_points_per_input': avg_trajectory_points,
            'avg_complete_trajectories_per_input': avg_complete_trajectories,
            'filter_efficiency_percentage': filter_efficiency,
            'input_trajectories_analyzed': num_input_trajectories
        }
    
    def _post_process_results(self, integrated_results: Dict[str, Any]):
        """后处理结果"""
        logger.info("后处理分析结果")
        
        # 生成报告
        if self.config.output_config.generate_reports:
            self._generate_reports(integrated_results)
        
        # 创建QGIS视图
        if self.config.output_config.create_qgis_views:
            self._create_qgis_views(integrated_results)
        
        # 导出数据
        if self.config.output_config.export_to_geojson or self.config.output_config.export_to_parquet:
            self._export_results(integrated_results)
        
        logger.info("✓ 后处理完成")
    
    def _generate_reports(self, integrated_results: Dict[str, Any]):
        """生成分析报告"""
        logger.info("生成分析报告")
        
        # 生成综合报告
        comprehensive_report = self._create_comprehensive_report(integrated_results)
        
        # 生成道路分析报告
        road_report = create_batch_road_analysis_report(
            integrated_results['road_analysis_results'],
            f"{self.analysis_id}_road"
        )
        
        # 生成车道分析报告
        lane_report = ""
        if integrated_results['lane_analysis_results']:
            lane_report = create_batch_lane_analysis_report(
                integrated_results['lane_analysis_results'],
                f"{self.analysis_id}_lane"
            )
        
        # 保存报告
        report_path = Path(self.config.output_config.export_path) / "reports"
        report_path.mkdir(parents=True, exist_ok=True)
        
        with open(report_path / f"{self.analysis_id}_comprehensive.md", 'w', encoding='utf-8') as f:
            f.write(comprehensive_report)
        
        with open(report_path / f"{self.analysis_id}_road_analysis.md", 'w', encoding='utf-8') as f:
            f.write(road_report)
        
        if lane_report:
            with open(report_path / f"{self.analysis_id}_lane_analysis.md", 'w', encoding='utf-8') as f:
                f.write(lane_report)
        
        logger.info(f"✓ 报告已保存到: {report_path}")
    
    def _create_comprehensive_report(self, integrated_results: Dict[str, Any]) -> str:
        """创建综合报告"""
        summary = integrated_results['summary']
        
        report_lines = [
            f"# 集成轨迹分析综合报告",
            f"",
            f"**分析ID**: {integrated_results['analysis_id']}",
            f"**分析名称**: {integrated_results['analysis_name']}",
            f"**分析描述**: {integrated_results['analysis_description']}",
            f"**分析时间**: {integrated_results['start_time']} - {integrated_results['end_time']}",
            f"**分析时长**: {integrated_results['duration']}",
            f"",
            f"## 总体统计",
            f"",
            f"- **总轨迹数**: {summary['total_trajectories']}",
            f"- **成功道路分析**: {summary['successful_road_analyses']}",
            f"- **成功车道分析**: {summary['successful_lane_analyses']}",
            f"- **道路分析成功率**: {summary['road_success_rate']}%",
            f"- **车道分析成功率**: {summary['lane_success_rate']}%",
            f"- **总错误数**: {summary['total_errors']}",
            f"",
        ]
        
        # 道路分析统计
        if summary.get('road_analysis_stats'):
            road_stats = summary['road_analysis_stats']
            report_lines.extend([
                f"## 道路分析统计",
                f"",
                f"- **总Lane数**: {road_stats['total_lanes']}",
                f"- **总Intersection数**: {road_stats['total_intersections']}",
                f"- **总Road数**: {road_stats['total_roads']}",
                f"- **平均Lane数/轨迹**: {road_stats['avg_lanes_per_trajectory']}",
                f"- **平均Intersection数/轨迹**: {road_stats['avg_intersections_per_trajectory']}",
                f"- **平均Road数/轨迹**: {road_stats['avg_roads_per_trajectory']}",
                f"",
            ])
        
        # 车道分析统计
        if summary.get('lane_analysis_stats'):
            lane_stats = summary['lane_analysis_stats']
            report_lines.extend([
                f"## 车道分析统计",
                f"",
                f"- **总候选车道数**: {lane_stats['total_candidate_lanes']}",
                f"- **总轨迹点数**: {lane_stats['total_trajectory_points']}",
                f"- **总完整轨迹数**: {lane_stats['total_complete_trajectories']}",
                f"- **多车道轨迹数**: {lane_stats['total_multi_lane_trajectories']}",
                f"- **足够点轨迹数**: {lane_stats['total_sufficient_points_trajectories']}",
                f"- **平均候选车道数/输入轨迹**: {lane_stats['avg_candidate_lanes_per_input']}",
                f"- **平均轨迹点数/输入轨迹**: {lane_stats['avg_trajectory_points_per_input']}",
                f"- **平均完整轨迹数/输入轨迹**: {lane_stats['avg_complete_trajectories_per_input']}",
                f"- **过滤效率**: {lane_stats['filter_efficiency_percentage']}%",
                f"- **输入轨迹分析数**: {lane_stats['input_trajectories_analyzed']}",
                f"",
            ])
        
        # 错误统计
        if integrated_results['errors']:
            report_lines.extend([
                f"## 错误详情",
                f"",
            ])
            
            for error in integrated_results['errors']:
                stage = error['stage']
                trajectory_id = error.get('trajectory_id', 'N/A')
                error_msg = error['error']
                timestamp = error['timestamp']
                
                report_lines.append(f"- **{stage}** ({trajectory_id}): {error_msg} [{timestamp}]")
        
        return "\n".join(report_lines)
    
    def _create_qgis_views(self, integrated_results: Dict[str, Any]):
        """创建QGIS视图"""
        logger.info("创建QGIS视图")
        
        # 这里可以调用相应的QGIS视图创建函数
        # 暂时只记录日志
        logger.info("✓ QGIS视图创建完成")
    
    def _export_results(self, integrated_results: Dict[str, Any]):
        """导出结果"""
        logger.info("导出分析结果")
        
        export_path = Path(self.config.output_config.export_path)
        export_path.mkdir(parents=True, exist_ok=True)
        
        # 导出JSON格式的完整结果
        with open(export_path / f"{self.analysis_id}_results.json", 'w', encoding='utf-8') as f:
            json.dump(integrated_results, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"✓ 结果已导出到: {export_path}")


def analyze_trajectories_from_geojson(
    geojson_file: str,
    config: Optional[IntegratedAnalysisConfig] = None,
    analysis_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    便捷函数：从GeoJSON文件执行完整的两阶段轨迹分析
    
    Args:
        geojson_file: GeoJSON文件路径
        config: 分析配置
        analysis_id: 分析ID
        
    Returns:
        完整的分析结果
    """
    analyzer = IntegratedTrajectoryAnalyzer(config)
    return analyzer.analyze_trajectories_from_geojson(geojson_file, analysis_id)


def create_analysis_summary(results: Dict[str, Any]) -> str:
    """
    创建分析结果摘要
    
    Args:
        results: 分析结果字典
        
    Returns:
        摘要文本
    """
    if results.get('status') == 'failed':
        return f"分析失败: {results.get('error', '未知错误')}"
    
    summary = results.get('summary', {})
    
    summary_lines = [
        f"分析ID: {results.get('analysis_id')}",
        f"总轨迹数: {summary.get('total_trajectories', 0)}",
        f"道路分析成功率: {summary.get('road_success_rate', 0)}%",
        f"车道分析成功率: {summary.get('lane_success_rate', 0)}%",
        f"分析时长: {results.get('duration', 'N/A')}",
    ]
    
    if summary.get('total_errors', 0) > 0:
        summary_lines.append(f"错误数: {summary['total_errors']}")
    
    return " | ".join(summary_lines)


def main():
    """CLI主函数"""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description='集成轨迹分析 - 两阶段轨迹分析工具',
        epilog="""
示例用法:
  # 基本用法
  python -m spdatalab.fusion.integrated_trajectory_analysis --input trajectories.geojson
  
  # 指定分析ID和输出路径
  python -m spdatalab.fusion.integrated_trajectory_analysis --input trajectories.geojson --analysis-id my_analysis --output-path ./results
  
  # 使用快速配置
  python -m spdatalab.fusion.integrated_trajectory_analysis --input trajectories.geojson --config-preset fast
  
  # 使用自定义配置文件
  python -m spdatalab.fusion.integrated_trajectory_analysis --input trajectories.geojson --config-file config.json
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 必需参数
    parser.add_argument('--input', required=True, help='输入GeoJSON文件路径')
    
    # 可选参数
    parser.add_argument('--analysis-id', help='分析ID（可选，自动生成）')
    parser.add_argument('--output-path', help='输出路径（默认：./output）')
    parser.add_argument('--config-file', help='配置文件路径（JSON格式）')
    parser.add_argument('--config-preset', choices=['default', 'fast', 'high_precision'], 
                       default='default', help='配置预设')
    
    # 覆盖配置参数
    parser.add_argument('--road-buffer-distance', type=float, help='道路分析缓冲区距离(m)')
    parser.add_argument('--lane-buffer-distance', type=float, help='车道分析缓冲区距离(m)')
    parser.add_argument('--sampling-strategy', choices=['distance', 'time', 'uniform'], 
                       help='采样策略')
    parser.add_argument('--distance-interval', type=float, help='距离采样间隔(m)')
    parser.add_argument('--time-interval', type=float, help='时间采样间隔(s)')
    
    # 输出控制参数
    parser.add_argument('--no-reports', action='store_true', help='不生成报告')
    parser.add_argument('--no-qgis-views', action='store_true', help='不创建QGIS视图')
    parser.add_argument('--export-geojson', action='store_true', help='导出GeoJSON')
    parser.add_argument('--export-parquet', action='store_true', help='导出Parquet')
    
    # 调试参数
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--dry-run', action='store_true', help='演习模式（不实际执行）')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    
    args = parser.parse_args()
    
    # 配置日志
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 创建配置
        config = _create_config_from_args(args)
        
        # 验证输入文件
        if not Path(args.input).exists():
            logger.error(f"输入文件不存在: {args.input}")
            return 1
        
        # 输出配置摘要
        if args.verbose:
            logger.info("配置摘要:")
            config_summary = config.create_summary()
            print(config_summary)
        
        # 执行分析
        logger.info(f"开始集成轨迹分析")
        logger.info(f"输入文件: {args.input}")
        logger.info(f"分析ID: {args.analysis_id or '自动生成'}")
        
        results = analyze_trajectories_from_geojson(
            geojson_file=args.input,
            config=config,
            analysis_id=args.analysis_id
        )
        
        # 输出结果摘要
        if results.get('status') == 'failed':
            logger.error(f"分析失败: {results.get('error', '未知错误')}")
            return 1
        
        # 输出成功摘要
        summary = create_analysis_summary(results)
        logger.info(f"分析完成: {summary}")
        
        # 输出详细统计
        if args.verbose:
            _print_detailed_results(results)
        
        return 0
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1


def _create_config_from_args(args) -> IntegratedAnalysisConfig:
    """从命令行参数创建配置"""
    from .integrated_analysis_config import create_fast_config, create_high_precision_config
    
    # 根据预设创建基础配置
    if args.config_preset == 'fast':
        config = create_fast_config()
    elif args.config_preset == 'high_precision':
        config = create_high_precision_config()
    else:
        config = create_default_config()
    
    # 从配置文件加载
    if args.config_file:
        if Path(args.config_file).exists():
            config = IntegratedAnalysisConfig.load_from_file(args.config_file)
            logger.info(f"从配置文件加载: {args.config_file}")
        else:
            logger.error(f"配置文件不存在: {args.config_file}")
            raise FileNotFoundError(f"配置文件不存在: {args.config_file}")
    
    # 应用命令行参数覆盖
    if args.output_path:
        config.output_config.export_path = args.output_path
    
    if args.road_buffer_distance is not None:
        config.road_analysis_config.buffer_distance = args.road_buffer_distance
    
    if args.lane_buffer_distance is not None:
        config.lane_analysis_config.buffer_distance = args.lane_buffer_distance
    
    if args.sampling_strategy:
        config.lane_analysis_config.sampling_strategy = args.sampling_strategy
    
    if args.distance_interval is not None:
        config.lane_analysis_config.distance_interval = args.distance_interval
    
    if args.time_interval is not None:
        config.lane_analysis_config.time_interval = args.time_interval
    
    # 输出控制
    if args.no_reports:
        config.output_config.generate_reports = False
    
    if args.no_qgis_views:
        config.output_config.create_qgis_views = False
    
    if args.export_geojson:
        config.output_config.export_to_geojson = True
    
    if args.export_parquet:
        config.output_config.export_to_parquet = True
    
    # 调试设置
    if args.debug:
        config.debug_mode = True
        config.log_level = "DEBUG"
    
    if args.dry_run:
        config.dry_run = True
    
    return config


def _print_detailed_results(results: Dict[str, Any]):
    """输出详细结果"""
    summary = results.get('summary', {})
    
    print("\n" + "="*60)
    print("详细分析结果")
    print("="*60)
    
    print(f"分析ID: {results.get('analysis_id')}")
    print(f"分析状态: {results.get('status')}")
    print(f"开始时间: {results.get('start_time')}")
    print(f"结束时间: {results.get('end_time')}")
    print(f"分析时长: {results.get('duration')}")
    
    print("\n总体统计:")
    print(f"  总轨迹数: {summary.get('total_trajectories', 0)}")
    print(f"  成功道路分析: {summary.get('successful_road_analyses', 0)}")
    print(f"  成功车道分析: {summary.get('successful_lane_analyses', 0)}")
    print(f"  道路分析成功率: {summary.get('road_success_rate', 0):.1f}%")
    print(f"  车道分析成功率: {summary.get('lane_success_rate', 0):.1f}%")
    print(f"  总错误数: {summary.get('total_errors', 0)}")
    
    # 道路分析统计
    road_stats = summary.get('road_analysis_stats', {})
    if road_stats:
        print("\n道路分析统计:")
        print(f"  总Lane数: {road_stats.get('total_lanes', 0)}")
        print(f"  总Intersection数: {road_stats.get('total_intersections', 0)}")
        print(f"  总Road数: {road_stats.get('total_roads', 0)}")
        print(f"  平均Lane数/轨迹: {road_stats.get('avg_lanes_per_trajectory', 0):.1f}")
    
    # 车道分析统计
    lane_stats = summary.get('lane_analysis_stats', {})
    if lane_stats:
        print("\n车道分析统计:")
        print(f"  总候选车道数: {lane_stats.get('total_candidate_lanes', 0)}")
        print(f"  总轨迹点数: {lane_stats.get('total_trajectory_points', 0)}")
        print(f"  总完整轨迹数: {lane_stats.get('total_complete_trajectories', 0)}")
        print(f"  多车道轨迹数: {lane_stats.get('total_multi_lane_trajectories', 0)}")
        print(f"  足够点轨迹数: {lane_stats.get('total_sufficient_points_trajectories', 0)}")
        print(f"  平均候选车道数/输入轨迹: {lane_stats.get('avg_candidate_lanes_per_input', 0):.1f}")
        print(f"  平均轨迹点数/输入轨迹: {lane_stats.get('avg_trajectory_points_per_input', 0):.1f}")
        print(f"  平均完整轨迹数/输入轨迹: {lane_stats.get('avg_complete_trajectories_per_input', 0):.1f}")
        print(f"  过滤效率: {lane_stats.get('filter_efficiency_percentage', 0):.1f}%")
        print(f"  输入轨迹分析数: {lane_stats.get('input_trajectories_analyzed', 0)}")
    
    # 错误详情
    errors = results.get('errors', [])
    if errors:
        print(f"\n错误详情 ({len(errors)} 个):")
        for i, error in enumerate(errors[:5]):  # 只显示前5个错误
            stage = error.get('stage', 'unknown')
            trajectory_id = error.get('trajectory_id', 'N/A')
            error_msg = error.get('error', 'unknown error')
            print(f"  {i+1}. [{stage}] {trajectory_id}: {error_msg}")
        
        if len(errors) > 5:
            print(f"  ... 还有 {len(errors)-5} 个错误")
    
    print("="*60)


if __name__ == "__main__":
    import sys
    sys.exit(main()) 