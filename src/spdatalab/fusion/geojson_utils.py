"""
GeoJSON工具模块

功能：
1. 解析geojson文件，提取轨迹信息
2. 验证必要的字段（scene_id、data_name、geometry）
3. 返回适合轨迹分析模块使用的数据结构
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, LineString
from shapely.wkt import dumps as wkt_dumps

logger = logging.getLogger(__name__)

@dataclass
class TrajectoryRecord:
    """轨迹记录数据结构"""
    scene_id: str
    data_name: str
    geometry_wkt: str
    properties: Dict[str, Any]
    
    def __str__(self):
        return f"TrajectoryRecord(scene_id={self.scene_id}, data_name={self.data_name})"

class GeoJSONTrajectoryLoader:
    """GeoJSON轨迹加载器"""
    
    def __init__(self):
        self.required_fields = ['scene_id', 'data_name']
    
    def load_trajectories_from_geojson(self, geojson_file: str) -> List[TrajectoryRecord]:
        """
        从GeoJSON文件加载轨迹记录
        
        Args:
            geojson_file: GeoJSON文件路径
            
        Returns:
            轨迹记录列表
        """
        logger.info(f"加载GeoJSON文件: {geojson_file}")
        
        try:
            # 使用geopandas加载GeoJSON
            gdf = gpd.read_file(geojson_file)
            
            if gdf.empty:
                logger.warning(f"GeoJSON文件为空: {geojson_file}")
                return []
            
            logger.info(f"加载到 {len(gdf)} 个几何要素")
            
            # 验证必要字段
            missing_fields = self._validate_required_fields(gdf)
            if missing_fields:
                raise ValueError(f"缺少必要字段: {missing_fields}")
            
            # 验证几何类型
            valid_records = self._validate_geometry_types(gdf)
            
            # 转换为TrajectoryRecord列表
            trajectories = []
            for idx, row in valid_records.iterrows():
                try:
                    # 转换几何为WKT
                    geometry_wkt = wkt_dumps(row.geometry)
                    
                    # 提取属性
                    properties = row.drop('geometry').to_dict()
                    
                    # 创建轨迹记录
                    trajectory = TrajectoryRecord(
                        scene_id=str(row['scene_id']),
                        data_name=str(row['data_name']),
                        geometry_wkt=geometry_wkt,
                        properties=properties
                    )
                    
                    trajectories.append(trajectory)
                    logger.debug(f"加载轨迹: {trajectory}")
                    
                except Exception as e:
                    logger.error(f"处理轨迹记录失败 (索引 {idx}): {e}")
                    continue
            
            logger.info(f"成功加载 {len(trajectories)} 条轨迹记录")
            return trajectories
            
        except Exception as e:
            logger.error(f"加载GeoJSON文件失败: {e}")
            raise
    
    def _validate_required_fields(self, gdf: gpd.GeoDataFrame) -> List[str]:
        """验证必要字段是否存在"""
        missing_fields = []
        
        for field in self.required_fields:
            if field not in gdf.columns:
                missing_fields.append(field)
        
        if missing_fields:
            logger.error(f"缺少必要字段: {missing_fields}")
            logger.info(f"可用字段: {list(gdf.columns)}")
        
        return missing_fields
    
    def _validate_geometry_types(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """验证几何类型，只保留LineString"""
        original_count = len(gdf)
        
        # 过滤出LineString几何
        linestring_mask = gdf.geometry.geom_type == 'LineString'
        valid_gdf = gdf[linestring_mask].copy()
        
        # 统计过滤结果
        filtered_count = original_count - len(valid_gdf)
        if filtered_count > 0:
            logger.warning(f"过滤掉 {filtered_count} 个非LineString几何")
        
        # 检查几何有效性
        invalid_mask = ~valid_gdf.geometry.is_valid
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            logger.warning(f"发现 {invalid_count} 个无效几何，尝试修复")
            # 尝试修复无效几何
            valid_gdf.loc[invalid_mask, 'geometry'] = valid_gdf.loc[invalid_mask, 'geometry'].apply(
                lambda geom: geom.buffer(0) if not geom.is_valid else geom
            )
        
        logger.info(f"几何验证完成: {len(valid_gdf)}/{original_count} 个有效轨迹")
        return valid_gdf
    
    def create_summary_report(self, trajectories: List[TrajectoryRecord]) -> str:
        """创建轨迹加载汇总报告"""
        if not trajectories:
            return "没有加载到任何轨迹记录"
        
        # 统计信息
        total_count = len(trajectories)
        unique_scenes = len(set(t.scene_id for t in trajectories))
        unique_data_names = len(set(t.data_name for t in trajectories))
        
        # 几何统计
        geometry_lengths = []
        for t in trajectories:
            try:
                from shapely.wkt import loads
                geom = loads(t.geometry_wkt)
                if isinstance(geom, LineString):
                    geometry_lengths.append(geom.length)
            except:
                pass
        
        # 构建报告
        report_lines = [
            "# GeoJSON轨迹加载汇总报告",
            "",
            f"**总轨迹数**: {total_count}",
            f"**唯一scene_id数**: {unique_scenes}",
            f"**唯一data_name数**: {unique_data_names}",
            "",
            "## 几何统计",
        ]
        
        if geometry_lengths:
            import numpy as np
            report_lines.extend([
                f"- 平均长度: {np.mean(geometry_lengths):.2f}",
                f"- 最大长度: {np.max(geometry_lengths):.2f}",
                f"- 最小长度: {np.min(geometry_lengths):.2f}",
            ])
        
        # 示例轨迹
        report_lines.extend([
            "",
            "## 示例轨迹",
            ""
        ])
        
        for i, trajectory in enumerate(trajectories[:3]):
            report_lines.extend([
                f"### 轨迹 {i+1}",
                f"- Scene ID: {trajectory.scene_id}",
                f"- Data Name: {trajectory.data_name}",
                f"- 几何长度: {len(trajectory.geometry_wkt)} 字符",
                ""
            ])
        
        return "\n".join(report_lines)

def load_trajectories_from_geojson(geojson_file: str) -> List[TrajectoryRecord]:
    """
    便捷函数：从GeoJSON文件加载轨迹记录
    
    Args:
        geojson_file: GeoJSON文件路径
        
    Returns:
        轨迹记录列表
    """
    loader = GeoJSONTrajectoryLoader()
    return loader.load_trajectories_from_geojson(geojson_file)

def validate_geojson_format(geojson_file: str) -> Tuple[bool, List[str]]:
    """
    验证GeoJSON文件格式是否符合要求
    
    Args:
        geojson_file: GeoJSON文件路径
        
    Returns:
        (是否有效, 错误信息列表)
    """
    errors = []
    
    try:
        # 检查文件是否存在
        if not Path(geojson_file).exists():
            errors.append(f"文件不存在: {geojson_file}")
            return False, errors
        
        # 加载并验证
        loader = GeoJSONTrajectoryLoader()
        
        # 尝试加载
        gdf = gpd.read_file(geojson_file)
        
        if gdf.empty:
            errors.append("文件为空")
            return False, errors
        
        # 验证必要字段
        missing_fields = loader._validate_required_fields(gdf)
        if missing_fields:
            errors.append(f"缺少必要字段: {missing_fields}")
        
        # 验证几何类型
        linestring_count = (gdf.geometry.geom_type == 'LineString').sum()
        if linestring_count == 0:
            errors.append("没有找到LineString几何")
        elif linestring_count < len(gdf):
            errors.append(f"只有 {linestring_count}/{len(gdf)} 个LineString几何")
        
        # 验证几何有效性
        invalid_count = (~gdf.geometry.is_valid).sum()
        if invalid_count > 0:
            errors.append(f"发现 {invalid_count} 个无效几何")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        errors.append(f"文件格式验证失败: {e}")
        return False, errors

def create_sample_geojson(output_file: str, sample_trajectories: List[Dict[str, Any]]) -> bool:
    """
    创建示例GeoJSON文件
    
    Args:
        output_file: 输出文件路径
        sample_trajectories: 示例轨迹列表
        
    Returns:
        创建是否成功
    """
    try:
        # 构建GeoJSON结构
        geojson_data = {
            "type": "FeatureCollection",
            "features": []
        }
        
        for i, traj in enumerate(sample_trajectories):
            feature = {
                "type": "Feature",
                "properties": {
                    "scene_id": traj.get("scene_id", f"sample_scene_{i}"),
                    "data_name": traj.get("data_name", f"sample_data_{i}")
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": traj.get("coordinates", [
                        [116.3 + i * 0.01, 39.9 + i * 0.01],
                        [116.31 + i * 0.01, 39.91 + i * 0.01],
                        [116.32 + i * 0.01, 39.92 + i * 0.01]
                    ])
                }
            }
            geojson_data["features"].append(feature)
        
        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"创建示例GeoJSON文件: {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"创建示例GeoJSON文件失败: {e}")
        return False

if __name__ == "__main__":
    # 测试用例
    logging.basicConfig(level=logging.INFO)
    
    # 创建示例数据
    sample_data = [
        {
            "scene_id": "test_scene_1",
            "data_name": "test_data_1",
            "coordinates": [[116.3, 39.9], [116.31, 39.91], [116.32, 39.92]]
        },
        {
            "scene_id": "test_scene_2", 
            "data_name": "test_data_2",
            "coordinates": [[116.4, 39.8], [116.41, 39.81], [116.42, 39.82]]
        }
    ]
    
    # 创建示例文件
    sample_file = "sample_trajectories.geojson"
    if create_sample_geojson(sample_file, sample_data):
        print(f"创建示例文件: {sample_file}")
        
        # 验证文件格式
        is_valid, errors = validate_geojson_format(sample_file)
        print(f"文件格式验证: {'通过' if is_valid else '失败'}")
        if errors:
            for error in errors:
                print(f"  - {error}")
        
        # 加载轨迹
        trajectories = load_trajectories_from_geojson(sample_file)
        print(f"加载轨迹数: {len(trajectories)}")
        
        # 生成报告
        loader = GeoJSONTrajectoryLoader()
        report = loader.create_summary_report(trajectories)
        print(report) 