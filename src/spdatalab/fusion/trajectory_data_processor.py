"""
轨迹数据处理模块

负责GeoJSON格式的轨迹数据加载、验证和预处理，
为综合分析提供标准化的轨迹数据结构。

功能：
1. GeoJSON格式规范定义
2. 数据验证和错误处理
3. 轨迹数据预处理和标准化
4. 缺失字段的默认值处理
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, field
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
from shapely.geometry.base import BaseGeometry
from datetime import datetime
import jsonschema
from jsonschema import validate, ValidationError

from .trajectory_integrated_analysis import TrajectoryInfo

logger = logging.getLogger(__name__)

# GeoJSON轨迹数据格式规范
TRAJECTORY_GEOJSON_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "enum": ["FeatureCollection"]
        },
        "features": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Feature"]
                    },
                    "geometry": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["LineString"]
                            },
                            "coordinates": {
                                "type": "array",
                                "minItems": 2,
                                "items": {
                                    "type": "array",
                                    "minItems": 2,
                                    "maxItems": 3,
                                    "items": {
                                        "type": "number"
                                    }
                                }
                            }
                        },
                        "required": ["type", "coordinates"]
                    },
                    "properties": {
                        "type": "object",
                        "properties": {
                            # 必需字段
                            "scene_id": {
                                "type": "string",
                                "minLength": 1
                            },
                            "data_name": {
                                "type": "string",
                                "minLength": 1
                            },
                            # 可选字段 - 时间相关
                            "start_time": {
                                "type": ["integer", "null"]
                            },
                            "end_time": {
                                "type": ["integer", "null"]
                            },
                            "duration": {
                                "type": ["integer", "null"],
                                "minimum": 0
                            },
                            # 可选字段 - 速度相关
                            "avg_speed": {
                                "type": ["number", "null"],
                                "minimum": 0
                            },
                            "max_speed": {
                                "type": ["number", "null"],
                                "minimum": 0
                            },
                            "min_speed": {
                                "type": ["number", "null"],
                                "minimum": 0
                            },
                            "std_speed": {
                                "type": ["number", "null"],
                                "minimum": 0
                            },
                            # 可选字段 - AVP相关
                            "avp_ratio": {
                                "type": ["number", "null"],
                                "minimum": 0,
                                "maximum": 1
                            }
                        },
                        "required": ["scene_id", "data_name"],
                        "additionalProperties": True  # 允许额外字段
                    }
                },
                "required": ["type", "geometry", "properties"]
            }
        }
    },
    "required": ["type", "features"]
}

@dataclass
class ValidationResult:
    """数据验证结果"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    total_features: int = 0
    valid_features: int = 0
    
    def add_error(self, error: str):
        """添加错误"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """添加警告"""
        self.warnings.append(warning)
    
    def get_summary(self) -> str:
        """获取验证结果摘要"""
        if self.is_valid:
            return f"验证成功: {self.valid_features}/{self.total_features} 个轨迹有效"
        else:
            return f"验证失败: {len(self.errors)} 个错误, {len(self.warnings)} 个警告"

class TrajectoryDataProcessor:
    """
    轨迹数据处理器
    
    负责GeoJSON格式的轨迹数据加载、验证和预处理
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def load_geojson(self, geojson_path: Union[str, Path]) -> gpd.GeoDataFrame:
        """
        加载GeoJSON文件
        
        Args:
            geojson_path: GeoJSON文件路径
            
        Returns:
            GeoDataFrame对象
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 文件格式错误
        """
        geojson_path = Path(geojson_path)
        
        if not geojson_path.exists():
            raise FileNotFoundError(f"GeoJSON文件不存在: {geojson_path}")
        
        try:
            # 使用geopandas加载GeoJSON
            gdf = gpd.read_file(geojson_path)
            
            self.logger.info(f"成功加载GeoJSON文件: {geojson_path}")
            self.logger.info(f"包含 {len(gdf)} 个轨迹feature")
            
            return gdf
            
        except Exception as e:
            raise ValueError(f"加载GeoJSON文件失败: {geojson_path}, 错误: {str(e)}")
    
    def validate_geojson_schema(self, geojson_data: Dict[str, Any]) -> ValidationResult:
        """
        验证GeoJSON数据格式
        
        Args:
            geojson_data: GeoJSON数据字典
            
        Returns:
            验证结果对象
        """
        result = ValidationResult()
        
        try:
            # 使用jsonschema验证
            validate(instance=geojson_data, schema=TRAJECTORY_GEOJSON_SCHEMA)
            
            # 统计feature数量
            features = geojson_data.get("features", [])
            result.total_features = len(features)
            result.valid_features = len(features)
            result.is_valid = True
            
            self.logger.info(f"GeoJSON格式验证成功: {result.total_features} 个feature")
            
        except ValidationError as e:
            result.add_error(f"JSON Schema验证失败: {e.message}")
            self.logger.error(f"GeoJSON格式验证失败: {e.message}")
            
        except Exception as e:
            result.add_error(f"验证过程出错: {str(e)}")
            self.logger.error(f"验证过程出错: {str(e)}")
        
        return result
    
    def validate_trajectory_features(self, gdf: gpd.GeoDataFrame) -> ValidationResult:
        """
        验证轨迹feature的业务逻辑
        
        Args:
            gdf: 轨迹GeoDataFrame
            
        Returns:
            验证结果对象
        """
        result = ValidationResult()
        result.total_features = len(gdf)
        
        valid_count = 0
        
        for idx, row in gdf.iterrows():
            feature_errors = []
            feature_warnings = []
            
            # 检查几何类型
            if not isinstance(row.geometry, LineString):
                feature_errors.append(f"Feature {idx}: 几何类型必须是LineString")
                continue
            
            # 检查几何有效性
            if not row.geometry.is_valid:
                feature_errors.append(f"Feature {idx}: 几何对象无效")
                continue
            
            # 检查坐标点数量
            if len(row.geometry.coords) < 2:
                feature_errors.append(f"Feature {idx}: LineString至少需要2个坐标点")
                continue
            
            # 检查必需字段
            if 'scene_id' not in row or pd.isna(row['scene_id']) or str(row['scene_id']).strip() == '':
                feature_errors.append(f"Feature {idx}: 缺少有效的scene_id")
                continue
            
            if 'data_name' not in row or pd.isna(row['data_name']) or str(row['data_name']).strip() == '':
                feature_errors.append(f"Feature {idx}: 缺少有效的data_name")
                continue
            
            # 检查可选字段的合理性
            if 'start_time' in row and pd.notna(row['start_time']):
                if not isinstance(row['start_time'], (int, float)) or row['start_time'] < 0:
                    feature_warnings.append(f"Feature {idx}: start_time应该是非负数值")
            
            if 'end_time' in row and pd.notna(row['end_time']):
                if not isinstance(row['end_time'], (int, float)) or row['end_time'] < 0:
                    feature_warnings.append(f"Feature {idx}: end_time应该是非负数值")
            
            # 检查时间逻辑
            if ('start_time' in row and pd.notna(row['start_time']) and 
                'end_time' in row and pd.notna(row['end_time'])):
                if row['end_time'] <= row['start_time']:
                    feature_warnings.append(f"Feature {idx}: end_time应该大于start_time")
            
            # 检查速度字段
            speed_fields = ['avg_speed', 'max_speed', 'min_speed', 'std_speed']
            for field in speed_fields:
                if field in row and pd.notna(row[field]):
                    if not isinstance(row[field], (int, float)) or row[field] < 0:
                        feature_warnings.append(f"Feature {idx}: {field}应该是非负数值")
            
            # 检查AVP比例
            if 'avp_ratio' in row and pd.notna(row['avp_ratio']):
                if not isinstance(row['avp_ratio'], (int, float)) or not (0 <= row['avp_ratio'] <= 1):
                    feature_warnings.append(f"Feature {idx}: avp_ratio应该在0-1之间")
            
            # 如果有错误，记录并跳过
            if feature_errors:
                for error in feature_errors:
                    result.add_error(error)
            else:
                valid_count += 1
                
                # 记录警告
                for warning in feature_warnings:
                    result.add_warning(warning)
        
        result.valid_features = valid_count
        result.is_valid = valid_count > 0
        
        self.logger.info(f"轨迹feature验证完成: {valid_count}/{result.total_features} 个有效")
        
        return result
    
    def preprocess_trajectory_data(self, gdf: gpd.GeoDataFrame) -> List[TrajectoryInfo]:
        """
        预处理轨迹数据
        
        Args:
            gdf: 轨迹GeoDataFrame
            
        Returns:
            TrajectoryInfo列表
        """
        trajectories = []
        
        for idx, row in gdf.iterrows():
            try:
                # 基础验证
                if not isinstance(row.geometry, LineString):
                    self.logger.warning(f"跳过非LineString几何: 索引 {idx}")
                    continue
                
                if pd.isna(row['scene_id']) or pd.isna(row['data_name']):
                    self.logger.warning(f"跳过缺少必需字段的feature: 索引 {idx}")
                    continue
                
                # 创建TrajectoryInfo对象
                trajectory_info = TrajectoryInfo(
                    scene_id=str(row['scene_id']).strip(),
                    data_name=str(row['data_name']).strip(),
                    geometry=row.geometry
                )
                
                # 处理可选字段
                optional_fields = {
                    'start_time': int,
                    'end_time': int,
                    'duration': int,
                    'avg_speed': float,
                    'max_speed': float,
                    'min_speed': float,
                    'std_speed': float,
                    'avp_ratio': float
                }
                
                for field, field_type in optional_fields.items():
                    if field in row and pd.notna(row[field]):
                        try:
                            setattr(trajectory_info, field, field_type(row[field]))
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"Feature {idx} {field}字段类型转换失败: {str(e)}")
                
                # 处理额外属性
                standard_fields = {'scene_id', 'data_name', 'geometry'} | set(optional_fields.keys())
                extra_properties = {}
                
                for col in gdf.columns:
                    if col not in standard_fields and col in row:
                        if pd.notna(row[col]):
                            extra_properties[col] = row[col]
                
                trajectory_info.properties = extra_properties
                
                trajectories.append(trajectory_info)
                
            except Exception as e:
                self.logger.error(f"预处理feature {idx} 失败: {str(e)}")
                continue
        
        self.logger.info(f"预处理完成: {len(trajectories)} 个轨迹")
        return trajectories
    
    def process_geojson_file(self, geojson_path: Union[str, Path]) -> Tuple[List[TrajectoryInfo], ValidationResult]:
        """
        处理GeoJSON文件的完整流程
        
        Args:
            geojson_path: GeoJSON文件路径
            
        Returns:
            (轨迹信息列表, 验证结果)
        """
        self.logger.info(f"开始处理GeoJSON文件: {geojson_path}")
        
        # 1. 加载GeoJSON文件
        gdf = self.load_geojson(geojson_path)
        
        # 2. 验证数据格式
        validation_result = self.validate_trajectory_features(gdf)
        
        # 3. 预处理轨迹数据
        trajectories = self.preprocess_trajectory_data(gdf)
        
        # 4. 更新验证结果
        validation_result.valid_features = len(trajectories)
        validation_result.is_valid = len(trajectories) > 0
        
        self.logger.info(f"GeoJSON文件处理完成: {len(trajectories)} 个有效轨迹")
        
        return trajectories, validation_result
    
    def create_sample_geojson(self, output_path: Union[str, Path]) -> str:
        """
        创建示例GeoJSON文件
        
        Args:
            output_path: 输出文件路径
            
        Returns:
            生成的文件路径
        """
        sample_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [116.3974, 39.9093],
                            [116.3984, 39.9103],
                            [116.3994, 39.9113],
                            [116.4004, 39.9123],
                            [116.4014, 39.9133]
                        ]
                    },
                    "properties": {
                        "scene_id": "scene_001",
                        "data_name": "trajectory_001",
                        "start_time": 1234567890,
                        "end_time": 1234567950,
                        "duration": 60,
                        "avg_speed": 15.5,
                        "max_speed": 25.0,
                        "min_speed": 5.0,
                        "std_speed": 3.2,
                        "avp_ratio": 0.8
                    }
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [116.4014, 39.9133],
                            [116.4024, 39.9143],
                            [116.4034, 39.9153],
                            [116.4044, 39.9163]
                        ]
                    },
                    "properties": {
                        "scene_id": "scene_002",
                        "data_name": "trajectory_002",
                        "start_time": 1234567960,
                        "end_time": 1234568020,
                        "duration": 60,
                        "avg_speed": 20.0,
                        "max_speed": 30.0,
                        "min_speed": 10.0,
                        "std_speed": 4.5,
                        "avp_ratio": 0.6
                    }
                }
            ]
        }
        
        output_path = Path(output_path)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"示例GeoJSON文件已创建: {output_path}")
        return str(output_path)
    
    def get_schema_documentation(self) -> str:
        """
        获取数据格式文档
        
        Returns:
            格式说明文档
        """
        doc = """
# 轨迹数据GeoJSON格式规范

## 基本结构
```json
{
  "type": "FeatureCollection",
  "features": [...]
}
```

## Feature结构
```json
{
  "type": "Feature",
  "geometry": {
    "type": "LineString",
    "coordinates": [[lon1, lat1], [lon2, lat2], ...]
  },
  "properties": {
    "scene_id": "必需字段",
    "data_name": "必需字段",
    "start_time": "可选，整数时间戳",
    "end_time": "可选，整数时间戳",
    "duration": "可选，持续时间(秒)",
    "avg_speed": "可选，平均速度",
    "max_speed": "可选，最大速度",
    "min_speed": "可选，最小速度",
    "std_speed": "可选，速度标准差",
    "avp_ratio": "可选，AVP比例(0-1)",
    "其他字段": "可选，任意额外属性"
  }
}
```

## 字段说明

### 必需字段
- **scene_id**: 场景ID，字符串，不能为空
- **data_name**: 数据名称，字符串，不能为空

### 可选字段
- **start_time**: 开始时间，整数时间戳，非负数
- **end_time**: 结束时间，整数时间戳，非负数，应大于start_time
- **duration**: 持续时间，整数秒数，非负数
- **avg_speed**: 平均速度，浮点数，非负数
- **max_speed**: 最大速度，浮点数，非负数
- **min_speed**: 最小速度，浮点数，非负数
- **std_speed**: 速度标准差，浮点数，非负数
- **avp_ratio**: AVP比例，浮点数，取值范围0-1

### 几何要求
- **geometry.type**: 必须是"LineString"
- **geometry.coordinates**: 至少包含2个坐标点
- 坐标格式: [经度, 纬度] 或 [经度, 纬度, 高程]

## 使用示例

从trajectory.py生成的轨迹表导出GeoJSON：

```sql
-- 导出轨迹数据为GeoJSON格式
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(
        jsonb_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geometry)::jsonb,
            'properties', jsonb_build_object(
                'scene_id', scene_id,
                'data_name', data_name,
                'start_time', start_time,
                'end_time', end_time,
                'duration', duration,
                'avg_speed', avg_speed,
                'max_speed', max_speed,
                'min_speed', min_speed,
                'std_speed', std_speed,
                'avp_ratio', avp_ratio
            )
        )
    )
) as geojson
FROM your_trajectory_table
WHERE geometry IS NOT NULL;
```
"""
        return doc

# 便捷函数
def load_trajectories_from_geojson(geojson_path: Union[str, Path]) -> List[TrajectoryInfo]:
    """
    从GeoJSON文件加载轨迹数据
    
    Args:
        geojson_path: GeoJSON文件路径
        
    Returns:
        轨迹信息列表
    """
    processor = TrajectoryDataProcessor()
    trajectories, validation_result = processor.process_geojson_file(geojson_path)
    
    if not validation_result.is_valid:
        logger.warning(f"数据验证有问题: {validation_result.get_summary()}")
        for error in validation_result.errors[:5]:  # 只显示前5个错误
            logger.error(error)
    
    return trajectories

def validate_geojson_file(geojson_path: Union[str, Path]) -> ValidationResult:
    """
    验证GeoJSON文件格式
    
    Args:
        geojson_path: GeoJSON文件路径
        
    Returns:
        验证结果
    """
    processor = TrajectoryDataProcessor()
    
    try:
        gdf = processor.load_geojson(geojson_path)
        return processor.validate_trajectory_features(gdf)
    except Exception as e:
        result = ValidationResult()
        result.add_error(f"文件加载失败: {str(e)}")
        return result

if __name__ == "__main__":
    # 测试示例
    import argparse
    
    parser = argparse.ArgumentParser(description='轨迹数据处理器测试')
    parser.add_argument('--create-sample', help='创建示例GeoJSON文件')
    parser.add_argument('--validate', help='验证GeoJSON文件')
    parser.add_argument('--process', help='处理GeoJSON文件')
    parser.add_argument('--schema-doc', action='store_true', help='显示格式文档')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    processor = TrajectoryDataProcessor()
    
    if args.create_sample:
        output_path = processor.create_sample_geojson(args.create_sample)
        print(f"示例文件已创建: {output_path}")
    
    elif args.validate:
        result = validate_geojson_file(args.validate)
        print(f"验证结果: {result.get_summary()}")
        
        if result.errors:
            print("错误列表:")
            for error in result.errors:
                print(f"  - {error}")
        
        if result.warnings:
            print("警告列表:")
            for warning in result.warnings:
                print(f"  - {warning}")
    
    elif args.process:
        trajectories, result = processor.process_geojson_file(args.process)
        print(f"处理结果: {len(trajectories)} 个轨迹")
        print(f"验证结果: {result.get_summary()}")
        
        # 显示前几个轨迹的信息
        for i, traj in enumerate(trajectories[:3]):
            print(f"轨迹 {i+1}: {traj.scene_id} ({traj.data_name})")
            print(f"  坐标点数: {len(traj.geometry.coords)}")
            if traj.avg_speed is not None:
                print(f"  平均速度: {traj.avg_speed}")
    
    elif args.schema_doc:
        print(processor.get_schema_documentation())
    
    else:
        print("请指定操作参数，使用 --help 查看帮助") 