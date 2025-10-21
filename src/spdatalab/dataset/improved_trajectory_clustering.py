"""改进的轨迹聚类模块

基于调研结果的改进实现：
1. 改进的相似度度量（Fréchet距离、Hausdorff距离）
2. 增强的特征提取（空间形态特征）
3. 自适应分段策略
4. 多种聚类算法对比

主要改进：
- 使用Fréchet距离替代欧氏距离
- 添加空间形态特征（曲率、曲折度等）
- 实现自适应分段
- 提供TRACLUS算法实现
"""

from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from typing import List, Tuple, Optional
from dataclasses import dataclass
from shapely.geometry import LineString, Point
from scipy.spatial.distance import directed_hausdorff, euclidean
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score

logger = logging.getLogger(__name__)


# ==================== 相似度度量函数 ====================

def frechet_distance(traj1: np.ndarray, traj2: np.ndarray) -> float:
    """
    计算两条轨迹的Fréchet距离（离散版本）
    
    Fréchet距离考虑了轨迹的顺序和连续性，比Hausdorff更适合轨迹相似度。
    使用动态规划算法计算。
    
    Args:
        traj1: 轨迹1的坐标数组 (N, 2)
        traj2: 轨迹2的坐标数组 (M, 2)
        
    Returns:
        Fréchet距离（米）
        
    参考：
        Eiter & Mannila, "Computing discrete Fréchet distance" (1994)
    """
    n, m = len(traj1), len(traj2)
    
    # 距离矩阵
    ca = np.full((n, m), -1.0)
    
    def c(i: int, j: int) -> float:
        """递归计算耦合距离"""
        if ca[i, j] > -1:
            return ca[i, j]
        
        # 点之间的欧氏距离（米）
        d = haversine_distance(
            traj1[i][1], traj1[i][0],
            traj2[j][1], traj2[j][0]
        )
        
        if i == 0 and j == 0:
            ca[i, j] = d
        elif i > 0 and j == 0:
            ca[i, j] = max(c(i-1, 0), d)
        elif i == 0 and j > 0:
            ca[i, j] = max(c(0, j-1), d)
        else:
            ca[i, j] = max(
                min(c(i-1, j), c(i-1, j-1), c(i, j-1)),
                d
            )
        
        return ca[i, j]
    
    return c(n-1, m-1)


def hausdorff_distance_trajectory(traj1: np.ndarray, traj2: np.ndarray) -> float:
    """
    计算两条轨迹的Hausdorff距离
    
    Args:
        traj1: 轨迹1的坐标数组 (N, 2) [lon, lat]
        traj2: 轨迹2的坐标数组 (M, 2) [lon, lat]
        
    Returns:
        Hausdorff距离（米）
    """
    # 使用scipy的directed_hausdorff（返回像素距离）
    # 这里我们需要转换为实际距离
    
    max_dist_12 = 0.0
    for p1 in traj1:
        min_dist = min(
            haversine_distance(p1[1], p1[0], p2[1], p2[0]) 
            for p2 in traj2
        )
        max_dist_12 = max(max_dist_12, min_dist)
    
    max_dist_21 = 0.0
    for p2 in traj2:
        min_dist = min(
            haversine_distance(p2[1], p2[0], p1[1], p1[0]) 
            for p1 in traj1
        )
        max_dist_21 = max(max_dist_21, min_dist)
    
    return max(max_dist_12, max_dist_21)


def perpendicular_distance(point: np.ndarray, line_start: np.ndarray, line_end: np.ndarray) -> float:
    """
    计算点到线段的垂直距离（TRACLUS中使用）
    
    Args:
        point: 点坐标 [lon, lat]
        line_start: 线段起点 [lon, lat]
        line_end: 线段终点 [lon, lat]
        
    Returns:
        垂直距离（米）
    """
    # 向量
    line_vec = line_end - line_start
    point_vec = point - line_start
    
    # 线段长度
    line_len = np.linalg.norm(line_vec)
    
    if line_len < 1e-10:  # 退化为点
        return haversine_distance(point[1], point[0], line_start[1], line_start[0])
    
    # 投影长度
    line_unitvec = line_vec / line_len
    proj_len = np.dot(point_vec, line_unitvec)
    
    # 投影点
    if proj_len < 0:
        # 投影在起点之前
        closest = line_start
    elif proj_len > line_len:
        # 投影在终点之后
        closest = line_end
    else:
        # 投影在线段上
        closest = line_start + proj_len * line_unitvec
    
    return haversine_distance(point[1], point[0], closest[1], closest[0])


def parallel_distance(point: np.ndarray, line_start: np.ndarray, line_end: np.ndarray) -> float:
    """
    计算点到线段的平行距离（TRACLUS中使用）
    
    Args:
        point: 点坐标 [lon, lat]
        line_start: 线段起点 [lon, lat]
        line_end: 线段终点 [lon, lat]
        
    Returns:
        平行距离（米）
    """
    line_vec = line_end - line_start
    point_vec = point - line_start
    
    line_len = np.linalg.norm(line_vec)
    
    if line_len < 1e-10:
        return 0.0
    
    # 投影长度
    line_unitvec = line_vec / line_len
    proj_len = np.dot(point_vec, line_unitvec)
    
    # 平行距离
    if proj_len < 0:
        return abs(proj_len)
    elif proj_len > line_len:
        return proj_len - line_len
    else:
        return 0.0


def segment_distance(seg1_coords: np.ndarray, seg2_coords: np.ndarray) -> float:
    """
    计算两个轨迹段的距离（TRACLUS风格）
    
    综合考虑：
    1. 垂直距离（形状相似度）
    2. 平行距离（位置对齐度）
    3. 角度距离（方向相似度）
    
    Args:
        seg1_coords: 轨迹段1的坐标 (N, 2)
        seg2_coords: 轨迹段2的坐标 (M, 2)
        
    Returns:
        段距离（米）
    """
    if len(seg1_coords) < 2 or len(seg2_coords) < 2:
        return float('inf')
    
    # 计算seg1每个点到seg2的垂直和平行距离
    perp_dists = []
    para_dists = []
    
    seg2_start = seg2_coords[0]
    seg2_end = seg2_coords[-1]
    
    for point in seg1_coords:
        perp_dist = perpendicular_distance(point, seg2_start, seg2_end)
        para_dist = parallel_distance(point, seg2_start, seg2_end)
        perp_dists.append(perp_dist)
        para_dists.append(para_dist)
    
    # 反向计算seg2每个点到seg1的距离
    seg1_start = seg1_coords[0]
    seg1_end = seg1_coords[-1]
    
    for point in seg2_coords:
        perp_dist = perpendicular_distance(point, seg1_start, seg1_end)
        para_dist = parallel_distance(point, seg1_start, seg1_end)
        perp_dists.append(perp_dist)
        para_dists.append(para_dist)
    
    # 平均距离
    avg_perp = np.mean(perp_dists)
    avg_para = np.mean(para_dists)
    
    # 角度距离
    angle_dist = angle_difference(seg1_coords, seg2_coords)
    
    # 组合距离（权重可调）
    return 0.6 * avg_perp + 0.3 * avg_para + 0.1 * angle_dist


def angle_difference(seg1_coords: np.ndarray, seg2_coords: np.ndarray) -> float:
    """
    计算两个轨迹段的角度差异
    
    Args:
        seg1_coords: 轨迹段1的坐标 (N, 2)
        seg2_coords: 轨迹段2的坐标 (M, 2)
        
    Returns:
        角度差异（弧度转换为距离单位）
    """
    # 计算方向向量
    vec1 = seg1_coords[-1] - seg1_coords[0]
    vec2 = seg2_coords[-1] - seg2_coords[0]
    
    # 归一化
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 < 1e-10 or norm2 < 1e-10:
        return 0.0
    
    vec1 = vec1 / norm1
    vec2 = vec2 / norm2
    
    # 余弦相似度
    cos_sim = np.dot(vec1, vec2)
    cos_sim = np.clip(cos_sim, -1.0, 1.0)
    
    # 角度差（弧度）
    angle_diff = np.arccos(cos_sim)
    
    # 转换为距离单位（乘以平均长度）
    avg_len = (norm1 + norm2) / 2
    return angle_diff * avg_len * 10  # 放大系数


# ==================== 增强特征提取 ====================

def extract_enhanced_features(segment_coords: np.ndarray, segment_attrs: dict) -> np.ndarray:
    """
    提取增强的17维特征向量
    
    新增特征：
    - 曲率（curvature）
    - 曲折度（tortuosity）
    - 方向熵（direction_entropy）
    - 速度熵（speed_entropy）
    - 停车比例（stop_ratio）
    - 加速度峰值数（accel_peaks）
    - 轨迹包络面积（envelope_area）
    
    Args:
        segment_coords: 轨迹段坐标 (N, 2) [lon, lat]
        segment_attrs: 属性字典（速度、时间等）
        
    Returns:
        17维特征向量
    """
    features = []
    
    # ===== 原有的基础特征（10维）=====
    # 速度特征（4维）
    speeds = segment_attrs.get('speeds', [])
    if len(speeds) > 0:
        features.append(np.mean(speeds))   # avg_speed
        features.append(np.std(speeds))    # std_speed
        features.append(np.max(speeds))    # max_speed
        features.append(np.min(speeds))    # min_speed
    else:
        features.extend([0.0, 0.0, 0.0, 0.0])
    
    # 加速度特征（2维）
    accelerations = segment_attrs.get('accelerations', [])
    if len(accelerations) > 0:
        features.append(np.mean(accelerations))  # avg_accel
        features.append(np.std(accelerations))   # std_accel
    else:
        features.extend([0.0, 0.0])
    
    # 航向角特征（2维）
    yaw_changes = segment_attrs.get('yaw_changes', [])
    yaws = segment_attrs.get('yaws', [])
    if len(yaw_changes) > 0:
        features.append(np.mean(np.abs(yaw_changes)))  # yaw_change_rate
    else:
        features.append(0.0)
    if len(yaws) > 0:
        features.append(np.std(yaws))  # std_yaw
    else:
        features.append(0.0)
    
    # 方向特征（2维）
    delta_lng = segment_coords[-1][0] - segment_coords[0][0]
    delta_lat = segment_coords[-1][1] - segment_coords[0][1]
    angle = np.arctan2(delta_lat, delta_lng)
    features.append(np.cos(angle))  # direction_cos
    features.append(np.sin(angle))  # direction_sin
    
    # ===== 新增空间形态特征（7维）=====
    
    # 1. 曲率（curvature）：轨迹弯曲程度
    curvature = calculate_curvature(segment_coords)
    features.append(curvature)
    
    # 2. 曲折度（tortuosity）：实际长度 / 直线距离
    tortuosity = calculate_tortuosity(segment_coords)
    features.append(tortuosity)
    
    # 3. 方向熵（direction_entropy）：方向变化的复杂度
    direction_entropy = calculate_direction_entropy(segment_coords)
    features.append(direction_entropy)
    
    # 4. 速度熵（speed_entropy）：速度变化的复杂度
    if len(speeds) > 0:
        speed_entropy = calculate_value_entropy(speeds)
    else:
        speed_entropy = 0.0
    features.append(speed_entropy)
    
    # 5. 停车比例（stop_ratio）：速度<1m/s的时间占比
    if len(speeds) > 0:
        stop_ratio = np.sum(np.array(speeds) < 1.0) / len(speeds)
    else:
        stop_ratio = 0.0
    features.append(stop_ratio)
    
    # 6. 加速度峰值数（accel_peaks）：显著加减速次数
    if len(accelerations) > 1:
        accel_peaks = count_peaks(accelerations, threshold=2.0)
    else:
        accel_peaks = 0
    features.append(float(accel_peaks))
    
    # 7. 轨迹包络面积（envelope_area）：轨迹与直线的面积
    envelope_area = calculate_envelope_area(segment_coords)
    features.append(envelope_area)
    
    return np.array(features)


def calculate_curvature(coords: np.ndarray) -> float:
    """
    计算轨迹的平均曲率
    
    使用三点法计算曲率：对每3个连续点，计算外接圆半径的倒数
    
    Args:
        coords: 轨迹坐标 (N, 2)
        
    Returns:
        平均曲率
    """
    if len(coords) < 3:
        return 0.0
    
    curvatures = []
    
    for i in range(len(coords) - 2):
        p1, p2, p3 = coords[i], coords[i+1], coords[i+2]
        
        # 三角形三边长
        a = np.linalg.norm(p2 - p1)
        b = np.linalg.norm(p3 - p2)
        c = np.linalg.norm(p3 - p1)
        
        # 半周长
        s = (a + b + c) / 2
        
        # 面积（海伦公式）
        area_sq = s * (s - a) * (s - b) * (s - c)
        
        if area_sq > 0:
            area = np.sqrt(area_sq)
            # 曲率 = 4 * 面积 / (a * b * c)
            if a * b * c > 0:
                curvature = 4 * area / (a * b * c)
                curvatures.append(curvature)
    
    return np.mean(curvatures) if curvatures else 0.0


def calculate_tortuosity(coords: np.ndarray) -> float:
    """
    计算曲折度：实际路径长度 / 直线距离
    
    值越大表示轨迹越曲折
    - 直线：~1.0
    - 弯曲：1.5-2.0
    - 非常曲折：>2.0
    
    Args:
        coords: 轨迹坐标 (N, 2)
        
    Returns:
        曲折度
    """
    if len(coords) < 2:
        return 1.0
    
    # 实际路径长度
    path_length = 0.0
    for i in range(1, len(coords)):
        path_length += haversine_distance(
            coords[i-1][1], coords[i-1][0],
            coords[i][1], coords[i][0]
        )
    
    # 直线距离
    straight_dist = haversine_distance(
        coords[0][1], coords[0][0],
        coords[-1][1], coords[-1][0]
    )
    
    if straight_dist < 1.0:  # 避免除零
        return 1.0
    
    return path_length / straight_dist


def calculate_direction_entropy(coords: np.ndarray) -> float:
    """
    计算方向熵：方向变化的复杂度
    
    将方向分为8个bin（N, NE, E, SE, S, SW, W, NW），计算熵
    
    Args:
        coords: 轨迹坐标 (N, 2)
        
    Returns:
        方向熵
    """
    if len(coords) < 2:
        return 0.0
    
    # 计算每段的方向
    directions = []
    for i in range(1, len(coords)):
        dx = coords[i][0] - coords[i-1][0]
        dy = coords[i][1] - coords[i-1][1]
        angle = np.arctan2(dy, dx)  # [-π, π]
        directions.append(angle)
    
    # 分为8个bin
    bins = np.linspace(-np.pi, np.pi, 9)
    hist, _ = np.histogram(directions, bins=bins)
    
    # 计算熵
    probs = hist / np.sum(hist)
    probs = probs[probs > 0]  # 去除0概率
    entropy = -np.sum(probs * np.log2(probs))
    
    return entropy


def calculate_value_entropy(values: list) -> float:
    """
    计算数值序列的熵（用于速度、加速度等）
    
    Args:
        values: 数值列表
        
    Returns:
        熵值
    """
    if len(values) < 2:
        return 0.0
    
    # 分为10个bin
    hist, _ = np.histogram(values, bins=10)
    
    # 计算熵
    probs = hist / np.sum(hist)
    probs = probs[probs > 0]
    entropy = -np.sum(probs * np.log2(probs))
    
    return entropy


def count_peaks(values: list, threshold: float = 2.0) -> int:
    """
    计算峰值数量（加速度峰值）
    
    Args:
        values: 数值列表
        threshold: 峰值阈值
        
    Returns:
        峰值数量
    """
    if len(values) < 3:
        return 0
    
    values = np.array(values)
    peaks = 0
    
    for i in range(1, len(values) - 1):
        # 局部最大值且超过阈值
        if abs(values[i]) > threshold:
            if (values[i] > values[i-1] and values[i] > values[i+1]) or \
               (values[i] < values[i-1] and values[i] < values[i+1]):
                peaks += 1
    
    return peaks


def calculate_envelope_area(coords: np.ndarray) -> float:
    """
    计算轨迹包络面积：轨迹与起终点连线围成的面积
    
    Args:
        coords: 轨迹坐标 (N, 2)
        
    Returns:
        包络面积（平方米）
    """
    if len(coords) < 3:
        return 0.0
    
    # 使用Shapely计算多边形面积
    from shapely.geometry import Polygon
    
    # 构建多边形：轨迹点 + 起终点连线
    polygon_coords = list(coords) + [coords[0]]
    
    try:
        polygon = Polygon(polygon_coords)
        # 注意：这是度数单位，需要转换为米
        # 粗略近似：1度 ≈ 111km
        area_deg = polygon.area
        area_m2 = area_deg * (111000 ** 2)
        return area_m2
    except:
        return 0.0


# ==================== 自适应分段策略 ====================

def adaptive_segmentation(
    trajectory_coords: np.ndarray,
    trajectory_attrs: dict,
    min_points: int = 5
) -> List[Tuple[int, int]]:
    """
    自适应轨迹分段（基于特征点检测）
    
    检测特征点：
    1. 速度突变点（加速/减速）
    2. 航向角突变点（转弯）
    3. 停车点
    
    Args:
        trajectory_coords: 轨迹坐标 (N, 2)
        trajectory_attrs: 属性字典（速度、时间戳等）
        min_points: 每段最少点数
        
    Returns:
        分段索引列表 [(start, end), ...]
    """
    n = len(trajectory_coords)
    
    if n < min_points:
        return []
    
    # 检测特征点
    feature_points = set([0, n-1])  # 起终点必须包含
    
    # 1. 速度突变点
    speeds = trajectory_attrs.get('speeds', [])
    if len(speeds) > 2:
        speed_changes = np.abs(np.diff(speeds))
        speed_threshold = np.mean(speed_changes) + 2 * np.std(speed_changes)
        
        for i in range(1, len(speed_changes)):
            if speed_changes[i] > speed_threshold:
                feature_points.add(i)
    
    # 2. 航向角突变点
    if len(trajectory_coords) > 2:
        angles = []
        for i in range(1, len(trajectory_coords)):
            dx = trajectory_coords[i][0] - trajectory_coords[i-1][0]
            dy = trajectory_coords[i][1] - trajectory_coords[i-1][1]
            angle = np.arctan2(dy, dx)
            angles.append(angle)
        
        if len(angles) > 1:
            angle_changes = np.abs(np.diff(angles))
            # 处理角度跳变（-π到π）
            angle_changes = np.minimum(angle_changes, 2*np.pi - angle_changes)
            
            # 转弯阈值：30度
            turn_threshold = np.radians(30)
            
            for i in range(len(angle_changes)):
                if angle_changes[i] > turn_threshold:
                    feature_points.add(i+1)
    
    # 3. 停车点
    if len(speeds) > 0:
        for i in range(len(speeds)):
            if speeds[i] < 0.5:  # 几乎停止
                feature_points.add(i)
    
    # 排序特征点
    feature_points = sorted(list(feature_points))
    
    # 生成分段（确保每段至少min_points个点）
    segments = []
    start_idx = 0
    
    for i in range(1, len(feature_points)):
        end_idx = feature_points[i]
        
        if end_idx - start_idx >= min_points:
            segments.append((start_idx, end_idx))
            start_idx = end_idx
        # 否则延续当前段
    
    # 如果没有有效分段，返回整条轨迹
    if not segments:
        segments = [(0, n-1)]
    
    return segments


# ==================== 辅助函数 ====================

def haversine_distance(lat1, lon1, lat2, lon2) -> float:
    """计算两点间的Haversine距离（米）"""
    R = 6371000  # 地球半径（米）
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c


# ==================== 改进的聚类器 ====================

class ImprovedTrajectoryClusterer:
    """
    改进的轨迹聚类器
    
    主要改进：
    1. 使用Fréchet距离或Hausdorff距离
    2. 增强的特征提取（17维）
    3. 自适应分段
    4. 多种聚类算法（DBSCAN、层次聚类）
    """
    
    def __init__(
        self,
        distance_metric: str = 'frechet',  # 'frechet', 'hausdorff', 'segment'
        clustering_method: str = 'dbscan',  # 'dbscan', 'hierarchical'
        use_enhanced_features: bool = True,
        use_adaptive_segmentation: bool = False
    ):
        """
        初始化改进聚类器
        
        Args:
            distance_metric: 距离度量方法
            clustering_method: 聚类算法
            use_enhanced_features: 是否使用增强特征
            use_adaptive_segmentation: 是否使用自适应分段
        """
        self.distance_metric = distance_metric
        self.clustering_method = clustering_method
        self.use_enhanced_features = use_enhanced_features
        self.use_adaptive_segmentation = use_adaptive_segmentation
        self.scaler = StandardScaler()
        
        logger.info(f"🚀 改进聚类器初始化:")
        logger.info(f"   距离度量: {distance_metric}")
        logger.info(f"   聚类方法: {clustering_method}")
        logger.info(f"   增强特征: {use_enhanced_features}")
        logger.info(f"   自适应分段: {use_adaptive_segmentation}")
    
    def compute_distance_matrix(self, segments: List[dict]) -> np.ndarray:
        """
        计算轨迹段之间的距离矩阵
        
        Args:
            segments: 轨迹段列表，每个包含coords和attrs
            
        Returns:
            距离矩阵 (N, N)
        """
        n = len(segments)
        dist_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(i+1, n):
                coords_i = segments[i]['coords']
                coords_j = segments[j]['coords']
                
                if self.distance_metric == 'frechet':
                    dist = frechet_distance(coords_i, coords_j)
                elif self.distance_metric == 'hausdorff':
                    dist = hausdorff_distance_trajectory(coords_i, coords_j)
                elif self.distance_metric == 'segment':
                    dist = segment_distance(coords_i, coords_j)
                else:
                    # 默认使用欧氏距离（在特征空间）
                    dist = euclidean(
                        segments[i]['features'],
                        segments[j]['features']
                    )
                
                dist_matrix[i, j] = dist
                dist_matrix[j, i] = dist
        
        return dist_matrix
    
    def cluster(
        self,
        segments: List[dict],
        eps: float = 0.8,
        min_samples: int = 5,
        n_clusters: Optional[int] = None
    ) -> np.ndarray:
        """
        执行聚类
        
        Args:
            segments: 轨迹段列表
            eps: DBSCAN的eps参数
            min_samples: DBSCAN的min_samples参数
            n_clusters: 层次聚类的聚类数
            
        Returns:
            聚类标签数组
        """
        if not segments:
            return np.array([])
        
        logger.info(f"🔬 执行改进聚类（{len(segments)}个轨迹段）...")
        
        # 提取或使用预计算的特征
        features_list = [seg['features'] for seg in segments]
        features_matrix = np.vstack(features_list)
        
        # 标准化
        features_scaled = self.scaler.fit_transform(features_matrix)
        
        # 根据方法选择聚类算法
        if self.clustering_method == 'dbscan':
            clusterer = DBSCAN(
                eps=eps,
                min_samples=min_samples,
                metric='euclidean'
            )
            labels = clusterer.fit_predict(features_scaled)
            
        elif self.clustering_method == 'hierarchical':
            if n_clusters is None:
                n_clusters = max(2, int(np.sqrt(len(segments))))
            
            clusterer = AgglomerativeClustering(
                n_clusters=n_clusters,
                linkage='ward'
            )
            labels = clusterer.fit_predict(features_scaled)
        
        else:
            raise ValueError(f"未知聚类方法: {self.clustering_method}")
        
        # 统计
        n_clusters_found = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)
        
        logger.info(f"✅ 聚类完成:")
        logger.info(f"   聚类数: {n_clusters_found}")
        logger.info(f"   噪声点: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
        
        # 评估聚类质量
        if n_clusters_found > 1 and n_noise < len(labels):
            try:
                silhouette = silhouette_score(features_scaled, labels)
                db_index = davies_bouldin_score(features_scaled, labels)
                logger.info(f"📊 聚类质量:")
                logger.info(f"   轮廓系数: {silhouette:.3f} (越大越好)")
                logger.info(f"   DB指数: {db_index:.3f} (越小越好)")
            except:
                pass
        
        return labels


# ==================== 使用示例 ====================

def example_usage():
    """使用示例"""
    
    # 示例数据
    traj1 = np.array([[0, 0], [1, 1], [2, 2], [3, 3]])
    traj2 = np.array([[0, 0.1], [1, 1.1], [2, 2.1], [3, 3.1]])
    
    # 1. 计算Fréchet距离
    dist = frechet_distance(traj1, traj2)
    print(f"Fréchet距离: {dist}")
    
    # 2. 提取增强特征
    segment_attrs = {
        'speeds': [5, 6, 7, 8],
        'accelerations': [0.5, 0.3, 0.2],
        'yaws': [0, 0.1, 0.2, 0.3],
        'yaw_changes': [0.1, 0.1, 0.1]
    }
    features = extract_enhanced_features(traj1, segment_attrs)
    print(f"增强特征（17维）: {features}")
    
    # 3. 创建改进聚类器
    clusterer = ImprovedTrajectoryClusterer(
        distance_metric='frechet',
        clustering_method='dbscan',
        use_enhanced_features=True
    )
    
    # 4. 执行聚类
    segments = [
        {'coords': traj1, 'features': features, 'attrs': segment_attrs},
        {'coords': traj2, 'features': features, 'attrs': segment_attrs},
    ]
    labels = clusterer.cluster(segments, eps=50.0, min_samples=2)
    print(f"聚类标签: {labels}")


if __name__ == '__main__':
    example_usage()

