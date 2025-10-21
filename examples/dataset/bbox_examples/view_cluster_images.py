#!/usr/bin/env python3
"""
场景图片查看器示例脚本
======================

从数据库查询场景ID，加载图片并生成HTML报告用于快速浏览。

使用场景：
1. 查看网格聚类分析结果的代表性场景图片
2. 查看指定场景的图片样本
3. 从聚类结果选择TOP N个cluster的场景

使用方法：
    # 1. 从聚类结果选择场景（指定grid_id和cluster）
    python view_cluster_images.py \\
        --grid-id 123 \\
        --cluster-label 0 \\
        --max-scenes 5 \\
        --frames-per-scene 3
    
    # 2. 指定具体的scene_ids
    python view_cluster_images.py \\
        --scene-ids scene_001 scene_002 scene_003 \\
        --frames-per-scene 5 \\
        --output my_report.html
    
    # 3. 从分析ID加载场景（选择TOP N个cluster）
    python view_cluster_images.py \\
        --analysis-id cluster_20231021 \\
        --top-clusters 3 \\
        --max-scenes-per-cluster 3 \\
        --frames-per-scene 3

输出结果：
    - HTML报告文件（包含base64编码的图片）
    - 终端统计信息

作者：spdatalab
"""

import sys
from pathlib import Path
import argparse
import logging
from datetime import datetime
from typing import List, Optional

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
    from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
    from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

from sqlalchemy import create_engine, text
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库连接
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"


def get_cluster_scenes(
    grid_id: int,
    cluster_label: Optional[int] = None,
    max_scenes: int = 10
) -> List[str]:
    """从聚类结果获取场景ID
    
    Args:
        grid_id: Grid ID
        cluster_label: 聚类标签（None表示所有cluster）
        max_scenes: 最大场景数
        
    Returns:
        场景ID列表（实际是dataset_name）
    """
    logger.info(f"查询Grid {grid_id} 的聚类场景...")
    
    engine = create_engine(LOCAL_DSN)
    
    # 构建查询
    if cluster_label is not None:
        sql = text("""
            SELECT DISTINCT dataset_name
            FROM public.grid_trajectory_segments
            WHERE grid_id = :grid_id
              AND cluster_label = :cluster_label
              AND quality_flag = true
            LIMIT :max_scenes
        """)
        params = {
            "grid_id": grid_id,
            "cluster_label": cluster_label,
            "max_scenes": max_scenes
        }
        logger.info(f"  Cluster标签: {cluster_label}")
    else:
        sql = text("""
            SELECT DISTINCT dataset_name
            FROM public.grid_trajectory_segments
            WHERE grid_id = :grid_id
              AND quality_flag = true
            LIMIT :max_scenes
        """)
        params = {"grid_id": grid_id, "max_scenes": max_scenes}
        logger.info(f"  所有Cluster")
    
    with engine.connect() as conn:
        result = conn.execute(sql, params)
        dataset_names = [row[0] for row in result]
    
    logger.info(f"✅ 找到 {len(dataset_names)} 个dataset_name")
    
    # dataset_name通常就是scene_id，或者需要进一步转换
    # 这里假设dataset_name可以直接用作scene_id
    return dataset_names


def get_top_clusters_scenes(
    analysis_id: str,
    top_n: int = 3,
    max_scenes_per_cluster: int = 5
) -> List[str]:
    """获取TOP N个cluster的场景
    
    Args:
        analysis_id: 分析ID
        top_n: 选择TOP N个cluster
        max_scenes_per_cluster: 每个cluster的最大场景数
        
    Returns:
        场景ID列表
    """
    logger.info(f"查询分析 {analysis_id} 的TOP {top_n} clusters...")
    
    engine = create_engine(LOCAL_DSN)
    
    # 1. 获取TOP N个cluster
    sql_clusters = text("""
        SELECT cluster_label, segment_count
        FROM public.grid_clustering_summary
        WHERE analysis_id = :analysis_id
          AND cluster_label >= 0
        ORDER BY segment_count DESC
        LIMIT :top_n
    """)
    
    with engine.connect() as conn:
        result = conn.execute(sql_clusters, {
            "analysis_id": analysis_id,
            "top_n": top_n
        })
        top_clusters = [row[0] for row in result]
    
    logger.info(f"  TOP {top_n} clusters: {top_clusters}")
    
    # 2. 获取每个cluster的场景
    all_scenes = []
    for cluster_label in top_clusters:
        sql_scenes = text("""
            SELECT DISTINCT dataset_name
            FROM public.grid_trajectory_segments
            WHERE analysis_id = :analysis_id
              AND cluster_label = :cluster_label
              AND quality_flag = true
            LIMIT :max_scenes
        """)
        
        with engine.connect() as conn:
            result = conn.execute(sql_scenes, {
                "analysis_id": analysis_id,
                "cluster_label": cluster_label,
                "max_scenes": max_scenes_per_cluster
            })
            scenes = [row[0] for row in result]
            all_scenes.extend(scenes)
        
        logger.info(f"  Cluster {cluster_label}: {len(scenes)} 个场景")
    
    logger.info(f"✅ 总共找到 {len(all_scenes)} 个场景")
    return all_scenes


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="从数据库加载场景图片并生成HTML报告",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法：
  # 查看某个grid的cluster图片
  python %(prog)s --grid-id 123 --cluster-label 0 --max-scenes 5

  # 查看指定场景
  python %(prog)s --scene-ids scene_001 scene_002 --frames-per-scene 5

  # 查看TOP 3 clusters
  python %(prog)s --analysis-id cluster_20231021 --top-clusters 3
        """
    )
    
    # 场景来源参数（互斥）
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        '--scene-ids', 
        nargs='+',
        help='直接指定场景ID列表'
    )
    source_group.add_argument(
        '--grid-id',
        type=int,
        help='从Grid聚类结果查询场景'
    )
    source_group.add_argument(
        '--analysis-id',
        type=str,
        help='从分析ID查询场景'
    )
    
    # Grid查询参数
    parser.add_argument(
        '--cluster-label',
        type=int,
        help='聚类标签（与--grid-id配合使用）'
    )
    parser.add_argument(
        '--max-scenes',
        type=int,
        default=10,
        help='最大场景数（与--grid-id配合使用，默认: 10）'
    )
    
    # Analysis查询参数
    parser.add_argument(
        '--top-clusters',
        type=int,
        default=3,
        help='选择TOP N个cluster（与--analysis-id配合使用，默认: 3）'
    )
    parser.add_argument(
        '--max-scenes-per-cluster',
        type=int,
        default=5,
        help='每个cluster的最大场景数（与--analysis-id配合使用，默认: 5）'
    )
    
    # 图片加载参数
    parser.add_argument(
        '--frames-per-scene',
        type=int,
        default=3,
        help='每个场景加载的帧数（默认: 3）'
    )
    parser.add_argument(
        '--camera-type',
        type=str,
        default='CAM_FRONT_WIDE_ANGLE',
        help='相机类型（默认: CAM_FRONT_WIDE_ANGLE）'
    )
    
    # 输出参数
    parser.add_argument(
        '--output',
        type=str,
        help='输出HTML文件路径（默认自动生成）'
    )
    parser.add_argument(
        '--title',
        type=str,
        help='HTML报告标题（默认自动生成）'
    )
    parser.add_argument(
        '--thumbnail-size',
        type=int,
        default=200,
        help='缩略图大小（默认: 200）'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("场景图片查看器")
    print("=" * 70)
    
    # 1. 获取场景ID列表
    scene_ids = []
    
    if args.scene_ids:
        scene_ids = args.scene_ids
        logger.info(f"使用指定的场景ID: {len(scene_ids)} 个")
        
    elif args.grid_id:
        scene_ids = get_cluster_scenes(
            args.grid_id,
            args.cluster_label,
            args.max_scenes
        )
        
    elif args.analysis_id:
        scene_ids = get_top_clusters_scenes(
            args.analysis_id,
            args.top_clusters,
            args.max_scenes_per_cluster
        )
    
    if not scene_ids:
        logger.error("❌ 未找到任何场景ID")
        return 1
    
    print(f"\n📋 待处理场景数: {len(scene_ids)}")
    print(f"🎬 每场景帧数: {args.frames_per_scene}")
    print(f"📷 相机类型: {args.camera_type}")
    
    # 2. 加载图片
    print(f"\n{'=' * 70}")
    print("开始加载图片...")
    print(f"{'=' * 70}\n")
    
    retriever = SceneImageRetriever(camera_type=args.camera_type)
    
    try:
        images_dict = retriever.batch_load_images(
            scene_ids,
            frames_per_scene=args.frames_per_scene
        )
    except Exception as e:
        logger.error(f"❌ 加载图片失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    if not images_dict:
        logger.error("❌ 未成功加载任何图片")
        return 1
    
    # 3. 生成HTML报告
    print(f"\n{'=' * 70}")
    print("生成HTML报告...")
    print(f"{'=' * 70}\n")
    
    # 确定输出路径
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if args.grid_id:
            suffix = f"grid{args.grid_id}"
            if args.cluster_label is not None:
                suffix += f"_cluster{args.cluster_label}"
        elif args.analysis_id:
            suffix = f"analysis_{args.analysis_id}"
        else:
            suffix = "scenes"
        output_path = f"scene_images_{suffix}_{timestamp}.html"
    
    # 确定标题
    if args.title:
        title = args.title
    else:
        if args.grid_id:
            title = f"Grid {args.grid_id}"
            if args.cluster_label is not None:
                title += f" - Cluster {args.cluster_label}"
            title += " 图片查看器"
        elif args.analysis_id:
            title = f"分析 {args.analysis_id} - TOP {args.top_clusters} Clusters"
        else:
            title = "场景图片查看器"
    
    viewer = SceneImageHTMLViewer()
    
    try:
        report_path = viewer.generate_html_report(
            images_dict,
            output_path,
            title=title,
            thumbnail_size=args.thumbnail_size
        )
    except Exception as e:
        logger.error(f"❌ 生成HTML报告失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # 4. 输出统计信息
    total_frames = sum(len(frames) for frames in images_dict.values())
    
    print(f"\n{'=' * 70}")
    print("✅ 处理完成！")
    print(f"{'=' * 70}")
    print(f"\n📊 统计信息:")
    print(f"  成功加载场景数: {len(images_dict)}")
    print(f"  总帧数: {total_frames}")
    print(f"  平均每场景帧数: {total_frames / len(images_dict):.1f}")
    print(f"\n📄 HTML报告路径:")
    print(f"  {report_path}")
    print(f"\n💡 提示: 在浏览器中打开HTML文件查看图片")
    print(f"{'=' * 70}\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

