#!/usr/bin/env python3
"""
Grid多模态相似性分析
===================

功能：
1. 从冗余分析结果中选择高冗余grid
2. 提取grid内的dataset_name列表
3. 调用多模态API检索相关图片
4. 分析相似度分布

使用方法：
    # 基础使用：分析A72城市的top1冗余grid
    python analyze_grid_multimodal_similarity.py --city A72
    
    # 指定grid排名和查询文本
    python analyze_grid_multimodal_similarity.py --city A72 --grid-rank 2 --query-text "夜晚"
    
    # 指定分析日期和返回结果数
    python analyze_grid_multimodal_similarity.py \
        --city A72 \
        --analysis-date 2025-10-09 \
        --max-results 200
    
    # 指定collection（相机）
    python analyze_grid_multimodal_similarity.py \
        --city A72 \
        --collection ddi_collection_camera_encoded_2

依赖：
    - city_grid_density 表（需要先运行 batch_grid_analysis.py）
    - clips_bbox_unified 视图
    - 多模态API配置（.env文件）
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import Counter
import statistics

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
    from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN
    from spdatalab.dataset.multimodal_data_retriever import APIConfig, MultimodalRetriever

from sqlalchemy import create_engine, text


def select_target_grid(conn, city_id: str, grid_rank: int = 1, 
                      analysis_date: Optional[str] = None) -> Optional[Dict]:
    """从city_grid_density选择目标grid
    
    Args:
        conn: 数据库连接
        city_id: 城市ID
        grid_rank: Grid排名（1表示最高冗余）
        analysis_date: 分析日期，None表示使用最新日期
        
    Returns:
        Grid信息字典，包含grid_x, grid_y, bbox_count等
    """
    print(f"\n🎯 选择目标Grid")
    print("=" * 60)
    
    # 构建查询SQL
    if analysis_date:
        sql = text("""
            SELECT 
                grid_x,
                grid_y,
                bbox_count,
                subdataset_count,
                scene_count,
                involved_subdatasets,
                involved_scenes,
                ST_AsText(geometry) as grid_geom,
                analysis_date
            FROM city_grid_density
            WHERE city_id = :city_id 
              AND analysis_date = :analysis_date
            ORDER BY bbox_count DESC
            LIMIT 1 OFFSET :offset
        """)
        result = conn.execute(sql, {
            'city_id': city_id,
            'analysis_date': analysis_date,
            'offset': grid_rank - 1
        }).fetchone()
    else:
        sql = text("""
            SELECT 
                grid_x,
                grid_y,
                bbox_count,
                subdataset_count,
                scene_count,
                involved_subdatasets,
                involved_scenes,
                ST_AsText(geometry) as grid_geom,
                analysis_date
            FROM city_grid_density
            WHERE city_id = :city_id 
              AND analysis_date = (SELECT MAX(analysis_date) FROM city_grid_density WHERE city_id = :city_id)
            ORDER BY bbox_count DESC
            LIMIT 1 OFFSET :offset
        """)
        result = conn.execute(sql, {
            'city_id': city_id,
            'offset': grid_rank - 1
        }).fetchone()
    
    if not result:
        print(f"❌ 未找到城市 {city_id} 的Grid数据")
        print(f"💡 提示：")
        print(f"   1. 确认城市ID是否正确")
        print(f"   2. 确认已运行过 batch_grid_analysis.py")
        print(f"   3. 尝试降低 --grid-rank 参数")
        return None
    
    grid_info = {
        'city_id': city_id,
        'grid_x': result.grid_x,
        'grid_y': result.grid_y,
        'bbox_count': result.bbox_count,
        'subdataset_count': result.subdataset_count,
        'scene_count': result.scene_count,
        'involved_subdatasets': result.involved_subdatasets or [],
        'involved_scenes': result.involved_scenes or [],
        'grid_geom': result.grid_geom,
        'analysis_date': str(result.analysis_date)
    }
    
    print(f"📍 选择Grid: {city_id} 城市, Rank #{grid_rank}")
    print(f"   分析日期: {grid_info['analysis_date']}")
    print(f"   Grid坐标: ({grid_info['grid_x']}, {grid_info['grid_y']})")
    print(f"   BBox数量: {grid_info['bbox_count']}")
    print(f"   Scene数量: {grid_info['scene_count']}")
    print(f"   Dataset数量: {grid_info['subdataset_count']}")
    
    return grid_info


def extract_grid_datasets(conn, grid_info: Dict) -> Tuple[List[str], Dict]:
    """提取grid内的dataset列表和统计信息
    
    Args:
        conn: 数据库连接
        grid_info: Grid信息
        
    Returns:
        (dataset_name列表, 统计信息字典)
    """
    print(f"\n📦 提取Grid内的数据")
    print("=" * 60)
    
    # 通过空间连接获取grid内的所有bbox
    sql = text("""
        SELECT DISTINCT 
            b.dataset_name,
            b.scene_token,
            b.subdataset_name
        FROM city_grid_density g
        JOIN clips_bbox_unified b ON ST_Intersects(g.geometry, b.geometry)
        WHERE g.city_id = :city_id 
          AND g.grid_x = :grid_x 
          AND g.grid_y = :grid_y
          AND g.analysis_date = :analysis_date
          AND b.all_good = true
        ORDER BY b.dataset_name, b.scene_token
    """)
    
    results = conn.execute(sql, {
        'city_id': grid_info['city_id'],
        'grid_x': grid_info['grid_x'],
        'grid_y': grid_info['grid_y'],
        'analysis_date': grid_info['analysis_date']
    }).fetchall()
    
    if not results:
        print(f"⚠️ Grid内没有找到有效的bbox数据")
        return [], {}
    
    # 提取唯一的dataset_name
    dataset_names = list(set(row.dataset_name for row in results if row.dataset_name))
    
    # 统计信息
    scene_tokens = list(set(row.scene_token for row in results if row.scene_token))
    subdataset_names = list(set(row.subdataset_name for row in results if row.subdataset_name))
    
    # 按dataset统计scene数量
    dataset_scene_count = Counter()
    for row in results:
        if row.dataset_name and row.scene_token:
            dataset_scene_count[row.dataset_name] += 1
    
    stats = {
        'total_datasets': len(dataset_names),
        'total_scenes': len(scene_tokens),
        'total_subdatasets': len(subdataset_names),
        'total_records': len(results),
        'dataset_scene_count': dataset_scene_count
    }
    
    print(f"✅ 提取完成:")
    print(f"   Dataset数量: {stats['total_datasets']}")
    print(f"   Scene数量: {stats['total_scenes']}")
    print(f"   子数据集数量: {stats['total_subdatasets']}")
    print(f"   总记录数: {stats['total_records']}")
    
    # 显示前10个dataset
    if dataset_names:
        print(f"\n📋 Grid内的数据集 (前10个):")
        for i, ds_name in enumerate(dataset_names[:10], 1):
            scene_cnt = dataset_scene_count.get(ds_name, 0)
            # 截断过长的dataset_name
            display_name = ds_name if len(ds_name) <= 60 else ds_name[:57] + "..."
            print(f"   {i}. {display_name} ({scene_cnt} scenes)")
        
        if len(dataset_names) > 10:
            print(f"   ... 还有 {len(dataset_names) - 10} 个数据集")
    
    return dataset_names, stats


def call_multimodal_api(retriever: MultimodalRetriever, query_text: str, 
                       collection: str, city_id: str, dataset_names: List[str],
                       max_results: int = 100) -> List[Dict]:
    """调用多模态API检索
    
    Args:
        retriever: MultimodalRetriever实例
        query_text: 查询文本
        collection: Collection名称
        city_id: 城市ID
        dataset_names: Dataset名称列表
        max_results: 最大返回结果数
        
    Returns:
        检索结果列表
    """
    print(f"\n🔍 调用多模态API")
    print("=" * 60)
    
    # 构建城市过滤条件
    filter_dict = {
        "conditions": [[{
            "field": "ddi_basic.city_code",
            "func": "$eq",
            "value": city_id,
            "format": "string"
        }]],
        "logic": ["$and"],
        "cursorKey": None
    }
    
    print(f"📝 查询参数:")
    print(f"   查询文本: '{query_text}'")
    print(f"   Collection: {collection}")
    print(f"   城市过滤: {city_id}")
    print(f"   Dataset过滤: {len(dataset_names)} 个")
    print(f"   最大结果数: {max_results}")
    
    try:
        # 调用API
        results = retriever.retrieve_by_text(
            text=query_text,
            collection=collection,
            count=max_results,
            dataset_name=dataset_names,
            filter_dict=filter_dict
        )
        
        print(f"✅ API调用成功: 返回 {len(results)} 条结果")
        
        return results
        
    except Exception as e:
        print(f"❌ API调用失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def analyze_similarity(results: List[Dict], top_n: int = 10) -> None:
    """分析相似度分布
    
    Args:
        results: 检索结果列表
        top_n: 显示top N个结果
    """
    print(f"\n📊 相似度分析")
    print("=" * 60)
    
    if not results:
        print("⚠️ 没有结果可分析")
        return
    
    # 提取相似度值
    similarities = [r.get('similarity', 0.0) for r in results]
    similarities = [s for s in similarities if s > 0]  # 过滤无效值
    
    if not similarities:
        print("⚠️ 没有有效的相似度数据")
        return
    
    # 基础统计
    min_sim = min(similarities)
    max_sim = max(similarities)
    avg_sim = statistics.mean(similarities)
    median_sim = statistics.median(similarities)
    
    print(f"📈 相似度统计:")
    print(f"   范围: {min_sim:.3f} ~ {max_sim:.3f}")
    print(f"   平均: {avg_sim:.3f}")
    print(f"   中位数: {median_sim:.3f}")
    print(f"   样本数: {len(similarities)}")
    
    # 相似度分布直方图
    print(f"\n📊 相似度分布直方图:")
    bins = [(i/10, (i+1)/10) for i in range(10)]  # 0.0-0.1, 0.1-0.2, ..., 0.9-1.0
    
    for low, high in bins:
        count = sum(1 for s in similarities if low <= s < high)
        if count > 0 or (low <= avg_sim < high):  # 显示有数据的区间或包含平均值的区间
            bar_length = int(count / len(similarities) * 50)  # 最多50个字符
            bar = "█" * bar_length
            pct = count / len(similarities) * 100
            print(f"   {low:.1f}-{high:.1f}: {bar} ({count}, {pct:.1f}%)")
    
    # 按dataset分组统计
    print(f"\n📦 按Dataset分组:")
    dataset_sims = {}
    for r in results:
        ds_name = r.get('dataset_name', 'unknown')
        sim = r.get('similarity', 0.0)
        if ds_name not in dataset_sims:
            dataset_sims[ds_name] = []
        dataset_sims[ds_name].append(sim)
    
    # 按平均相似度排序
    dataset_stats = []
    for ds_name, sims in dataset_sims.items():
        dataset_stats.append({
            'dataset': ds_name,
            'count': len(sims),
            'avg_similarity': statistics.mean(sims),
            'max_similarity': max(sims)
        })
    
    dataset_stats.sort(key=lambda x: x['avg_similarity'], reverse=True)
    
    # 显示前5个dataset
    print(f"   Top 5 Dataset (按平均相似度):")
    for i, ds in enumerate(dataset_stats[:5], 1):
        ds_display = ds['dataset'] if len(ds['dataset']) <= 50 else ds['dataset'][:47] + "..."
        print(f"   {i}. {ds_display}")
        print(f"      结果数: {ds['count']}, 平均相似度: {ds['avg_similarity']:.3f}, 最高: {ds['max_similarity']:.3f}")
    
    # Top N 最相似结果
    print(f"\n🔝 Top {top_n} 最相似结果:")
    sorted_results = sorted(results, key=lambda x: x.get('similarity', 0.0), reverse=True)
    
    for i, r in enumerate(sorted_results[:top_n], 1):
        sim = r.get('similarity', 0.0)
        ds_name = r.get('dataset_name', 'unknown')
        timestamp = r.get('timestamp', 0)
        img_path = r.get('metadata', {}).get('img_path', 'N/A')
        
        # 截断长路径
        if len(ds_name) > 50:
            ds_name_display = ds_name[:47] + "..."
        else:
            ds_name_display = ds_name
        
        if len(img_path) > 80:
            img_path_display = img_path[:77] + "..."
        else:
            img_path_display = img_path
        
        print(f"\n   {i}. 相似度: {sim:.3f}")
        print(f"      Dataset: {ds_name_display}")
        print(f"      Timestamp: {timestamp}")
        print(f"      图片路径: {img_path_display}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Grid多模态相似性分析',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 基础使用
  python %(prog)s --city A72
  
  # 完整参数
  python %(prog)s --city A72 --grid-rank 2 --query-text "夜晚" --max-results 200
        """
    )
    
    parser.add_argument('--city', required=True, 
                       help='城市ID（如 A72, A263）')
    parser.add_argument('--grid-rank', type=int, default=1,
                       help='Grid排名，1表示最高冗余（默认: 1）')
    parser.add_argument('--query-text', default='白天',
                       help='查询文本（默认: "白天"）')
    parser.add_argument('--collection', default='ddi_collection_camera_encoded_1',
                       help='Collection名称（默认: ddi_collection_camera_encoded_1）')
    parser.add_argument('--max-results', type=int, default=100,
                       help='最大返回结果数（默认: 100）')
    parser.add_argument('--analysis-date', type=str,
                       help='分析日期（格式: YYYY-MM-DD），默认使用最新日期')
    parser.add_argument('--top-n', type=int, default=10,
                       help='显示top N个最相似结果（默认: 10）')
    
    args = parser.parse_args()
    
    print("🚀 Grid多模态相似性分析")
    print("=" * 60)
    
    try:
        # 1. 连接数据库
        print("\n🔌 连接数据库...")
        engine = create_engine(LOCAL_DSN, future=True)
        
        with engine.connect() as conn:
            # 测试连接
            conn.execute(text("SELECT 1"))
            print("✅ 数据库连接成功")
            
            # 2. 选择目标Grid
            grid_info = select_target_grid(
                conn, 
                args.city, 
                args.grid_rank,
                args.analysis_date
            )
            
            if not grid_info:
                return 1
            
            # 3. 提取Grid内的数据
            dataset_names, stats = extract_grid_datasets(conn, grid_info)
            
            if not dataset_names:
                print("\n❌ Grid内没有有效的dataset，无法继续")
                return 1
            
            print(f"\n💡 提示: 将使用 {len(dataset_names)} 个dataset进行过滤")
            if len(dataset_names) > 50:
                print(f"⚠️ Dataset数量较多，API调用可能需要较长时间")
        
        # 4. 初始化多模态API
        print(f"\n🔧 初始化多模态API...")
        api_config = APIConfig.from_env()
        retriever = MultimodalRetriever(api_config)
        print(f"✅ API配置加载成功")
        
        # 5. 调用多模态API
        results = call_multimodal_api(
            retriever,
            args.query_text,
            args.collection,
            args.city,
            dataset_names,
            args.max_results
        )
        
        if not results:
            print("\n⚠️ API未返回结果")
            print("💡 可能原因:")
            print("   - 查询文本与grid内的数据不匹配")
            print("   - Dataset过滤过于严格")
            print("   - API配置或网络问题")
            return 1
        
        # 6. 分析相似度
        analyze_similarity(results, args.top_n)
        
        # 7. 总结
        print(f"\n" + "=" * 60)
        print(f"✅ 分析完成")
        print(f"=" * 60)
        print(f"📍 Grid: {args.city} ({grid_info['grid_x']}, {grid_info['grid_y']})")
        print(f"📦 Dataset数量: {len(dataset_names)}")
        print(f"🔍 检索结果: {len(results)}")
        print(f"📊 相似度范围: {min(r.get('similarity', 0) for r in results):.3f} ~ "
              f"{max(r.get('similarity', 0) for r in results):.3f}")
        
        print(f"\n💡 后续可以:")
        print(f"   1. 尝试不同的查询文本（--query-text）")
        print(f"   2. 分析其他排名的grid（--grid-rank）")
        print(f"   3. 下载图片进行视觉相似性分析（待实现）")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

