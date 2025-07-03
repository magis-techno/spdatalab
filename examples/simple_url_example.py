#!/usr/bin/env python3
"""
简化的URL格式处理示例

本示例展示最简单的URL格式使用方式：
1. 直接在索引文件中放URL（无需@duplicate后缀）
2. 自动格式检测，无需特殊参数
3. 一步生成数据集并处理边界框
"""

import logging
from pathlib import Path
from src.spdatalab.dataset.dataset_manager import DatasetManager
from src.spdatalab.dataset.bbox import run_with_partitioning

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """简单示例主函数"""
    
    # 1. 创建包含URL的索引文件
    index_file = "simple_urls.txt"
    
    # 直接放URL，无需@duplicate后缀
    urls = [
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535",
        "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536"
    ]
    
    with open(index_file, 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(f"{url}\n")
    
    logger.info(f"创建索引文件: {index_file}")
    
    try:
        # 2. 创建数据集管理器（自动支持URL和传统格式）
        dataset_manager = DatasetManager()
        
        # 3. 构建数据集（自动检测URL格式）
        dataset = dataset_manager.build_dataset_from_index(
            index_file,
            "简单URL数据集",
            "直接从URL生成的数据集示例"
        )
        
        logger.info(f"数据集创建成功:")
        logger.info(f"- 名称: {dataset.name}")
        logger.info(f"- 子数据集数量: {len(dataset.subdatasets)}")
        logger.info(f"- 总场景数: {dataset.total_scenes}")
        
        # 4. 保存数据集
        dataset_file = "simple_url_dataset.json"
        dataset_manager.save_dataset(dataset, dataset_file)
        logger.info(f"数据集已保存: {dataset_file}")
        
        # 5. 可选：处理边界框（注释掉实际执行）
        logger.info("可以使用以下命令处理边界框:")
        logger.info(f"run_with_partitioning('{dataset_file}')")
        
        # 取消注释以下代码来实际执行边界框处理：
        # run_with_partitioning(dataset_file, batch=50, use_parallel=True)
        
        logger.info("示例完成！")
        
        # 显示生成的JSON文件内容（前几行）
        with open(dataset_file, 'r', encoding='utf-8') as f:
            content = f.read()[:500]  # 显示前500个字符
            logger.info(f"生成的数据集文件预览:\n{content}...")
            
    except Exception as e:
        logger.error(f"处理失败: {str(e)}")
        
    finally:
        # 清理测试文件
        for file in [index_file, "simple_url_dataset.json"]:
            Path(file).unlink(missing_ok=True)

if __name__ == "__main__":
    main() 