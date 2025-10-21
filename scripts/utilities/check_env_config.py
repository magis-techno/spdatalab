#!/usr/bin/env python3
"""
环境变量配置诊断脚本
用于检查 spdatalab 项目所需的所有环境变量是否正确配置

使用方法：
    python scripts/utilities/check_env_config.py
    
或在 Docker 容器中：
    python -m scripts.utilities.check_env_config
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root / "src"))

# 加载 .env 文件
from dotenv import load_dotenv
load_dotenv(project_root / '.env')


def check_status(condition: bool, success_msg: str, error_msg: str) -> bool:
    """打印检查状态"""
    if condition:
        print(f"  ✅ {success_msg}")
        return True
    else:
        print(f"  ❌ {error_msg}")
        return False


def mask_sensitive(value: str, show_chars: int = 5) -> str:
    """隐藏敏感信息"""
    if not value:
        return "未设置"
    if len(value) <= show_chars:
        return "*" * len(value)
    return value[:show_chars] + "***"


def check_obs_config() -> bool:
    """检查 OBS 配置"""
    print("\n" + "="*70)
    print("1. OBS（华为云对象存储）配置检查")
    print("="*70)
    
    all_ok = True
    
    # 检查 S3_ENDPOINT
    endpoint = os.getenv('S3_ENDPOINT')
    all_ok &= check_status(
        endpoint is not None and len(endpoint) > 0,
        f"S3_ENDPOINT: {endpoint}",
        "S3_ENDPOINT 未配置（必需）"
    )
    
    # 检查 S3_USE_HTTPS
    use_https = os.getenv('S3_USE_HTTPS', '0')
    print(f"  ℹ️  S3_USE_HTTPS: {use_https}")
    
    # 检查 ADS_DATALAKE_USERNAME
    username = os.getenv('ADS_DATALAKE_USERNAME')
    all_ok &= check_status(
        username is not None and len(username) > 0,
        f"ADS_DATALAKE_USERNAME: {mask_sensitive(username)}",
        "ADS_DATALAKE_USERNAME 未配置（必需）"
    )
    
    # 检查 ADS_DATALAKE_PASSWORD
    password = os.getenv('ADS_DATALAKE_PASSWORD')
    all_ok &= check_status(
        password is not None and len(password) > 0,
        f"ADS_DATALAKE_PASSWORD: {mask_sensitive(password)}",
        "ADS_DATALAKE_PASSWORD 未配置（必需）"
    )
    
    # 尝试初始化 moxing
    if all_ok:
        print("\n  正在测试 OBS 连接...")
        try:
            from spdatalab.common.io_obs import init_moxing
            init_moxing()
            print("  ✅ OBS 环境初始化成功")
        except Exception as e:
            print(f"  ⚠️  OBS 环境初始化失败: {e}")
            all_ok = False
    
    return all_ok


def check_multimodal_config() -> bool:
    """检查多模态配置"""
    print("\n" + "="*70)
    print("2. 多模态轨迹检索系统配置检查")
    print("="*70)
    
    all_ok = True
    
    # 检查必需配置
    api_key = os.getenv('MULTIMODAL_API_KEY')
    all_ok &= check_status(
        api_key and api_key != 'your_actual_api_key_here',
        f"MULTIMODAL_API_KEY: {mask_sensitive(api_key)}",
        "MULTIMODAL_API_KEY 未配置或使用默认值"
    )
    
    username = os.getenv('MULTIMODAL_USERNAME')
    all_ok &= check_status(
        username and username != 'your_username_here',
        f"MULTIMODAL_USERNAME: {username}",
        "MULTIMODAL_USERNAME 未配置或使用默认值"
    )
    
    base_url = os.getenv('MULTIMODAL_API_BASE_URL')
    all_ok &= check_status(
        base_url and base_url != 'https://your-api-server.com',
        f"MULTIMODAL_API_BASE_URL: {base_url}",
        "MULTIMODAL_API_BASE_URL 未配置或使用默认值"
    )
    
    # 检查可选配置
    print("\n  可选配置:")
    print(f"  ℹ️  MULTIMODAL_PROJECT: {os.getenv('MULTIMODAL_PROJECT', 'your_project')}")
    print(f"  ℹ️  MULTIMODAL_PLATFORM: {os.getenv('MULTIMODAL_PLATFORM', 'xmodalitys-external')}")
    print(f"  ℹ️  MULTIMODAL_REGION: {os.getenv('MULTIMODAL_REGION', 'RaD-prod')}")
    print(f"  ℹ️  MULTIMODAL_TIMEOUT: {os.getenv('MULTIMODAL_TIMEOUT', '30')}")
    print(f"  ℹ️  MULTIMODAL_MAX_RETRIES: {os.getenv('MULTIMODAL_MAX_RETRIES', '3')}")
    
    # 尝试加载配置
    if all_ok:
        try:
            from spdatalab.dataset.multimodal_data_retriever import APIConfig
            config = APIConfig.from_env()
            print("  ✅ 多模态配置加载成功")
        except Exception as e:
            print(f"  ⚠️  多模态配置加载失败: {e}")
            all_ok = False
    
    return all_ok


def check_database_config() -> bool:
    """检查数据库配置"""
    print("\n" + "="*70)
    print("3. 数据库配置检查")
    print("="*70)
    
    print("\n  本地数据库配置:")
    print(f"  ℹ️  LOCAL_DB_HOST: {os.getenv('LOCAL_DB_HOST', 'local_pg')}")
    print(f"  ℹ️  LOCAL_DB_PORT: {os.getenv('LOCAL_DB_PORT', '5432')}")
    print(f"  ℹ️  LOCAL_DB_NAME: {os.getenv('LOCAL_DB_NAME', 'postgres')}")
    print(f"  ℹ️  LOCAL_DB_USER: {os.getenv('LOCAL_DB_USER', 'postgres')}")
    print(f"  ℹ️  LOCAL_DB_PASSWORD: {mask_sensitive(os.getenv('LOCAL_DB_PASSWORD', 'postgres'))}")
    
    print("\n  远程数据库配置:")
    remote_host = os.getenv('REMOTE_DB_HOST')
    if remote_host and remote_host != 'your_remote_db_host':
        print(f"  ℹ️  REMOTE_DB_HOST: {remote_host}")
        print(f"  ℹ️  REMOTE_DB_PORT: {os.getenv('REMOTE_DB_PORT', '9001')}")
        print(f"  ℹ️  REMOTE_DB_NAME: {os.getenv('REMOTE_DB_NAME', 'your_database_name')}")
        print(f"  ℹ️  REMOTE_DB_USER: {os.getenv('REMOTE_DB_USER', 'your_db_username')}")
        print(f"  ℹ️  REMOTE_DB_PASSWORD: {mask_sensitive(os.getenv('REMOTE_DB_PASSWORD', ''))}")
    else:
        print("  ⚠️  远程数据库未配置（如不需要可忽略）")
    
    # 尝试加载配置
    try:
        from spdatalab.fusion.config import Config
        local_config = Config.get_local_db_config()
        print(f"\n  ✅ 本地数据库配置加载成功")
        print(f"     DSN: {local_config.dsn}")
        return True
    except Exception as e:
        print(f"\n  ⚠️  数据库配置加载失败: {e}")
        return False


def check_batch_config() -> bool:
    """检查批处理配置"""
    print("\n" + "="*70)
    print("4. 批处理配置检查")
    print("="*70)
    
    print(f"  ℹ️  TEMP_TABLE_PREFIX: {os.getenv('TEMP_TABLE_PREFIX', 'temp_bbox_batch')}")
    print(f"  ℹ️  DEFAULT_BATCH_SIZE: {os.getenv('DEFAULT_BATCH_SIZE', '1000')}")
    print(f"  ℹ️  MAX_RETRIES: {os.getenv('MAX_RETRIES', '3')}")
    print(f"  ℹ️  TIMEOUT_SECONDS: {os.getenv('TIMEOUT_SECONDS', '300')}")
    
    try:
        from spdatalab.fusion.config import Config
        batch_config = Config.get_batch_config()
        print("  ✅ 批处理配置加载成功")
        return True
    except Exception as e:
        print(f"  ⚠️  批处理配置加载失败: {e}")
        return False


def check_env_file_exists() -> bool:
    """检查 .env 文件是否存在"""
    env_file = project_root / '.env'
    env_example = project_root / 'env.example'
    
    print("="*70)
    print("0. 环境文件检查")
    print("="*70)
    
    if env_file.exists():
        print(f"  ✅ .env 文件存在: {env_file}")
        return True
    else:
        print(f"  ❌ .env 文件不存在: {env_file}")
        if env_example.exists():
            print(f"  ℹ️  请从模板创建 .env 文件:")
            print(f"     cp {env_example} {env_file}")
        return False


def print_summary(results: dict):
    """打印总结"""
    print("\n" + "="*70)
    print("配置检查总结")
    print("="*70)
    
    for name, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"  {status_icon} {name}")
    
    all_critical_ok = results.get('OBS配置', False)
    
    print("\n" + "="*70)
    if all_critical_ok:
        print("✅ 核心配置检查通过！")
        print("\n可以运行数据集构建命令:")
        print("  python -m spdatalab build-dataset --training-dataset-json ... --output ...")
    else:
        print("❌ 配置检查未通过，请修复以上问题")
        print("\n请确保 .env 文件中包含正确的配置:")
        print("  1. OBS 对象存储配置（必需）")
        print("     - S3_ENDPOINT")
        print("     - ADS_DATALAKE_USERNAME")
        print("     - ADS_DATALAKE_PASSWORD")
        print("\n  2. 根据需要配置其他功能模块")
    print("="*70)


def main():
    """主函数"""
    print("\n" + "="*70)
    print("spdatalab 环境变量配置诊断工具")
    print("="*70)
    
    # 检查环境文件
    env_exists = check_env_file_exists()
    
    if not env_exists:
        print("\n⚠️  由于 .env 文件不存在，跳过详细检查")
        sys.exit(1)
    
    # 执行各项检查
    results = {
        'OBS配置': check_obs_config(),
        '多模态配置': check_multimodal_config(),
        '数据库配置': check_database_config(),
        '批处理配置': check_batch_config(),
    }
    
    # 打印总结
    print_summary(results)
    
    # 返回退出码
    sys.exit(0 if results['OBS配置'] else 1)


if __name__ == '__main__':
    main()

