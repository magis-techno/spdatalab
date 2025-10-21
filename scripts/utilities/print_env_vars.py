#!/usr/bin/env python3
"""
快速打印环境变量脚本
用于验证环境变量是否正确加载到 Python 中

使用方法：
    python scripts/utilities/print_env_vars.py
    
或在 Docker 容器中：
    python -m scripts.utilities.print_env_vars
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root / "src"))


def mask_sensitive(value: str, show_chars: int = 5) -> str:
    """隐藏敏感信息"""
    if not value:
        return "未设置"
    if len(value) <= show_chars:
        return "*" * len(value)
    return value[:show_chars] + "***"


def main():
    print("="*70)
    print("环境变量快速检查")
    print("="*70)
    
    # 首先尝试加载 .env 文件
    print("\n[1] 加载 .env 文件...")
    try:
        from dotenv import load_dotenv
        env_file = project_root / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            print(f"    ✅ 已加载: {env_file}")
        else:
            print(f"    ❌ 文件不存在: {env_file}")
    except Exception as e:
        print(f"    ⚠️  加载失败: {e}")
    
    # 使用 spdatalab 的 config 模块
    print("\n[2] 通过 spdatalab.common.config 读取...")
    try:
        from spdatalab.common.config import getenv
        print("    ✅ 模块导入成功")
    except Exception as e:
        print(f"    ❌ 模块导入失败: {e}")
        print("    使用 os.getenv 作为替代")
        getenv = lambda key, default=None, required=False: os.getenv(key, default)
    
    # 打印所有相关环境变量
    print("\n" + "="*70)
    print("OBS 配置")
    print("="*70)
    
    env_vars = {
        'S3_ENDPOINT': os.getenv('S3_ENDPOINT', '未设置'),
        'S3_USE_HTTPS': os.getenv('S3_USE_HTTPS', '未设置'),
        'ADS_DATALAKE_USERNAME': mask_sensitive(os.getenv('ADS_DATALAKE_USERNAME', '')),
        'ADS_DATALAKE_PASSWORD': mask_sensitive(os.getenv('ADS_DATALAKE_PASSWORD', '')),
    }
    
    for key, value in env_vars.items():
        status = "✅" if value != "未设置" else "❌"
        print(f"  {status} {key:30s} = {value}")
    
    print("\n" + "="*70)
    print("多模态配置")
    print("="*70)
    
    multimodal_vars = {
        'MULTIMODAL_API_KEY': mask_sensitive(os.getenv('MULTIMODAL_API_KEY', '')),
        'MULTIMODAL_USERNAME': os.getenv('MULTIMODAL_USERNAME', '未设置'),
        'MULTIMODAL_API_BASE_URL': os.getenv('MULTIMODAL_API_BASE_URL', '未设置'),
        'MULTIMODAL_PROJECT': os.getenv('MULTIMODAL_PROJECT', 'your_project'),
        'MULTIMODAL_TIMEOUT': os.getenv('MULTIMODAL_TIMEOUT', '30'),
    }
    
    for key, value in multimodal_vars.items():
        status = "✅" if value != "未设置" else "⚠️ "
        print(f"  {status} {key:30s} = {value}")
    
    print("\n" + "="*70)
    print("数据库配置")
    print("="*70)
    
    db_vars = {
        'LOCAL_DB_HOST': os.getenv('LOCAL_DB_HOST', 'local_pg'),
        'LOCAL_DB_PORT': os.getenv('LOCAL_DB_PORT', '5432'),
        'LOCAL_DB_NAME': os.getenv('LOCAL_DB_NAME', 'postgres'),
        'LOCAL_DB_USER': os.getenv('LOCAL_DB_USER', 'postgres'),
        'LOCAL_DB_PASSWORD': mask_sensitive(os.getenv('LOCAL_DB_PASSWORD', '')),
        'REMOTE_DB_HOST': os.getenv('REMOTE_DB_HOST', '未设置'),
        'REMOTE_DB_PORT': os.getenv('REMOTE_DB_PORT', '9001'),
    }
    
    for key, value in db_vars.items():
        print(f"  ℹ️  {key:30s} = {value}")
    
    # 检查关键配置
    print("\n" + "="*70)
    print("关键配置检查")
    print("="*70)
    
    critical_vars = ['S3_ENDPOINT', 'ADS_DATALAKE_USERNAME', 'ADS_DATALAKE_PASSWORD']
    all_set = all(os.getenv(var) for var in critical_vars)
    
    if all_set:
        print("  ✅ 所有关键 OBS 配置已设置")
        
        # 尝试初始化 OBS
        print("\n  尝试初始化 OBS 连接...")
        try:
            # 直接设置环境变量（模拟 init_moxing）
            os.environ['S3_ENDPOINT'] = os.getenv('S3_ENDPOINT')
            os.environ['S3_USE_HTTPS'] = os.getenv('S3_USE_HTTPS', '0')
            os.environ['ACCESS_KEY_ID'] = os.getenv('ADS_DATALAKE_USERNAME')
            os.environ['SECRET_ACCESS_KEY'] = os.getenv('ADS_DATALAKE_PASSWORD')
            
            # 检查是否能导入 moxing
            import moxing as mox
            print("  ✅ moxing 模块导入成功")
            print(f"  ℹ️  moxing 版本: {mox.__version__ if hasattr(mox, '__version__') else '未知'}")
            
            # 检查环境变量是否正确设置
            print(f"  ✅ ACCESS_KEY_ID 已设置: {os.environ['ACCESS_KEY_ID'][:5]}***")
            print(f"  ✅ S3_ENDPOINT 已设置: {os.environ['S3_ENDPOINT']}")
            
        except ImportError as e:
            print(f"  ⚠️  无法导入 moxing: {e}")
            print("     这在本地开发环境中是正常的")
        except Exception as e:
            print(f"  ⚠️  OBS 初始化失败: {e}")
    else:
        print("  ❌ 关键 OBS 配置缺失，请检查:")
        for var in critical_vars:
            if not os.getenv(var):
                print(f"     - {var}")
    
    print("\n" + "="*70)
    print("诊断完成")
    print("="*70)
    
    if all_set:
        print("✅ 配置检查通过，可以运行数据集构建命令")
    else:
        print("❌ 配置不完整，请补充缺失的环境变量")
        print("\n请编辑 .env 文件，参考 env.example 进行配置")
    
    print("="*70 + "\n")


if __name__ == '__main__':
    main()

