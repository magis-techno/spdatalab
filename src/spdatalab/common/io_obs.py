import os
from pathlib import Path
from spdatalab.common.config import getenv
import logging

logger = logging.getLogger(__name__)

def init_moxing():
    # ============ 临时调试代码 START ============
    logger.info("="*70)
    logger.info("[调试] 开始初始化 OBS 环境")
    logger.info("="*70)
    
    # 读取环境变量
    s3_endpoint = getenv('S3_ENDPOINT', required=True)
    s3_use_https = getenv('S3_USE_HTTPS', default='0')
    access_key_id = getenv('ADS_DATALAKE_USERNAME', required=True)
    secret_access_key = getenv('ADS_DATALAKE_PASSWORD', required=True)
    
    # 打印配置信息（隐藏敏感信息）
    logger.info(f"[调试] S3_ENDPOINT: {s3_endpoint}")
    logger.info(f"[调试] S3_USE_HTTPS: {s3_use_https}")
    logger.info(f"[调试] ACCESS_KEY_ID: {access_key_id[:5]}*** (长度: {len(access_key_id)})")
    logger.info(f"[调试] SECRET_ACCESS_KEY: {secret_access_key[:5]}*** (长度: {len(secret_access_key)})")
    
    # 先设置环境变量和取消代理
    os.environ['S3_ENDPOINT'] = s3_endpoint
    os.environ['S3_USE_HTTPS'] = s3_use_https
    os.environ['ACCESS_KEY_ID'] = access_key_id
    os.environ['SECRET_ACCESS_KEY'] = secret_access_key
    
    # 检查是否有代理设置
    removed_proxies = []
    if 'http_proxy' in os.environ:
        removed_proxies.append(f"http_proxy={os.environ['http_proxy']}")
    if 'https_proxy' in os.environ:
        removed_proxies.append(f"https_proxy={os.environ['https_proxy']}")
    
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)
    
    if removed_proxies:
        logger.info(f"[调试] 已移除代理设置: {', '.join(removed_proxies)}")
    else:
        logger.info("[调试] 无代理设置需要移除")
    
    # 验证环境变量已设置
    logger.info(f"[调试] 验证 os.environ['S3_ENDPOINT']: {os.environ.get('S3_ENDPOINT')}")
    logger.info(f"[调试] 验证 os.environ['ACCESS_KEY_ID']: {os.environ.get('ACCESS_KEY_ID', '')[:5]}***")
    
    # 再 import moxing 并 shift
    logger.info("[调试] 导入 moxing 模块...")
    import moxing as mox
    logger.info("[调试] 执行 mox.file.shift('os', 'mox')...")
    mox.file.shift('os', 'mox')
    logger.info("[调试] OBS 环境初始化完成")
    logger.info("="*70)
    # ============ 临时调试代码 END ============

def download(obs_path: str, local_path: Path, retries: int = 3):
    if not obs_path.startswith('obs://'):
        obs_path = f'obs://{obs_path}'
    local_path.parent.mkdir(parents=True, exist_ok=True)
    for i in range(retries):
        try:
            mox.file.copy(obs_path, str(local_path))
            if local_path.exists() and local_path.stat().st_size > 0:
                return
        except Exception as e:
            if i == retries -1:
                raise e