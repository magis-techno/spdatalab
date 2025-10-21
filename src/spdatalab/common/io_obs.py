import os
import logging
from pathlib import Path
from spdatalab.common.config import getenv

logger = logging.getLogger(__name__)

def init_moxing():
    # 先设置环境变量和取消代理
    s3_endpoint = getenv('S3_ENDPOINT', required=True)
    s3_use_https = getenv('S3_USE_HTTPS', default='0')
    access_key = getenv('ADS_DATALAKE_USERNAME', required=True)
    secret_key = getenv('ADS_DATALAKE_PASSWORD', required=True)
    
    # 【调试】打印 moxing 初始化配置
    logger.info(f"[OBS调试] 初始化 moxing 环境:")
    logger.info(f"[OBS调试]   S3_ENDPOINT = {s3_endpoint}")
    logger.info(f"[OBS调试]   S3_USE_HTTPS = {s3_use_https}")
    logger.info(f"[OBS调试]   ACCESS_KEY_ID = {access_key[:5]}*** (长度:{len(access_key)})")
    logger.info(f"[OBS调试]   SECRET_ACCESS_KEY = {secret_key[:5]}*** (长度:{len(secret_key)})")
    
    os.environ['S3_ENDPOINT'] = s3_endpoint
    os.environ['S3_USE_HTTPS'] = s3_use_https
    os.environ['ACCESS_KEY_ID'] = access_key
    os.environ['SECRET_ACCESS_KEY'] = secret_key
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)
    
    # 再 import moxing 并 shift
    import moxing as mox
    mox.file.shift('os', 'mox')
    logger.info("[OBS调试] moxing 初始化完成")

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