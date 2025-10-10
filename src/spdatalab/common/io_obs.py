import os
from pathlib import Path
from spdatalab.common.config import getenv

def init_moxing():
    # 先设置环境变量和取消代理
    os.environ['S3_ENDPOINT'] = getenv('S3_ENDPOINT', required=True)
    os.environ['S3_USE_HTTPS'] = getenv('S3_USE_HTTPS', default='0')
    os.environ['ACCESS_KEY_ID'] = getenv('ADS_DATALAKE_USERNAME', required=True)
    os.environ['SECRET_ACCESS_KEY'] = getenv('ADS_DATALAKE_PASSWORD', required=True)
    os.environ.pop('http_proxy', None)
    os.environ.pop('https_proxy', None)
    # 再 import moxing 并 shift
    import moxing as mox
    mox.file.shift('os', 'mox')

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