"""文件读取工具模块，提供统一的文件读取接口。"""

import logging
from typing import Union, TextIO, BinaryIO, Optional
from pathlib import Path
import moxing as mox
from contextlib import contextmanager
from spdatalab.common.io_obs import init_moxing

logger = logging.getLogger(__name__)

@contextmanager
def open_file(path: Union[str, Path], mode: str = 'r') -> Union[TextIO, BinaryIO]:
    """统一的文件打开接口，支持本地文件和OBS文件。
    
    Args:
        path: 文件路径，可以是本地路径或OBS路径（以obs://开头）
        mode: 打开模式，'r'为文本模式，'rb'为二进制模式
        
    Yields:
        文件对象，支持with语句
        
    Raises:
        FileNotFoundError: 文件不存在
        IOError: 文件打开失败
    """
    path = str(path)
    is_obs = path.startswith('obs://')
    
    try:
        if is_obs:
            init_moxing()  # 初始化 moxing 环境
            file_obj = mox.file.File(path, mode)
        else:
            file_obj = open(path, mode)
            
        yield file_obj
        
    except Exception as e:
        logger.error(f"打开文件失败 {path}: {str(e)}")
        raise
        
    finally:
        if 'file_obj' in locals():
            file_obj.close()

def is_obs_path(path: Union[str, Path]) -> bool:
    """判断是否为OBS路径。
    
    Args:
        path: 文件路径
        
    Returns:
        是否为OBS路径
    """
    return str(path).startswith('obs://')

def ensure_dir(path: Union[str, Path]) -> None:
    """确保目录存在，如果不存在则创建。
    
    Args:
        path: 目录路径
    """
    Path(path).mkdir(parents=True, exist_ok=True)
