"""数据解码工具模块，提供高效的数据解码功能。"""

import base64
import gzip
import json
import logging
from typing import Dict, Optional, Union
from io import BytesIO
import pickle
import io

logger = logging.getLogger(__name__)

class DecodeError(Exception):
    """解码错误基类。"""
    pass

class JsonDecodeError(DecodeError):
    """JSON解码错误。"""
    pass

class GzipDecodeError(DecodeError):
    """Gzip解码错误。"""
    pass

class Base64DecodeError(DecodeError):
    """Base64解码错误。"""
    pass

def decode_json(data: Union[str, bytes]) -> Dict:
    """解码JSON数据。
    
    Args:
        data: JSON字符串或字节
        
    Returns:
        解码后的字典
        
    Raises:
        JsonDecodeError: JSON解码失败
    """
    try:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return json.loads(data)
    except Exception as e:
        raise JsonDecodeError(f"JSON解码失败: {str(e)}")

def decode_gzip(data: bytes) -> bytes:
    """解码Gzip数据。
    
    Args:
        data: Gzip压缩的字节数据
        
    Returns:
        解压后的字节数据
        
    Raises:
        GzipDecodeError: Gzip解码失败
    """
    try:
        return gzip.decompress(data)
    except Exception as e:
        raise GzipDecodeError(f"Gzip解码失败: {str(e)}")

def decode_base64(data: str) -> bytes:
    """解码Base64数据。
    
    Args:
        data: Base64编码的字符串
        
    Returns:
        解码后的字节数据
        
    Raises:
        Base64DecodeError: Base64解码失败
    """
    try:
        return base64.b64decode(data)
    except Exception as e:
        raise Base64DecodeError(f"Base64解码失败: {str(e)}")

def decode_shrink_line(line: str) -> Optional[Dict]:
    """解码.shrink文件中的一行数据。
    
    支持两种格式：
    1. 纯JSON格式：以{开头的JSON字符串
    2. 压缩格式：base64(gzip(json))
    
    Args:
        line: 要解码的字符串
        
    Returns:
        解码后的字典，如果解码失败则返回None
    """
    line = line.strip()
    if not line:
        return None
        
    try:
        # 尝试直接解析JSON
        if line.startswith('{'):
            return decode_json(line)
            
        # 尝试解析压缩格式
        try:
            # base64 -> gzip -> json
            decoded = decode_base64(line)
            decompressed = decode_gzip(decoded)
            try:
                # 先尝试直接json解码
                return decode_json(decompressed)
            except JsonDecodeError:
                # 如果失败，尝试pickle解码
                try:
                    json_str = pickle.load(io.BytesIO(decompressed))
                    return decode_json(json_str)
                except Exception as e:
                    logger.warning(f"pickle解码失败: {str(e)}")
                    return None
        except (Base64DecodeError, GzipDecodeError) as e:
            logger.warning(f"压缩格式解码失败: {str(e)}")
            return None
            
    except JsonDecodeError as e:
        logger.warning(f"JSON解码失败: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"未知解码错误: {str(e)}")
        return None 