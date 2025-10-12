"""文件读取工具模块，提供统一的文件读取接口。"""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Optional, TextIO, Union

logger = logging.getLogger(__name__)

# 尝试导入 moxing，如果不可用则设为 None
try:
    import moxing as mox  # type: ignore
    from spdatalab.common.io_obs import init_moxing  # type: ignore
    MOXING_AVAILABLE = True
except ImportError:  # pragma: no cover - 依赖按需安装
    mox = None  # type: ignore
    init_moxing = None  # type: ignore
    MOXING_AVAILABLE = False
    logger.debug("moxing 模块不可用，OBS 支持已禁用")


@contextmanager
def open_file(path: Union[str, Path], mode: str = "r") -> Union[TextIO, BinaryIO]:
    """统一的文件打开接口，支持本地文件和 OBS 文件。

    Args:
        path: 文件路径，可以是本地路径或 OBS 路径（以 ``obs://`` 开头）
        mode: 打开模式，例如 ``"r"``（文本）或 ``"rb"``（二进制）

    Yields:
        文件对象，支持 ``with`` 语句。

    Raises:
        FileNotFoundError: 文件不存在。
        IOError: 文件打开失败。
        ImportError: 尝试访问 OBS 文件但 moxing 模块不可用。
    """

    path = str(path)
    is_obs = path.startswith("obs://")

    file_obj: Optional[Union[TextIO, BinaryIO]] = None
    try:
        if is_obs:
            if not MOXING_AVAILABLE or mox is None or init_moxing is None:
                raise ImportError(
                    "尝试访问 OBS 文件但 moxing 模块不可用。"
                    "请安装 moxing: pip install moxing"
                )
            init_moxing()
            file_obj = mox.file.File(path, mode)  # type: ignore[attr-defined]
        else:
            file_obj = open(path, mode)

        yield file_obj

    except Exception as exc:  # pragma: no cover - 直接透出异常
        logger.error("打开文件失败 %s: %s", path, exc)
        raise
    finally:
        if file_obj is not None:
            file_obj.close()


def is_obs_path(path: Union[str, Path]) -> bool:
    """判断是否为 OBS 路径。"""

    return str(path).startswith("obs://")


def ensure_dir(path: Union[str, Path]) -> None:
    """确保目录存在，如果不存在则创建。"""

    Path(path).mkdir(parents=True, exist_ok=True)
