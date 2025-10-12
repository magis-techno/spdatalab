"""Hive 访问工具。

Use ``hive_cursor(<catalog>)`` to get a cursor bound to one of several Kyuubi
catalogs:

* **app_gy1**        – 业务库（scene_token ↔ dataset_name）
* **dataset_gy1**    – 轨迹数据库
* **rcdatalake_gy1** – RoadCode 加密数据湖
* **tagdatalake_gy1** – 单片段标签数据湖
"""

from contextlib import contextmanager


__all__ = ["hive_cursor"]

# 尝试导入 di_datalake，如果不可用则设为 None
try:  # pragma: no cover - 依赖在特定环境下存在
    from di_datalake.hive_connector import HiveConnector  # type: ignore
    HIVE_AVAILABLE = True
except ImportError:  # pragma: no cover - 允许在无 Hive 环境下导入
    HiveConnector = None  # type: ignore
    HIVE_AVAILABLE = False


def _get_conn(catalog: str):
    """Return a :class:`HiveConnector` bound to *catalog*."""

    if not HIVE_AVAILABLE or HiveConnector is None:
        raise ImportError(
            "di_datalake.hive_connector 模块不可用。"
            "此功能需要在华为云环境中运行。"
        )

    return HiveConnector(  # type: ignore[call-arg]
        configuration={"kyuubi.engine.type": "dws"},
        catalog=catalog,
    )


@contextmanager
def hive_cursor(catalog: str = "app_gy1"):
    """Yield a cursor for the chosen *catalog*.

    Raises
    ------
    ImportError
        如果 di_datalake 模块不可用。
    """

    conn = _get_conn(catalog)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()
