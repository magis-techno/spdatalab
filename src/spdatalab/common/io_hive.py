"""HiveConnector helpers.

Use ``hive_cursor(<catalog>)`` to get a cursor bound to one of several
Kyuubi catalogs:

* **app_gy1**        – 业务库（scene_token ↔ dataset_name）
* **dataset_gy1**    – 轨迹数据库
* **rcdatalake_gy1** – RoadCode 加密数据湖
* **tagdatalake_gy1** – 单片段标签数据湖
"""
from contextlib import contextmanager
from di_datalake.hive_connector import HiveConnector
from spdatalab.common.config import getenv

__all__ = ["hive_cursor"]


def _get_conn(catalog: str) -> HiveConnector:
    """Return a HiveConnector bound to *catalog*."""
    return HiveConnector(
        configuration={"kyuubi.engine.type": "dws"},
        catalog=catalog,
    )


@contextmanager
def hive_cursor(catalog: str = "app_gy1"):
    """Yield a cursor for the chosen *catalog*.

    Example
    -------
    >>> with hive_cursor("dataset_gy1") as cur:
    ...     cur.execute("SELECT ...")
    """
    conn = _get_conn(catalog)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()