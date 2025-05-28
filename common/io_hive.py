from contextlib import contextmanager
from di_datalake.hive_connector import HiveConnector
from spdatalab.common.config import getenv

@contextmanager
def hive_cursor():
    conn = HiveConnector(
        configuration={'kyuubi.engine.type':'dws'},
        catalog='app_gy1',
        username=getenv('ADS_DATALAKE_USERNAME', required=True),
        password=getenv('ADS_DATALAKE_PASSWORD', required=True),
        service_url=getenv('DATALAKE_SERVICE_URL', required=True)
    )
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()