FROM python:3.11-slim
# Internal APT mirror
RUN echo "deb http://mirrors.tools.huawei.com/debian bookworm main contrib non-free deb http://mirrors.tools.huawei.com/debian-security bookworm-security main contrib non-free deb http://mirrors.tools.huawei.com/debian bookworm-updates main contrib non-free" > /etc/apt/sources.list &&         apt-get update &&         apt-get install -y --no-install-recommends gdal-bin postgresql-client &&         rm -rf /var/lib/apt/lists/*

# di-datalake from internal repo
RUN pip install --index-url https://cmc-cd-mirror.rnd.huawei.com/pypi/simple/          --extra-index-url https://cmc.centralrepo.rnd.huawei.com/artifactory/product_pypi/simple          --trusted-host cmc-cd-mirror.rnd.huawei.com          --trusted-host cmc.centralrepo.rnd.huawei.com          hw-ads-di-datalake

COPY deps/moxing_framework-*.whl /tmp/

RUN pip config set global.index-url https://mirrors.tools.huawei.com/pypi/simple/ &&         pip config set global.trusted-host mirrors.tools.huawei.com &&         pip config set global.timeout 120

RUN pip install --no-cache-dir /tmp/moxing_framework-*.whl geopandas shapely pyproj pandas sqlalchemy psycopg2-binary

WORKDIR /workspace