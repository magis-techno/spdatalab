# —— 基础镜像 ——
FROM python:3.11-slim           # Debian bookworm‑slim

# —— 切换 APT 源到公司内源（华为镜像示例）——
# 一些极简 slim 变体没有 /etc/apt/sources.list；统一先写一份新的再换源，避免 sed 报错
RUN echo "deb http://mirrors.tools.huawei.com/debian bookworm main contrib non-free
\
deb http://mirrors.tools.huawei.com/debian-security bookworm-security main contrib non-free
\
deb http://mirrors.tools.huawei.com/debian bookworm-updates main contrib non-free" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin \
        postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# —— Pip 内源配置 ——
RUN python -m pip install --upgrade pip && \
    pip config set global.trusted-host "mirrors.tools.huawei.com cmc.centralrepo.rnd.huawei.com pypi.org github.com raw.githubusercontent.com" && \
    pip config set global.index-url "http://mirrors.tools.huawei.com/pypi/simple/" && \
    pip config set global.extra-index-url "https://cmc.centralrepo.rnd.huawei.com/artifactory/product_pypi/simple/ https://cmc.centralrepo.rnd.huawei.com/artifactory/pypi-central-repo/simple/" && \
    pip install --no-cache-dir geopandas shapely pyproj moxing di-datalake.hive_connector

WORKDIR /workspace