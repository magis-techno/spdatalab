# —— 基础镜像 ——
FROM python:3.11-slim           
# Debian bookworm‑slim

# —— 切换 APT 源到公司内源（华为镜像示例）——
# bookworm‑slim 默认没有 sources.list；直接写新的镜像源避免 sed 报错
RUN echo "deb http://mirrors.tools.huawei.com/debian bookworm main contrib non-free \
deb http://mirrors.tools.huawei.com/debian-security bookworm-security main contrib non-free \
deb http://mirrors.tools.huawei.com/debian bookworm-updates main contrib non-free" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin \
        postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# —— 复制公司内网提供的 moxing wheel ——
# 将 moxing‑*.whl 放在仓库 docker 目录旁的 deps/ 下；构建时优先使用该文件
COPY deps/moxing-*.whl /tmp/

# —— Pip 内源配置 & 依赖安装 ——
RUN python -m pip install --upgrade pip && \
    pip config set global.trusted-host "mirrors.tools.huawei.com cmc.centralrepo.rnd.huawei.com pypi.org github.com raw.githubusercontent.com" && \
    pip config set global.index-url "http://mirrors.tools.huawei.com/pypi/simple/" && \
    pip config set global.extra-index-url "https://cmc.centralrepo.rnd.huawei.com/artifactory/product_pypi/simple/ https://cmc.centralrepo.rnd.huawei.com/artifactory/pypi-central-repo/simple/" && \
    # 先装本地 whl，如不存在再 fallback
    if ls /tmp/moxing-*.whl 1> /dev/null 2>&1; then \
        pip install /tmp/moxing_framework-2.2.9-py2-none-any.whl; \
    else \
        pip install moxing; \
    fi && \
    pip install --no-cache-dir geopandas shapely pyproj di-datalake.hive_connector

WORKDIR /workspace
