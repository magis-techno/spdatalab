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
#   • 先写 pip.conf 避免 **任何** 请求打到 pypi.org
#   • 不再显式 "pip install --upgrade pip"，直接用镜像自带版本即可
RUN mkdir -p /root/.pip && \
    echo "[global]"                                        >  /root/.pip/pip.conf && \
    echo "index-url = https://cmc-cd-mirror.rnd.huawei.com/pypi/simple/"  >> /root/.pip/pip.conf && \
    echo "extra-index-url = https://cmc.centralrepo.rnd.huawei.com/artifactory/product_pypi/simple/ https://cmc.centralrepo.rnd.huawei.com/artifactory/pypi-central-repo/simple/" >> /root/.pip/pip.conf && \
    echo "trusted-host = cmc-cd-mirror.rnd.huawei.com cmc.centralrepo.rnd.huawei.com mirrors.tools.huawei.com" >> /root/.pip/pip.conf && \
    # 先装核心依赖（moxing 依赖 di‑datalake）
    pip install --no-cache-dir di-datalake.hive_connector hw-ads-di-datalake && \
    # 再装本地 moxing whl，如不存在则从内源拉取
    if ls /tmp/moxing-*.whl 1> /dev/null 2>&1; then \
        pip install /tmp/moxing-*.whl; \
    else \
        pip install moxing; \
    fi && \
    # 其他常用空间依赖
    pip install --no-cache-dir geopandas shapely pyproj

WORKDIR /workspace