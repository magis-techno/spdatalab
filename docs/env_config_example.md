# 环境变量配置示例

## 📋 .env配置文件示例

在项目根目录创建 `.env` 文件，并填入以下配置：

```bash
# 多模态轨迹检索系统配置示例
# 复制此内容到项目根目录的 .env 文件中

# ==============================================
# 多模态API配置 (必需)
# ==============================================

# 项目名称
MULTIMODAL_PROJECT=your_project_name

# API密钥
MULTIMODAL_API_KEY=your_api_key_here

# 用户名
MULTIMODAL_USERNAME=your_username

# ==============================================
# 多模态API配置 (可选)
# ==============================================

# API服务地址 (默认: https://driveinsight-api.ias.huawei.com/xmodalitys)
MULTIMODAL_API_URL=https://driveinsight-api.ias.huawei.com/xmodalitys

# API超时时间，单位秒 (默认: 30)
MULTIMODAL_TIMEOUT=30

# 最大重试次数 (默认: 3)
MULTIMODAL_MAX_RETRIES=3

# ==============================================
# 数据库配置 (现有系统配置)
# ==============================================

# 本地PostgreSQL配置
LOCAL_DB_HOST=local_pg
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=postgres
LOCAL_DB_USER=postgres
LOCAL_DB_PASSWORD=postgres

# Hive配置 (如需要)
# HIVE_HOST=your_hive_host
# HIVE_PORT=10000
# HIVE_DATABASE=your_database

# ==============================================
# 其他配置
# ==============================================

# 日志级别 (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO

# 开发模式标志
DEV_MODE=true
```

## 🚀 快速配置

1. **复制配置模板**：
   ```bash
   cp docs/env_config_example.md .env
   # 然后编辑 .env 文件，删除markdown格式，只保留配置内容
   ```

2. **填入实际配置**：
   - `MULTIMODAL_PROJECT`: 从Ring平台获取的项目名称
   - `MULTIMODAL_API_KEY`: 从Ring平台获取的API密钥
   - `MULTIMODAL_USERNAME`: 您的用户名

3. **可选配置调整**：
   - `MULTIMODAL_API_URL`: 如果使用不同的API环境
   - `MULTIMODAL_TIMEOUT`: 根据网络情况调整超时时间
   - `MULTIMODAL_MAX_RETRIES`: 根据需要调整重试次数

## 🔒 安全注意事项

- **不要提交.env文件**：`.env`文件包含敏感信息，已被gitignore忽略
- **生产环境**：在生产环境中直接设置环境变量，不使用.env文件
- **权限控制**：确保.env文件权限设置为600（仅所有者可读写）

## 📖 使用方式

配置完成后，可以直接使用命令行工具：

```bash
# 基础使用
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "bicycle crossing intersection" \
    --collection "ddi_collection_camera_encoded_1" \
    --output-table "discovered_trajectories"

# 详细输出
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "red car turning left" \
    --collection "ddi_collection_camera_encoded_1" \
    --verbose
```

## 🐛 配置问题排查

### 检查环境变量是否正确加载

```bash
# 检查必需变量
echo $MULTIMODAL_PROJECT
echo $MULTIMODAL_API_KEY
echo $MULTIMODAL_USERNAME

# 检查可选变量
echo $MULTIMODAL_API_URL
echo $MULTIMODAL_TIMEOUT
echo $MULTIMODAL_MAX_RETRIES
```

### 常见错误

1. **缺少必需变量**：
   ```
   ❌ API配置不完整，请设置环境变量：
   RuntimeError: Missing env MULTIMODAL_PROJECT
   ```
   **解决**：确保.env文件中设置了所有必需变量

2. **URL格式错误**：
   ```
   requests.exceptions.InvalidURL: Invalid URL
   ```
   **解决**：检查MULTIMODAL_API_URL格式是否正确

3. **超时错误**：
   ```
   requests.exceptions.Timeout
   ```
   **解决**：增加MULTIMODAL_TIMEOUT值或检查网络连接

## 🔄 配置更新

修改.env文件后，需要重新启动应用或重新加载环境变量：

```bash
# 重新加载环境变量
source .env

# 或重新启动应用
python -m spdatalab.fusion.multimodal_trajectory_retrieval --help
```
