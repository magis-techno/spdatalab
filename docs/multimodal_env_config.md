# 多模态轨迹检索系统 - 环境变量配置指南

## 📋 必需环境变量

以下环境变量必须配置才能正常使用多模态轨迹检索功能：

```bash
# API密钥（必需）
export MULTIMODAL_API_KEY="your_actual_api_key"

# 用户名（必需）
export MULTIMODAL_USERNAME="your_username"
```

## 🔧 可选环境变量

以下环境变量有默认值，可根据需要进行调整：

```bash
# 项目名称（默认：driveinsight）
export MULTIMODAL_PROJECT="driveinsight"

# 平台标识（默认：xmodalitys-external）
export MULTIMODAL_PLATFORM="xmodalitys-external"

# 区域标识（默认：RaD-prod）
export MULTIMODAL_REGION="RaD-prod"

# 入口版本（默认：v2）
export MULTIMODAL_ENTRYPOINT_VERSION="v2"

# API基础URL（默认：https://driveinsight-api.ias.huawei.com）
export MULTIMODAL_API_BASE_URL="https://driveinsight-api.ias.huawei.com"

# API路径（默认：/xmodalitys/retrieve）
export MULTIMODAL_API_PATH="/xmodalitys/retrieve"

# 超时时间（秒，默认：30）
export MULTIMODAL_TIMEOUT="30"

# 最大重试次数（默认：3）
export MULTIMODAL_MAX_RETRIES="3"
```

## 🚀 快速配置

### 方法1：直接导出环境变量

```bash
# 必需配置
export MULTIMODAL_API_KEY="your_actual_api_key"
export MULTIMODAL_USERNAME="your_username"

# 测试配置是否生效
python -c "
from spdatalab.dataset.multimodal_data_retriever import APIConfig
config = APIConfig.from_env()
print(f'✅ 配置成功！API URL: {config.api_url}')
print(f'✅ 用户名: {config.username}')
print(f'✅ 项目: {config.project}')
"
```

### 方法2：使用.env文件

1. **创建.env文件**：
```bash
# 在项目根目录创建 .env 文件
cat > .env << 'EOF'
# 多模态API配置
MULTIMODAL_API_KEY=your_actual_api_key
MULTIMODAL_USERNAME=your_username
MULTIMODAL_PROJECT=driveinsight
EOF
```

2. **验证配置**：
```bash
# 加载环境变量后测试
source .env
python test_api_integration.py
```

## 📊 API请求格式

配置完成后，系统会按照以下格式发送API请求：

### 请求URL
```
https://driveinsight-api.ias.huawei.com/xmodalitys/retrieve
```

### 请求头
```json
{
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Authorization": "Bearer your_actual_api_key",
  "Content-Type": "application/json",
  "Deepdata-Platform": "xmodalitys-external",
  "Deepdata-Project": "driveinsight",
  "Deepdata-Region": "RaD-prod",
  "Entrypoint-Version": "v2",
  "Host": "driveinsight-api.ias.huawei.com",
  "User-Agent": "spdatalab-multimodal/1.0.0",
  "username": "your_username"
}
```

### 请求体

**文本检索**：
```json
{
  "text": "bicycle crossing intersection",
  "collection": "ddi_collection_camera_encoded_2",
  "camera": "camera_2",
  "start": 0,
  "count": 5,
  "modality": 1,
  "start_time": 1234567891011,
  "end_time": 1234567891111
}
```

**图片检索**：
```json
{
  "images": ["base64_encoded_image_1", "base64_encoded_image_2"],
  "collection": "ddi_collection_camera_encoded_2",
  "camera": "camera_2",
  "start": 0,
  "count": 5,
  "modality": 2,
  "start_time": 1234567891011,
  "end_time": 1234567891111
}
```

## 🧪 测试配置

### 基础配置测试
```bash
# 测试环境变量导入
python test_multimodal_basic.py

# 测试API集成
python test_api_integration.py
```

### CLI测试
```bash
# 测试完整CLI功能（需要有效的API密钥）
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "bicycle crossing intersection" \
    --collection "ddi_collection_camera_encoded_2" \
    --count 10 \
    --start 0 \
    --start-time 1234567891011 \
    --end-time 1234567891111 \
    --output-json "test_results.json" \
    --verbose
```

## ⚠️ 注意事项

1. **API密钥安全**：
   - 不要将API密钥提交到版本控制系统
   - 使用环境变量或安全的密钥管理服务

2. **API限制**：
   - 单次查询最多10,000条记录
   - 累计查询最多100,000条记录（会话级别）

3. **网络访问**：
   - 确保能够访问 `https://driveinsight-api.ias.huawei.com`
   - 如果在内网环境，可能需要配置代理

4. **相机自动匹配**：
   - `ddi_collection_camera_encoded_1` → `camera_1`
   - `ddi_collection_camera_encoded_2` → `camera_2`
   - 其他格式将使用默认值 `camera_1`

## 🔍 故障排除

### 常见错误

1. **环境变量未设置**：
```
❌ API配置不完整，请设置环境变量：
   MULTIMODAL_API_KEY=<your_api_key> (必需)
   MULTIMODAL_USERNAME=<your_username> (必需)
```

**解决方案**：设置必需的环境变量

2. **API调用失败**：
```
❌ 文本检索失败: 401 Unauthorized
```

**解决方案**：检查API密钥是否正确

3. **网络连接问题**：
```
❌ API调用失败: Connection timeout
```

**解决方案**：检查网络连接和防火墙设置

### 调试模式

使用详细输出模式进行调试：

```bash
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "test query" \
    --collection "ddi_collection_camera_encoded_1" \
    --verbose
```

这将显示详细的API调用信息，包括：
- 构建的请求头
- 请求参数
- 响应状态
- 错误详情

---

**更新时间**：2024年当前时间  
**版本**：v1.0 - 新API格式适配
