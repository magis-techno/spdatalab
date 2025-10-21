# OBS 调试信息说明

## 调试目的

针对性地在 moxing 读取 OBS 文件的核心路径上添加详细的调试信息，用于排查数据集构建时的 OBS 访问问题。

## 添加的调试位置

### 1. `src/spdatalab/common/io_obs.py`
**功能**: 打印 moxing 初始化的环境变量配置

调试信息包括：
- S3_ENDPOINT 地址
- S3_USE_HTTPS 设置
- ACCESS_KEY_ID (脱敏显示)
- SECRET_ACCESS_KEY (脱敏显示)
- moxing 初始化完成状态

### 2. `src/spdatalab/common/file_utils.py`
**功能**: 打印文件打开的详细过程

调试信息包括：
- 文件路径
- 打开模式 (r/rb)
- 是否 OBS 路径
- mox.file.File() 调用参数
- 文件打开成功/失败状态
- 详细的错误信息（类型、消息）

### 3. `src/spdatalab/dataset/scene_list_generator.py`
**功能**: 打印文件读取和场景解析过程

调试信息包括：
- 开始读取文件标记
- 文件路径和类型
- 逐行读取进度（每10行打印一次）
- 成功解析的场景数
- 文件读取完成统计
- 详细的错误信息和完整异常堆栈

### 4. `src/spdatalab/dataset/dataset_manager.py`
**功能**: 打印数据项处理过程

调试信息包括：
- 处理的数据项信息（文件名、OBS路径）
- 缓存加载状态
- scene_id 提取过程
- 提取到的 scene_id 数量

## 调试信息格式

所有调试信息使用统一的前缀 `[OBS调试]`，方便过滤和查看：

```bash
# 只查看 OBS 调试信息
python -m spdatalab build-dataset ... 2>&1 | grep "\[OBS调试\]"
```

## 使用方法

### 运行带调试的命令

```bash
# 正常运行，会输出详细的 OBS 调试信息
python -m spdatalab build-dataset \
  --training-dataset-json data/0818_training_dataset.json \
  --output data/0818_golden_20251021.parquet \
  --format parquet
```

### 查看关键调试信息

```bash
# 只看 OBS 调试信息
python -m spdatalab build-dataset ... 2>&1 | grep "\[OBS调试\]"

# 保存到文件
python -m spdatalab build-dataset ... 2>&1 | tee debug_output.log

# 然后分析
grep "\[OBS调试\]" debug_output.log
grep "❌" debug_output.log  # 查看错误
```

## 预期输出

正常情况下，你应该看到类似的调试信息流程：

```
[OBS调试] ========================================
[OBS调试] 开始处理数据项:
[OBS调试]   文件名: train_god_xxx.jsonl.shrink
[OBS调试]   OBS路径: obs://yw-ads-training-gy1/data/ide/...
[OBS调试] ========================================

[OBS调试] extract_scene_ids_from_file 调用
[OBS调试]   文件路径: obs://yw-ads-training-gy1/data/ide/...

[OBS调试] 缓存不存在，需要从文件读取

[OBS调试] ========== 开始读取文件 ==========
[OBS调试] 文件路径: obs://yw-ads-training-gy1/...
[OBS调试] 是否OBS路径: True

[OBS调试] open_file 调用:
[OBS调试]   路径: obs://yw-ads-training-gy1/...
[OBS调试]   模式: r
[OBS调试]   是否OBS: True

[OBS调试] 正在初始化 moxing...

[OBS调试] 初始化 moxing 环境:
[OBS调试]   S3_ENDPOINT = http://10.170.30.79:80
[OBS调试]   S3_USE_HTTPS = 0
[OBS调试]   ACCESS_KEY_ID = l0088*** (长度:9)
[OBS调试]   SECRET_ACCESS_KEY = L2f44*** (长度:16)
[OBS调试] moxing 初始化完成

[OBS调试] 正在打开 OBS 文件: obs://yw-ads-training-gy1/...
[OBS调试] 调用 mox.file.File(path='obs://...', mode='r')
[OBS调试] OBS 文件打开成功

[OBS调试] 文件已打开，开始逐行读取
[OBS调试] 已读取 10 行，成功解析 10 个场景
[OBS调试] 已读取 20 行，成功解析 20 个场景
...
[OBS调试] 文件读取完成!
[OBS调试]   总行数: 100
[OBS调试]   成功解析场景数: 100
[OBS调试]   失败解析行数: 0

[OBS调试] 关闭文件: obs://yw-ads-training-gy1/...
[OBS调试] ========== 文件处理完成 ==========

[OBS调试] 提取完成: 100 个scene_id
[OBS调试] 数据项处理完成:
[OBS调试]   提取到 100 个 scene_id
[OBS调试] ========================================
```

## 错误诊断

如果出现错误，会看到详细的错误信息：

```
[OBS调试] ❌ 打开文件失败!
[OBS调试]   路径: obs://yw-ads-training-gy1/...
[OBS调试]   错误类型: OSError
[OBS调试]   错误信息: [具体错误信息]
```

### 常见错误及解决方案

1. **环境变量未设置**
   ```
   RuntimeError: Missing env S3_ENDPOINT
   ```
   解决：检查 .env 文件，运行 `python scripts/utilities/check_env_config.py`

2. **认证失败**
   ```
   ERROR: Access Denied / Authentication Failed
   ```
   解决：检查 ADS_DATALAKE_USERNAME 和 ADS_DATALAKE_PASSWORD 是否正确

3. **文件不存在**
   ```
   NoSuchKey / File not found
   ```
   解决：检查 OBS 路径是否正确，文件是否存在

4. **网络问题**
   ```
   Connection timeout / Network unreachable
   ```
   解决：检查网络连接，检查 S3_ENDPOINT 是否可访问

## 移除调试信息

调试完成后，需要移除这些调试代码。可以搜索并删除包含 `[OBS调试]` 的日志语句：

```bash
# 查找所有调试代码
grep -r "\[OBS调试\]" src/spdatalab/

# 文件列表
# - src/spdatalab/common/io_obs.py
# - src/spdatalab/common/file_utils.py  
# - src/spdatalab/dataset/scene_list_generator.py
# - src/spdatalab/dataset/dataset_manager.py
```

## 快速恢复版本

如果需要恢复到没有调试信息的版本：

```bash
# 查看当前修改
git diff src/spdatalab/common/io_obs.py
git diff src/spdatalab/common/file_utils.py
git diff src/spdatalab/dataset/scene_list_generator.py
git diff src/spdatalab/dataset/dataset_manager.py

# 恢复（如果需要）
git checkout src/spdatalab/common/io_obs.py
git checkout src/spdatalab/common/file_utils.py
git checkout src/spdatalab/dataset/scene_list_generator.py
git checkout src/spdatalab/dataset/dataset_manager.py
```

## 测试步骤

1. **提交代码到远程**
   ```bash
   git add src/spdatalab/common/io_obs.py \
           src/spdatalab/common/file_utils.py \
           src/spdatalab/dataset/scene_list_generator.py \
           src/spdatalab/dataset/dataset_manager.py
   
   git commit -m "临时添加 OBS 读取调试信息"
   git push
   ```

2. **在远程环境拉取代码**
   ```bash
   cd ~/liminzhen/1-code/spdatalab
   git pull
   ```

3. **运行测试命令**
   ```bash
   # 在 Docker 容器中
   python -m spdatalab build-dataset \
     --training-dataset-json data/0818_training_dataset.json \
     --output data/0818_golden_20251021.parquet \
     --format parquet 2>&1 | tee obs_debug.log
   ```

4. **分析输出**
   ```bash
   # 查看所有 OBS 调试信息
   grep "\[OBS调试\]" obs_debug.log
   
   # 查看错误
   grep "❌" obs_debug.log
   grep "ERROR" obs_debug.log
   
   # 查看完整异常
   grep -A 20 "Traceback" obs_debug.log
   ```

5. **根据输出定位问题**
   - 检查环境变量是否正确
   - 检查文件路径是否正确
   - 检查文件是否能成功打开
   - 检查文件内容是否能正常读取

6. **问题解决后移除调试代码**
   ```bash
   # 按照上面的"移除调试信息"部分操作
   ```

## 注意事项

- 这些调试信息会产生大量输出，建议重定向到文件
- 调试信息包含敏感信息（虽然已脱敏），不要提交日志文件到 Git
- 问题解决后记得移除调试代码
- 多线程环境下日志可能交错，使用分隔线辅助识别

