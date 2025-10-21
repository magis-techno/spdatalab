# 临时调试代码说明

## 背景

为了诊断 OBS 访问问题，在代码中添加了临时的详细日志输出。这些代码标记为"临时调试代码"，用于跟踪：

1. 环境变量的加载和传递
2. OBS 连接的初始化过程
3. 文件读取的详细流程
4. 错误发生时的完整堆栈跟踪

## 添加调试代码的位置

### 1. `src/spdatalab/common/io_obs.py`

**修改内容**：在 `init_moxing()` 函数中添加详细的环境变量打印

**输出信息**：
- S3_ENDPOINT 值
- S3_USE_HTTPS 值
- ACCESS_KEY_ID（隐藏敏感部分）
- SECRET_ACCESS_KEY（隐藏敏感部分）
- 代理设置的移除情况
- moxing 模块导入状态

**示例输出**：
```
======================================================================
[调试] 开始初始化 OBS 环境
======================================================================
[调试] S3_ENDPOINT: http://10.170.30.79:80
[调试] S3_USE_HTTPS: 0
[调试] ACCESS_KEY_ID: l0088*** (长度: 9)
[调试] SECRET_ACCESS_KEY: L2f44*** (长度: 40)
[调试] 无代理设置需要移除
[调试] 验证 os.environ['S3_ENDPOINT']: http://10.170.30.79:80
[调试] 验证 os.environ['ACCESS_KEY_ID']: l0088***
[调试] 导入 moxing 模块...
[调试] 执行 mox.file.shift('os', 'mox')...
[调试] OBS 环境初始化完成
======================================================================
```

### 2. `src/spdatalab/common/file_utils.py`

**修改内容**：在 `open_file()` 函数中添加文件打开过程的跟踪

**输出信息**：
- OBS 文件路径
- 文件打开模式
- 打开成功/失败状态
- 错误类型和详情

**示例输出**：
```
[调试] 准备打开 OBS 文件: obs://yw-ads-training-gy1/data/ide/cleantask/...
[调试] 打开模式: r
[调试] 调用 mox.file.File(obs://yw-ads-training-gy1/data/ide/..., r)
[调试] 成功打开 OBS 文件
```

### 3. `src/spdatalab/dataset/scene_list_generator.py`

**修改内容**：在 `iter_scenes_from_file()` 函数中添加文件读取过程的跟踪

**输出信息**：
- 开始处理文件
- 文件打开状态
- 前3行的解码情况
- 读取完成的总行数
- 异常的详细堆栈跟踪

**示例输出**：
```
[调试] SceneListGenerator.iter_scenes_from_file 开始处理: obs://yw-ads-training-gy1/...
[调试] 文件已打开，开始逐行读取
[调试] 成功解码第 1 行，scene_id: 041982022100714850forced...
[调试] 成功解码第 2 行，scene_id: 041982022100714851forced...
[调试] 成功解码第 3 行，scene_id: 041982022100714852forced...
[调试] 文件读取完成，共处理 2000 行
```

### 4. `src/spdatalab/dataset/dataset_manager.py`

**修改内容**：在数据处理流程中添加跟踪

- `_process_single_item()`: 处理每个数据项
- `extract_scene_ids_from_file()`: 提取场景ID的过程

**输出信息**：
- 数据项名称和OBS路径
- 缓存使用情况
- 场景ID提取的进度
- 异常的详细堆栈跟踪

**示例输出**：
```
[调试] 处理数据项: train_god_20250903_183005_178_4088_2frames.jsonl.shrink
[调试] OBS路径: obs://yw-ads-training-gy1/data/ide/cleantask/...
[调试] extract_scene_ids_from_file: obs://yw-ads-training-gy1/...
[调试] 缓存不存在，需要从OBS读取文件
[调试] 开始迭代场景数据...
[调试] 迭代完成，共收集 2000 个scene_id
[调试] 提取到 2000 个 scene_id
```

## 使用调试日志

### 运行带调试日志的命令

```bash
# 正常运行，会自动输出调试日志
python -m spdatalab build-dataset \
  --training-dataset-json data/0818_training_dataset.json \
  --output data/0818_golden_20251021.parquet \
  --format parquet
```

### 保存日志到文件

```bash
# 同时输出到屏幕和文件
python -m spdatalab build-dataset \
  --training-dataset-json data/0818_training_dataset.json \
  --output data/0818_golden_20251021.parquet \
  --format parquet 2>&1 | tee debug_output.log

# 只保存到文件
python -m spdatalab build-dataset \
  --training-dataset-json data/0818_training_dataset.json \
  --output data/0818_golden_20251021.parquet \
  --format parquet > debug_output.log 2>&1
```

### 过滤调试日志

```bash
# 只查看调试信息
grep "\[调试\]" debug_output.log

# 只查看错误信息
grep -E "(ERROR|\[调试\].*失败)" debug_output.log

# 查看 OBS 相关的调试信息
grep "\[调试\].*OBS" debug_output.log
```

## 移除调试代码

### 自动移除（推荐）

使用提供的 Python 脚本：

```bash
# 在项目根目录执行
python scripts/utilities/remove_debug_logging.py
```

这个脚本会：
1. 自动识别所有包含调试代码的文件
2. 创建备份文件（.backup 后缀）
3. 移除所有标记为"临时调试代码"的代码块
4. 显示处理结果

### 手动移除

如果需要手动移除，查找并删除以下标记之间的所有内容（包括标记行）：

```python
# ============ 临时调试代码 START ============
... 这里是调试代码 ...
# ============ 临时调试代码 END ============
```

### 验证移除结果

```bash
# 检查是否还有调试代码残留
grep -r "临时调试代码" src/

# 如果没有输出，说明已全部移除
```

### 清理备份文件

验证代码无误后，删除备份文件：

```bash
# 查看备份文件
find src -name "*.backup"

# 删除所有备份文件
find src -name "*.backup" -delete
```

## 问题诊断指南

### 1. 环境变量未生效

**症状**：
```
RuntimeError: Missing env S3_ENDPOINT
```

**查看调试日志**：
- 检查 `check_env_config.py` 的输出
- 查看 `[调试] 开始初始化 OBS 环境` 部分

### 2. OBS 连接失败

**症状**：
```
WARNING:root:Retry=9, Wait=0.1, Function=getObject...
```

**查看调试日志**：
- 检查 S3_ENDPOINT 是否正确
- 检查 ACCESS_KEY_ID 和 SECRET_ACCESS_KEY 长度是否合理
- 查看是否有代理设置被移除

### 3. 文件读取失败

**症状**：
文件打开或读取时出错

**查看调试日志**：
- 查看 `[调试] 准备打开 OBS 文件` 相关输出
- 检查文件路径是否正确
- 查看错误类型和详情
- 查看完整的堆栈跟踪

### 4. 数据提取失败

**症状**：
场景ID数量为0或异常

**查看调试日志**：
- 查看 `[调试] 处理数据项` 相关输出
- 检查缓存使用情况
- 查看场景ID提取的进度

## 注意事项

1. **调试代码是临时的**：这些代码仅用于诊断，不应长期保留在生产代码中
2. **性能影响**：大量的日志输出会影响性能，诊断完成后应及时移除
3. **备份重要**：移除调试代码前确保已创建备份
4. **安全性**：虽然已隐藏敏感信息，但仍需注意日志文件的安全
5. **测试验证**：移除调试代码后，运行测试确保功能正常

## 相关脚本

- `scripts/utilities/check_env_config.py` - 环境变量配置诊断
- `scripts/utilities/print_env_vars.py` - 快速打印环境变量
- `scripts/utilities/remove_debug_logging.py` - 自动移除调试代码
- `scripts/utilities/remove_debug_logging.sh` - Shell版本的移除脚本

## 问题反馈

如果调试日志帮助你发现了问题，或者有改进建议，请：

1. 保存完整的调试日志输出
2. 记录问题的具体表现
3. 记录解决方案（如果已解决）
4. 与团队分享经验

## 清理检查清单

在移除调试代码前：

- [ ] 已运行程序并收集完整的调试日志
- [ ] 已诊断出问题的根本原因
- [ ] 已验证问题已解决
- [ ] 已备份当前代码（或已提交到 Git）

移除调试代码后：

- [ ] 已运行自动移除脚本或手动移除
- [ ] 已检查无残留的调试代码标记
- [ ] 已运行测试验证功能正常
- [ ] 已清理备份文件
- [ ] 已提交更改到 Git

