# bbox.py 大规模数据处理优化

## 新增功能

为处理500万clips的大规模数据，bbox.py新增了以下优化功能：

### 1. 按城市分schema存储 (`--use-city-schema`)

**目的**: 在QGIS中加载数据时避免数据过多，按城市分别组织数据

**功能**:
- 自动分析数据中的城市信息
- 为每个城市创建独立的schema（如`city_beijing`, `city_shanghai`等）
- 每个schema下有独立的`clips_bbox`表
- 未知城市数据存储在`city_unknown.clips_bbox`

**使用方法**:
```bash
python bbox.py --input dataset.json --use-city-schema --create-table
```

**数据库结构**:
```
├── city_beijing.clips_bbox
├── city_shanghai.clips_bbox  
├── city_shenzhen.clips_bbox
├── city_unknown.clips_bbox
└── public.clips_bbox_preview (如果启用预览)
```

### 2. 预览数据采样 (`--enable-preview`)

**目的**: 创建整体数据的1%采样，提供全局预览

**功能**:
- 使用一致性hash采样，确保采样的稳定性
- 默认采样率1%，可通过`--preview-rate`调整
- 所有城市的预览数据统一存储在`public.clips_bbox_preview`表

**使用方法**:
```bash
python bbox.py --input dataset.json --enable-preview --preview-rate 0.01
```

**采样算法**:
- 基于`data_name`的MD5 hash进行一致性采样
- 保证相同数据多次运行时采样结果一致
- 各城市按比例贡献预览数据

### 3. 组合使用

推荐的大规模数据处理配置：

```bash
python bbox.py \
  --input your_500w_dataset.json \
  --use-city-schema \
  --enable-preview \
  --preview-rate 0.01 \
  --create-table \
  --batch 2000 \
  --insert-batch 1000 \
  --work-dir ./large_scale_logs
```

## QGIS使用建议

### 1. 预览模式
- 首次查看数据时，连接到`public.clips_bbox_preview`表
- 快速了解数据整体分布和质量

### 2. 详细分析
- 根据感兴趣的城市，连接到对应的schema
- 例如：连接到`city_beijing.clips_bbox`查看北京地区详细数据

### 3. 连接字符串示例
```
预览数据: SELECT * FROM public.clips_bbox_preview
北京数据: SELECT * FROM city_beijing.clips_bbox  
上海数据: SELECT * FROM city_shanghai.clips_bbox
```

## 性能优化

### 1. 索引优化
- 每个表都有`geometry`, `data_name`, `scene_token`, `city_id`索引
- 支持快速空间查询和属性过滤

### 2. 批处理优化  
- 支持按城市分组并行插入
- 减少跨schema的数据操作开销

### 3. 内存管理
- 使用Parquet格式的进度跟踪
- 缓冲区机制减少磁盘I/O

## 兼容性

- 保持与原有单表模式的完全兼容
- 不使用新参数时行为与原版完全一致
- 支持从传统模式迁移到新模式

## 故障恢复

- 每个城市表独立，单个城市失败不影响其他城市
- 完整的进度跟踪和失败重试机制
- 支持断点续传

## 监控建议

```bash
# 查看各城市数据量
SELECT 
    schemaname, 
    tablename, 
    n_tup_ins as inserted_rows
FROM pg_stat_user_tables 
WHERE tablename = 'clips_bbox';

# 查看预览数据采样情况
SELECT 
    city_id, 
    COUNT(*) as preview_count 
FROM public.clips_bbox_preview 
GROUP BY city_id;
``` 