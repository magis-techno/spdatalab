# 轨迹生成模块开发 TODO List

## 📋 项目背景

### 业务需求
在现有的bbox空间分析基础上，需要更精细的轨迹级分析能力：
1. **现状**: bbox模块提供区域级分析（polygon），适合大范围空间筛选
2. **需求**: 在bbox分析后，对特定scene_id进行轨迹级精细分析（linestring + events）
3. **工作流**: `bbox分析 → QGIS筛选 → scene_id列表 → 轨迹分析`

### 技术目标
- 从scene_id列表生成连续轨迹线（LineString）
- 检测轨迹中的关键变化点（AVP状态、速度突变等）
- 支持dataset属性继承，保持数据一致性
- 独立模块设计，不影响现有bbox功能

### 数据架构
```
轨迹表: trajectories_[table_name] (LineString + 统计信息)
变化点表: trajectory_events_[table_name] (Point + 变化信息)
```

## 🚀 开发阶段

### Phase 1: 基础轨迹生成 [P0]
**目标**: 实现从scene_id到轨迹线的基础功能
**估时**: 5-7天

#### ✅ 完成项目
- [ ] **环境搭建**
  - [ ] 创建 `src/spdatalab/dataset/trajectory.py`
  - [ ] 设置基础依赖导入
  - [ ] 配置数据库连接常量

#### 🔄 进行中项目
- [ ] **数据加载模块**
  - [ ] 实现 `load_scene_ids()` - 从txt文件加载scene_id列表
  - [ ] 添加数据验证和错误处理
  - [ ] 支持空行和注释过滤

#### 📋 待办项目
- [ ] **轨迹点查询**
  - [ ] 实现 `fetch_trajectory_points()` - 查询单个scene的轨迹点
  - [ ] 编写SQL查询语句（从ddi_data_points表）
  - [ ] 批量查询优化
  - [ ] 点数据时间排序

- [ ] **轨迹几何构建**
  - [ ] 实现 `build_trajectory()` - 构建LineString几何
  - [ ] 轨迹长度计算（PostGIS ST_Length）
  - [ ] 时间跨度和点数量统计
  - [ ] 速度统计计算（avg, max, min, std）
  - [ ] AVP点统计

- [ ] **数据库表管理**
  - [ ] 实现 `create_trajectory_table()` - 创建轨迹表
  - [ ] 表结构SQL + PostGIS几何列
  - [ ] 索引创建（geometry, scene_token）
  - [ ] 实现 `insert_trajectory_data()` - 批量数据插入

- [ ] **CLI基础接口**
  - [ ] 基础命令行参数解析
  - [ ] 简单的进度显示
  - [ ] 错误日志记录

### Phase 2: 变化点检测 [P1]
**目标**: 识别轨迹中的关键变化点
**估时**: 3-4天

#### 📋 待办项目
- [ ] **AVP变化检测**
  - [ ] 实现 `detect_avp_changes()` - AVP状态变化检测
  - [ ] 状态切换点识别（0↔1）
  - [ ] 变化点位置和属性记录

- [ ] **速度突变检测**
  - [ ] 实现 `detect_speed_spikes()` - 基于统计的速度异常检测
  - [ ] 可配置阈值参数（标准差倍数）
  - [ ] 突变幅度计算

- [ ] **变化点表管理**
  - [ ] 实现 `create_events_table()` - 创建变化点表
  - [ ] 外键约束到轨迹表
  - [ ] 实现 `insert_events_data()` - 变化点数据插入

- [ ] **其他变化点类型**
  - [ ] workstage变化检测（可选）
  - [ ] 可扩展的变化点检测框架

### Phase 3: 增强功能 [P2]
**目标**: Dataset属性继承和CLI完善
**估时**: 4-5天

#### 📋 待办项目
- [ ] **Dataset属性继承**
  - [ ] 实现 `load_dataset_metadata()` - 解析dataset文件
  - [ ] 支持JSON/Parquet格式
  - [ ] scene_token与dataset属性映射
  - [ ] 动态表字段创建
  - [ ] 属性数据类型转换

- [ ] **CLI接口完善**
  - [ ] 完整命令行参数支持
  - [ ] `--dataset` 参数支持
  - [ ] `--inherit-attributes` 选项
  - [ ] `--detect-avp-changes` 选项
  - [ ] `--detect-speed-spikes` 选项
  - [ ] `--speed-spike-threshold` 参数

- [ ] **进度跟踪增强**
  - [ ] 详细进度显示
  - [ ] 错误统计和汇总
  - [ ] 处理日志优化

### Phase 4: 测试验证 [P1]
**目标**: 确保功能正确性和性能
**估时**: 2-3天

#### 📋 待办项目
- [ ] **功能测试**
  - [ ] 基础轨迹生成测试
  - [ ] 变化点检测准确性测试
  - [ ] 不同数据规模测试
  - [ ] 错误场景处理测试

- [ ] **性能验证**
  - [ ] 1000个scene_id处理性能测试
  - [ ] 内存使用监控
  - [ ] 数据库操作性能测试

- [ ] **集成验证**
  - [ ] 与现有系统兼容性测试
  - [ ] 端到端工作流验证

## 🎯 当前优先级

### 立即开始
1. **环境搭建** - 创建基础文件结构
2. **数据加载** - scene_id列表加载功能
3. **轨迹点查询** - 核心数据获取功能

### 本周目标
- [ ] 完成Phase 1的核心功能
- [ ] 实现基础的轨迹生成pipeline
- [ ] 基础CLI接口可用

### 验收标准
- [ ] 能够从scene_id列表生成轨迹表
- [ ] 轨迹几何正确（LineString格式）
- [ ] 统计指标计算准确
- [ ] 基础变化点检测功能
- [ ] CLI接口友好易用

## 📝 开发记录

### 2025-01-XX - 项目启动
- [x] 创建开发TODO文档
- [x] 开始环境搭建

### 2025-01-XX - Phase 1完成
- [x] **环境搭建完成**
  - [x] 创建 `src/spdatalab/dataset/trajectory.py`
  - [x] 设置基础依赖导入和数据库连接
  - [x] 配置日志和信号处理

- [x] **核心功能实现完成**
  - [x] 实现 `load_scene_ids()` - 从txt文件加载scene_id列表
  - [x] 实现 `fetch_trajectory_points()` - 查询轨迹点数据
  - [x] 实现 `build_trajectory()` - 构建LineString几何和统计信息
  - [x] 实现 `create_trajectory_table()` - 创建轨迹表
  - [x] 实现 `insert_trajectory_data()` - 批量数据插入
  - [x] 基础CLI接口完成

### 2025-01-XX - Phase 2完成
- [x] **变化点检测功能完成**
  - [x] 实现 `detect_avp_changes()` - AVP状态变化检测
  - [x] 实现 `detect_speed_spikes()` - 速度突变检测
  - [x] 实现 `create_events_table()` - 创建变化点表
  - [x] 实现 `insert_events_data()` - 变化点数据插入
  - [x] CLI参数完善（--detect-avp, --detect-speed, --speed-threshold）

### 当前状态
- ✅ **Phase 1: 基础轨迹生成** - 100%完成
- ✅ **Phase 2: 变化点检测** - 100%完成
- ⏳ **Phase 3: 增强功能** - 待开发
- ⏳ **Phase 4: 测试验证** - 待开发

### 已实现功能
1. **轨迹生成**：从scene_id列表生成连续轨迹线（LineString）
2. **统计计算**：轨迹长度、时间跨度、速度统计、AVP统计
3. **变化点检测**：AVP状态变化、速度突变检测
4. **数据库管理**：自动创建轨迹表和变化点表
5. **CLI接口**：完整的命令行参数支持

### 使用示例
```bash
# 基础轨迹生成
python -m spdatalab.dataset.trajectory --input test_trajectory_sample.txt --table test_trajectories

# 包含变化点检测
python -m spdatalab.dataset.trajectory \
    --input test_trajectory_sample.txt \
    --table test_trajectories \
    --detect-avp \
    --detect-speed \
    --speed-threshold 2.5 \
    --verbose

# 批量处理（调整批次大小）
python -m spdatalab.dataset.trajectory \
    --input large_scene_list.txt \
    --table large_trajectories \
    --batch-size 50 \
    --detect-avp \
    --detect-speed
```

### 数据表结构
**轨迹表 (trajectories_[table_name])**:
- id, scene_id, point_count, start_time, end_time, duration
- avg_speed, max_speed, min_speed, std_speed
- avp_point_count, avp_ratio
- geometry (LineString), created_at

**变化点表 (trajectories_[table_name]_events)**:
- id, scene_id, timestamp, event_type
- from_value, to_value, speed_value, speed_mean, z_score
- description, geometry (Point), created_at

### 进展更新
*在这里记录开发进展和重要决策*

---

**开发原则**: 
- 保持功能独立，不影响bbox模块
- 优先核心功能，逐步增强
- 重视数据质量和性能
- 保持代码简洁可维护 