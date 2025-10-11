# 分析脚本与 Notebook 管理最佳实践（结合现有 `src/` 结构）

经过梳理 `src/spdatalab/` 目录可以看到：

- `spdatalab.dataset` 中的 `bbox.py`、`quality_check_trajectory_query.py` 等文件同时承担了 **数据访问、批处理调度、命令行入口** 等职责；
- `spdatalab.fusion` 模块内已有较规范的分析器（如 `toll_station_analysis.py`、`integrated_trajectory_analysis.py`），早期的 `multimodal_cli.py` 现已收敛为薄包装，真正的 CLI 入口迁移到 `fusion.cli.multimodal`；
- `spdatalab.common` 中提供了数据库、文件系统等工具，可以继续沉淀通用依赖；
- `examples/` 目录下的脚本（如 `dataset/bbox_examples/run_overlap_analysis.py`）为了兼容 Docker 做了大量环境配置，与核心分析逻辑耦合紧密。

因此在规划 Notebook 与分析脚本的归宿时，需要先明确“**核心分析逻辑**”“**命令行入口**”“**一次性脚本 / Notebook**”三类产物的边界，再沿用项目现有的包结构去拆分。

---

## 目标

1. **逻辑沉淀到 `src/`**：核心算法、数据库查询、数据整理等逻辑应存在于 `spdatalab.*` 包中，供 CLI、Notebook、测试复用。
2. **CLI 仅做编排**：命令行程序只负责解析参数、拼装配置、调用模块函数，减少状态管理、日志、信号处理等重复实现。
3. **Notebook 保持轻量**：Notebook 作为探索、展示或回归复现的载体，统一通过导入 `spdatalab` 中的函数来运行。
4. **测试可覆盖**：拆出的函数要能够在 `tests/` 中编写单测或集成测试（可借助模拟的 SQLAlchemy engine 或临时文件）。

## 项目聚焦清单（实时更新）

**关键信息**
- 环境：使用 `venv\Scripts\python.exe` 运行 Python，敏感配置放在 `.env`，常用流程可通过 `Makefile` 中的命令触发。
- 核心回归：`pytest -k bbox`、`pytest tests/test_bbox_cli.py tests/test_bbox_core.py` 用于验证 bbox 模块拆分后的主流程。
- 数据基线：`tests/data/baseline/` 保存关键指标输出，对比脚本依赖该目录作为参考值。

**待办（Open）**
- [x] CLI 与示例收口：完善 `spdatalab.dataset.bbox.cli` 的参数校验与日志输出，让 `examples/` 下脚本全部转调 `main()`。
- [x] Notebook 精简：抽离 Notebook 中复用函数到 `analysis/notebook_support.py`（或按领域拆分模块），并启用 `nbstripout` 钩子清理输出。
- [x] 验证脚本：实现 `scripts/testing/compare_analysis_output.py`，在迁移前后对比核心 CSV/图表结果。
- [x] 文档同步：更新 `README.md` 与 Notebook 指南，补充新的命令行入口与运行步骤。
- [x] CI 扩展：在流水线增加 `pytest --nbmake` 与最小化 CLI 冒烟用例，防止回归被遗漏。

**已完成**
- [x] bbox 核心逻辑迁移至 `core.py`，覆盖成功、失败与重试路径的单元测试已经生效。
- [x] `legacy.py` 通过依赖注入复用新的 IO/几何工具，Notebook 在迁移期间仍能调用旧入口。

---

## 推荐的模块分层

### 1. 按业务域拆分核心模块

| 现有位置 | 推荐调整 | 说明 |
| --- | --- | --- |
| `src/spdatalab/dataset/bbox.py` | `src/spdatalab/dataset/bbox/` 包<br> ├─ `core.py`：bbox 导入、重叠分析等纯逻辑<br> ├─ `io.py`：和数据库、Hive、Parquet 的交互<br> └─ `pipeline.py`：批处理调度、进度跟踪 | 让原先单文件中耦合的 CLI、工具类、全局状态拆分成可组合的函数/类；`LightweightProgressTracker` 等可以迁移到 `pipeline.py` 或 `common/progress.py`。 |
| `examples/dataset/bbox_examples/run_overlap_analysis.py` | `src/spdatalab/dataset/bbox/cli.py` | CLI 负责解析参数、构造 engine、调用 `core.run_overlap`。示例脚本可以保留极简入口，仅转调 `spdatalab.dataset.bbox.cli.main`。 |
| `src/spdatalab/fusion/multimodal_cli.py`（兼容层） | `src/spdatalab/fusion/cli/multimodal.py`（已落地） | 将 CLI 与 `fusion` 下的分析器解耦，便于未来增加更多 CLI；核心逻辑继续放在 `fusion/*.py` 中，示例命令改用 `python -m spdatalab.fusion.cli.multimodal`。 |
| 一次性的分析脚本 (`examples/one_time/*.py`) | `scripts/one_time/`（若需要长期留存） | 与部署相关的脚本应放在 `scripts/`，通过 `Makefile`/`tox`/`poetry` 命令调度。 |

> **命名建议**：保持 `spdatalab.<domain>.<feature>.core` 负责业务逻辑，`spdatalab.<domain>.<feature>.cli` 负责参数解析和编排，`examples/` 只保留调用示例或 Notebook。

### 2. Notebook 辅助模块

- 在 `src/spdatalab/analysis/notebook_support.py`（或按领域拆分到 `fusion/notebook.py`、`dataset/notebook.py`）中提供绘图、表格渲染、参数管理等工具，Notebook 直接导入这些函数。
- 若 Notebook 中有数据校验、指标统计等可复用逻辑，应优先放到 `spdatalab.analysis.metrics` 等模块。

### 3. 公共基础设施

- 复用 `spdatalab.common` 下的 `io_hive.py`、`io_obs.py`、`config.py` 等工具，并将新的通用能力（如信号处理、进度记录）也沉淀到这里，避免散落在 CLI 中。
- 针对 SQLAlchemy engine、配置文件等初始化逻辑，可在 `spdatalab.common.factories` 中集中定义。

---

## Notebook 与 CLI 的工作流

1. **提炼逻辑**（以 bbox 重叠分析为例）
   - 在 `spdatalab.dataset.bbox.core` 中实现 `run_overlap`、`calculate_density` 等纯函数。
   - 将现有 `LightweightProgressTracker` 等状态类拆出，并在核心函数中通过依赖注入使用。
   - 对数据库访问写成 `Repository`/`Gateway` 模式，方便在测试中替换成内存实现。

2. **封装命令行**
   - 在 `spdatalab.dataset.bbox.cli.build_parser()` 中集中定义参数；
   - `main()` 负责解析参数、加载配置、调用 `core.run_overlap()`，并返回退出码；
   - 通过 `if __name__ == "__main__": raise SystemExit(main())` 提升可测试性；
   - `examples/dataset/bbox_examples/run_overlap_analysis.py` 精简为：
     ```python
     from spdatalab.dataset.bbox.cli import main

     if __name__ == "__main__":
         raise SystemExit(main())
     ```

3. **整理 Notebook**
   - Notebook 只导入 `spdatalab.dataset.bbox.core` 中的函数与配置；
   - 所有绘图、转换、导出逻辑放在 `analysis`/`common` 模块中，以 Python 脚本形式保存；
   - 提交前通过 `nbstripout` 或 `jupyter nbconvert --ClearOutputPreprocessor` 清理输出；
   - 若 Notebook 需要批量运行，可配合 `papermill` 或 `pytest --nbmake` 执行。

4. **测试与验证**
   - 在 `tests/dataset/test_bbox_overlap.py` 中构造小型数据集（或使用 SQLite 内存库）验证 `run_overlap` 的关键路径；
   - 对 CLI 层使用 `pytest` 的 `CliRunner`（或 `subprocess.run`）验证参数解析和输出文件；
   - 为 `fusion` 模块的分析器编写集成测试，校验配置读取与数据处理的协同。

---

## 迁移步骤建议

1. **创建新包结构**：先建立 `src/spdatalab/dataset/bbox/{__init__,core,cli,io,pipeline}.py`、`src/spdatalab/fusion/cli/` 等文件，并补充导出。
2. **搬运核心逻辑**：把 `examples/dataset/bbox_examples/run_overlap_analysis.py`、`src/spdatalab/dataset/bbox.py` 中的核心函数移动到 `core.py`，保留原有调用路径以兼容旧接口（例如在 `bbox.py` 中保留对新函数的包装并标记弃用）。
3. **统一 CLI**：在 `scripts/` 或 `examples/` 中的入口统一调用新的 `main()`，保留旧脚本但打印迁移提示；
4. **整理 Notebook**：按 Notebook 引用的函数逐个迁移到 `src/`；对 Notebook 进行 `nbstripout` 处理并在 README 中记录运行方式；
5. **补充测试与自动化**：在 `tests/` 中新增单测，并为常用任务编写 `make analysis-bbox-overlap` 等命令；
6. **持续演进**：后续新增分析逻辑时先在 `src/` 中实现，再决定是否发布到 CLI/Notebook/示例脚本中。

---

## 渐进式推进与质量保障

即便整体目标看上去跨度较大，也可以通过以下做法拆解为安全、可验证的迭代，避免迁移过程中“跑偏”：

1. **建立里程碑与验收标准**
   - 先在项目管理工具或 `docs/` 中记录目标结构、命名规则、完成标准，确保所有人对“成功长什么样”有统一认知；
   - 每个里程碑限定范围（如“完成 bbox 模块拆分并回归测试通过”），PR 只聚焦一个里程碑。
2. **保持双写期**
   - 刚开始拆分时保留旧入口（如 `bbox.py` 的原函数）并调用新实现，对比运行日志或结果文件，确认输出一致后再移除旧代码；
   - 对 Notebook 采用“导入新模块 + 保留旧单元”方式验证差异，通过 `nbdime` 等工具查看输出差别。
3. **强化自动化验证**
   - 为关键函数编写单测，并在 CI 中引入 `pytest -k bbox`、`pytest -k fusion` 等分组用例，避免回归被忽略；
   - CLI 层使用冒烟测试（运行小型数据集、比对核心指标）锁定行为，必要时把指标快照存入 `tests/data/expected/`；
   - 对 Notebook 可使用 `papermill` 或 `pytest --nbmake` 定期执行，以确保迁移后的引用仍然生效。
4. **小步提交 + 代码评审**
   - 拆解为“创建新文件夹”“迁移核心函数”“替换 CLI 调用”“更新示例脚本”等多个 PR，避免一次性大改；
   - 在 PR 描述中记录迁移影响范围、回滚方式以及下一步计划，评审时重点检查接口兼容性和测试覆盖。
5. **数据与配置回归**
   - 在迁移前后保存关键分析结果（例如指标 CSV、图表 JSON），通过自动比对或人工 spot check 确认无异常；
   - 若涉及配置文件或环境变量，先在 `.env.example`/`docs` 中同步更新并告知使用者迁移步骤。

这样处理可以让结构调整在“随做随验证”的节奏下推进，既保证现有分析任务持续可用，也为后续扩展打好基础。

---

## 实施路线图与 Codex 协同指南

为了把上述目标落地，可以按“准备 → 模块拆分 → CLI/Notebook 收口 → 回归验证”的节奏推进，并结合 Codex（或其他 AI 编码助手）提
高迭代效率。以下给出一个可直接执行的路线图：

### 当前进度快照（2024-04-27）

| 阶段 | 子任务 | 状态 | 说明 |
| --- | --- | --- | --- |
| 1. 模块拆分 | 迁移核心函数到 `spdatalab.dataset.bbox.core` | ✅ 已完成 | 新的 `run_overlap_analysis` 管线以及配套的仓库协议、GeoDataFrame 工具函数已经在 `core.py` 内落地，并通过针对成功/失败/重试场景的单元测试覆盖。 |
| 1. 模块拆分 | legacy 层兼容补丁 | ✅ 已完成 | `legacy.py` 现已通过依赖注入复用新的 IO、合并与几何工具，确保 Notebook 与现有测试在迁移期间仍可调用旧入口。 |
| 2. CLI 与 Notebook 收口 | 更新 CLI 入口、Notebook 引导 | ⏳ 进行中 | 后续需要将示例脚本切换为调用新的核心函数，并在 Notebook 指南中同步运行方式。 |

### 0. 准备阶段（1 天）

1. **梳理现状**
   - 使用 `tree src/spdatalab/dataset -L 2`、`tree examples -L 2` 快速确认涉及的脚本范围。
   - 记录每个脚本/Notebook 的入口、输入输出路径，整理到一张“现状 -> 目标”对照表（可放在 `docs/analysis_migration_tracker.md`）。
2. **搭建基线**
   - 本地跑通当前 Notebook/脚本的关键流程（至少 `run_overlap_analysis.py`、`batch_top1_analysis.py` 各跑一次），把结果 CSV/图表保存到
     `tests/data/baseline/` 供后续比对。
   - 若还未建立自动化环境，在 `Makefile` 增加 `make test-analysis`，里面执行最小化数据集的冒烟用例。
3. **Codex 使用准备**
   - 明确一次只让 Codex 修改一个小目标（例如“把 run_overlap_analysis 迁移到 core.py”），并把上下文（现状文件、目标文件、接口约束）
     写成提示模板，后续直接复用。
   - 约定提示结构：`[背景] → [当前代码片段] → [期望改动] → [验收标准]`，避免“请帮我重构整个模块”这类宽泛描述。

### 1. 模块拆分（3~5 天）

1. **新目录骨架**
   - 先创建空文件：`touch src/spdatalab/dataset/bbox/{__init__,core,cli,io,pipeline}.py`。
   - 更新 `src/spdatalab/dataset/__init__.py` 导出新的子模块，以便 Notebook/CLI 后续导入。
2. **迁移核心函数**
   - 以 `bbox.py` 为例：
     1. 把纯逻辑函数（数据过滤、重叠计算等）复制到 `core.py`，保留原测试。
     2. 将数据库连接、配置读取移到 `io.py`，在 `core.py` 中通过参数传入。
     3. `pipeline.py` 只保留调度器、进度条。
   - 每一步迁移都在旧文件中添加 shim，并在 docstring 中标记 `Deprecated: use spdatalab.dataset.bbox.core.*`。
3. **Codex 协同技巧**
   - 让 Codex 先生成目标文件的骨架（类/函数签名和 docstring），再手动或分批迁移实现，避免一次 diff 过大。
   - 对于需要理解上下文的片段，先让 Codex 帮忙生成“迁移 checklist”或“依赖图”，再根据 checklist 分段执行。
   - 每次生成后立即使用 `pytest tests/dataset/test_bbox_overlap.py -k smoke` 验证，失败再补充提示说明错误信息。

### 2. CLI 与 Notebook 收口（2~3 天）

1. **CLI 调整**
   - 在 `cli.py` 中写清 `build_parser()`、`main()`，并把参数解析逻辑与核心函数串联。
   - 原 `examples/*` 脚本改为调用 `main()`，保留一段迁移说明日志（`logger.warning("This script now proxies to spdatalab..." )`）。
   - 为新的 CLI 写 `tests/cli/test_bbox_cli.py`，至少包含参数解析、输出路径存在等用例。
2. **Notebook 清理**
   - 把 Notebook 中的函数提炼到 `analysis/notebook_support.py`，Notebook 单元只保留 `import` 和可视化调用。
   - 引入 `nbstripout` 到 git hook（可在 `.pre-commit-config.yaml` 新增配置）保证输出为空。
   - 若需要批量运行，新增 `scripts/examples/batch_notebook_runner.py`，统一调 `papermill`。
3. **Codex 协同技巧**
   - 用 Codex 生成 CLI 的 argparse 模板、测试样例；但在提示中明确“保留 main 函数可调用”“不要执行副作用”。
   - 对 Notebook 重构时，让 Codex 把 Notebook 里粘贴的函数转换成 Python 模块，并提醒它使用现有的 `core` API。

### 3. 回归验证与收尾（1~2 天）

1. **结果对比**
   - 编写 `scripts/testing/compare_analysis_output.py`，读取 `tests/data/baseline/` 与最新运行结果，输出差异。
   - 在 CI 中加入该脚本或 `pytest --nbmake`，保证 Notebook/CLI 在最小样本下可跑通。
2. **文档与培训**
   - 更新 `docs/analysis_workflow_guidelines.md`（本文）、`README.md` 中的运行方式。
   - 组织一次分享或录屏，演示“如何使用新的 CLI + Notebook 支撑流程”。
3. **Codex 使用复盘**
   - 汇总高效的提示模板和常见误区（如忽略相对路径、改动过多 import），存入团队知识库。
   - 后续遇到新的分析模块时，直接复用上述模板和步骤。

通过上述阶段化推进 + Codex 辅助，可以把大型重构拆解成一系列可验证的小步骤：每个步骤都能在 PR 中清晰展示改动、配套测试与验证结果，
并且任何时候都能通过双写和基线对比快速发现偏差。

---

## 额外工具建议

- **配置管理**：结合 `pydantic-settings` 或 `hydra` 将分析参数、数据库连接集中维护。
- **进度与日志**：将进度追踪、日志格式等封装在 `spdatalab.common.logging` 中，CLI 统一复用。
- **分析产出管理**：使用 `mlflow`/`wandb` 等工具记录实验参数与结果，便于 Notebook、CLI、批处理共享产出。

通过以上调整，可以在保留现有业务模块划分的基础上，清晰地区分“核心分析能力”“命令行入口”和“示例/一次性脚本”，显著降低 Notebook 与脚本的维护成本，同时提升代码复用度与测试覆盖率。
