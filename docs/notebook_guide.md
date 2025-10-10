# Notebook 使用与规范

本指南帮助你在迁移后的结构中编写和维护 Notebook，确保分析逻辑与可复用模块解耦。

## 1. 项目路径与轻量模式

```python
from spdatalab.analysis.notebook_support import (
    NB_FAST_ENV_VAR,
    configure_project_paths,
    display_config,
    is_fast_mode,
)

ctx = configure_project_paths()
display_config(ctx)
print(f"Fast mode: {is_fast_mode()} (设置 {NB_FAST_ENV_VAR}=1 可跳过耗时步骤)")
```

- `configure_project_paths()` 会自动将项目根目录与 `src/` 添加到 `sys.path`。
- 通过设置环境变量 `SPDATALAB_NOTEBOOK_FAST=1` 可在 CI 或测试环境下跳过数据库连接，转而使用示例数据。

## 2. 复用分析模块

- Notebook 中的核心分析逻辑应放在 `src/spdatalab/*` 模块内，通过 `import` 调用。
- 例如城市热点 Notebook 使用 `spdatalab.dataset.bbox.summary.BatchTop1Config` 与 `BBoxHotspotBatch` 来读取分析结果、生成汇总表。
- Notebook 仅负责加载配置、调用模块函数以及展示结果（如 DataFrame 预览、图表渲染）。

## 3. 清理输出：启用 nbstripout

1. 安装依赖（一次性）：
   ```bash
   pip install nbstripout
   nbstripout --install --attributes .gitattributes
   ```
2. `.gitattributes` 中已经声明 `*.ipynb filter=nbstripout`，提交前会自动清理执行输出。
3. 如需手动清理，执行：
   ```bash
   nbstripout examples/notebooks/city_hotspot_analysis.ipynb
   ```

## 4. Notebook 回归测试

- `Makefile` 提供 `make test-notebooks`，内部执行 `SPDATALAB_NOTEBOOK_FAST=1 pytest --nbmake`。
- 在 CI 中建议与 `pytest -k bbox` 搭配运行，确保 Notebook 与 CLI 的核心路径都被覆盖。

## 5. 输出校验

- 使用 `scripts/testing/compare_analysis_output.py` 对比 Notebook 导出的 CSV/JSON 与 `tests/data/baseline/` 中的基线。
- 若分析逻辑更新，需要同时刷新基线数据并记录变更原因。

遵循以上规范，可以让 Notebook 保持可复现、易维护，并与核心代码共享同一套测试和验证流程。
