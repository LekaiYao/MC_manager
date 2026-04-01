# MC_manager

这个目录用于批量读取和提交 CRAB 任务。

## 放在哪里

本目录放在各个 `CMSSW` 目录的同一级，例如：

```text
<work area>/
├── MC_manager/
├── CMSSW_10_6_20_patch1/
├── CMSSW_10_6_17_patch1/
└── CMSSW_10_2_16_UL/
```

## 如何使用

### 1. 检查某一步的 CRAB 任务状态

```bash
python3 CrabTask_manager.py <STEP>
```

`STEP` 可选：`GEN`、`SIM`、`DIGI`、`HLT`、`RECO`、`MINIAOD`。

作用：

- 读取对应步骤的所有crab job的status
- 把已完成任务的 output dataset 列表写到 `./txt/CrabTask_manager_OUTPUT_DIRs_<STEP>.txt`
- 如果输出列表里有已经提交过的条目，需要手动删除

### 2. 基于上一步输出，为下一步批量准备或提交任务

```bash
python3 CrabTask_large_submission_handler.py <STEP>
```

`STEP` 可选：`GEN`、`SIM`、`DIGI`、`HLT`、`RECO`。

这里的 `STEP` 表示已经完成的步骤。脚本会自动推进到下一步，并自动修改下一步的 `crab3_Config.py`,然后提交。

### 3. 真正提交前先看安全开关

`CrabTask_large_submission_handler.py` 里默认是：

```python
submit_jobs = False
```

如果确认要正式提交，把它改成：

```python
submit_jobs = True
```

再重新运行脚本。

## 推荐使用顺序

1. 运行 `CrabTask_manager.py <STEP>` 检查这一步是否完成。
2. 确认 `./txt/CrabTask_manager_OUTPUT_DIRs_<STEP>.txt` 已生成。
3. 运行 `CrabTask_large_submission_handler.py <STEP>`。
4. 先用 SAFE MODE 检查。
5. 确认无误后，把 `submit_jobs` 改为 `True` 再正式提交。

## 运行前提

- 已在可用的 CMSSW 环境中
- 命令行可直接使用 `crab`
- 对应 `CMSSW` 目录下已经存在 `src/crab3_Config.py` 和 `BPH_<NEXT_STEP>_13TeV_cfg.py`
