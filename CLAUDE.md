# ESPResso V2 -- Coding Rules

## Overview
6-layer LLM-orchestrated synthetic data pipeline generating ~50K validated training records for product-level carbon footprint estimation in textiles. Data generation is largely complete; active development is now on model design and evaluation.

## Languages
- **Python 3.10+** -- data generation (layers 1-5), model design and training
- **C99** -- deterministic carbon calculation engine (layer 6)
- Coding standards for both languages are in global memory. Do not duplicate here.

## File Conventions
- Maximum 300 lines per source file. Split proactively at ~200 lines.
- No .md files except this CLAUDE.md.
- No emojis in any file, output, or commit message.
- Model logic in plain `.py` files, never in notebooks.

## Project Structure

```
ESPResso-V2/
+-- data/
|   +-- data_generation/              # 6-layer pipeline (complete)
|   |   +-- layer_1/ ... layer_5/     # Python LLM generation + validation
|   |   +-- layer_6/                  # C99 carbon calculation engine
|   |   +-- shared/                   # Common utilities, API clients
|   |   +-- scripts/                  # Entry points
|   |   +-- tests/
|   +-- datasets/
|       +-- pre-model/final/          # Reference data (taxonomy, materials)
|       +-- model/                    # Pipeline outputs
+-- model/
|   +-- data/                         # Shared dataset (single source of truth)
|   |   +-- VERSION                   # Hash for Colab cache validation (not LFS)
|   |   +-- full_dataset.parquet      # Clean copy of final dataset
|   |   +-- train/                    # Generated splits
|   |   +-- val/
|   |   +-- test/
|   +-- <approach-name>/              # One directory per model approach
|       +-- src/                      # Python files, organized by subdirectory
|       |   +-- preprocessing/
|       |   +-- training/
|       |   +-- evaluation/
|       |   +-- utils/
|       +-- notebooks/
|           +-- manager.ipynb         # Colab-compatible orchestrator
```

## Data Pipeline
Layers 1-6 are complete. ~50K validated records across 17 product categories.
- Build layer 6: `cd data/data_generation/layer_6 && make clean && make all`
- Run layer 6: `./data/data_generation/layer_6/layer6_calculate`

## Model Design Rules

**Approach isolation:** Each model approach gets its own directory under `model/`. Never mix approaches. Shared code across approaches goes in `model/shared/` if needed.

**Separation of concerns:** All model logic lives in `.py` files under `src/`, organized into subdirectories (preprocessing, training, evaluation, utils). The manager notebook only imports and calls these modules -- it contains no model logic itself. This exists because Claude Code iterates on `.py` files far more effectively than notebooks.

**Data discipline:** All approaches reference `model/data/`. Never copy the dataset into an approach directory. Never commit model weights, checkpoints, or large outputs to git.

**Adding a new approach:**
1. Create `model/<approach-name>/` with `src/` and `notebooks/` subdirectories.
2. Organize `src/` into logical subdirectories by concern.
3. Create `manager.ipynb` that imports and runs the src modules.

## Google Colab Workflow

The manager notebook supports Colab with bandwidth-efficient data handling via LFS-skip cloning:

1. **Initialization block** -- username, token, repo URL. Mount Google Drive.
2. **Check cache** -- look for dataset at `drive/MyDrive/ESPResso-V2/data/`.
3. **Validate freshness** -- compare `model/data/VERSION` (committed normally, not LFS) against cached copy. This file contains a dataset hash so staleness detection works without downloading the data.
4. **Clone without LFS** when cached data is current:
   ```bash
   GIT_LFS_SKIP_SMUDGE=1 git clone <repo-url>
   ```
   Then symlink or copy data from Drive into the working tree.
5. **Pull LFS only when needed** -- if dataset is new or changed:
   ```bash
   git lfs pull --include="model/data/**"
   ```
   Then cache to Drive for subsequent sessions.

This avoids re-downloading large Parquet files on every Colab session.

## Local Smoke Testing

Every training script must support a smoke test mode to validate the full pipeline on CPU before running on Colab GPU. This catches import errors, shape mismatches, data loading bugs, and config problems in seconds instead of after an 8-hour training run.

**Mechanism:** `SMOKE_TEST=1` environment variable or `--smoke` CLI flag. When active:
- Load only ~50-100 rows from the dataset.
- Run 1-2 training batches + 1 validation batch.
- Force CPU device regardless of GPU availability.
- Execute the full cycle: data load, preprocess, train step, val step, checkpoint save/load, metric log.
- If using PyTorch Lightning, use `Trainer(fast_dev_run=True)`.

**Device-agnostic code:** All scripts must use `device = torch.device("cuda" if torch.cuda.is_available() else "cpu")` or equivalent. Never hardcode `cuda`. All tensors and models must be moved via `.to(device)`.

**Workflow:** The manager notebook runs smoke test first. If it fails, abort before consuming GPU time. The local development loop is: edit `.py` files, run smoke test, fix, repeat -- only push to Colab when smoke passes.

## Training Viability Check

Separate from smoke testing. The smoke test validates "does it run?" -- the viability check validates "is it worth running?" Every full training run begins with 3-5 canary epochs. After those epochs, automated diagnostics evaluate the loss trajectory and abort early with a report if the run is not viable.

**Checks after canary epochs:**
- Loss not decreasing: likely bad learning rate or dead gradients. Abort.
- Loss stuck at random-chance baseline: data not reaching model, label mismatch, or preprocessing bug. Abort.
- Validation loss diverging from training loss immediately: severe overfitting, model too large. Abort.
- NaN or Inf in loss: numerical instability, learning rate too high. Abort.
- Gradients near zero across layers: vanishing gradients, architecture too deep without residuals. Abort.

**On abort:** Print a diagnostic report containing: what was observed (e.g., "training loss flat at 2.30 after 5 epochs"), likely causes, and suggested fixes (e.g., "try reducing LR by 10x", "check label encoding"). Do not silently fail -- the report is the whole point.

**On pass:** Log the canary metrics and continue into the full training run. The canary epochs count toward total training (they are not discarded).

This saves GPU hours during model experimentation by failing fast on bad configurations instead of running the full training to discover the approach does not converge.

## Checkpointing and Resumability

Colab sessions die without warning -- 90-minute idle timeout, 12-hour hard cap, random disconnects. Every training run must checkpoint to Google Drive so progress survives session loss.

**Requirements:**
- Save checkpoint every N epochs (configurable, default every 5 or every 30 min).
- Checkpoint contains: model state, optimizer state, epoch number, config, best metrics so far.
- Save to `drive/MyDrive/ESPResso-V2/checkpoints/<approach-name>/`.
- On notebook restart, auto-detect latest checkpoint and resume. Never restart from scratch silently.
- The manager notebook init block must check for existing checkpoints before starting training.

## Experiment Logging

Every training run logs its full config and results automatically. Without this, experimentation becomes guesswork and you repeat failed configurations.

**Per-run log entry must contain:**
- Approach name, timestamp, git commit hash.
- Full hyperparameter config (learning rate, batch size, architecture choices, etc.).
- Final metrics (loss, accuracy, or task-specific metrics) and best-epoch metrics.
- Canary epoch diagnostics (pass/fail, loss trajectory).
- Runtime (wall clock, GPU hours).

**Storage:** Each approach keeps a `runs.jsonl` file (append-only, one JSON object per line per run). This stays in the approach directory and is gitignored. Summary results can be committed manually when an approach shows promise.

This prevents re-running dead-end configs and makes it trivial to compare approaches.

## Data Loading Efficiency

A slow data pipeline means the GPU idles at 0% while the CPU prepares the next batch. Even with ~50K records this matters across many experiment iterations.

**Rules:**
- Always set `num_workers >= 2` in DataLoader (4 on Colab, scale to CPU count).
- Enable `pin_memory=True` when training on GPU.
- Preprocess and cache transformed data to disk once, not on every epoch.
- If preprocessing is expensive, do it as a separate step that writes a processed file. Training scripts read the processed file.
- Use `persistent_workers=True` to avoid respawning worker processes each epoch.

## Anti-Patterns
- Never put model logic in notebooks. Notebooks orchestrate only.
- Never copy datasets into approach directories.
- Never hardcode hyperparameters. Use config files or dataclasses.
- Never commit weights, checkpoints, or large outputs to git.
- Never train or evaluate without setting all random seeds.
- Never hardcode `cuda` as device. Always use device-agnostic patterns.
