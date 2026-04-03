"""Optimizer construction and LR scheduling for the carbon footprint model.

Implements differential learning rates: attention-based components
(StepLocProxy, MaterialEncoder self-attention) get a lower LR and lower
weight decay than MLP components (trunk, heads, ProductEncoder).

Embedding layers get zero weight decay (standard practice).
"""

import logging
import math
from typing import Any, Dict, List

import torch
import torch.nn as nn

from model.carbon_footprint.src.utils.config import CarbonConfig

logger = logging.getLogger(__name__)


def _log(msg: str) -> None:
    """Print to stdout (works in notebooks) and logger.info."""
    print(msg, flush=True)
    logger.info(msg)


def get_lr_scheduler(
    optimizer: torch.optim.Optimizer,
    warmup_epochs: int,
    max_epochs: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Linear warmup for warmup_epochs, then cosine decay to zero.

    Supports multiple parameter groups -- each group gets the same warmup/
    cosine schedule (the per-group base LR is set in the optimizer, not here).
    """
    def lr_lambda(epoch: int) -> float:
        if epoch < warmup_epochs:
            return max(epoch / warmup_epochs, 1e-4)
        progress = (epoch - warmup_epochs) / max(max_epochs - warmup_epochs, 1)
        return 0.5 * (1.0 + math.cos(math.pi * progress))
    # Same lambda for all param groups -- differential base LR is in optimizer
    n_groups = len(optimizer.param_groups)
    return torch.optim.lr_scheduler.LambdaLR(
        optimizer, [lr_lambda] * n_groups,
    )


def build_param_groups(
    model: nn.Module, config: CarbonConfig,
) -> List[Dict[str, Any]]:
    """Split model parameters into three groups with different LR/weight_decay.

    Groups:
      1. attention -- self_attn weights, LayerNorm, CLS parameters in
         StepLocProxy and MaterialEncoder. Lower LR (base * attn_lr_ratio)
         and lower weight decay to stabilize early training and preserve
         attention rank.
      2. embedding -- nn.Embedding layers. Zero weight decay (regularizing
         embeddings via weight decay is counterproductive).
      3. mlp -- everything else (ProductEncoder, TransportEncoder, trunk,
         output heads, auxiliary heads, missing-embedding Parameters).
         Full base LR and standard weight decay.

    Any parameter not matched to attention or embedding falls into mlp.
    """
    attn_params: List[torch.Tensor] = []
    emb_params: List[torch.Tensor] = []
    mlp_params: List[torch.Tensor] = []

    attn_names: List[str] = []
    emb_names: List[str] = []
    mlp_names: List[str] = []

    modules_by_name = dict(model.named_modules())

    for name, param in model.named_parameters():
        if not param.requires_grad:
            continue

        # Attention group: MultiheadAttention submodules, LayerNorm in
        # attention encoders, and CLS token parameters.
        is_attn = (
            "self_attn" in name
            or ("norm" in name and (
                "material_enc" in name or "step_loc_proxy" in name
            ))
            or name.startswith("step_loc_proxy.cls_")
        )

        # Embedding group: nn.Embedding layer weights.
        is_emb = False
        parts = name.rsplit(".", 1)
        if len(parts) == 2:
            parent = modules_by_name.get(parts[0])
            if isinstance(parent, nn.Embedding):
                is_emb = True

        if is_attn:
            attn_params.append(param)
            attn_names.append(name)
        elif is_emb:
            emb_params.append(param)
            emb_names.append(name)
        else:
            mlp_params.append(param)
            mlp_names.append(name)

    attn_lr = config.lr * config.attn_lr_ratio
    _log(
        f"Param groups: attention={len(attn_params)} params "
        f"(lr={attn_lr:.1e}, wd={config.attn_weight_decay}), "
        f"embedding={len(emb_params)} params "
        f"(lr={config.lr:.1e}, wd={config.emb_weight_decay}), "
        f"mlp={len(mlp_params)} params "
        f"(lr={config.lr:.1e}, wd={config.weight_decay})"
    )
    logger.debug("Attention params: %s", attn_names)
    logger.debug("Embedding params: %s", emb_names)
    logger.debug("MLP params: %s", mlp_names)

    return [
        {
            "params": attn_params,
            "lr": attn_lr,
            "weight_decay": config.attn_weight_decay,
        },
        {
            "params": emb_params,
            "lr": config.lr,
            "weight_decay": config.emb_weight_decay,
        },
        {
            "params": mlp_params,
            "lr": config.lr,
            "weight_decay": config.weight_decay,
        },
    ]


def build_optimizer_and_scheduler(
    model: nn.Module, config: CarbonConfig,
) -> tuple:
    """Build AdamW with differential param groups and cosine+warmup scheduler.

    Returns:
        (optimizer, scheduler) tuple.
    """
    param_groups = build_param_groups(model, config)
    optimizer = torch.optim.AdamW(param_groups)
    scheduler = get_lr_scheduler(
        optimizer, config.warmup_epochs, config.max_epochs,
    )
    return optimizer, scheduler
