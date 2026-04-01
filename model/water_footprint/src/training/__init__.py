# model.water_footprint.src.training -- Model, loss, and training loop

from model.water_footprint.src.training.model import WA1Model
from model.water_footprint.src.training.loss import UWSOLoss
from model.water_footprint.src.training.encoders import (
    MaterialEncoder,
    StepEncoder,
    LocationEncoder,
    ProductEncoder,
    PackagingEncoder,
)
from model.water_footprint.src.training.cross_attention import (
    CrossAttentionModule,
    ConfidenceGate,
    GeoAttentionBlock,
)
from model.water_footprint.src.training.trainer import WA1Trainer
from model.water_footprint.src.training.checkpoint import smoke_test

__all__ = [
    "WA1Model",
    "UWSOLoss",
    "MaterialEncoder",
    "StepEncoder",
    "LocationEncoder",
    "ProductEncoder",
    "PackagingEncoder",
    "CrossAttentionModule",
    "ConfidenceGate",
    "GeoAttentionBlock",
    "WA1Trainer",
    "smoke_test",
]
