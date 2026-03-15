# Layer 6: Coordinate Conversion -- Lat/Lon to Sin/Cos Encoding

## Context

Layer 3 outputs geographic coordinates as standard WGS84 latitude/longitude
(decimal degrees). This is the format that the LLM (Claude Sonnet) returns
because lat/lon is the most natural and accurate representation for a language
model working with a geographic reference table.

However, for downstream model training (particularly neural networks and
transformer architectures), a sin/cos encoding of coordinates produces
better results because Euclidean distance in the encoded space more closely
approximates real geographic distance. Tree-based models (LightGBM) work
equally well with either representation.

## Decision

The coordinate conversion from lat/lon to sin/cos encoding is NOT performed
in Layer 3. Layer 3 stores raw WGS84 lat/lon as produced by the LLM.

The conversion is performed in Layer 6 (or in the model's feature engineering
pipeline) because:

1. Layer 3's job is data generation, not feature engineering
2. Raw lat/lon is the universal interchange format -- it can always be
   converted to any other encoding, but the reverse may lose precision
3. Different model architectures may need different encodings; keeping
   the raw form preserves flexibility
4. The conversion is trivial and deterministic -- it adds no value to
   precompute it in Layer 3

## Conversion Formulas

Given a coordinate pair (latitude, longitude) in decimal degrees:

```python
import math

def latlon_to_sincos(lat_deg, lon_deg):
    """Convert WGS84 lat/lon to sin/cos encoding.

    Returns 4 features per coordinate point:
      sin_lat, cos_lat, sin_lon, cos_lon

    This encoding preserves geographic distance relationships:
    Euclidean distance in the 4D encoded space approximates
    great-circle distance better than raw lat/lon space.
    """
    lat_rad = math.radians(lat_deg)
    lon_rad = math.radians(lon_deg)

    return {
        "sin_lat": math.sin(lat_rad),
        "cos_lat": math.cos(lat_rad),
        "sin_lon": math.sin(lon_rad),
        "cos_lon": math.cos(lon_rad),
    }
```

## Inverse Conversion

The encoding is fully invertible:

```python
def sincos_to_latlon(sin_lat, cos_lat, sin_lon, cos_lon):
    """Recover WGS84 lat/lon from sin/cos encoding."""
    lat_deg = math.degrees(math.atan2(sin_lat, cos_lat))
    lon_deg = math.degrees(math.atan2(sin_lon, cos_lon))
    return lat_deg, lon_deg
```

## Where to Apply

When building the feature matrix for model training, convert the following
Layer 3 output fields:

| Layer 3 field (raw)     | Encoded features                                       |
|-------------------------|--------------------------------------------------------|
| `origin_lat/lon`        | `origin_sin_lat, origin_cos_lat, origin_sin_lon, origin_cos_lon` |
| `destination_lat/lon`   | `dest_sin_lat, dest_cos_lat, dest_sin_lon, dest_cos_lon`         |

For LightGBM: provide BOTH raw lat/lon AND sin/cos encoded features. The
tree model will use whichever gives better splits.

For neural networks / transformers: use only the sin/cos encoded features.

## Implementation Location

This conversion should be implemented in one of:
- Layer 6's feature enrichment step (if computing training targets)
- The model's preprocessing/feature engineering pipeline (if building
  inference features)

It should NOT be in Layer 3 output generation.
