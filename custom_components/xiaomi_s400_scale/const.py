"""Constants for the Xiaomi S400 Scale integration."""

from __future__ import annotations

DOMAIN = "xiaomi_s400_scale"

CONF_TOKEN = "token"

MANUFACTURER = "Xiaomi"
MODEL_NAME = "Body Composition Scale S400 (MJTZC01YM)"

# Xiaomi MiBeacon (0xFE95) 16-bit product IDs for Xiaomi Body Composition Scale S400 (MJTZC01YM)
S400_PRODUCT_IDS = frozenset({0x30D9, 0x3BD5, 0x48CF})

RECONNECT_COOLDOWN_SECONDS = 3.0
MAX_RECONNECT_COOLDOWN_SECONDS = 30.0
POST_MEASUREMENT_COOLDOWN_SECONDS = 15.0
SETTLE_DELAY_SECONDS = 0.3
CMTP_IDLE_TIMEOUT_SECONDS = 60.0
