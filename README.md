# Xiaomi S400 Scale for Home Assistant

A custom Home Assistant integration for the **Xiaomi Body Composition Scale S400** (model `MJTZC01YM` / `yunmai.scales.ms104`), converted from [nokistin/xiaomi-s400-live](https://github.com/nokistin/xiaomi-s400-live).

Unlike passive BLE advertisement scanning (which only emits a final reading several seconds after stepping off), this integration authenticates to the scale with the Mi Home v2 protocol over BLE and streams live weight readings in real time while standing on the scale.

---

## Important Notes & Limitations

- **Token-Based Authentication Only**: This integration **only** supports manual token-based authentication. Automatic cloud login and cloud credential fetching are not supported. You must extract your scale's 12-byte (24-hex-character) BLE login token using [Xiaomi-cloud-tokens-extractor](https://github.com/PiotrMachowski/Xiaomi-cloud-tokens-extractor).
- **Body Composition Removed**: On-device body composition calculations (BMI, body fat %, muscle mass, water, visceral fat, etc.) have been completely removed from this integration to keep it focused and lightweight. If you want body composition calculations in Home Assistant from your weight and impedance values, consider using the [BodyMiscale](https://github.com/dckiller51/bodymiscale) custom integration.
- **Bluetooth & AI Assistance**: While I have experience writing and maintaining Home Assistant integrations, I have no prior experience with Bluetooth/BLE development and have relied heavily on AI to help implement and refine the Bluetooth side of this integration.
- **Coexistence with Built-in `xiaomi_ble`**: Home Assistant's built-in Xiaomi BLE (`xiaomi_ble`) integration may also discover the scale. Both integrations can be installed simultaneously (Home Assistant will attach both integrations' entities to the same device entry in the Device Registry via the shared Bluetooth MAC address), though because this integration maintains an active GATT connection to stream live readings, the scale may not broadcast the passive BLE measurement advertisements that `xiaomi_ble` listens for.
- **ESPHome Bluetooth Proxies**: Active GATT connections through ESPHome Bluetooth proxies are supported. Because the scale does not change its BLE advertisements when stepped on until a measurement is already finished, this integration maintains a persistent connection to the scale, which uses 1 active connection slot on the proxy (ESPHome proxies typically support up to 3 simultaneous active connections).
- **Mi Home App Contention**: Because the scale only accepts one active BLE GATT connection at a time, the Mi Home mobile app and Home Assistant cannot be connected to the scale at the same time.
- **No Offline Measurement Sync**: The scale only streams live and final readings while an authenticated BLE session is active. Weigh-ins completed while Home Assistant or your Bluetooth adapter/proxy is offline or disconnected are not replayed when the connection is restored.
- **Multi-User Households**: `sensor.<device>_weight`, `sensor.<device>_impedance`, and `sensor.<device>_low_frequency_impedance` update on every completed weigh-in regardless of who steps on the scale, so their long-term statistics will combine readings across all users. In multi-user households, split readings per person using `event.<device>_measurement` (filtering on `profile_id` or weight range via trigger-based template sensors or [BodyMiscale](https://github.com/dckiller51/bodymiscale)).

---

## Features

- **Live weight stream** - updates every ~0.3s while standing on the scale
- **Weight Stable indicator** - binary sensor indicating when the measurement has stabilized/settled
- **Final weight reading** - locked reading retained and restored across Home Assistant restarts
- **Impedance sensors** - high and low impedance readings from the dual-frequency sensors

---

## Getting Your BLE Login Token

Because the scale requires authentication before it will stream data:

1. Use [Xiaomi-cloud-tokens-extractor](https://github.com/PiotrMachowski/Xiaomi-cloud-tokens-extractor) to log in to your Xiaomi account.
2. Locate your S400 scale in the extracted output.
3. Note the **Bluetooth MAC address** (e.g. `AA:BB:CC:DD:EE:FF`) and the **24-hex-character Token** (`TOKEN` / 12 bytes).

---

## Installation

### Via HACS (Custom Repository)

1. In Home Assistant, open **HACS** → **Integrations**.
2. Click the three dots (⋮) in the top-right corner and select **Custom repositories**.
3. Add the repository URL: `https://github.com/jaydeethree/Home-Assistant-xiaomi-s400-scale` with category **Integration**.
4. Search for **Xiaomi S400 Scale** and click **Download**.
5. Restart Home Assistant.

### Manual Installation

1. Download the latest release from [Releases](https://github.com/jaydeethree/Home-Assistant-xiaomi-s400-scale/releases) (or clone this repository).
2. Copy the `custom_components/xiaomi_s400_scale` folder into your Home Assistant `<config>/custom_components/` directory.
3. Restart Home Assistant.

---

## Configuration

1. In Home Assistant, go to **Settings** → **Devices & Services**.
   - If auto-discovered via Bluetooth, click **Configure**.
   - Otherwise, click **Add Integration**, search for **Xiaomi S400 Scale**, and select your scale or enter its Bluetooth MAC address.
2. Enter the 24-character hex **BLE Login Token**.
3. Submit the form to complete the setup.

### Token Updates / Re-authentication

If the scale is factory reset or re-paired in the Mi Home app, the BLE token rotates. You can update the token anytime by clicking **Reconfigure** on the integration entry menu or by following the automatic re-authentication prompt if Home Assistant detects an authentication failure.

---

## Entities Provided

| Entity | Domain | Description |
| --- | --- | --- |
| `sensor.<device>_live_weight` | Sensor (`kg`) | Real-time weight stream updating every ~0.3s while on the scale (`unknown` when idle or disconnected) |
| `sensor.<device>_weight` | Sensor (`kg`) | Final locked weight measurement (restored across HA restarts) |
| `sensor.<device>_impedance` | Sensor (`Ω`) | High-frequency impedance measurement (`unknown` when weighed with shoes/socks) |
| `sensor.<device>_low_frequency_impedance` | Sensor (`Ω`) | Low-frequency impedance measurement (`unknown` when weighed with shoes/socks) |
| `sensor.<device>_profile_id` | Sensor | User profile ID matched by the scale for the last completed measurement (restored across HA restarts) |
| `sensor.<device>_last_measurement` | Sensor (`timestamp`) | Timestamp of the last completed measurement (restored across HA restarts) |
| `binary_sensor.<device>_weight_stable` | Binary Sensor | Indicates when the live weight measurement has stabilized during an active weigh-in (`off` when idle or disconnected) |
| `binary_sensor.<device>_connected` | Binary Sensor | Diagnostic sensor indicating an active authenticated BLE connection |
| `event.<device>_measurement` | Event | Fired on each completed weigh-in (`measurement_completed`) with `weight_kg`, `impedance_ohm`, `impedance_low_ohm`, `profile_id`, and `measured_at` attributes |

> [!TIP]
> **Triggering Automations**: Because `sensor.<device>_weight` does not have extra state attributes, two consecutive weigh-ins with the exact same weight will not fire a state-changed event on the weight entity. Trigger automations on `event.<device>_measurement` (which carries all measurement values and `profile_id` in a single atomic event) or `sensor.<device>_last_measurement` instead.

---

## Credits

- [nokistin/xiaomi-s400-live](https://github.com/nokistin/xiaomi-s400-live) — original library and reverse engineering of the S400 real-time GATT stream
- [dnandha/miauth](https://github.com/dnandha/miauth) — Mi Home v2 auth protocol reverse engineering
- [PiotrMachowski/Xiaomi-cloud-tokens-extractor](https://github.com/PiotrMachowski/Xiaomi-cloud-tokens-extractor) — token extraction tool
- [dckiller51/bodymiscale](https://github.com/dckiller51/bodymiscale) — body composition calculations for Home Assistant
- [Bluetooth-Devices/xiaomi-ble](https://github.com/Bluetooth-Devices/xiaomi-ble) — Home Assistant Bluetooth integration foundation

---

## License

Apache 2.0
