# Xiaomi S400 Scale for Home Assistant

> [!WARNING]
> **Early Work in Progress**: This integration is currently in early experimental development and is **not ready for general use yet**. Expect breaking changes, bugs, or connection quirks.

A custom Home Assistant integration for the **Xiaomi Body Composition Scale S400** (model `MJTZC01YM` / `yunmai.scales.ms104`), converted from [nokistin/xiaomi-s400-live](https://github.com/nokistin/xiaomi-s400-live).

Unlike passive BLE advertisement scanning (which only emits a final reading several seconds after stepping off), this integration authenticates to the scale with the Mi Home v2 protocol over BLE and streams live weight readings in real time while standing on the scale.

---

## Important Notes & Limitations

- **Token-Based Authentication Only**: This integration **only** supports manual token-based authentication. Automatic cloud login and cloud credential fetching are not supported. You must extract your scale's 12-byte (24-hex-character) BLE login token using [Xiaomi-cloud-tokens-extractor](https://github.com/PiotrMachowski/Xiaomi-cloud-tokens-extractor).
- **Body Composition Removed**: On-device body composition calculations (BMI, body fat %, muscle mass, water, visceral fat, etc.) have been completely removed from this integration to keep it focused and lightweight. If you want body composition calculations in Home Assistant from your weight and impedance values, consider using the [BodyMiscale](https://github.com/dckiller51/bodymiscale) custom integration.
- **Early Development**: This project is still in early development and not intended for production environments.

---

## Features

- **Live weight stream** — updates every ~0.3s while standing on the scale
- **Weight Stable indicator** — binary sensor indicating when the measurement has stabilized/settled
- **Final weight reading** — locked reading retained and restored across Home Assistant restarts
- **Impedance sensors** — bio-electrical impedance and low-frequency impedance readings from the dual-frequency sensors
- **Event firing** — fires a `xiaomi_s400_scale_measurement` event on the Home Assistant bus upon completion of a measurement
- **Re-authentication flow** — easily update the BLE login token if the scale is re-paired in the Mi Home app

---

## Getting Your BLE Login Token

Because the scale requires Mi Home v2 cryptographic authentication before it will stream data:

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

1. Download or clone this repository.
2. Copy the `custom_components/xiaomi-s400-scale` folder into your Home Assistant `<config>/custom_components/` directory.
3. Restart Home Assistant.

---

## Configuration

1. Step on the scale to wake it up and begin BLE advertising.
2. In Home Assistant, go to **Settings** → **Devices & Services**.
   - If auto-discovered via Bluetooth, click **Configure**.
   - Otherwise, click **Add Integration**, search for **Xiaomi S400 Scale**, and enter your scale's Bluetooth MAC address.
3. Enter the 24-character hex **BLE Login Token**.
4. Submit the form to complete the setup.

### Token Updates / Re-authentication

If the scale is factory reset or re-paired in the Mi Home app, the BLE token rotates. You can update the token anytime by clicking **Configure** on the integration card or by following the automatic re-authentication prompt if Home Assistant detects an authentication failure.

---

## Entities Provided

| Entity | Domain | Description |
| --- | --- | --- |
| `sensor.<device>_live_weight` | Sensor (`kg`) | Real-time weight stream updating every ~0.3s while on the scale |
| `sensor.<device>_weight` | Sensor (`kg`) | Final locked weight measurement (restored across HA restarts) |
| `sensor.<device>_impedance` | Sensor (`Ω`) | Bio-electrical impedance measurement |
| `sensor.<device>_low_frequency_impedance` | Sensor (`Ω`) | Low-frequency bio-electrical impedance measurement |
| `binary_sensor.<device>_weight_stable` | Binary Sensor | Indicates when the live weight measurement has stabilized |
| `binary_sensor.<device>_connected` | Binary Sensor | Diagnostic sensor indicating an active authenticated BLE connection |

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
