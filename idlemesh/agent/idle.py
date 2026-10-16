"""Decide whether this laptop is idle enough to lend out.

Signals: CPU load, battery/charging state and (TODO) time since last keyboard/mouse
input. Input-idle time is OS-specific:
  - Linux/X11: `xprintidle`; Wayland: org.freedesktop.ScreenSaver / logind IdleHint
  - Windows:  GetLastInputInfo via ctypes
  - macOS:    `ioreg -c IOHIDSystem` -> HIDIdleTime
"""

from __future__ import annotations

from dataclasses import dataclass

import psutil


@dataclass
class IdleSnapshot:
    cpu_percent: float
    on_battery: bool
    battery_percent: float | None
    input_idle_s: float | None

    def lendable(
        self, max_cpu: float = 30.0, min_input_idle_s: float = 120.0, allow_battery: bool = False
    ) -> bool:
        if self.on_battery and not allow_battery:
            return False
        if self.cpu_percent > max_cpu:
            return False
        if self.input_idle_s is not None and self.input_idle_s < min_input_idle_s:
            return False
        return True


class IdleMonitor:
    def snapshot(self, interval: float = 0.5) -> IdleSnapshot:
        cpu = psutil.cpu_percent(interval=interval)
        batt = psutil.sensors_battery() if hasattr(psutil, "sensors_battery") else None
        return IdleSnapshot(
            cpu_percent=cpu,
            on_battery=bool(batt and not batt.power_plugged),
            battery_percent=batt.percent if batt else None,
            input_idle_s=self.input_idle_seconds(),
        )

    def input_idle_seconds(self) -> float | None:
        return None  # TODO(phase 4): per-OS implementations listed in module docstring
