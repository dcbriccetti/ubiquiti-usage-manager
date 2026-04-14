from dataclasses import dataclass

@dataclass(frozen=True, kw_only=True)
class SpeedLimit:
    'UniFi speed-limit profile with optional up/down caps.'
    id: str
    name: str
    up_kbps: int | None = None
    down_kbps: int | None = None

    @property
    def is_unlimited(self) -> bool:
        'Return True when neither upload nor download cap is set.'
        return self.up_kbps is None and self.down_kbps is None

    def __str__(self) -> str:
        'Render a human-readable speed-limit label for reports.'
        if self.is_unlimited:
            return ''

        up = self.up_kbps if self.up_kbps else '∞'
        down = self.down_kbps if self.down_kbps else '∞'

        return f'{self.name} ({up}/{down})'
