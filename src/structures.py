from dataclasses import dataclass

@dataclass(frozen=True, kw_only=True)
class SpeedLimit:
    id: str
    name: str
    up_kbps: int | None = None
    down_kbps: int | None = None

    @property
    def is_unlimited(self) -> bool:
        return self.up_kbps is None and self.down_kbps is None

    def __str__(self) -> str:
        if self.is_unlimited:
            return ''

        up = self.up_kbps if self.up_kbps else '∞'
        down = self.down_kbps if self.down_kbps else '∞'

        return f'{self.name} ({up}/{down})'
