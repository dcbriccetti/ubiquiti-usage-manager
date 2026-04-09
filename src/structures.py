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

    def __str__(self):
        return f'{self.name} ({self.up_kbps}/{self.up_kbps})'

