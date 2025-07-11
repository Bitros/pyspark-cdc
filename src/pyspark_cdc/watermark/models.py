from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


@dataclass(order=True, slots=True)
class Watermark:
    table_name: str = field(compare=False)
    column_name: str = field(compare=False)
    value: str | int
    type: str = field(compare=False)

    def __str__(self) -> str:
        return json.dumps(asdict(self))

    def dict(self) -> dict[str, Any]:
        return asdict(self)
