from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime

WATERMARK_TYPES = {"int", "bigint", "long", "timestamp"}


@dataclass
class Watermark:
    table_name: str
    column_name: str
    value: str | int
    type: str

    def dict(self) -> dict[str, str]:
        return asdict(self)

    def __str__(self) -> str:
        return json.dumps(self.dict())

    def _cmp_check(self, other: Watermark) -> None:
        if (
            self.table_name != other.table_name
            or self.column_name != other.column_name
            or self.type != other.type
        ):
            raise TypeError(
                f"Can't compare "
                f"\n'{self.table_name}' to '{other.table_name}'"
                f"\n'{self.column_name}' to '{other.column_name}'"
                f"\n'{self.type}' to '{other.type}'"
            )

    def _formatted_value(self) -> int | float:
        if isinstance(self.value, int):
            return self.value
        elif isinstance(self.value, str):
            return datetime.fromisoformat(self.value).timestamp()
        else:
            raise ValueError(f"Unsupported watermark type: {self.type}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Watermark):
            raise TypeError(
                f"Can't compare '{type(self).__name__}' to '{type(other).__name__}'"
            )
        self._cmp_check(other)
        return self._formatted_value() == other._formatted_value()

    def __le__(self, other: Watermark) -> bool:
        self._cmp_check(other)
        return self._formatted_value() <= other._formatted_value()

    def __lt__(self, other: Watermark) -> bool:
        self._cmp_check(other)
        return self._formatted_value() < other._formatted_value()

    def __ge__(self, other: Watermark) -> bool:
        self._cmp_check(other)
        return self._formatted_value() >= other._formatted_value()

    def __gt__(self, other: Watermark) -> bool:
        self._cmp_check(other)
        return self._formatted_value() > other._formatted_value()
