from __future__ import annotations

import random
from typing import TYPE_CHECKING

from faker import Faker

if TYPE_CHECKING:
    from typing import Any


class EmployeeGenerator:
    def __init__(self, start_id: int = 0):
        self.current_id = start_id
        self.fake = Faker()
        self.genders = ["M", "F", "O"]
        self.countries = ["US", "UK", "IN", "CA", "AU", "DE", "FR", "JP", "CN", "BR"]
        self.schema = (
            "ID",
            "COUNTRY",
            "FIRST_NAME",
            "SURNAME",
            "GENDER",
            "AGE",
            "EMAIL",
            "CREATED_AT",
            "UPDATED_AT",
            "STATUS",
        )

    def generate(
        self,
        count: int | None = None,
        id_start: int | None = None,
        id_end: int | None = None,
        watermark_start: str = "now",
        watermark_end: str = "now",
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """
        Generate employee data either by count or by ID range.
        Returns a tuple: (data, schema)
        """
        if count is not None:
            ids = range(self.current_id + 1, self.current_id + count + 1)
            self.current_id += count
        elif id_start is not None and id_end is not None:
            ids = range(id_start, id_end + 1)
        else:
            raise ValueError(
                "Must provide either 'count' or both 'id_start' and 'id_end'."
            )

        data = [
            self._create_employee(emp_id, watermark_start, watermark_end)
            for emp_id in ids
        ]
        return data, self.schema

    def _create_employee(
        self, emp_id: int, watermark_start: str, watermark_end: str
    ) -> tuple[Any, ...]:
        """
        Create a single employee record.
        """
        country = random.choice(self.countries)
        gender = random.choice(self.genders)

        first_name = (
            self.fake.first_name_male()
            if gender == "M"
            else self.fake.first_name_female()
        )
        surname = self.fake.last_name()
        age = random.randint(21, 65)
        email = f"{first_name.lower()}.{surname.lower()}@example.{country.lower()}"

        created_at = self.fake.date_time_between(start_date="-3y", end_date="-1y")
        updated_at = self.fake.date_time_between(
            start_date=watermark_start, end_date=watermark_end
        )
        status = "active" if random.random() > 0.1 else "inactive"

        return (
            emp_id,
            country,
            first_name,
            surname,
            gender,
            age,
            email,
            created_at,
            updated_at,
            status,
        )
