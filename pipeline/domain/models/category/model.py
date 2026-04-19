"""Pydantic model for ``categories`` (see ``docker/sql/01_schema.sql``)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Mapping

from pydantic import BaseModel, ConfigDict, Field


class Category(BaseModel):

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    category_id: Annotated[int, Field(ge=1)]
    name: Annotated[str, Field(max_length=50)]
    description: Annotated[str | None, Field(max_length=200)] = None
    updated_at: datetime


def parse_category(data: Mapping[str, Any]) -> Category:
    return Category.model_validate(dict(data))
