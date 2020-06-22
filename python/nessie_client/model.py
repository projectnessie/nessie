# -*- coding: utf-8 -*-
"""Nessie Data objects."""
import attr
import desert
from marshmallow import fields


@attr.dataclass
class Branch:
    """Dataclass for Nessie Branch."""

    name: str = desert.ib(fields.Str())
    id: str = attr.ib(default=None, metadata=desert.metadata(field=fields.Str(allow_none=True)))
    createMillis: int = attr.ib(default=-9223372036854775808, metadata=desert.metadata(field=fields.Int()))
    expireMillis: int = attr.ib(default=None, metadata=desert.metadata(field=fields.Int(allow_none=True)))
    isDeleted: bool = attr.ib(default=False, metadata=desert.metadata(field=fields.Bool()))
    updateTime: int = attr.ib(default=-9223372036854775808, metadata=desert.metadata(field=fields.Int()))


BranchSchema = desert.schema_class(Branch)


@attr.dataclass
class Table:
    """Dataclass for Nessie Table."""

    id: str = desert.ib(fields.Str())
    name: str = desert.ib(fields.Str())
    metadataLocation: str = desert.ib(fields.Str())
    namespace: str = attr.ib(default=None, metadata=desert.metadata(field=fields.Str(allow_none=True)))
    isDeleted: bool = attr.ib(default=False, metadata=desert.metadata(field=fields.Bool()))
    updateTime: int = attr.ib(default=-9223372036854775808, metadata=desert.metadata(field=fields.Int()))


TableSchema = desert.schema_class(Table)
