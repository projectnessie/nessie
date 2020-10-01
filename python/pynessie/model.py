# -*- coding: utf-8 -*-
"""Nessie Data objects."""
from typing import List

import attr
import desert
from marshmallow import fields
from marshmallow_oneofschema import OneOfSchema


@attr.dataclass
class Contents:
    """Dataclass for Nessie Contents."""


@attr.dataclass
class IcebergTable(Contents):
    """Dataclass for Nessie Contents."""

    metadata_location: str = desert.ib(fields.Str(data_key="metadataLocation"))


IcebergTableSchema = desert.schema_class(IcebergTable)


@attr.dataclass
class DeltaLakeTable(Contents):
    """Dataclass for Nessie Contents."""

    metadata_location: str = desert.ib(fields.Str(data_key="metadataLocation"))


DeltaLakeTableSchema = desert.schema_class(DeltaLakeTable)


@attr.dataclass
class SqlView(Contents):
    """Dataclass for Nessie SQL View."""

    dialect: str = desert.ib(fields.Str())
    sql_test: str = desert.ib(fields.Str(data_key="sqlTest"))


SqlViewSchema = desert.schema_class(SqlView)


class ContentsSchema(OneOfSchema):
    """Schema for Nessie Content."""

    type_schemas = {
        "ICEBERG_TABLE": IcebergTableSchema,
        "DELTA_LAKE_TABLE": DeltaLakeTableSchema,
        "VIEW": SqlViewSchema,
    }

    def get_obj_type(self: "ContentsSchema", obj: Contents) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, IcebergTable):
            return "ICEBERG_TABLE"
        elif isinstance(obj, DeltaLakeTable):
            return "DELTA_LAKE_TABLE"
        elif isinstance(obj, SqlView):
            return "VIEW"
        else:
            raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


@attr.dataclass
class Reference:
    """Dataclass for Nessie Reference."""

    name: str = desert.ib(fields.Str())
    hash_: str = desert.ib(fields.Str(data_key="hash"))
    kind: str = desert.ib(fields.Str(data_key="type"))


ReferenceSchema = desert.schema_class(Reference)


@attr.dataclass
class EntryName:
    """Dataclass for Nessie Entry Name."""

    elements: List[str] = desert.ib(fields.List(fields.Str()))


EntryNameSchema = desert.schema_class(EntryName)


@attr.dataclass
class Entry:
    """Dataclass for Nessie Entry."""

    kind: str = desert.ib(fields.Str(data_key="type"))
    name: EntryName = desert.ib(fields.Nested(EntryNameSchema))


EntrySchema = desert.schema_class(Entry)


@attr.dataclass
class Entries:
    """Dataclass for Content Entries."""

    entries: List[Entry] = desert.ib(fields.List(fields.Nested(EntrySchema())))
    has_more: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="hasMore")))
    token: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))


EntriesSchema = desert.schema_class(Entries)
