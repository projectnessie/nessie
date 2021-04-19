# -*- coding: utf-8 -*-
"""Nessie Data objects."""
from datetime import datetime
from typing import List
from typing import Optional

import attr
import desert
from marshmallow import fields
from marshmallow_oneofschema import OneOfSchema


@attr.dataclass
class Contents:
    """Dataclass for Nessie Contents."""

    id: str = desert.ib(fields.Str())

    def pretty_print(self: "Contents") -> str:
        """Print out for cli."""
        pass


@attr.dataclass
class IcebergTable(Contents):
    """Dataclass for Nessie Contents."""

    metadata_location: str = desert.ib(fields.Str(data_key="metadataLocation"))

    def pretty_print(self: "IcebergTable") -> str:
        """Print out for cli."""
        return "Iceberg table:\n\t{}".format(self.metadata_location)


IcebergTableSchema = desert.schema_class(IcebergTable)


@attr.dataclass
class DeltaLakeTable(Contents):
    """Dataclass for Nessie Contents."""

    last_checkpoint: str = desert.ib(fields.Str(data_key="metadataLocation"))
    checkpoint_location_history: List[str] = desert.ib(fields.List(fields.Str))
    metadata_location_history: List[str] = desert.ib(fields.List(fields.Str))

    def pretty_print(self: "DeltaLakeTable") -> str:
        """Print out for cli."""
        deltas = "\n\t\t".join(self.metadata_location_history)
        checkpoints = "\n\t\t".join(self.checkpoint_location_history)
        return "Iceberg table:\n\tLast Checkpoint: {}\n\tDelta History: {}\n\tCheckpoint History: {}".format(
            self.last_checkpoint, deltas, checkpoints
        )


DeltaLakeTableSchema = desert.schema_class(DeltaLakeTable)


@attr.dataclass
class SqlView(Contents):
    """Dataclass for Nessie SQL View."""

    dialect: str = desert.ib(fields.Str())
    sql_test: str = desert.ib(fields.Str(data_key="sqlTest"))

    def pretty_print(self: "SqlView") -> str:
        """Print out for cli."""
        return "Iceberg table:\n\tDialect: {}\n\tSql: {}".format(self.dialect, self.sql_test)  # todo use a sql parser to pretty print this


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
class ContentsKey:
    """ContentsKey."""

    elements: List[str] = desert.ib(fields.List(fields.Str))


ContentsKeySchema = desert.schema_class(ContentsKey)


@attr.dataclass
class Operation:
    """Single Commit Operation."""

    key: ContentsKey = desert.ib(fields.Nested(ContentsKeySchema))


@attr.dataclass
class Put(Operation):
    """Single Commit Operation."""

    contents: Contents = desert.ib(fields.Nested(ContentsSchema))


PutOperationSchema = desert.schema_class(Put)


@attr.dataclass
class Delete(Operation):
    """Delete single key."""

    pass


DeleteOperationSchema = desert.schema_class(Delete)


@attr.dataclass
class Unchanged(Operation):
    """Unchanged single key."""

    pass


UnchangedOperationSchema = desert.schema_class(Unchanged)


class OperationsSchema(OneOfSchema):
    """Schema for Nessie Operations."""

    type_schemas = {
        "PUT": PutOperationSchema,
        "UNCHANGED": UnchangedOperationSchema,
        "DELETE": DeleteOperationSchema,
    }

    def get_obj_type(self: "OperationsSchema", obj: Operation) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, Put):
            return "PUT"
        elif isinstance(obj, Unchanged):
            return "UNCHANGED"
        elif isinstance(obj, Delete):
            return "DELETE"
        else:
            raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


@attr.dataclass
class Reference:
    """Dataclass for Nessie Reference."""

    name: str = desert.ib(fields.Str())
    hash_: Optional[str] = desert.ib(fields.Str(data_key="hash"))


@attr.dataclass
class Branch(Reference):
    """Dataclass for Nessie Branch."""

    pass


BranchSchema = desert.schema_class(Branch)


@attr.dataclass
class Tag(Reference):
    """Dataclass for Nessie Tag."""

    pass


TagSchema = desert.schema_class(Tag)


@attr.dataclass
class Hash(Reference):
    """Dataclass for Nessie Hash."""

    pass


HashSchema = desert.schema_class(Hash)


class ReferenceSchema(OneOfSchema):
    """Schema for Nessie Reference."""

    type_schemas = {
        "BRANCH": BranchSchema,
        "TAG": TagSchema,
        "HASH": HashSchema,
    }

    def get_obj_type(self: "ReferenceSchema", obj: Reference) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, Branch):
            return "BRANCH"
        elif isinstance(obj, Tag):
            return "TAG"
        elif isinstance(obj, Hash):
            return "HASH"
        else:
            raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


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


@attr.dataclass
class CommitMeta:
    """Dataclass for commit metadata."""

    hash_: str = desert.ib(fields.Str(data_key="hash"), default=None)
    commitTime: datetime = desert.ib(fields.DateTime(), default=None)
    authorTime: datetime = desert.ib(fields.DateTime(), default=None)
    committer: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    email: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    author: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    signedOffBy: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    message: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    properties: dict = desert.ib(fields.Dict(), default=None)


CommitMetaSchema = desert.schema_class(CommitMeta)


@attr.dataclass
class LogResponse:
    """Dataclass for Log Response."""

    operations: List[CommitMeta] = desert.ib(fields.List(fields.Nested(CommitMetaSchema())))
    has_more: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="hasMore")))
    token: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))


LogResponseSchema = desert.schema_class(LogResponse)


@attr.dataclass
class Transplant:
    """Dataclass for Transplant operation."""

    hashes_to_transplant: List[str] = attr.ib(metadata=desert.metadata(fields.List(fields.Str(), data_key="hashesToTransplant")))


TransplantSchema = desert.schema_class(Transplant)


@attr.dataclass
class Merge:
    """Dataclass for Merge operation."""

    from_hash: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(data_key="fromHash")))


MergeSchema = desert.schema_class(Merge)


@attr.dataclass
class MultiContents:
    """Contents container for commit."""

    commit_meta: CommitMeta = desert.ib(fields.Nested(CommitMetaSchema, data_key="commitMeta"))
    operations: List[Operation] = desert.ib(fields.List(fields.Nested(OperationsSchema())))


MultiContentsSchema = desert.schema_class(MultiContents)
