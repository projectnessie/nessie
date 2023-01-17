# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Nessie Data objects."""
import re
from datetime import datetime
from typing import List, Optional, Tuple

import attr
import desert
from marshmallow import fields
from marshmallow_oneofschema import OneOfSchema

# regex taken from org.projectnessie.model.Validation
__RE_REFERENCE_NAME_RAW = "[A-Za-z](((?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9._-])"
__RE_REFERENCE_NAME: re.Pattern = re.compile(f"^{__RE_REFERENCE_NAME_RAW}$")
__RE_HASH_RAW = "[0-9a-fA-F]{8,64}"
__RE_HASH: re.Pattern = re.compile(f"^{__RE_HASH_RAW}$")
__RE_REFERENCE_WITH_HASH: re.Pattern = re.compile(f"^({__RE_REFERENCE_NAME_RAW})([@]|[*])({__RE_HASH_RAW})$")


DETACHED_REFERENCE_NAME = "DETACHED"


def is_valid_reference_name(ref: str) -> bool:
    """Checks whether 'ref' is a valid reference name."""
    return __RE_REFERENCE_NAME.match(ref) is not None


def is_valid_hash(ref: str) -> bool:
    """Checks whether 'ref' is a valid commit id/hash."""
    return __RE_HASH.match(ref) is not None


def split_into_reference_and_hash(ref_with_hash: Optional[str]) -> Tuple[str, Optional[str]]:
    """Returns a tuple of reference-name + hash, if the given string represents a ref-name + hash tuple 'ref_name@commit_id'."""
    if not ref_with_hash:
        return "<UNKNOWN>", None
    match = __RE_REFERENCE_WITH_HASH.match(ref_with_hash)
    if not match:
        if is_valid_hash(ref_with_hash):
            return DETACHED_REFERENCE_NAME, ref_with_hash
        return ref_with_hash, None
    return match.group(1), match.group(5)


@attr.dataclass
class Content:
    """Dataclass for Nessie Content."""

    id: str = desert.ib(fields.Str())

    @staticmethod
    def requires_expected_state() -> bool:
        """Checks whether this Content object requires the "expected" state to be provided for Put operations."""
        return False

    def pretty_print(self) -> str:
        """Print out for cli."""
        raise NotImplementedError


@attr.dataclass
class IcebergTable(Content):
    """Dataclass for Nessie Content."""

    metadata_location: str = desert.ib(fields.Str(data_key="metadataLocation"))
    snapshot_id: int = desert.ib(fields.Int(data_key="snapshotId"))
    schema_id: int = desert.ib(fields.Int(data_key="schemaId"))
    spec_id: int = desert.ib(fields.Int(data_key="specId"))
    sort_order_id: int = desert.ib(fields.Int(data_key="sortOrderId"))

    @staticmethod
    def requires_expected_state() -> bool:
        """Returns True - expected state should be provided for Put operations on Iceberg tables."""
        return True

    def pretty_print(self) -> str:
        """Print out for cli."""
        return (
            f"Iceberg table:\n\tmetadata-location: {self.metadata_location}\n\tsnapshot-id: {self.snapshot_id}"
            f"\n\tschema-id: {self.schema_id}"
            f"\n\tpartition-spec-id: {self.spec_id}\n\tdefault-sort-order-id: {self.sort_order_id}"
        )


IcebergTableSchema = desert.schema_class(IcebergTable)


@attr.dataclass
class DeltaLakeTable(Content):
    """Dataclass for Nessie Content."""

    last_checkpoint: str = desert.ib(fields.Str(data_key="lastCheckpoint"))
    checkpoint_location_history: List[str] = desert.ib(fields.List(fields.Str(), data_key="checkpointLocationHistory"))
    metadata_location_history: List[str] = desert.ib(fields.List(fields.Str(), data_key="metadataLocationHistory"))

    def pretty_print(self) -> str:
        """Print out for cli."""
        deltas = "\n\t\t".join(self.metadata_location_history)
        checkpoints = "\n\t\t".join(self.checkpoint_location_history)
        return "DeltaLake table:\n\tLast Checkpoint: {}\n\tDelta History: {}\n\tCheckpoint History: {}".format(
            self.last_checkpoint, deltas, checkpoints
        )


DeltaLakeTableSchema = desert.schema_class(DeltaLakeTable)


@attr.dataclass
class IcebergView(Content):
    """Dataclass for Nessie Iceberg View."""

    metadata_location: str = desert.ib(fields.Str(data_key="metadataLocation"))
    version_id: int = desert.ib(fields.Int(data_key="versionId"))
    schema_id: int = desert.ib(fields.Int(data_key="schemaId"))
    dialect: str = desert.ib(fields.Str())
    sql_text: str = desert.ib(fields.Str(data_key="sqlText"))

    def pretty_print(self) -> str:
        """Print out for cli."""
        return "IcebergView:\n\tmetadata-location: {}\n\tversion-id: {}\n\tschema-id: {}\n\tDialect: {}\n\tSql: {}".format(
            self.metadata_location, self.version_id, self.schema_id, self.dialect, self.sql_text
        )  # todo use a sql parser to pretty print this


IcebergViewSchema = desert.schema_class(IcebergView)


class ContentSchema(OneOfSchema):
    """Schema for Nessie Content."""

    type_schemas = {
        "ICEBERG_TABLE": IcebergTableSchema,
        "DELTA_LAKE_TABLE": DeltaLakeTableSchema,
        "ICEBERG_VIEW": IcebergViewSchema,
    }

    def get_obj_type(self, obj: Content) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, IcebergTable):
            return "ICEBERG_TABLE"
        if isinstance(obj, DeltaLakeTable):
            return "DELTA_LAKE_TABLE"
        if isinstance(obj, IcebergView):
            return "ICEBERG_VIEW"

        raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


@attr.dataclass
class ContentKey:
    """ContentKey."""

    elements: List[str] = desert.ib(fields.List(fields.Str))

    def to_string(self) -> str:
        """Convert this key to friendly CLI string."""
        # false positives in pylint
        # pylint: disable=E1133
        return ".".join(f'"{i!s}"' if "." in i else i for i in self.elements)  # noqa: B028

    def to_path_string(self) -> str:
        """Convert this key to a url encoded path string."""
        # false positives in pylint
        # pylint: disable=E1133
        return ".".join(i.replace(".", "\00") if i else "" for i in self.elements)

    @staticmethod
    def from_path_string(key: str) -> "ContentKey":
        """Convert from path encoded string to normal string."""
        return ContentKey([i for i in ContentKey._split_key_based_on_regex(key) if i])

    @staticmethod
    def _split_key_based_on_regex(raw_key: str) -> List[str]:
        # Find all occurrences of strings between double quotes
        # E.g: a.b."c.d"
        regex = re.compile('"[^"]*"')

        # Replace any dot that is inside double quotes with null char '\00' and remove the double quotes
        key_with_null = regex.sub(lambda x: x.group(0).replace(".", "\00").replace('"', ""), raw_key)

        # Split based on the dot
        splitted_key = key_with_null.split(".")

        # Return back the splitted elements and make sure to change back '/0' to '.'
        return [i.replace("\00", ".") for i in splitted_key]


ContentKeySchema = desert.schema_class(ContentKey)


@attr.dataclass
class Operation:
    """Single Commit Operation."""

    key: ContentKey = desert.ib(fields.Nested(ContentKeySchema))

    def pretty_print(self) -> str:
        """Print out for cli."""
        raise NotImplementedError


@attr.dataclass
class Put(Operation):
    """Single Commit Operation."""

    content: Content = desert.ib(fields.Nested(ContentSchema))
    expectedContent: Optional[Content] = attr.ib(default=None, metadata=desert.metadata(fields.Nested(ContentSchema, allow_none=True)))

    def pretty_print(self) -> str:
        """Print out for cli."""
        # pylint: disable=E1101
        return f"Put of {self.key.to_string()} : {self.content.pretty_print()}"


PutOperationSchema = desert.schema_class(Put)


@attr.dataclass
class Delete(Operation):
    """Delete single key."""

    def pretty_print(self) -> str:
        """Print out for cli."""
        # pylint: disable=E1101
        return f"Delete of {self.key.to_string()}"


DeleteOperationSchema = desert.schema_class(Delete)


@attr.dataclass
class Unchanged(Operation):
    """Unchanged single key."""

    def pretty_print(self) -> str:
        """Print out for cli."""
        # pylint: disable=E1101
        return f"Unchanged of {self.key.to_string()}"


UnchangedOperationSchema = desert.schema_class(Unchanged)


class OperationsSchema(OneOfSchema):
    """Schema for Nessie Operations."""

    type_schemas = {
        "PUT": PutOperationSchema,
        "UNCHANGED": UnchangedOperationSchema,
        "DELETE": DeleteOperationSchema,
    }

    def get_obj_type(self, obj: Operation) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, Put):
            return "PUT"
        if isinstance(obj, Unchanged):
            return "UNCHANGED"
        if isinstance(obj, Delete):
            return "DELETE"

        raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


@attr.dataclass
class CommitMeta:
    """Dataclass for commit metadata."""

    hash_: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(data_key="hash")))
    commitTime: datetime = attr.ib(default=None, metadata=desert.metadata(fields.DateTime()))
    authorTime: datetime = attr.ib(default=None, metadata=desert.metadata(fields.DateTime()))
    committer: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    author: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    signedOffBy: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    message: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))
    properties: dict = desert.ib(fields.Dict(), default=None)


CommitMetaSchema = desert.schema_class(CommitMeta)


@attr.dataclass
class ReferenceMetadata:
    """Dataclass for Nessie ReferenceMetadata."""

    commit_meta_of_head: CommitMeta = desert.ib(fields.Nested(CommitMetaSchema, data_key="commitMetaOfHEAD", allow_none=True))
    num_commits_ahead: int = attr.ib(default=None, metadata=desert.metadata(fields.Int(allow_none=True, data_key="numCommitsAhead")))
    num_commits_behind: int = attr.ib(default=None, metadata=desert.metadata(fields.Int(allow_none=True, data_key="numCommitsBehind")))
    common_ancestor_hash: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True, data_key="commonAncestorHash")))
    num_total_commits: str = attr.ib(default=None, metadata=desert.metadata(fields.Int(allow_none=True, data_key="numTotalCommits")))


ReferenceMetadataSchema = desert.schema_class(ReferenceMetadata)


@attr.dataclass
class Reference:
    """Dataclass for Nessie Reference."""

    name: str = desert.ib(fields.Str())
    hash_: Optional[str] = attr.ib(default=None, metadata=desert.metadata(fields.Str(data_key="hash", allow_none=True)))
    metadata: Optional[ReferenceMetadata] = attr.ib(
        default=None, metadata=desert.metadata(fields.Nested(ReferenceMetadataSchema, allow_none=True, data_key="metadata"))
    )


@attr.dataclass
class Detached(Reference):
    """Dataclass for Nessie detached commit id."""

    pass


DetachedSchema = desert.schema_class(Detached)


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


class ReferenceSchema(OneOfSchema):
    """Schema for Nessie Reference."""

    type_schemas = {
        "BRANCH": BranchSchema,
        "TAG": TagSchema,
        "DETACHED": DetachedSchema,
    }

    def get_obj_type(self, obj: Reference) -> str:
        """Returns the object type based on its class."""
        if isinstance(obj, Branch):
            return "BRANCH"
        if isinstance(obj, Tag):
            return "TAG"
        if isinstance(obj, Detached):
            return "DETACHED"

        raise Exception("Unknown object type: {}".format(obj.__class__.__name__))


@attr.dataclass
class ReferencesResponse:
    """Dataclass for References."""

    references: List[Reference] = desert.ib(fields.List(fields.Nested(ReferenceSchema())))
    has_more: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="hasMore")))
    token: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))


ReferencesResponseSchema = desert.schema_class(ReferencesResponse)


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
class LogEntry:
    """Dataclass for commit log entries."""

    commit_meta: CommitMeta = desert.ib(fields.Nested(CommitMetaSchema, data_key="commitMeta"))
    parent_commit_hash: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True, data_key="parentCommitHash")))
    operations: List[Operation] = desert.ib(marshmallow_field=fields.List(fields.Nested(OperationsSchema()), allow_none=True), default=None)


LogEntrySchema = desert.schema_class(LogEntry)


@attr.dataclass
class LogResponse:
    """Dataclass for Log Response."""

    log_entries: List[LogEntry] = desert.ib(fields.List(fields.Nested(LogEntrySchema()), data_key="logEntries"))
    has_more: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="hasMore")))
    token: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))


LogResponseSchema = desert.schema_class(LogResponse)


@attr.dataclass
class Transplant:
    """Dataclass for Transplant operation."""

    from_ref_name: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="fromRefName")))
    hashes_to_transplant: List[str] = attr.ib(metadata=desert.metadata(fields.List(fields.Str(), data_key="hashesToTransplant")))
    keep_individual_commits: bool = attr.ib(
        default=True, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="keepIndividualCommits"))
    )


TransplantSchema = desert.schema_class(Transplant)


@attr.dataclass
class Merge:
    """Dataclass for Merge operation."""

    from_ref_name: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="fromRefName")))
    from_hash: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(data_key="fromHash")))


MergeSchema = desert.schema_class(Merge)


@attr.dataclass
class MergeResponseDetails:
    """Dataclass for details in MergeResponse."""

    key: ContentKey = attr.ib(metadata=desert.metadata(fields.Nested(ContentKeySchema())))
    merge_behaviour: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="mergeBehavior")))
    conflict_type: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="conflictType")))
    source_commits: List[str] = attr.ib(metadata=desert.metadata(fields.List(fields.Str(), data_key="sourceCommits")))
    target_commits: List[str] = attr.ib(metadata=desert.metadata(fields.List(fields.Str(allow_none=True), data_key="targetCommits")))


MergeResponseDetailsSchema = desert.schema_class(MergeResponseDetails)


@attr.dataclass
class MergeResponse:
    """Dataclass for a MergeResponse."""

    target_branch: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="targetBranch")))
    effective_target_hash: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="effectiveTargetHash")))
    source_commits: List[LogEntry] = attr.ib(
        metadata=desert.metadata(fields.List(fields.Nested(LogEntrySchema()), data_key="sourceCommits"))
    )
    details: List[MergeResponseDetails] = attr.ib(
        metadata=desert.metadata(fields.List(fields.Nested(MergeResponseDetailsSchema()), data_key="details"))
    )
    resultant_target_hash: str = attr.ib(metadata=desert.metadata(fields.Str(allow_none=True, data_key="resultantTargetHash")))
    common_ancestor: str = attr.ib(metadata=desert.metadata(fields.Str(allow_none=True, data_key="commonAncestor")))
    expected_hash: str = attr.ib(metadata=desert.metadata(fields.Str(allow_none=True, data_key="expectedHash")))
    target_commits: List[LogEntry] = attr.ib(
        metadata=desert.metadata(fields.List(fields.Nested(LogEntrySchema()), allow_none=True, data_key="targetCommits"))
    )
    was_applied: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(data_key="wasApplied")))
    was_successful: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(data_key="wasSuccessful")))


MergeResponseSchema = desert.schema_class(MergeResponse)


@attr.dataclass
class MultiContents:
    """Contents container for commit."""

    commit_meta: CommitMeta = desert.ib(fields.Nested(CommitMetaSchema, data_key="commitMeta"))
    operations: List[Operation] = desert.ib(fields.List(fields.Nested(OperationsSchema())))


MultiContentSchema = desert.schema_class(MultiContents)


@attr.dataclass
class DiffEntry:
    """Dataclass for a Diff."""

    content_key: ContentKey = desert.ib(fields.Nested(ContentKeySchema, data_key="key"))
    from_content: Content = desert.ib(fields.Nested(ContentSchema, default=None, data_key="from", allow_none=True))
    to_content: Content = desert.ib(fields.Nested(ContentSchema, default=None, data_key="to", allow_none=True))

    def pretty_print(self) -> str:
        """Print out for cli."""
        # pylint: disable=E1101
        from_output = f"{self.from_content.pretty_print()}" if self.from_content else ""
        to_output = f"{self.to_content.pretty_print()}" if self.to_content else ""
        return (
            f"ContentKey: {self.content_key.to_path_string()}\nFROM:\n\t{from_output}\nTO:\n{to_output}"
            f"\n----------------------------------------"
        )


DiffEntrySchema = desert.schema_class(DiffEntry)


@attr.dataclass
class DiffResponse:
    """Dataclass for a DiffResponse."""

    diffs: List[DiffEntry] = desert.ib(fields.List(fields.Nested(DiffEntrySchema(), data_key="diffs")))


DiffResponseSchema = desert.schema_class(DiffResponse)


@attr.dataclass
class ReflogEntry:
    """Dataclass for reflog entries."""

    reflog_id: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="refLogId")))
    ref_name: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="refName")))
    ref_type: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="refType")))
    commit_hash: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="commitHash")))
    parent_reflog_id: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="parentRefLogId")))
    operation_time: int = attr.ib(metadata=desert.metadata(fields.Int(data_key="operationTime")))
    operation: str = attr.ib(metadata=desert.metadata(fields.Str(data_key="operation")))
    source_hashes: List[str] = attr.ib(metadata=desert.metadata(fields.List(fields.Str(), data_key="sourceHashes")))


ReflogEntrySchema = desert.schema_class(ReflogEntry)


@attr.dataclass
class ReflogResponse:
    """Dataclass for reflog Response."""

    log_entries: List[ReflogEntry] = desert.ib(fields.List(fields.Nested(ReflogEntrySchema()), data_key="logEntries"))
    has_more: bool = attr.ib(default=False, metadata=desert.metadata(fields.Bool(allow_none=True, data_key="hasMore")))
    token: str = attr.ib(default=None, metadata=desert.metadata(fields.Str(allow_none=True)))


ReflogResponseSchema = desert.schema_class(ReflogResponse)
