# CEL filters

Several Nessie APIs take a `filter` query parameter. The value is a
[CEL](https://github.com/google/cel-spec/blob/master/doc/intro.md) expression
that must evaluate to `true` for an item to be returned.

The Java client exposes the same string on `.filter(...)`. There is no helper
that builds expressions for you; the variables below are what the server
actually binds.

```java
api.getCommitLog()
    .refName("main")
    .filter("commit.author=='nessie_author'")
    .stream();
```

REST:

```
GET /api/v2/trees/main/history?filter=commit.author=='nessie_author'
```

URL-encode the expression in HTTP clients. An intro to CEL is in the
[CEL spec](https://github.com/google/cel-spec/blob/master/doc/intro.md).
Nessie uses [cel-java](https://github.com/projectnessie/cel-java).

Authorization rules are a separate CEL environment (`ref`, `path`, `role`,
`roles`, `op`, `contentType`). This page covers listing filters only.

## Entries

`GET /api/v2/trees/{ref}/entries`

The expression is evaluated against `entry`:

| Field | Type | Notes |
| --- | --- | --- |
| `namespace` | string | Parent namespace, dotted path |
| `contentType` | string | e.g. `ICEBERG_TABLE`, `ICEBERG_VIEW`, `NAMESPACE` |
| `key` | string | Full key (`ContentKey.toString()`) |
| `encodedKey` | string | Dotted path (`ContentKey.toPathString()`) |
| `name` | string | Last key element (table name) |
| `keyElements` / `namespaceElements` | list of strings | Split form of the key |

Examples:

```
entry.namespace.startsWith('a.b.c')
entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
entry.namespace.startsWith('some.name.space') && entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
size(entry.keyElements) == 4
entry.encodedKey.startsWith('foo.')
entry.encodedKey == 'foo' || entry.encodedKey.startsWith('foo.')
entry.contentType == 'NAMESPACE' && size(entry.keyElements) == 2 && entry.encodedKey.startsWith('foo.')
```

`encodedKey` is the dotted path of the content key. A prefix match with a
trailing `.` selects children; `==` plus that prefix also keeps the namespace
entry itself.

## Commit log

`GET /api/v2/trees/{ref}/history`

The expression is evaluated against `commit` and `operations`:

| Field | Type | Notes |
| --- | --- | --- |
| `commit.author` | string | |
| `commit.committer` | string | |
| `commit.commitTime` | timestamp | |
| `commit.hash` | string | |
| `commit.message` | string | |
| `commit.properties` | map | |

`operations` is a list. Each element has:

| Field | Type | Notes |
| --- | --- | --- |
| `type` | string | `PUT` or `DELETE` |
| `key` | string | Full key |
| `encodedKey` | string | Dotted path |
| `keyElements` | list of strings | |
| `namespace` | string | |
| `namespaceElements` | list of strings | |
| `name` | string | Simple table name |

`operations` is only populated when `fetch=ALL`. Without that, an expression
that inspects `operations` never sees any ops.

Examples:

```
commit.author=='nessie_author'
commit.committer=='nessie_committer'
timestamp(commit.commitTime) > timestamp('2021-05-31T08:23:15Z')
operations.exists(op, op.name == 'BaseTable')
operations.exists(op, op.type == 'PUT')
operations.exists(op, op.key.startsWith('some.name.space.'))
```

Filtered commits disappear from the log. You can still tell a gap by comparing
`LogEntry.parentCommitHash` with the previous entry's hash.

## References

`GET /api/v2/trees/`

The expression is evaluated against:

| Field | Type | Notes |
| --- | --- | --- |
| `ref` | object | `name`, `hash`, and metadata on the reference |
| `refMeta` | object | `ReferenceMetadata` (never null, may be empty) |
| `commit` | object | HEAD commit meta (never null, may be empty) |
| `refType` | string | `BRANCH` or `TAG` |

`refMeta` and `commit` are only useful when `fetch=ALL`.

Examples:

```
refType == 'BRANCH'
ref.name == 'my-tag-or-branch'
commit.message == 'invent awesome things'
```

## Diff

`GET /api/v2/trees/{from-ref}/diff/{to-ref}`

The expression is evaluated against `key` (a content-key object, not `entry`):

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string | Simple table name |
| `namespace` | string | Parent namespace, dotted path |
| `key` | string | Full key |
| `encodedKey` | string | Dotted path |
| `keyElements` | list of strings | |
| `namespaceElements` | list of strings | |

Examples:

```
key.namespace=='foo'
key.name=='table'
```

## Tips

* `startsWith` / `in` / `exists` / `size` are the usual operators. `==` is exact match.
* Namespace elements in keys are joined with `.` in `encodedKey` and `namespace`.
