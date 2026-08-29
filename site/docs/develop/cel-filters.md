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

Variables on `entry`:

| Field | Type | Notes |
| --- | --- | --- |
| `namespace` | string | Parent namespace of the key |
| `contentType` | string | e.g. `ICEBERG_TABLE`, `ICEBERG_VIEW`, `NAMESPACE` |
| `key` | string | Full key |
| `name` | string | Last key element (table name) |
| `keyElements` / `namespaceElements` | list of strings | Split form of the key |

Examples:

```
entry.namespace.startsWith('a.b.c')
entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
entry.namespace.startsWith('some.name.space') && entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
```

## Commit log

`GET /api/v2/trees/{ref}/history`

Variables:

* `commit` with `author` (string), `committer` (string), `commitTime` (timestamp),
  `hash` (string), `message` (string), `properties` (map)
* `operations` (list). Each operation has `type` (`PUT` or `DELETE`), `key`,
  `keyElements`, `namespace`, `namespaceElements`, `name`

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

Variables:

* `ref` — `name`, `hash`, `metadata`
* `metadata` — shortcut to `ref.metadata` (never null, may be empty)
* `commit` — shortcut to `ref.metadata.commitMetaOfHEAD` (never null, may be empty)
* `refType` — `BRANCH` or `TAG`

`metadata` and `commit` are only useful when `fetch=ALL`.

Examples:

```
refType == 'BRANCH'
ref.name == 'my-tag-or-branch'
commit.message == 'invent awesome things'
```

## Diff

`GET /api/v2/trees/{from-ref}/diff/{to-ref}`

The expression is evaluated against the content key, not `entry`:

| Field | Type |
| --- | --- |
| `key` | string |
| `keyElements` | list of strings |
| `namespace` | string |
| `namespaceElements` | list of strings |
| `name` | string |

Example: `key.namespace=='foo'` or `key.name=='table'`.

## Tips

* Start from the OpenAPI examples on the `filter` parameter; those are the
  expressions the tests use.
* `startsWith` / `in` / `exists` are the usual operators. `==` is exact match.
* Namespace elements in keys are joined with `.` in the string form of `key`
  and `namespace`.
