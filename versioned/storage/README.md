# Nessie's new storage

Nessie's old ("database adapter") storage model was originally developed with the later removed
ability to track some so called "global state", which played a central role in the design. But
on top of that, it has a few "inconveniences" around flexibility for future enhancements but also
big disadvantages around how keys and content is managed. The "old" model is practically hard to
use with many content keys. It was also practically not doable to implement stricter content
validations during commits.

This "new" storage model is on the one hand much simpler, but on the other hand way more flexible
_and_ more efficient, achieved by the following key design choices:

* Named references can be anything, not just branches and tags.
* Generic objects model, serving commits, spilled out index segments, content values and more.
* Abstracted, space efficient and sorted index structures.

## Old database adapter model deprecation + Upgrading to the new model

With the merge of this new storage model, the old "database adapter" model will be deprecated and
_scheduled_ for removal. Nessie will support both the old and new model for a while to allow
seamless migration.

To upgrade a Nessie repo, use the export + import functionality:

1. Stop the Nessie server(s)
2. Export the Nessie repository (it is safe to use the latest Nessie version with support for
   "database adapters").
3. Change the settings in your `application.properties` to use the new version store types.
4. Import the Nessie repository (using a _new_ version store types).
5. Start the Nessie servers (using a _new_ version store types).

## More design choices

Some more in-depth design choices:

* Each object has a unique ID, which is _always_ derived from the hash over the object's content.
  This makes objects in the database "immutable", which allows caching objects without having to
  implement distributed cache coherency (so not opening that can of worms).
* Each object has a type (attribute).
* Object attributes are stored using native database types/structures/columns, where possible and
  feasible.
* Space efficient and sorted indexes that provide point and range lookups. Abstractions of the
  index contract provide implementation for a striped index (serving the case when a single segment
  becomes "too big"), layered index (serving the use case to have a "smaller" incremental index
  in every commit and a potentially striped reference index in other objects) and of course the
  index segment itself. Each index element can be looked up using its "store key" and contains a
  tuple of payload, object ID of the content and the content ID (if it is strictly a UUID).
* Named references point to an object, which does not necessarily need to be a commit, but any type
  that is either a commit or a pointer to a commit. This allows adding attributes, a message and a
  cryptographic signature to tags.
* Some named references are internal references, which also use the same commit logic as "normal"
  commits to describe the repository and to implement a fail-safe mechanism to create and drop
  named references, including a transparent recovery mechanism, and the required index structure to
  list named references or search for named references. The recovery mechanism is required for
  non-transactional databases. Internal references including their commits must never be exposed
  to users, internal references are not included in the index of named references and their commits
  are marked as internal (but that's the only difference to "normal" commits).
* Since key lookups are rather cheap (especially compared to the "database adapter" model), it is
  also cheap to perform much stricter validations during commits.
* All operations that provide an iterator are by design pageable.

## Performance

The new storage model is more efficient and as a result much faster and less prone to Nessie commit
retry timeouts. There are a bunch of characteristics that make it more efficient:

* Idempotent object IDs & immutable objects: since objects are idempotent and immutable by design,
  the object's ID can be safely derived from the object's content. The same object will always get
  the same object ID. This enables the implementation of an object cache, which eliminates (recent)
  database accesses by removing the need to fetch the object again. This also enables the ability
  to write the same object more than once, which is happens when a commit is being retried, think:
  content value objects (the so-called "put operations" of a commit that add new content).
* (Mostly) constant low access times when accessing content keys and content values are achieved by
  having the "commit's view" of the keys and content objects directly "in" the commit, with its
  incremental and (if necessary) reference index structures.

### Experiment with asymmetric operation counts

Imagine two processes that constantly commit to the same branch. One process produces one
put-operation per commit, the other produces ten put-operations per commit. Since both processes
commit to the same branch, (retryable) conflicts happen a lot. The problem is that each
put-operation requires some fixed amount of time - validating and handling the put-operations, and
eventually storing the contents of those. This means, that commits with 10 put-operations take
longer than commits with just one put-operation.

If both mentioned processes commit "as fast as possible", only the process committing just one
put-operation will make progress, the process committing 10 put-operations will run into a
"commit timeout" for every commit it attempts. The reason is: time.

With the new storage model, both processes of the above scenario are able to make progress.
However, the commits with 10 put-operations run into commit retries. But the expensive pieces
have been mostly eliminated by the objects cache and the fact that already stored objects are not
stored again.

The problem is not 100% solvable though, at least not without introducing a more coordinated
"commit-target-lease mechanism". But the "barrier" at which the problem happens has been moved way
ahead, mostly "out of sight".

### Coordinated commit targets

Nessie (server) is and must stay stateless, which means that Nessie servers do not communicate
with each other. There is no form of "distributed consensus" or the like, which would be expensive.

But it would be possible to let the logic that handles commits against branches make sure that only
one commit against each branch happens concurrently, on each Nessie server individually of course.
To make the in-server coordination work properly, the network infrastructure that distributes
requests against Nessie servers would need to be smarter by routing commits against the same branch
to the same server - as a best-effort approach of course.

## Indexes

Every commits contains a so-called "incremental index". This "incremental index" contains the
differences to the "reference index". Each element in an index has an associated "action", which
can be one of these:

* `ADD` saying that the element has been added by the current commit
* `REMOVE` saying that the element has been removed by the current commit
* `INCREMENTAL_ADD` saying that the element has been added by a previous commit
* `INCREMENTAL_REMOVE` saying that the element has been removed by a previous commit
* `NONE` (see below)

All operations are accumulated in the incremental index, which is included in every commit. If the
incremental index becomes "too big" (size is configurable), the incremental operations mentioned
above are spilled out to a reference index. Reference indexes contain only "existing" keys and
always use the "action" `NONE` (see above).

When a key is looked up, the incremental index is consulted first. If the key exists in the
incremental index, it's value is used. If the key is not contained in the incremental index, the
reference index is consulted. (Range) scanning operations work similar, just require a bit more
logic to merge iterators over two indexes. `INCREMENTAL_REMOVE` actions are used to indicate that
a previous commit removed the key, which is (still) present in the reference index.

Since even a reference index can become "too big" (size is configurable), a reference index can
be striped across multiple segments.

## Stricter validations

The version store and especially the new commit logic implementations perform much stricter checks
compared to the old "database adapter" model. The validations are:

* New content: passing a content-ID for new content is not allowed.
* New content: passing an already existing key using a "Put operation" representing new content
  is not allowed.
* Existing content: the "Put operation" must include the "expectedContent" value.
* Existing content: new value's content-ID must be the same as the existing content's ID.
* Existing content: new expectedValue's content-ID must be the same as the existing content's ID.
* Existing content: the expectedValue must match the existing content.
* Existing content: changing the payload is not allowed.
* Existing content: changing the content ID is not allowed.

Implementing stricter checks for "Delete" and "Unchanged" operations is not yet possible due to
model/API missing the "expectedValue" parameter.

Checks against the expected/existing content only need to lookup the _pointer_ (object ID) of the
content's value, means only an index lookup is required. Neither will the commit log be scanned nor
content values being fetched, which makes commit operations much more efficient.

(Note: if the content-ID of an "old" Nessie repository does _not_ represent a UUID, a content fetch
operation _is_ required, because the index structure only supports 2x64-bit content-IDs.)

## (Squashing) merge and transplant

A squashing merge (or a squashing transplant) is implemented by first producing the diff of the
latest to the parent of the first commit to be merged or tranplanted. The diff is then used to
produce the squashed commit.

Non-squashing merge and transplant operations work similarly, but basically replay the source commit
operations on top of the target commit.

For a squash operation it would be in theory enough to read the latest and parent of the oldest
commit to produce the diff and continue from there. The current commit message rewrite however
requires reading all commits in between as well. Due to technical reasons, that happens in addition
to previous validations and identification of the common ancestor. It would not be "nice" (potential
huge heap pressure) to memoize all commits "seen" during these validations and common ancestor
identification.

## Diff operation improvements

Producing the diff between two arbitrary commits only involves reading the commits and scanning
their indexes.

## Pageable operations

All pageable operations:

* Keys and/or values that exist on a commit, including key start and end restrictions
* Diff between two commits, including key start and end restrictions
* Listing references, including key start and end restrictions
* Commit ID log
* Commit log

## Importing a repository

Imports (via the Nessie Quarkus CLI) is supported from exports from the "old" ("database adapter")
and "new" model. Exports from the new model cannot be imported into the "old" model. The old model
uses `V1` export data, the new model writes `V2` export data, but can also read `V1`.

Imports for `V2` are more expensive by design of the new model, which _strictly requires_ consistent
indexes on every commit object. There were two options to satisfy the requirement: add code to
handle "incomplete commits" or have logic to create the mandatory index structures for every commit.
The first option (adding code) complicates the code base a lot and ensuring correctness for all
possible code paths is very hard. The second option (adding the indexes after the import) is what
has been implemented. It takes more time, but keeps the (production) code base clean.

The code to create the missing index structures is the only code that is allowed to actually
_update_ a (commit) object in the database.

Note: commits without the mandatory index information are marked as "incomplete". Accessing these
commits from a production code path results in a hard error.

## Misc

The API/SPI of the new storage is divided into a low-level `Persist` interface, which provides the
primitives to manage the rows in the "named references table" and the rows in the "objects table",
plus a few specialized operations to iterate over all objects and erase the repository - both are
inefficient, because those _will_ require full table scan (and delete) operations.

A key differentiator to the "database adapter" model is that all `Persist` implementation _must_
perform CAS(-like) operations. This means, that it is not allowed to store an object with the same
ID twice, reference creations and updates must be conditional, too.

The number of configuration options has been drastically reduced (compare `StoreConfig` with
`DatabaseAdapterConfig` + its extensions).

## Serialized index structure

Indexes maintain the exact (binary) order of the keys. This allows saving space in the serialized
representation. Only the difference to the previously serialized key is serialized, for example
if the two keys "abc.def.ghi" and "abc.def.jkl" are serialized, the second key is serialized as
a tuple of characters to remove from the previous key (3 for "ghi") plus the keys that need to be
appended ("jkl").

Beside the above "diff-ish" key encoding, indexes use variable length integers to save space for
small integer values, almost always serializing a single byte for all 32- or 64-bit integer values.

## Key case sensitivity

Making the key index case-insensitive would be possible by converting all keys to either lower or
upper case. But that would lose the original case sensitivity, and it would make Nessie incompatible
with catalogs that rely on case-sensitive keys.

Making keys case-insensitive while preserving the original key value does not work with the store
index structure.

## TODO / Later

* DynamoDB: Handle `ProvisionedThroughputExceededException` + `RequestLimitExceededException`
  via/for `CommitRetry`?
* PostgreSQL/CockroachDB: Handle `PostgreSQLDatabaseSpecific.isRetryTransaction`
  via/for `CommitRetry`?

* CommitLogic: have some logic here that detects "too tiny" reference index segments to combine
  segments.


## (Possibly opinionated) performance test results

A bunch of index space efficiency, index performance (heap + CPU) as well as some local (not so
really) load tests have been performed and the results are very promising.

Microbenchmarks have been added to the code base to illustrate the space, CPU and heap efficiency
of serializations, deserializations and lookups.

Local setups using one and multiple Nessie Quarkus servers accessing the same database, loaded by
multiple instances of the Nessie "content generator" tool. Subjective results from these "load
tests" are as follows. Note that all database servers were running in a Docker container and none
of the databases has been "tuned" in any way (yes, file I/O performance in a "plain" container is
not really great for a database) - it was _not_ the goal to get "proper" performance test results
but to get an _impression_ of the relative performance. And also: yes, none of the databases was
run in a "real" cluster setup.

* RocksDB is, obviously, the database that offers the best performance - in terms of request
  duration, leading to a quite high commit throughput. But, yes, it's a single-server local
  database - backups et al require downtime.
* MongoDB - works fine - probably a good choice
* DynamoDB - not tested yet (don't wanna test against the `amazon/dynamodb-local` Docker image,
  because it is not the real DynamoDB...)
* PostgreSQL works fine, but is definitely not the right choice for higher throughput requirements.
  CockroachDB wasn't tested, but is likely also not a good choice for "larger scale" Nessie.
  However, PostgrsSQL + CockroachDB are definitely valid choices for up to a couple of Nessie
  commits per second.

Obvious results:

* Commit performance does not degrade with the number of commits.
* Commit performance is negligibly affected by a "huge" amount of keys (30k and more) visible on
  commits.
* Nessie web UI reacts instantaneous - including namespace navigation et al
* Reading API functionalities (via Swagger UI and curl) react instantaneous
* Commits happen quickly, no commit timeouts

Note: The above represents the databases for which `Persist` implementations exist. If you miss any
database you'd like to see supported, fire up your IDE, add it and open a PR!

## Databases test setup

### RocksDB

```bash
HTTP_ACCESS_LOG_LEVEL=ERROR java -Xms2g -Xmx2G \
  -Dquarkus.http.port=19121 \
  -Dnessie.server.send-stacktrace-to-client=true \
  -Dnessie.version.store.type=ROCKSDB \
  -Dnessie.version.store.persist.rocks.database-path=$HOME/tmp/nessie-rocks \
  -Dnessie.version.store.persist.cache-capacity-mb=1024 \
  -Dnessie.version.store.persist.commit-timeout-millis=10000 \
  -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
```

### Postgres

```bash

docker run -ti --rm -p 5432:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_DB=my_database postgres:latest

HTTP_ACCESS_LOG_LEVEL=ERROR java -Xms2g -Xmx2G \
  -Dquarkus.http.port=19121 \
  -Dnessie.server.send-stacktrace-to-client=true \
  -Dnessie.version.store.type=JDBC \
  -Dnessie.version.store.persist.cache-capacity-mb=1024 \
  -Dnessie.version.store.persist.commit-timeout-millis=10000 \
  -Dquarkus.datasource.username=postgres \
  -Dquarkus.datasource.password=test \
  -Dquarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/my_database \
  -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
```

### CockroachDB

```bash

docker run -ti --rm -p 26257:26257 --name roach1 -p 8080:8080 cockroachdb/cockroach:latest start --insecure --join=localhost
docker exec -it roach1 ./cockroach init --insecure

HTTP_ACCESS_LOG_LEVEL=ERROR java -Xms2g -Xmx2G \
  -Dquarkus.http.port=19121 \
  -Dnessie.server.send-stacktrace-to-client=true \
  -Dnessie.version.store.type=JDBC \
  -Dnessie.version.store.persist.cache-capacity-mb=1024 \
  -Dnessie.version.store.persist.commit-timeout-millis=10000 \
  -Dquarkus.datasource.username=root \
  -Dquarkus.datasource.password=root \
  -Dquarkus.datasource.jdbc.url=jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable \
  -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
```

### Mongo

```bash
docker run -ti --rm -p 27017:27017 mongo:latest

HTTP_ACCESS_LOG_LEVEL=ERROR java -Xms2g -Xmx2G \
  -Dquarkus.http.port=19121 \
  -Dnessie.server.send-stacktrace-to-client=true \
  -Dnessie.version.store.type=MONGODB \
  -Dnessie.version.store.persist.cache-capacity-mb=1024 \
  -Dnessie.version.store.persist.commit-timeout-millis=10000 \
  -Dquarkus.mongodb.connection-string=mongodb://localhost:27017 \
  -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
```

### Cassandra

```bash
docker run -ti --rm --name cassandra -p 9042:9042 \
  -e CASSANDRA_SNITCH=GossipingPropertyFileSnitch \
  -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
  -e CASSANDRA_DC=datacenter1 \
  -e JVM_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0" \
  cassandra:latest
docker exec -it cassandra cqlsh
# --> TYPE:
#    create keyspace nessie with replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

HTTP_ACCESS_LOG_LEVEL=ERROR java -Xms2g -Xmx2G \
  -Dquarkus.http.port=19121 \
  -Dnessie.server.send-stacktrace-to-client=true \
  -Dnessie.version.store.type=CASSANDRA \
  -Dnessie.version.store.persist.cache-capacity-mb=1024 \
  -Dnessie.version.store.persist.commit-timeout-millis=10000 \
  -Dquarkus.cassandra.contact-points=127.0.0.1:9042 \
  -Dquarkus.cassandra.local-datacenter=datacenter1 \
  -Dquarkus.cassandra.keyspace=nessie \
  -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
```

### Content Generator

```bash
java -jar tools/content-generator/build/libs/nessie-content-generator-0.44.1-SNAPSHOT.jar generate \
  -u http://localhost:19121/api/v1 \
  -t 5000 -n 1000000
```

### Content Generator / quite random & long keys

```bash
java -jar tools/content-generator/build/libs/nessie-content-generator-0.44.1-SNAPSHOT.jar generate \
  -u http://localhost:19121/api/v1 \
  -t 300000 -n 200000 \
  --puts-per-commit 10 \
  --key-pattern 'stuff-folders.stuff-${every,150,uuid}.foolish-key_${every,20,uuid}.${uuid}_0'
```

### Utilities

```bash
curl  -X 'GET' 'http://127.0.0.1:19121/api/v1/trees/tree/main?fetch=ALL' \
  -H 'accept: application/json' 2>/dev/null | jq '.metadata.numTotalCommits'

nessie --endpoint http://localhost:19121/api/v1 content list| wc -l
```

#### Tracing

```
docker run -ti --rm\
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411\
  -e COLLECTOR_OTLP_ENABLED=true\
  -p 6831:6831/udp\
  -p 6832:6832/udp\
  -p 5778:5778\
  -p 16686:16686\
  -p 4317:4317\
  -p 4318:4318\
  -p 14250:14250\
  -p 14268:14268\
  -p 14269:14269\
  -p 9411:9411\
  jaegertracing/all-in-one:latest
```

* Open browser at URL http://127.0.0.1:16686/
* Run Quarkus with these options:
  ```
  -Dquarkus.otel.exporter.otlp.endpoint=http://localhost:4317 \
  -Dquarkus.otel.traces.sampler=always_on
  ```

