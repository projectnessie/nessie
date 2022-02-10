# Nessie Spark SQL Extensions
Spark SQL extensions provide an easy way to execute common Nessie commands via SQL.

## How to use them


### Spark 3.1

In order to be able to use Nessie's custom Spark SQL extensions, one needs to configure
`org.projectnessie:nessie-spark-extensions:{{ versions.java }}` along with `org.apache.iceberg:iceberg-spark-runtime-{{ versions.iceberg_spark32 }}:{{ versions.iceberg }}`.
Here's an example of how this is done when starting the `spark-sql` shell:

```
bin/spark-sql 
  --packages "org.apache.iceberg:iceberg-spark3-runtime-{{ versions.iceberg }}:{{ versions.iceberg }},org.projectnessie:nessie-spark-extensions:{{ versions.java }}"
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
  --conf <other settings>
```

### Spark 3.2

In order to be able to use Nessie's custom Spark SQL extensions with Spark 3.2.x, one needs to configure
`org.projectnessie:nessie-spark-3.2-extensions:{{ versions.java }}` along with `org.apache.iceberg:iceberg-spark-runtime-{{ versions.iceberg_spark32 }}:{{ versions.iceberg }}`.
Here's an example of how this is done when starting the `spark-sql` shell:

```
bin/spark-sql 
  --packages "org.apache.iceberg:iceberg-spark-runtime-{{ versions.iceberg_spark32 }}:{{ versions.iceberg }},org.projectnessie:nessie-spark-3.2-extensions:{{ versions.java }}"
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSpark32SessionExtensions"
  --conf <other settings>
```

Additional configuration details can be found in the [Spark via Iceberg](iceberg/spark.md) docs.

## Grammar
The current grammar is shown below:
```
: CREATE (BRANCH|TAG) (IF NOT EXISTS)? reference=identifier (IN catalog=identifier)? (FROM fromRef=identifier)?
| DROP (BRANCH|TAG) identifier (IN catalog=identifier)?
| USE REFERENCE reference=identifier (AT tsOrHash=identifier)?  (IN catalog=identifier)?
| LIST REFERENCES (IN catalog=identifier)?
| SHOW REFERENCE (IN catalog=identifier)?
| MERGE BRANCH (reference=identifier)? (INTO toRef=identifier)?  (IN catalog=identifier)?
| SHOW LOG (reference=identifier)? (IN catalog=identifier)?
| ASSIGN (BRANCH|TAG) (reference=identifier)? (TO toRef=identifier)? (IN catalog=identifier)?
```

## Creating Branches/Tags

Creating a branch `dev` in the `nessie` catalog (in case it doesn't already exist):

* `CREATE BRANCH IF NOT EXISTS dev IN nessie`

Creating a tag `devTag` in the `nessie` catalog (in case it doesn't already exist):

* `CREATE TAG IF NOT EXISTS devTag IN nessie`

Creating a branch `dev` in the `nessie` catalog off of an existing branch/tag `base`:

* `CREATE BRANCH IF NOT EXISTS dev IN nessie FROM base`

Note that in case `base` doesn't exist, Nessie will fall back to the default branch (`main`).

## Dropping Branches/Tags

Dropping a branch `dev` in the `nessie` catalog (in case it exists):

* `DROP BRANCH IF EXISTS dev IN nessie`

Dropping a tag `devTag` in the `nessie` catalog (in case it exists):

* `DROP TAG IF EXISTS devTag IN nessie`

## Switching to a Branch/Tag

In order to switch to the HEAD of the branch/tag `ref` in the `nessie` catalog:

* `USE REFERENCE ref IN nessie`

It is also possible to switch to a specific timestamp on a given branch/tag:

* ``USE REFERENCE ref AT `2021-10-06T08:50:37.157602` IN nessie``

Additionally, one can switch to a specific hash on a given branch/tag:

* `USE REFERENCE ref AT dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9b IN nessie`

## Listing available Branches/Tags

One can list available branches/tags in the `nessie` catalog via:

* `LIST REFERENCES IN nessie`

## Showing details of the current Branch/Tag

One can see details about the current branch/tag in the `nessie` catalog via:

* `SHOW REFERENCE IN nessie`

## Showing the Commit Log of the current Branch/Tag

It is possible to look at the commit log of a particular branch/tag in the `nessie` catalog via:

* `SHOW LOG dev IN nessie`

## Assigning Branches/Tags

Assigning a branch `dev` to `base` in catalog `nessie` can be done via:

* `ASSIGN BRANCH dev TO base IN nessie`

Assigning a tag `devTag` to `base` in catalog `nessie` can be done via:

* `ASSIGN TAG devTag TO base IN nessie`

Note that in case `base` doesn't exist, Nessie will fall back to the default branch (`main`).

It is also possible to assign a branch/tag to a `base` at a particular `hash`:

* `ASSIGN TAG devTag TO base AT dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9b IN nessie`


## Merging a Branch into another Branch

Merging branch `dev` into `base` for the `nessie` catalog can be done via:

* `MERGE BRANCH dev INTO base IN nessie`

Note that in case `base` doesn't exist, Nessie will fall back to the default branch (`main`).
