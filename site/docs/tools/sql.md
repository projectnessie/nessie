# Nessie Spark SQL Extensions
Spark SQL extensions provide an easy way to execute common Nessie commands via SQL.

## How to use them

In order to be able to use Nessie's custom Spark SQL extensions, one needs to configure
`org.projectnessie:nessie-spark-extensions:{{ versions.java }}` along with `org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }}`.
Here's an example of how this is done when starting the `spark-sql` shell:

```
bin/spark-sql 
  --packages "org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }},org.projectnessie:nessie-spark-extensions:{{ versions.java }}"
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
  --conf <other settings>
```

Additional configuration details can be found in the [Spark via Iceberg](iceberg/spark.md) docs.

## Grammar
The current grammar is shown below:
```
: CREATE (BRANCH|TAG) (IF NOT EXISTS)? identifier (IN catalog=identifier)? (AS reference=identifier)?
| DROP (BRANCH|TAG) identifier (IN catalog=identifier)?
| USE REFERENCE identifier (AT ts=identifier)?  (IN catalog=identifier)?
| LIST REFERENCES (IN catalog=identifier)?
| SHOW REFERENCE (IN catalog=identifier)?
| MERGE BRANCH (identifier)? (INTO toRef=identifier)?  (IN catalog=identifier)?
| SHOW LOG (identifier)? (IN catalog=identifier)?
| ASSIGN (BRANCH|TAG) (identifier)? (AS toRef=identifier)? (IN catalog=identifier)?
```

## Creating Branches/Tags

Creating a branch `dev` in the `nessie` catalog (in case it doesn't already exist):

* `CREATE BRANCH IF NOT EXISTS dev IN nessie`

Creating a tag `devTag` in the `nessie` catalog (in case it doesn't already exist):

* `CREATE TAG IF NOT EXISTS devTag IN nessie`

Creating a branch `dev` in the `nessie` catalog off of an existing branch/tag `base`:

* `CREATE BRANCH IF NOT EXISTS dev IN nessie AS base`

Note that in case `base` doesn't exist, Nessie will fallback to the default branch (`main`).

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

* `ASSIGN BRANCH dev AS base IN nessie`

Assigning a tag `devTag` to `base` in catalog `nessie` can be done via:

* `ASSIGN TAG devTag AS base IN nessie`

Note that in case `base` doesn't exist, Nessie will fallback to the default branch (`main`).


## Merging a Branch into another Branch

Merging branch `dev` into `base` for the `nessie` catalog can be done via:

* `MERGE BRANCH dev INTO base IN nessie`

Note that in case `base` doesn't exist, Nessie will fallback to the default branch (`main`).
