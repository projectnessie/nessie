# Transactions

Nessie extends existing table formats to provide a single serial view of transaction 
history. This is enabled across an unlimited number of tables. A user can view a commit log either 
through the UI or by using the Nessie CLI. Operations against each table are listed along 
with timestamp, user and helpful information about the operation. 

## Cross-Table Transactions

Nessie is the first technology to provide an open facility for cross-table transactions 
within your data lake. There are two ways that this can be accomplished:

* Via Branches: Because Nessie allows branches to be created and then reassigned, a 
  sequence of commits on one branch can be exposed as a single consistent view to other 
  users through the use of a merge operation. This allows use of systems that don't have internal 
  cross-table transaction support to still control the visibility of connected changes. 
* Single Commits: Nessie allows a single commit operation to include many object changes 
  simultaneously. While Nessie operations internally use this capability, tools will need to be enhanced to take advantage of this powerful 
  new capability.

## BEGIN TRANSACTION.. COMMIT

The Nessie community is working with tool developers to introduce traditional data 
warehouse cross-table transactions. Nessie's catalog implementations already support 
the underlying capability of multi-table transactions.

## Isolation Levels

Nessie exposes APIs to support three-levels of isolation: Read Committed, 
Repeated Read and Serialized. By supporting the recording of reads as part of a commit (via the Unchanged 
operation), tools can introduce full per operation serialized isolation. This is a 
transaction mode that has been traditionally limited to OLTP systems and unavailable 
to OLAP systems and data warehouses[^1]. 


### Read Committed (Optimistic)

|||
|-|-|
|Read|Each time metadata for a table is retrieved, the latest version of that ref for the that current branch is exposed.|
|Ownership|A transaction only needs to be created on the client. There is not concept of a long-lived transaction.|
|Write|Safe writes are allowed.|
|How|Client goes to server to retrieve latest version of data for each operation|


### Repeated Read (Optimistic)

|           |                                                              |
| :-------- | :----------------------------------------------------------- |
| Read      | When a transaction is started, a ref is turned into a specific commit id. All metadata retrieved is locked to this hash or later, as long as future hashes have not changed any table already read. |
| Ownership | A transaction only needs to be created on the client. There is no concept of a long-lived transaction on the server. |
| Write     | Safe writes are allowed. Unsafe writes fail.                 |
| How       | Client resolves commit hash on first read and uses that for all subsequent operations. |

Note: this is stricter than the formal definition of repeatable read since that will allow new records to also be viewed on a second operation within the same transaction. However, both implementations are of similar complexity and a stricter form of repeated read seems easier to understand.

### Serializable (Optimistic)

|||
|-|-|
|Read|When a transaction is started, a ref is turned into a specific commit id. During the transaction, a recording of all **read** tables is recorded.|
|Ownership|A transaction only needs to be created on the client. There is no concept of a long-lived transaction on the server.|
|Write|All tables touched as part of the read operations must be in the same state when the commit operation is attempted. If they are not, the write operation is rejected. This is done internally via the Unchanged operation.|
|How|Client resolves commit hash on first read and uses that for all subsequent operations.|

Serializable transactions allow one to do guaranteed exactly once operations. For example- 
move all this data from table1 to table2. At the end of this operation there is a guarantee 
that any operations done against table1 will either show up in table2 or fail to apply 
to table1 (true before & after).

### Pessimistic Locking

Currently, all three isolation models are supported only via optimistic 
locking. In the future, it is likely that we will also add support for pessimistic 
locking. To support pessimistic locking, transaction state must be held by the Nessie 
service as opposed to Nessie clients requiring a more formal protocol around start 
transaction, commit transaction with relevant timeouts, deadlock detection and 
cancellation capabilities.

### Lock Coarseness and Resolution

Nessie maintains state and locks at table granularity. If a conflict is found at the 
table level, Nessie will either reject the operation or delegate the operation to the 
underlying table format to see if further conflict resolution can occur.  
format to provide additional 

[^1]: Delta Lake does support Serializable isolation against a [single table](https://docs.databricks.com/delta/optimizations/isolation-level.html) 
but does not currently support it against multiple tables.