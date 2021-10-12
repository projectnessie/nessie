# Authorization

Nessie allows performing authorization on branches/tags and contents where rule definitions are using a Common Expression Language (CEL) expression (an intro to CEL can be found at [CEL Specs](https://github.com/google/cel-spec/blob/master/doc/intro.md)).
Rule definitions are of the form `nessie.server.authorization.rules.<ruleId>=<rule_expression>`. Available variables within the `<rule_expression>` are: `op` / `role` / `ref` / `path`.

* `op`:  This variable in the `<rule_expression>` can be any of: `VIEW_REFERENCE`, `CREATE_REFERENCE`, `DELETE_REFERENCE`, `READ_ENTRIES`, `LIST_COMMIT_LOG`, `COMMIT_CHANGE_AGAINST_REFERENCE`, `ASSIGN_REFERENCE_TO_HASH`, `UPDATE_ENTITY`, `READ_ENTITY_VALUE`, `DELETE_ENTITY`.
* `role`: This variable refers to the user's role and can be any string.
* `ref`: This variable refers to a string representing a branch/tag name.
* `path`: This variable refers to the Key for the contents of an object and can be any string.

To enable authorization, the following [configuration](./configuration.md) properties need to be set
for the Nessie Server process:

* `nessie.server.authorization.enabled=true`  

## Examples

Some "use-case-based" example rules are shown below (in practice you might rather create a single rule that allows e.g. branch creation/deletion/commits/...):

```
nessie.server.authorization.rules.allow_branch_listing=\
   op=='VIEW_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_branch_creation=\
   op=='CREATE_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_branch_deletion=\
   op=='DELETE_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_listing_commitlog=\
   op=='LIST_COMMIT_LOG' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_entries_reading=\
   op=='READ_ENTRIES' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_assigning_ref_to_hash=\
   op=='ASSIGN_REFERENCE_TO_HASH' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_commits=\
   op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role.startsWith('test_user') && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_reading_entity_value=\
   op=='READ_ENTITY_VALUE' && role=='test_user' && path.startsWith('allowed.')
   
nessie.server.authorization.rules.allow_updating_entity=\
   op=='UPDATE_ENTITY' && role=='test_user' && path.startsWith('allowed.')
   
nessie.server.authorization.rules.allow_deleting_entity=\
   op=='DELETE_ENTITY' && role=='test_user' && path.startsWith('allowed.')
   
nessie.server.authorization.rules.allow_commits_without_entity_changes=\
   op=='COMMIT_CHANGE_AGAINST_REFERENCE' && role=='test_user2' && ref.startsWith('allowedBranch')
   
nessie.server.authorization.rules.allow_all=\
   op in ['VIEW_REFERENCE','CREATE_REFERENCE','DELETE_REFERENCE','LIST_COMMITLOG','READ_ENTRIES','LIST_COMMIT_LOG',\
   'COMMIT_CHANGE_AGAINST_REFERENCE','ASSIGN_REFERENCE_TO_HASH','UPDATE_ENTITY','READ_ENTITY_VALUE','DELETE_ENTITY'] \
   && role=='admin_user'   
```
