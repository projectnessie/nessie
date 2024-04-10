---
date: 2022-05-31
authors:
  - snazy
---

# Rolling upgrade issue to 0.26.0

## Symptom

During or after a rolling upgrade from Nessie version <= 0.25.0 to >= 0.26.0, exceptions/errors like
`org.projectnessie.versioned.ReferenceNotFoundException: Global log entry '<hex>’ not does not exist.`
and/or `Iceberg content from reference must have global state, but has none` may occur.
<!-- more -->

## Background

When Nessie runs against non-transactional databases, it uses a [“global pointer”](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/serialize-proto/src/main/proto/persist.proto#L105),
which holds the mapping of all named references to their HEAD commit IDs, the HEAD of the ref-log
and the HEAD of the global-log. Every update to the Nessie repository ends in a CAS[^1] on that
single global pointer. If the CAS is successful, the change, for example a commit or merge
operation, was atomically & consistently applied.[^2]

The approach to maintain these three HEADs in a single "row" works, but it does not scale well.
This “single point of contention” was never meant to stay forever, just as long as we need it
and/or do not have a better solution for it.

We implemented Nessie using this concrete global pointer mechanism, because certain decisions
haven’t been made at that time, and we wanted to be on the “safe side” and then see what can be
improved.

## Analysis

Recently we were certain that having the so-called “global state” for Iceberg tables and views is
actually not such a great thing. So the team decided that the “global state” can go away. This was
implemented in the Nessie PR #3866. Since that change reduced the amount of global-log-entries to
nearly 0, we could also get rid of the fact that every single change to the Nessie repository, even
creating a branch or tag, creates a potentially empty global-log-entry. Not writing unnecessary
global-log-entries was implemented in the Nessie PR #3909. Both PRs, 3866 and 3909, were released
together as Nessie 0.26.0. All Nessie tests were passing and nobody realised that a little devil
sneaked into these code changes, waiting to be woken up in production.

The situation that the global-pointer contains a “broken” list of global-log parent IDs is
definitely confusing and cannot be explained by only looking at the code base of the target 0.26.0
release. It does not even help to only look at the code base of the source 0.25.0 release. Both
code bases are completely fine, when only considering those in isolation.

The “fun part” happens, when both versions are active at the same time and requests against the same
Nessie repository are served by both versions.

## Involved parts in the code base

The [`GlobalStatePointer`](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/serialize-proto/src/main/proto/persist.proto#L105)
before PR #3909 has a single field called `global_id`, which served two purposes.
First, it served as the “condition field” for the CAS[^1] operation. The same field
`global_id` also pointed to the HEAD of the global-log. PR #3909 changed this. The `global_id`
field only serves as the “condition field” for the CAS operation, the HEAD of the global log is
held in `global_log_head`. Since `global_id` is no longer related to the HEAD of the global log,
its value is a
[random value](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java#L649).

As you may already guess, if a Nessie version before PR #3909 performs an update, it
[interprets the value of `global_id` as the HEAD of the global log](https://github.com/projectnessie/nessie/blob/nessie-0.25.0/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java#L915).
The linked part of the code then evaluates
[this if-condition](https://github.com/projectnessie/nessie/blob/nessie-0.25.0/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java#L929)
to true, which is some other backwards compatibility code, and fills the list of
global-log parents [only](https://github.com/projectnessie/nessie/blob/nessie-0.25.0/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java#L932-L934)
with the value of `global_id`, because `currentEntry` is always `null`, because
`global_id` does not point to a global log entry.

Later, the function
[updateGlobalStatePointer populates the list of global-log-entries](https://github.com/projectnessie/nessie/blob/nessie-0.25.0/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java#L669-L670)
with the ID of the new global-log-entry and the collected parents, which is just that random
global-id. So the list of global-log IDs in the global-pointer contains two entries - one that
points to a “valid” global log entry and one that does not seem to exist. This is exactly what has
been [seen](#identification-of-the-issue-and-mitigation-in-the-live-system).

## Identification of the issue and mitigation in the live system

Whether the symptom is actually caused by Nessie global pointer corruption can be validated by 
accessing the Nessie storage data directly.

Some tooling is required for this because Nessie stores its data as binary blobs. 
The `servers/quarkus-cli` module is to be enhanced (in a follow-up PR) with additional commands so that
these operations could be performed without additional coding work.

Meanwhile, here's the outline of how to confirm and fix the problem in a live system.

How to confirm the symptom:

1. Fetch the Nessie Global Pointer
2. For each [global_log_head](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/serialize-proto/src/main/proto/persist.proto#L116)

   - Parse it as a Nessie hash
   - Check whether there's an entry in the Global Log table keyed by this hash

3. If at least one of the parent hashes does not have a corresponding global log entry, that will mean that 
   the Global Pointer data has been corrupted

How to fix the problem:

1. Do a full scan of the Global Log table
2. Find the last good Global Log entry

   - Normally, if Nessie has substantial history good global log entries will have 20 parents 
     (or whatever was configured)
   - Use log entry timestamps and common sense to identify the last good entry

3. Check all global log entries referred to from the Global Pointer directly

   - Check whether they have any ["puts"](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/serialize-proto/src/main/proto/persist.proto#L78),
      i.e. contain Iceberg metadata information

4. If those entries have "puts" construct a new entry that collectively contains their "put" data
   and refers to the last good parent as its parent. Now this new entry becomes the last good log entry.
5. If the entries from step 4 do not have "puts" they can be ignored.
6. Construct a new Global Pointer using its all of its current data, but put the hash of the last
   good global log entry as the only element in the  [global_log_head](https://github.com/projectnessie/nessie/blob/nessie-0.26.0/versioned/persist/serialize-proto/src/main/proto/persist.proto#L116) list.
7. Store the new Global Pointer overwriting the old (broken) Global Pointer data.
8. Re-run the verification procedure (above) to validate the new Global Pointer and Global Log.

## Additional testing effort

Nessie already had a bunch of tests regarding compatibility and upgrades. There are tests exercising
older Nessie API versions against current in-tree Nessie server, current in-tree Nessie API against
older Nessie server versions, and tests exercising single-instance upgrade paths.

Sadly, there were no tests that exercised rolling upgrade scenarios, especially none that exercised
the case that hit both the old and new versions for multiple requests. For example, create a branch
against the server running the “old” Nessie version, then a commit to that branch against the server
running the “new” Nessie version, and other situations.

Today, Nessie has a test suite to validate rolling upgrades, implemented via Nessie PR #4350. As all
compatibility tests, the new rolling upgrade tests are now part of the normal CI workflows for all
PRs and the main branch.

Upgrade paths are now documented on projectnessie.org here [Nessie Server upgrade notes](../../server-upgrade.md)
(via PR #4364 + issue #4348).

## Big changes in upcoming releases

Heads up: there will be more big changes coming in the next releases, that are already known to be
not safe for a rolling upgrade. The Nessie PR #4234 eliminates the remaining contention issues in
the global pointer. Because it does fundamentally change how named references and the ref-log are
maintained, a rolling upgrade from Nessie <= 0.30.0 would definitely cause issues and is therefore
not supported.

## Future releases

The actual problem at play was not really the fact that upgrading Nessie <= 0.25.0 to 
Nessie >= 0.26.0 can cause global-pointer corruption, which is bad, no question. The actual issue
is that this fact was not noticed earlier.

Learnings from this escalation:

* Implement regularly run rolling-upgrade tests in CI (#4350)
* Clearly document which versions do [support rolling upgrades](../../server-upgrade.md) and which
  combinations do not work

[^1]: CAS means "compare and swap". See [Wikipedia article](https://en.wikipedia.org/wiki/Compare-and-swap)
[^2]: If the CAS operation was not successful, Nessie will retry using a exponential backoff,
  configured [here](../../nessie-latest/configuration.md#version-store-advanced-settings).
