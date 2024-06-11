---
date: 2024-06-05
authors:
  - snazy
---

# Nessie cache improvements

Caches are there to improve performance by holding the results of expensive operations and make those quickly available.
In this post we explain a recent improvement coming in the next Nessie release 0.83.0.
<!-- more -->

Nessie stores information in its backing data store in two tables: the `refs` ("references") table, which essentially
stores the current "tip" of branches and tags, and the `objs` ("objects") table, which stores all commits and content
objects. Objects in Nessie are (usually, see below) immutable. Immutable objects are great candidates to be cached - so
we did exactly that: cache the contents of the `objs` table. With "just" Caching for the `objs` table we see very few
reads against that table in production systems.

However, nothing is cached for the references (`refs`) table. This means that basically all Nessie API calls hit the
database, no matter whether it is "just" a read operation like "get-the-tip-of-a-branch" or a commit operation. Given
that most operations against a lakehouse are reads, we could save a lot of read operations if there's a way to cache
references.

The [PR to cache references](https://github.com/projectnessie/nessie/pull/8111) introduced the capability to cache
references, but only in the "local" Nessie instance. A commit is immediately visible as a change to the "tip"/HEAD
of the branch on the Nessie instance that performed the commit - but another Nessie instance serving the same Nessie
repository would not know that the reference has changed and "happily" serve and work with the stale cache information.
This was a known (and for development/"cognitive review burden" reasons accepted) limitation for that PR, and the most
important reason why the functionality to cache references is labeled "experimental" and is disabled by default. It
would be fine to enable it, but only if **only** a single Nessie instance accesses the repository.

[Distributed cache invalidation](https://github.com/projectnessie/nessie/pull/8463) introduced the ability for Nessie
instances to tell other Nessie instances to invalidate certain cached objects, so that the cache stays consistent. A
cache invalidation message instructs a Nessie instance to remove certain key(s) from its cache.

Cache invalidation messages are rather "management" concerns and the endpoints, even if protected by any mechanism,
should not be publicly exposed. Thankfully Quarkus recently added the ability to [separate management endpoints from
public ones](https://quarkus.io/guides/management-interface-reference), Nessie incorporated this in the 0.82.0 release -
management endpoints are available on port 9000 (default) and the Nessie REST APIs stay on the known port 19120. In
other words, management endpoints are not publicly available, which is what we want.

The Nessie endpoint to receive cache invalidation messages is only available on the management port. If you deploy
Nessie via the [Helm chart](../../guides/kubernetes.md), or via our
[Kubernetes operator](https://github.com/projectnessie/nessie/pull/7967) (not released yet), you don't need any
additional configuration for distributed cache invalidations - it's setup and configured automatically. If you have
your own Helm chart or custom deployment, make sure to configure the IPs of all Nessie instances via
`nessie.version.store.persist.cache-invalidations.service-names`.

Reference caching itself is _not_ enabled by default. You can give it a try by setting these configuration options:

* `nessie.version.store.persist.reference-cache-ttl` defines the time how long a reference will be kept in the cache.
  A probably good value for this setting is a couple minutes, for example `PT15M`. If this parameter is not configured,
  reference caching is disabled (default for now).
* `nessie.version.store.persist.reference-cache-negative-ttl` defines the time how long the fact that a reference
  does _not exist_ will be kept in the cache. If you enable reference caching, it is _strongly_ recommended to
  configure this value as well. Nessie uses Git-like names for tags (`refs/tags/<tag-name>`) and branches
  (`refs/heads/<branch-name>`), but the Nessie APIs use "simplified"/short "human friendly" names - so just the
  branch/tag name without the `refs/tags/`/`refs/heads` prefixes. To look up a reference by its name, Nessie performs
  _two_ read operations - one against each of the branch/tag prefixes - one exists and the other doesn't - therefore
  we need to store "negative cache sentinels" as well.

PS: Kudos to Ben Manes for his [awesome Window-Tiny-LFU implementation Caffeine](https://github.com/ben-manes/caffeine)
and to the [Quarkus project](https://quarkus.io/) for their responsiveness!
