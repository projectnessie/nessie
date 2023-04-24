# Nessie Specification

Nessie Specifications define the behaviour of Nessie Servers. The same behaviour can be expected to be observable
via all supported API (unless noted otherwise).

[Semver](https://semver.org/spec/v2.0.0.html) principles are followed in bumping the Nessie spec version.
For example, a major behaviour change will result in incrementing the major version number.

The patch version number will be incremented only on minute spec corrections that do not significantly alter the 
server's behaviour.

If a server returns a specific spec version via `NessieConfiguration.getSpecVersion()` it MUST comply with the
specified behaviour (see sections below).

Servers SHOULD return the spec version that defines its behaviour most completely.

A server SHOULD return `null` from `NessieConfiguration.getSpecVersion()`, when it behaves in a way that is not
compatible with any known spec version.

Servers MAY show behaviours that are not covered by the spec, as long as those behaviours do not contradict the
well-specified behaviours.

Refer to the Nessie API documentation (under the `model` module) for the meaning of Nessie-specific terms.

# 2.0.0-beta.1

* Namespaces are expected to be created before they are referenced. Namespaces may be referenced withing the same 
  commit that creates them. 
* If a non-existent Namespace is referenced in a commit. The server should fail the corresponding change (commit 
  / merge / transplant) with the `NAMESPACE_ABSENT` conflict type.
* `Unchanged` Operations may be submitted in commits, but the server does not persist them. Consequently, `Unchanged`
  operations are not considered during merges and transplants, even when they were part of the original commit.
* The server MAY consider `Unchanged` operations during the handling of plain commits and MAY raise commit conflicts
  if they clash with operations in the commits log since the `expected hash` provided by the client.
