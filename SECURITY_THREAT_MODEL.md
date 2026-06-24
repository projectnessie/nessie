<!--
  Copyright (C) 2026 Dremio

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Project Nessie Security Threat Model

Status: runtime-server threat model.

This document describes the security model for Project Nessie runtime-server deployments. It is
intended for Nessie maintainers, operators, integrators, and vulnerability triagers. It documents
Nessie's security boundaries, assumptions, and non-goals so that security reports can be evaluated
against the intended project contract.

Security vulnerabilities should be reported using the process in [SECURITY.md](SECURITY.md).

## Claim Provenance

Security-relevant claims in this document use these provenance markers:

- `(documented)` The claim is stated in project documentation, configuration, code comments, or
  runtime warnings.
- `(maintainer)` The claim has been confirmed by a Nessie maintainer during preparation of this
  document.
- `(inferred)` The claim is inferred from code structure or configuration and should be treated as
  weaker than a documented or maintainer-confirmed contract.

## Scope

This threat model covers the Nessie runtime server and its primary production deployment surfaces:

- native Nessie REST APIs under the Nessie API paths, such as `/api/v1` and `/api/v2`;
- Iceberg REST APIs under `/iceberg`;
- server-side authentication, authorization, and principal propagation;
- version-store and persistence boundaries used by the server;
- Iceberg catalog metadata, object-store configuration, credential vending, and request signing;
- server-side secrets resolution for catalog and storage credentials;
- Docker, Helm, TLS, reverse-proxy, and Kubernetes deployment assumptions that affect runtime
  security.

Clients, the Nessie CLI, GC tooling, admin tooling, examples, tests, and performance-test code are
secondary in this model. They are included only where they clarify a runtime-server trust boundary or
operator responsibility. `(maintainer)`

## Non-Goals

This document is not:

- a vulnerability assessment or penetration-test report;
- a list of known bugs or recommended hardening tasks;
- a supply-chain, release-signing, dependency-management, or CI threat model except where those
  topics affect runtime security assumptions;
- a promise that every deployment example is production-ready;
- a replacement for dependency documentation such as Quarkus, OpenID Connect, Iceberg, cloud IAM,
  JDBC, object-store, Kubernetes, or reverse-proxy security guidance.

Examples, tests, performance tests, and local demo Docker configurations are not covered as
production security posture unless they are explicitly documented as production deployment material.
`(maintainer)`

## Security Objectives

Nessie aims to protect repository and catalog metadata according to the configured authentication,
authorization, storage, and deployment controls. `(maintainer)`

For runtime-server deployments, the main security objectives are:

- accept requests only from principals admitted by the configured authentication mechanism;
- apply authorization checks to repository references, content keys, repository configuration, and
  catalog operations when authorization is enabled;
- keep Nessie server-side object-store credentials and persistence credentials out of normal client
  responses, except for explicitly configured credential-vending or request-signing mechanisms;
- preserve version-store consistency and optimistic-concurrency semantics across concurrent clients;
- avoid treating Nessie metadata authorization as complete authorization for underlying data files
  when those files can also be reached outside Nessie.

## Component Families

| Component family | Runtime surface | Primary security boundary |
| --- | --- | --- |
| Native Nessie REST API | `/api/v1`, `/api/v2`, REST resource and service layers | Network clients to Nessie repository metadata and version-control operations |
| Iceberg REST API | `/iceberg`, Iceberg REST v1 catalog endpoints | Query engines and Iceberg clients to catalog metadata, table/view metadata, and object-store access material |
| Authentication | Quarkus HTTP security, OIDC bearer-token validation, configured anonymous paths | HTTP request to authenticated or anonymous principal |
| Authorization | Nessie `Authorizer`, `BatchAccessChecker`, CEL rules | Principal, API context, reference, content key, operation, and repository config to allow/deny decision |
| Version store | In-memory, JDBC, RocksDB, DynamoDB, MongoDB, Cassandra, Bigtable backends | Nessie service layer to persistent repository state |
| Catalog/object-store integration | S3, GCS, ADLS, warehouse and bucket/filesystem configuration | Nessie catalog metadata to object-store location and credential material |
| Secrets handling | Quarkus config secrets and configured external secrets managers | Server configuration to runtime credentials |
| Deployment | Docker image, Helm chart, Kubernetes, reverse proxy, TLS | Operator-controlled infrastructure to public or private network exposure |
| Secondary tooling | CLI, clients, GC, admin/export/import tools | Trusted local/operator process to Nessie server or data lake |

## Native Nessie REST API And Iceberg REST API

The native Nessie REST API and the Iceberg REST API must be modeled separately. They share server,
authentication, authorization, deployment, version-store, and storage layers, but they expose different
resources and different security consequences. `(maintainer)`

The native Nessie REST API exposes Nessie's repository and version-control semantics: references,
commits, diffs, content keys, repository configuration, merges, transplants, and related metadata
operations. Authorization checks are expressed in terms of operations such as viewing references,
creating/deleting references, reading entries, listing commit logs, committing changes, reading or
modifying entities, and reading/updating repository configuration. `(documented)`

The Iceberg REST API exposes Iceberg catalog semantics: namespaces, tables, views, transactions,
metadata snapshots, warehouse-derived locations, object-store configuration, request signing, and
credential vending. A security report may affect only this catalog surface, only the native Nessie
surface, or a shared lower layer. `(documented)`

## Actors

| Actor | Trust level | Notes |
| --- | --- | --- |
| Anonymous network caller | Untrusted unless deployment intentionally allows anonymous access | Receives the Quarkus anonymous identity when authentication is disabled or a path is anonymously allowed. |
| Authenticated API caller | Trusted only for identity, not for requested action | Must still be authorized when authorization is enabled. |
| Iceberg query engine or catalog client | Authenticated or anonymous API caller plus Iceberg-specific behavior | May receive object-store configuration, short-lived credentials, or signed-request URLs depending on server configuration. |
| Nessie operator | Trusted administrator | Controls server configuration, storage backends, secrets, TLS, reverse proxies, Helm values, and cloud IAM. |
| Storage backend administrator | Trusted for backend integrity and availability | Controls database or object-store service outside Nessie. |
| Object-store principal used by Nessie | Powerful service identity | Its IAM scope determines the maximum object-store authority Nessie can exercise or delegate. |
| Local client or CLI user | Trusted local actor | Clients and CLI are not a security boundary against the local user or compromised local machine. `(maintainer)` |
| GC/admin tooling user | Trusted operator | These tools can affect repository metadata or data-lake files depending on command and configuration. |

## Protected Assets

Nessie treats the following as security-sensitive within this runtime-server threat model:

- bearer tokens, OIDC client secrets, server-side object-store credentials, persistence-backend
  credentials, signing keys, and other server-side secrets;
- repository references, commit metadata, repository configuration, content keys, table/view
  metadata, namespace metadata, and catalog metadata whose visibility or mutation is governed by
  Nessie authorization;
- table locations, warehouse locations, metadata-file locations, object-store URIs, and other
  URI-bearing metadata used to find or authorize access to catalog-managed data;
- temporary object-store credentials, signed requests, credential-vending responses, storage-policy
  fragments, IAM policies, access-boundary inputs, and provider-specific policy expressions;
- security-relevant configuration, including authentication, authorization, anonymous paths,
  reverse-proxy handling, storage backends, object stores, secrets managers, and credential-vending
  modes;
- logs, generated configuration, local profiles, backups, database snapshots, and operational
  artifacts when they contain credentials, tokens, metadata, or other security-sensitive values.

## Authentication Model

By default, Nessie server authentication is disabled and requests are processed as an anonymous user
identity. `(documented)`

Authentication-disabled-by-default is intended for local evaluation and trusted deployments only.
Production deployments exposed to untrusted clients are expected to use appropriate authentication
and authorization controls, such as OIDC authentication and CEL authorization. `(maintainer)`

When authentication is enabled, Nessie uses Quarkus HTTP security and supports bearer-token
validation through OpenID Connect configuration. `(documented)`

Nessie has configured anonymous paths and path prefixes. When authentication is enabled, requests to
those paths may be served without authentication; other requests must be authenticated by a higher
priority Quarkus mechanism or fail as unauthorized. `(documented)`

The public OAuth token endpoint under the Iceberg REST path is present for Iceberg REST compatibility
but returns `NotImplemented` in the current server implementation. `(documented)`

## Authorization Model

Nessie authorization is optional and disabled by default. `(documented)`

When authorization is disabled, any authenticated or anonymous caller accepted by the server may
perform API operations allowed by the API itself. `(maintainer)`

When authorization is enabled, the default authorizer type is CEL. Authorization rules are configured
as CEL expressions. The checks include reference operations, content-key/entity operations, commit
operations, and repository-config operations. `(documented)`

Native REST and Iceberg REST calls both use Nessie service-layer authorization checks. Iceberg REST
also adds catalog-specific operation labels to some request metadata paths. CEL rule inputs are not
identical for every check shape, so operators should validate authorization rules against the concrete
native API and Iceberg REST operations they intend to control. `(inferred)`

The built-in CEL authorizer exposes only its documented CEL variables. API context and Iceberg
catalog action labels are available to custom `Authorizer` implementations through Nessie's
authorization check metadata, but they are not CEL rule inputs unless explicitly added to the CEL
implementation. `(documented)`

Authorization decisions protect Nessie metadata and catalog operations. They do not guarantee that
underlying data files are protected if users can access the object store through other credentials,
other services, or direct object-store permissions. `(maintainer)`

## Metadata And Data Boundaries

Nessie stores and serves metadata, references, commits, and catalog state. It does not make the object
store itself inaccessible to a caller who already has direct object-store permissions. `(maintainer)`

For Iceberg tables and views, Nessie may return metadata that contains or derives object-store
locations. Operators must assume that table and view metadata can reveal storage layout information to
callers authorized to read that metadata. `(inferred)`

Removing or hiding metadata in Nessie is not the same as securely deleting underlying data. Historical
metadata and object-store files may remain reachable until table-format cleanup and garbage collection
remove obsolete files and the object store enforces the desired permissions. `(documented)`

## Iceberg REST Catalog And Object-Store Access

Nessie's Iceberg REST support manages table and view metadata and provides object-store access
configuration to Iceberg clients. It requires at least one configured warehouse and object store before
use. `(documented)`

Nessie uses table metadata locations and warehouse configuration to determine the object-store settings
returned to clients. Warehouses determine default creation locations; existing table locations are used
to look up matching object-store configuration when tables are loaded. `(documented)`

Nessie supports mechanisms such as S3 request signing and short-lived/down-scoped credentials for
object-store access. These mechanisms are configuration-dependent and must be tailored to the query
engine and deployment environment. `(documented)`

Request-signing paths must not convert an authorization failure while resolving catalog metadata into
a missing-table or future-location fallback. Missing content may be handled as a create/future-table
case where the signer endpoint was already minted for that location; authorization and unexpected
lookup failures are fail-closed conditions. `(documented)`

Object-store credentials or temporary access material exposed through catalog features are in scope only
for configured credential-vending or request-signing modes. Operators are responsible for cloud IAM
policy scope, role trust policies, external IDs, token/session duration, and bucket or filesystem
permissions. `(maintainer)`

Nessie server-side S3 access keys configured for object-store access are not intended to be exposed to
clients directly. `(documented)`

ADLS delegation has coarser permission granularity than S3 and GCS because ADLS user-delegation SAS
tokens are scoped at filesystem-level capabilities rather than the path-level IAM and STS mechanisms
available for S3 and GCS. Operators must account for that weaker granularity when enabling ADLS
delegation.
`(documented)`

## Secrets And Credential Configuration

Server-side secrets can be supplied through Quarkus configuration mechanisms and supported external
secrets managers. `(documented)`

Secret validation for catalog references is optional. If disabled, a misconfigured or missing secret
may be detected only when the related runtime path is exercised. `(documented)`

Secrets used for persistence backends, catalog object stores, cloud credentials, OIDC clients, and
Kubernetes image pulls are operator-controlled deployment inputs. Nessie does not protect a deployment
from an operator who configures overbroad credentials or exposes secrets through Kubernetes, environment,
logging, shell history, or other infrastructure. `(inferred)`

## Deployment And Network Assumptions

Nessie's Docker image and Helm chart are deployment mechanisms, not complete security boundaries.
Operators are responsible for production-appropriate network exposure, TLS termination, authentication,
authorization, persistence backends, secret storage, and cloud IAM. `(inferred)`

The Helm chart defaults have authentication and authorization disabled. `(documented)`

The server logs startup warnings when authentication and/or authorization are disabled, and warns that
with both disabled everybody with network access can read, write, and change everything. `(documented)`

The Helm chart exposes a headless management service for health checks, metrics, and distributed cache
invalidation between Nessie pods. The chart-derived cache invalidation token is deterministic
configuration material, not a high-entropy deployment secret. Operators must keep the management
service internal to trusted cluster workloads or protect it with Kubernetes network controls.
`(documented)`

The default in-memory version store is for testing and experimentation, not production, because state is
lost when the process stops. H2-backed JDBC deployments are also warned as non-production test or
experimentation configurations. `(documented)`

TLS may be provided by direct Quarkus configuration, Kubernetes ingress, gateway, service mesh, or a
reverse proxy. Operators are responsible for certificate trust, TLS termination, and preventing
untrusted clients from spoofing forwarded headers. `(documented)`

Reverse-proxy forwarding headers are security-sensitive. Nessie documentation explicitly warns not to
enable forwarded-header support unless the proxy is configured to set those headers and filter them from
incoming requests. This matters especially for Iceberg REST because Nessie returns follow-up URLs to
clients. `(documented)`

## Persistence And Consistency Assumptions

Nessie relies on the configured version-store backend for durable repository state. Production deployments
must use a backend appropriate for durability, availability, backup, authentication, and access control.
`(inferred)`

Nessie's repository semantics rely on commit hashes, references, and conflict handling to preserve
version-control behavior under concurrent clients. A caller that can commit to a reference can change the
metadata visible at that reference subject to API-level and authorization checks. `(inferred)`

Database credentials, database network access, object-store credentials, and cloud service permissions
are outside Nessie's control once provided by the operator. A compromised storage backend or overprivileged
storage principal can invalidate Nessie's metadata and data-access assumptions. `(inferred)`

## Supported Configuration And Variant Boundaries

Nessie behavior depends on the configured authentication mode, authorization mode, persistence backend,
object-store provider, credential-vending mode, request-signing mode, and deployment packaging.
Security analysis should identify the coherent runtime configuration under which a behavior is
reachable. `(inferred)`

Do not mix assumptions from mutually exclusive or unsupported configurations. For example, a finding is
weaker if it requires combining test-only storage, disabled authentication, mocked credentials,
production-only object-store behavior, and an unrelated packaging path that cannot occur together in a
supported deployment. `(inferred)`

A finding may still be in scope when it affects one supported backend, one supported object-store
provider, one documented authentication or authorization mode, or one optional feature that is present
in official artifacts and activated by configuration. The finding should identify that variant rather
than implying that all Nessie deployments are affected. `(inferred)`

Findings against unreleased code can be useful, especially when the issue is likely to ship or reveals a
broader weakness in shared code, defaults, extension points, or documentation. They are not automatically
the same as findings against released artifacts. `(inferred)`

## Secondary Tooling

Clients and CLI tools are trusted local tools. They are not designed to protect against the local user or
a compromised local machine. `(maintainer)`

GC and admin tools are trusted operator tools. GC can identify and delete data-lake files when configured
to run expire/delete phases. The mark phase reads Nessie repository state; sweep/delete phases access the
data lake and require table-format and object-store credentials. `(documented)`

GC derives deletion candidate scope from table metadata stored in Nessie. For content committed through
the native Nessie API, Nessie cannot infer an external warehouse boundary from arbitrary metadata alone.
Object-store permissions on the credentials used for destructive GC phases are therefore the primary
deletion boundary for those deployments. `(documented)`

Because these tools can have destructive effects, their safe use depends on operator identity, command
configuration, object-store permissions, database configuration, and review of deferred-deletion outputs
where applicable. Deferred deletion review is especially important when less-trusted users can commit
table metadata into Nessie. `(documented)`

## Vulnerability Triage Guidance

A report is more likely to be in scope for this threat model when it shows that, under a supported runtime
configuration:

- an unauthenticated caller can reach a non-anonymous path despite authentication being enabled;
- an authenticated caller can perform a native Nessie operation that configured authorization should deny;
- an Iceberg REST caller can read or mutate catalog metadata that configured authorization should deny;
- object-store credentials, signed requests, or temporary access material exceed the configured and
  documented delegation scope;
- server-side secrets or persistence credentials are exposed through normal API responses, logs, or error
  responses without an explicit configuration that allows that exposure;
- client-visible properties, generated configuration, catalog metadata, local profiles, command output,
  or logs are used as a secret store or expose values that should remain server-side;
- reverse-proxy, forwarded-header, or external-base-URI behavior lets an untrusted caller cause Nessie to
  emit attacker-controlled follow-up URLs in a production-relevant configuration;
- version-store conflict handling allows unauthorized metadata overwrite, reference movement, or repository
  configuration change;
- URI-bearing table, view, namespace, or warehouse metadata is accepted, persisted, used for storage
  access, or used for credential vending without the validation required by the configured storage model;
- provider-specific policy documents, access-boundary inputs, or request-signing material unsafe-encode
  caller-controlled identifiers, paths, or property values.

A report is less likely to be a Nessie vulnerability by itself when it depends only on:

- running the server with authentication and authorization intentionally disabled on an untrusted network;
- overbroad object-store, database, Kubernetes, or cloud IAM permissions configured outside Nessie;
- direct access to data files through non-Nessie credentials or services;
- compromise of the local machine running a trusted client, CLI, GC, or admin tool;
- use of examples, tests, or local demo deployments as if they were production hardening guidance;
- supply-chain or CI concerns that do not affect the runtime-server security contract described here;
- a dependency advisory without showing that the vulnerable behavior is present, reachable through Nessie,
  and crosses a Nessie trust boundary.

For each non-trivial report, identify the affected version or commit, artifact, API surface, runtime
configuration, authentication mode, authorization mode, persistence backend, object-store provider,
credential-vending or request-signing mode, actor, protected asset, and trust boundary. `(inferred)`

## Dependency And Documentation Findings

Nessie depends on Apache Iceberg, Quarkus, storage-provider SDKs, database drivers, and other open
source projects. A vulnerability in a dependency can affect Nessie when Nessie ships the affected code
in a supported artifact, exposes the vulnerable behavior through a reachable path, relies on the
vulnerable behavior for a security decision, or uses unsafe configuration that changes the impact.
`(inferred)`

A dependency vulnerability is not automatically a Nessie vulnerability. Triage should determine whether
Nessie needs a release, mitigation, configuration change, documentation update, dependency update, or
coordinated disclosure with the upstream project. `(inferred)`

Documentation, examples, generated references, and default values can be security-relevant when they
could reasonably cause operators or integrators to misunderstand a security boundary, protected asset,
trust assumption, default behavior, supported configuration, credential-handling rule, or operational
responsibility. `(inferred)`

Security-relevant documentation findings are not automatically vulnerabilities or advisory candidates.
They should still be handled as hardening or operator-safety issues when unclear guidance creates a
realistic path to unsafe deployment, credential exposure, authorization mistakes, object-store policy
mistakes, reverse-proxy mistakes, or misuse of development defaults. `(inferred)`

## Maintainer Checklist For Security-Sensitive Changes

Update this document when a change modifies any of the following:

- default authentication, authorization, anonymous-path, or startup-warning behavior;
- authorization check types, rule inputs, or API contexts;
- native Nessie REST or Iceberg REST endpoint semantics;
- object-store credential vending, request signing, token/session duration, or storage-location matching;
- server-side secret resolution, secret validation, or credential exposure behavior;
- Helm, Docker, TLS, reverse-proxy, or Kubernetes defaults that change production security assumptions;
- version-store consistency assumptions, supported production backends, or repository-config authorization;
- GC/admin tooling behavior that changes data-lake deletion authority or repository mutation authority.
