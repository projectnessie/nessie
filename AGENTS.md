<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Agent Instructions For Project Nessie

Before making security-sensitive runtime changes, read
[SECURITY_THREAT_MODEL.md](SECURITY_THREAT_MODEL.md).

Security-sensitive runtime changes include authentication, authorization, anonymous paths, native
Nessie REST APIs, Iceberg REST APIs, catalog/object-store credential vending, request signing,
secrets handling, persistence backend assumptions, reverse-proxy behavior, TLS/deployment defaults,
and GC/admin tooling behavior that affects repository or data-lake authority.

When such a change modifies Nessie's security contract, update
[SECURITY_THREAT_MODEL.md](SECURITY_THREAT_MODEL.md) in the same change or explicitly document why no
threat-model update is needed.

Security vulnerability reports should follow [SECURITY.md](SECURITY.md).
