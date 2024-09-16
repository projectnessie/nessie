/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.services.authz;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import java.security.Principal;
import java.util.Set;

/** Provides some context about a role/principal that accesses Nessie resources. */
public interface AccessContext {
  /** Provide the user identity. */
  Principal user();

  default Set<String> roleIds() {
    String name = user().getName();
    return name == null || name.isEmpty() ? emptySet() : singleton(name);
  }

  default boolean isAnonymous() {
    String name = user().getName();
    return name == null || name.isEmpty();
  }
}
