/*
 * Copyright (C) 2022 Dremio
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

/**
 * Authorizers are used to bulk-check permissions.
 *
 * <p>Authorizers produce {@link AccessChecker} instances, which collect all access checks required
 * for a certain version store operation and perform all access checks in a batch.
 */
public interface Authorizer {

  /**
   * Start an access-check batch/bulk operation.
   *
   * @param context The context carrying the principal information.
   * @return access checker
   */
  AccessChecker startAccessCheck(AccessContext context);
}
