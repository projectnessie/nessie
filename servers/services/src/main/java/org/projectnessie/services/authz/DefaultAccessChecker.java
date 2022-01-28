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

import java.security.AccessControlException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.NamedRef;

public class DefaultAccessChecker implements AccessChecker {

  @Override
  public void canViewReference(AccessContext context, NamedRef ref) throws AccessControlException {}

  @Override
  public void canCreateReference(AccessContext context, NamedRef ref)
      throws AccessControlException {}

  @Override
  public void canAssignRefToHash(AccessContext context, NamedRef ref)
      throws AccessControlException {}

  @Override
  public void canDeleteReference(AccessContext context, NamedRef ref)
      throws AccessControlException {}

  @Override
  public void canReadEntries(AccessContext context, NamedRef ref) throws AccessControlException {}

  @Override
  public void canListCommitLog(AccessContext context, NamedRef ref) throws AccessControlException {}

  @Override
  public void canCommitChangeAgainstReference(AccessContext context, NamedRef ref)
      throws AccessControlException {}

  @Override
  public void canReadEntityValue(
      AccessContext context, NamedRef ref, ContentKey key, String contentId)
      throws AccessControlException {}

  @Override
  public void canUpdateEntity(AccessContext context, NamedRef ref, ContentKey key, String contentId)
      throws AccessControlException {}

  @Override
  public void canDeleteEntity(AccessContext context, NamedRef ref, ContentKey key, String contentId)
      throws AccessControlException {}

  @Override
  public void canViewRefLog(AccessContext context) throws AccessControlException {}
}
