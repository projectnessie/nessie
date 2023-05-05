/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.service.util;

import org.projectnessie.events.api.ImmutableReference;
import org.projectnessie.events.api.Reference;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

public final class ReferenceMapping {

  private ReferenceMapping() {}

  public static Reference map(NamedRef refName) {
    return ImmutableReference.builder()
        .type(getReferenceType(refName))
        .fullName(getFullReferenceName(refName))
        .simpleName(refName.getName())
        .build();
  }

  private static String getReferenceType(NamedRef ref) {
    if (ref instanceof BranchName) return Reference.BRANCH;
    if (ref instanceof TagName) return Reference.TAG;
    if (ref instanceof DetachedRef) return "DETACHED";
    else return "UNKNOWN";
  }

  private static String getFullReferenceName(NamedRef ref) {
    if (ref instanceof BranchName) return "refs/heads/" + ref.getName();
    if (ref instanceof TagName) return "refs/tags/" + ref.getName();
    else return "refs/???/" + ref.getName();
  }
}
