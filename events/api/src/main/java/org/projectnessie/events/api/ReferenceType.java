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
package org.projectnessie.events.api;

/** An enum of all possible reference types. */
public enum ReferenceType {
  BRANCH("refs/heads/"),
  TAG("refs/tags/");

  private final String prefix;

  ReferenceType(String prefix) {
    this.prefix = prefix;
  }

  /** The prefix of the reference type, e.g. "refs/heads/". */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Returns the {@link ReferenceType} for the given full reference name, e.g. {@code
   * "refs/heads/branch1"} returns {@link ReferenceType#BRANCH}.
   */
  public static ReferenceType fromFullReferenceName(String fullName) {
    if (fullName.startsWith(BRANCH.getPrefix())) {
      return BRANCH;
    } else if (fullName.startsWith(TAG.getPrefix())) {
      return TAG;
    } else {
      throw new IllegalArgumentException(
          "Full name does not correspond to a known reference type: " + fullName);
    }
  }
}
