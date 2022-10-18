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
package org.projectnessie.versioned.storage.common.objtypes;

import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.InternalRef;

/**
 * Used to distinguish commits against {@link InternalRef internal references}, which are purely to
 * track internal concerns, from other "normal" user commits.
 *
 * <p>Internal commits are for example excluded from reference calculations like {@link
 * CommitLogic#identifyAllHeadsAndForkPoints(int, Consumer)}.
 */
public enum CommitType {
  /** Normal user commits. */
  NORMAL("n"),
  /** Commits against {@link InternalRef internal references}. */
  INTERNAL("i");

  private static final CommitType[] ALL_COMMIT_TYPES = CommitType.values();

  private final String shortName;

  CommitType(String shortName) {
    this.shortName = shortName;
  }

  public static CommitType fromShortName(String shortName) {
    for (CommitType type : ALL_COMMIT_TYPES) {
      if (type.shortName().equals(shortName)) {
        return type;
      }
    }
    throw new IllegalStateException("Unknown commit short type name " + shortName);
  }

  public String shortName() {
    return shortName;
  }
}
