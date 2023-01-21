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
package org.projectnessie.services.impl;

import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/**
 * Enum intended to be used a test method parameter to transform a {@link Reference} in multiple
 * ways.
 */
public enum ReferenceMode {
  /** Removes the {@link Reference#getHash()} from the reference. */
  NAME_ONLY {
    @Override
    Reference transform(Reference ref) {
      switch (ref.getType()) {
        case TAG:
          return Tag.of(ref.getName(), null);
        case BRANCH:
          return Branch.of(ref.getName(), null);
        default:
          throw new IllegalArgumentException(ref.toString());
      }
    }
  },
  /** Keep the reference unchanged. */
  UNCHANGED {
    @Override
    Reference transform(Reference ref) {
      return ref;
    }
  },
  /**
   * Make the reference a {@link Detached} with its {@link Detached#getHash()} using the hash of the
   * given reference.
   */
  DETACHED {
    @Override
    Reference transform(Reference ref) {
      return Detached.of(ref.getHash());
    }
  };

  abstract Reference transform(Reference ref);
}
