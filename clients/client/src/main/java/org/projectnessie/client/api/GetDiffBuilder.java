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
package org.projectnessie.client.api;

import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.Reference;

/**
 * Request builder for retrieving a diff between two references.
 *
 * @since {@link NessieApiV1}
 */
public interface GetDiffBuilder {

  GetDiffBuilder fromRefName(String fromRefName);

  GetDiffBuilder fromHashOnRef(String fromHashOnRef);

  default GetDiffBuilder fromRef(Reference fromRef) {
    GetDiffBuilder r = fromRefName(fromRef.getName());
    if (fromRef.getHash() != null) {
      r = r.fromHashOnRef(fromRef.getHash());
    }
    return r;
  }

  GetDiffBuilder toRefName(String toRefName);

  GetDiffBuilder toHashOnRef(String toHashOnRef);

  default GetDiffBuilder toRef(Reference toRef) {
    GetDiffBuilder r = toRefName(toRef.getName());
    if (toRef.getHash() != null) {
      r = r.toHashOnRef(toRef.getHash());
    }
    return r;
  }

  DiffResponse get() throws NessieNotFoundException;
}
