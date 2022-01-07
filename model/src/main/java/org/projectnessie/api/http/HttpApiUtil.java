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
package org.projectnessie.api.http;

import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transaction;

public final class HttpApiUtil {
  private HttpApiUtil() {}

  public static String referenceTypeName(Reference reference) {
    if (reference instanceof Transaction) {
      return "transaction";
    }
    if (reference instanceof Branch) {
      return "branch";
    }
    if (reference instanceof Tag) {
      return "tag";
    }
    throw new IllegalArgumentException(String.format("Unsupported reference type '%s'", reference));
  }
}
