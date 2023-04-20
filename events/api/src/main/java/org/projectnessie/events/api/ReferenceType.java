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

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** An enum of all possible reference types. */
@Schema(
    description = "An enum of all possible reference types.",
    enumeration = {"BRANCH", "TAG"})
public enum ReferenceType {
  BRANCH,
  TAG
}
